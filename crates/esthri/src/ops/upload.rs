/*
 * Copyright (C) 2021 Swift Navigation Inc.
 * Contact: Swift Navigation <dev@swiftnav.com>
 *
 * This source is subject to the license found in the file 'LICENSE' which must
 * be be distributed together with this source. All other rights reserved.
 *
 * THIS CODE AND INFORMATION IS PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND,
 * EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A PARTICULAR PURPOSE.
 */

use std::{borrow::Cow, collections::HashMap, path::Path};

use bytes::{Bytes, BytesMut};
use futures::{future, stream, Future, Stream, StreamExt, TryStreamExt};
use log::{debug, info, warn};
use log_derive::logfn;
use parking_lot::Mutex;
use tokio::{
    fs::File,
    io::{AsyncRead, AsyncReadExt},
};

use crate::{
    bufferpool::{Buffer, Pool},
    compression::compress_to_tempfile,
    config::Config,
    errors::{Error, Result},
    handle_dispatch_error,
    rusoto::*,
};

static PENDING_UPLOADS: Mutex<Vec<PendingUpload>> = parking_lot::const_mutex(Vec::new());

#[derive(Clone)]
pub struct PendingUpload {
    pub bucket: String,
    pub key: String,
    pub upload_id: String,
}

impl PendingUpload {
    pub fn new(bucket: &str, key: &str, upload_id: &str) -> Self {
        PendingUpload {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            upload_id: upload_id.to_owned(),
        }
    }

    pub fn all() -> Vec<Self> {
        PENDING_UPLOADS.lock().clone()
    }

    /// Abort this multipart upload
    #[logfn(err = "ERROR")]
    pub async fn abort<T>(&self, s3: &T) -> Result<()>
    where
        T: S3,
    {
        info!(
            "abort: bucket={}, key={}, upload_id={}",
            self.bucket, self.key, self.upload_id
        );
        handle_dispatch_error(|| async {
            let amur = AbortMultipartUploadRequest {
                bucket: self.bucket.clone(),
                key: self.key.clone(),
                upload_id: self.upload_id.clone(),
                ..Default::default()
            };
            s3.abort_multipart_upload(amur).await
        })
        .await?;
        Ok(())
    }

    /// Abort this multipart upload
    #[cfg(feature = "blocking")]
    #[tokio::main]
    pub async fn abort_blocking<T>(self, s3: &T) -> Result<()>
    where
        T: S3 + Send,
    {
        self.abort(s3).await
    }
}

fn remove_pending(id: &str) {
    let mut lock = PENDING_UPLOADS.lock();
    if let Some(idx) = lock.iter().position(|u| &u.upload_id == id) {
        lock.swap_remove(idx);
    }
}

fn add_pending(u: PendingUpload) {
    PENDING_UPLOADS.lock().push(u.clone());
}

#[logfn(err = "ERROR")]
pub async fn upload<T>(
    s3: &T,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    file: impl AsRef<Path>,
) -> Result<()>
where
    T: S3,
{
    info!(
        "put: bucket={}, key={}, file={}",
        bucket.as_ref(),
        key.as_ref(),
        file.as_ref().display()
    );

    let compressed = false;
    upload_file_helper(s3, bucket.as_ref(), key.as_ref(), file.as_ref(), compressed).await
}

#[logfn(err = "ERROR")]
pub async fn upload_compressed<T>(
    s3: &T,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    file: impl AsRef<Path>,
) -> Result<()>
where
    T: S3,
{
    info!(
        "put(compressed): bucket={}, key={}, file={}",
        bucket.as_ref(),
        key.as_ref(),
        file.as_ref().display()
    );

    let compressed = true;
    upload_file_helper(s3, bucket.as_ref(), key.as_ref(), file.as_ref(), compressed).await
}

#[logfn(err = "ERROR")]
pub async fn upload_from_reader<T, R>(
    s3: &T,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    reader: R,
    file_size: u64,
    metadata: Option<HashMap<String, String>>,
) -> Result<()>
where
    T: S3,
    R: AsyncRead,
{
    let (bucket, key) = (bucket.as_ref(), key.as_ref());
    info!(
        "put: bucket={}, key={}, file_size={}",
        bucket, key, file_size
    );

    if file_size == 0 {
        debug!("uploading empty file");
        empty_upload(s3, bucket, key, metadata).await
    } else if file_size < Config::global().upload_part_size() {
        debug!("uploading singlepart file");
        singlepart_upload(s3, bucket, key, reader, file_size, metadata).await
    } else {
        debug!("uploading multipart file");
        multipart_upload(s3, bucket, key, reader, file_size, metadata).await
    }
}

async fn upload_file_helper<T>(
    s3: &T,
    bucket: &str,
    key: &str,
    path: &Path,
    compressed: bool,
) -> Result<()>
where
    T: S3,
{
    let key = format_key(key, path)?;
    if compressed {
        let (mut tmp, size) = compress_to_tempfile(path).await?;
        upload_from_reader(
            s3,
            bucket,
            key,
            tmp.file_mut(),
            size,
            Some(crate::compression::compressed_file_metadata()),
        )
        .await
    } else {
        let f = File::open(path).await?;
        let size = f.metadata().await?.len();
        debug!("upload: file size: {}", size);
        upload_from_reader(s3, bucket, key, f, size, None).await
    }
}

async fn empty_upload<T>(
    s3: &T,
    bucket: &str,
    key: &str,
    metadata: Option<HashMap<String, String>>,
) -> Result<()>
where
    T: S3,
{
    handle_dispatch_error(|| async {
        s3.put_object(PutObjectRequest {
            bucket: bucket.into(),
            key: key.into(),
            acl: Some("bucket-owner-full-control".into()),
            metadata: metadata.as_ref().cloned(),
            ..Default::default()
        })
        .await
    })
    .await
    .map_err(Error::PutObjectFailed)?;
    Ok(())
}

async fn singlepart_upload<T, R>(
    s3: &T,
    bucket: &str,
    key: &str,
    reader: R,
    file_size: u64,
    metadata: Option<HashMap<String, String>>,
) -> Result<()>
where
    T: S3,
    R: AsyncRead,
{
    let mut buf = BytesMut::with_capacity(file_size as usize);
    let mut total = 0;
    futures::pin_mut!(reader);
    while total < file_size {
        let read = reader.read_buf(&mut buf).await?;
        total += read as u64;
    }
    let body = buf.freeze();
    handle_dispatch_error(|| async {
        s3.put_object(PutObjectRequest {
            bucket: bucket.into(),
            key: key.into(),
            content_length: Some(body.len() as i64),
            body: Some(into_byte_stream(body.clone())),
            acl: Some("bucket-owner-full-control".into()),
            metadata: metadata.as_ref().cloned(),
            ..Default::default()
        })
        .await
    })
    .await
    .map_err(Error::PutObjectFailed)?;
    Ok(())
}

async fn multipart_upload<T, R>(
    s3: &T,
    bucket: &str,
    key: &str,
    reader: R,
    file_size: u64,
    metadata: Option<HashMap<String, String>>,
) -> Result<()>
where
    T: S3,
    R: AsyncRead,
{
    let upload_id = create_multipart_upload(s3, bucket, key, metadata)
        .await?
        .upload_id
        .ok_or(Error::UploadIdNone)?;
    debug!("upload_id: {}", upload_id);
    add_pending(PendingUpload::new(bucket, key, &upload_id));

    let completed_parts = {
        futures::pin_mut!(reader);
        let mut parts: Vec<_> = upload_request_stream(
            s3,
            bucket,
            key,
            &upload_id,
            reader,
            file_size,
            Config::global().upload_part_size(),
        )
        .await
        .buffer_unordered(Config::global().concurrent_upload_tasks())
        .try_collect()
        .await?;
        parts.sort_unstable_by_key(|a| a.part_number);
        parts
    };

    complete_multipart_upload(s3, bucket, key, &upload_id, &completed_parts).await?;
    remove_pending(&upload_id);

    Ok(())
}

async fn upload_request_stream<'a, T, R>(
    s3: &'a T,
    bucket: &'a str,
    key: &'a str,
    upload_id: &'a str,
    reader: R,
    file_size: u64,
    part_size: u64,
) -> impl Stream<Item = impl Future<Output = Result<CompletedPart>> + 'a> + 'a
where
    T: S3,
    R: AsyncRead + Unpin + 'a,
{
    let reqs = upload_part_requests(bucket, key, upload_id, file_size, part_size);
    let chunks = read_file_parts(reader, file_size, part_size);
    reqs.zip(chunks).map(move |(req, chunk)| async move {
        match chunk {
            Ok(chunk) => {
                let chunk = chunk.freeze();
                let res = handle_dispatch_error(|| async {
                    let into_byte_stream = into_byte_stream(chunk.clone());
                    s3.upload_part(UploadPartRequest {
                        bucket: req.bucket.clone(),
                        key: req.key.clone(),
                        upload_id: req.upload_id.clone(),
                        part_number: req.part_number,
                        content_length: req.content_length,
                        body: Some(into_byte_stream),
                        ..Default::default()
                    })
                    .await
                })
                .await?;
                if res.e_tag.is_none() {
                    warn!(
                        "upload_part e_tag was not present (part_number: {})",
                        req.part_number,
                    )
                }
                Ok(CompletedPart {
                    e_tag: res.e_tag,
                    part_number: Some(req.part_number),
                })
            }
            Err(e) => Err(e),
        }
    })
}

/// Returns an iterator of requests used to upload an object.
/// Notably these requests do not contain the body which must be added later.
fn upload_part_requests<'a>(
    bucket: &'a str,
    key: &'a str,
    upload_id: &'a str,
    file_size: u64,
    part_size: u64,
) -> impl Stream<Item = UploadPartRequest> + 'a {
    let last_part = {
        let remaining = file_size % part_size;
        (remaining > 0).then(|| (file_size / part_size + 1, remaining))
    };
    let sizes = (1..=file_size / part_size)
        .map(move |part_number| (part_number, part_size))
        .chain(last_part);
    let requests = sizes.map(move |(part_number, part_size)| UploadPartRequest {
        bucket: bucket.to_owned(),
        key: key.to_owned(),
        upload_id: upload_id.to_owned(),
        part_number: part_number as i64,
        content_length: Some(part_size as i64),
        ..Default::default()
    });
    stream::iter(requests)
}

fn read_file_parts<'a, R>(
    reader: R,
    file_size: u64,
    part_size: u64,
) -> impl Stream<Item = Result<Buffer<'static>>> + 'a
where
    R: AsyncRead + Unpin + 'a,
{
    stream::try_unfold((reader, 0), move |(mut reader, bytes_read)| async move {
        if bytes_read == file_size {
            Ok(None)
        } else {
            let read_size = (file_size - bytes_read).min(part_size);
            let mut buf = Pool::global().get().await;
            // buf.resize(read_size as usize, 0);
            let slice = buf.as_mut();
            reader.read_exact(&mut slice[..read_size as usize]).await?;
            Ok(Some((buf, (reader, bytes_read + read_size))))
        }
    })
}

fn into_byte_stream(chunk: Bytes) -> ByteStream {
    let size_hint = chunk.len();
    ByteStream::new_with_size(stream::once(future::ready(Ok(chunk))), size_hint)
}

// This is to allow a user to upload to a prefix in a bucket, but relies on
// them putting a trailing slash.
//
// If, for example, there's an existing prefix of
// "s3://mybucket/myprefix1/myprefix" and this function is invoked with
// key="s3://mybucket/myprefix1/myprefix" then both there will be BOTH a
// prefix and an object available with the same name. This is currently the
// same implementation as the official aws tool.
//
// If instead this function is invoked with
// key="s3://mybucket/myprefix1/myprefix/" then the file will be uploaded as
// s3://mybucket/myprefix1/myprefix/{filename from path}.
fn format_key<'a>(key: &'a str, path: &Path) -> Result<Cow<'a, str>> {
    if !key.ends_with('/') {
        Ok(Cow::Borrowed(key))
    } else {
        let filename = path
            .file_name()
            .map(|s| s.to_str())
            .flatten()
            .ok_or(Error::CouldNotParseS3Filename)?;
        Ok(Cow::Owned(format!("{}{}", key, filename)))
    }
}
