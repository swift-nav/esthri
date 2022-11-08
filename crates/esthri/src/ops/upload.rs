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

use std::{
    borrow::Cow, collections::HashMap, convert::TryInto, io::SeekFrom, path::Path, sync::Arc,
};

use bytes::{Bytes, BytesMut};
use futures::{future, stream, Future, Stream, StreamExt, TryStreamExt};
use log::{debug, info, warn};
use log_derive::logfn;
use tokio::{
    fs::File,
    io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt},
    sync::Mutex,
};

use crate::{
    compression::compress_to_tempfile,
    config::Config,
    errors::{Error, Result},
    handle_dispatch_error,
    opts::*,
    rusoto::*,
};

static PENDING_UPLOADS: parking_lot::Mutex<Vec<PendingUpload>> =
    parking_lot::const_mutex(Vec::new());

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
    if let Some(idx) = lock.iter().position(|u| u.upload_id == id) {
        lock.swap_remove(idx);
    }
}

fn add_pending(u: PendingUpload) {
    PENDING_UPLOADS.lock().push(u);
}

#[logfn(err = "ERROR")]
pub async fn upload<T>(
    s3: &T,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    file: impl AsRef<Path>,
    opts: EsthriPutOptParams,
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

    upload_file_helper(
        s3,
        bucket.as_ref(),
        key.as_ref(),
        file.as_ref(),
        opts.transparent_compression,
        opts.storage_class
            .unwrap_or_else(|| Config::global().storage_class()),
    )
    .await
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
    R: AsyncRead + AsyncSeek + Unpin + Send + 'static,
{
    upload_from_reader_with_storage_class(
        s3,
        bucket,
        key,
        reader,
        file_size,
        metadata,
        Config::global().storage_class(),
    )
    .await
}

#[logfn(err = "ERROR")]
pub async fn upload_from_reader_with_storage_class<T, R>(
    s3: &T,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    reader: R,
    file_size: u64,
    metadata: Option<HashMap<String, String>>,
    storage_class: S3StorageClass,
) -> Result<()>
where
    T: S3,
    R: AsyncRead + AsyncSeek + Unpin + Send + 'static,
{
    let (bucket, key) = (bucket.as_ref(), key.as_ref());
    info!(
        "put: bucket={}, key={}, file_size={}, storage_class={}",
        bucket, key, file_size, storage_class
    );

    if file_size == 0 {
        debug!("uploading empty file");
        empty_upload(s3, bucket, key, metadata, storage_class).await
    } else if file_size < Config::global().upload_part_size() {
        debug!("uploading singlepart file");
        singlepart_upload(s3, bucket, key, reader, file_size, metadata, storage_class).await
    } else {
        debug!("uploading multipart file");
        multipart_upload(s3, bucket, key, reader, file_size, metadata, storage_class).await
    }
}

#[logfn(err = "ERROR")]
pub async fn upload_file_helper<T>(
    s3: &T,
    bucket: &str,
    key: &str,
    path: &Path,
    compressed: bool,
    storage_class: S3StorageClass,
) -> Result<()>
where
    T: S3,
{
    let key = format_key(key, path)?;
    if compressed {
        let (mut tmp, size) = compress_to_tempfile(path).await?;
        upload_from_reader_with_storage_class(
            s3,
            bucket,
            key,
            tmp.take_file(),
            size,
            Some(crate::compression::compressed_file_metadata()),
            storage_class,
        )
        .await?;
        Ok(())
    } else {
        let f = File::open(path).await?;
        let size = f.metadata().await?.len();
        debug!("upload: file size: {}", size);
        upload_from_reader_with_storage_class(s3, bucket, key, f, size, None, storage_class).await
    }
}

async fn empty_upload<T>(
    s3: &T,
    bucket: &str,
    key: &str,
    metadata: Option<HashMap<String, String>>,
    storage_class: S3StorageClass,
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
            storage_class: Some(storage_class.to_string()),
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
    storage_class: S3StorageClass,
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
            storage_class: Some(storage_class.to_string()),
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
    storage_class: S3StorageClass,
) -> Result<()>
where
    T: S3,
    R: AsyncRead + AsyncSeek + Unpin + Send + 'static,
{
    let upload_id = create_multipart_upload(s3, bucket, key, metadata, storage_class)
        .await?
        .upload_id
        .ok_or(Error::UploadIdNone)?;
    debug!("upload_id: {}", upload_id);
    add_pending(PendingUpload::new(bucket, key, &upload_id));

    let completed_parts = {
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

// Creates a stream of smaller chunks for a larger chunk of data. This
// is to reduce memory usage when uploading, as we currently do
// multipart uploads of 8MB. Giving the rusuto uploading a stream that
// it can consume causes less memory to be used than giving it whole 8MB
// chunks as there is less data waiting around to be uploaded.
fn create_stream_for_chunk<R>(
    reader: Arc<Mutex<R>>,
    file_size: u64,
    chunk_number: u64,
) -> ByteStream
where
    R: AsyncRead + AsyncSeek + Send + Unpin + 'static,
{
    let chunk_size = Config::global().upload_part_size();
    let read_part_size = Config::global().upload_read_size();

    // The total amount to read for this chunk
    let to_read = u64::min(file_size - (chunk_size * chunk_number), chunk_size);
    let init_state = (to_read, /* part_number = */ 0, reader);
    let stream = Box::pin(stream::try_unfold(
        init_state,
        move |(remaining, part_number, reader)| async move {
            if remaining == 0 {
                Ok(None)
            } else {
                let read_size = u64::min(remaining, read_part_size);
                let mut buf = BytesMut::with_capacity(read_size as usize);
                buf.resize(read_size as usize, 0);
                {
                    let mut guard = reader.lock().await;
                    let seek = chunk_size * chunk_number + part_number * read_part_size;
                    guard.seek(SeekFrom::Start(seek)).await?;
                    let slice = buf.as_mut();
                    guard.read_exact(&mut slice[..read_size as usize]).await?;
                }
                if read_size == 0 {
                    Err(Error::ReadZero)
                } else {
                    Ok(Some((
                        buf.freeze(),
                        (remaining - read_size, part_number + 1, reader),
                    )))
                }
            }
        },
    ));
    let stream = stream.map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err));
    ByteStream::new_with_size(stream, to_read as usize)
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
    R: AsyncRead + AsyncSeek + Unpin + Send + 'static,
{
    let reqs = upload_part_requests(bucket, key, upload_id, file_size, part_size);
    let shared_reader = Arc::new(Mutex::new(reader));
    reqs.map(move |req| {
        let reader = shared_reader.clone();
        (reader, req)
    })
    .map(move |(reader, req)| async move {
        let res = handle_dispatch_error(|| async {
            let body = create_stream_for_chunk(
                reader.clone(),
                file_size,
                (req.part_number - 1).try_into().expect("Part number < 0"),
            );
            s3.upload_part(UploadPartRequest {
                bucket: req.bucket.clone(),
                key: req.key.clone(),
                upload_id: req.upload_id.clone(),
                part_number: req.part_number,
                content_length: req.content_length,
                body: Some(body),
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
        if remaining > 0 {
            Some((file_size / part_size + 1, remaining))
        } else {
            None
        }
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
            .and_then(|s| s.to_str())
            .ok_or(Error::CouldNotParseS3Filename)?;
        Ok(Cow::Owned(format!("{}{}", key, filename)))
    }
}
