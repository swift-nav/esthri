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

use std::{borrow::Cow, collections::HashMap, io::SeekFrom, path::Path, sync::Arc};

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedPart, ObjectCannedAcl, StorageClass};
use aws_sdk_s3::Client;
use bytes::{Bytes, BytesMut};
use futures::{stream, Future, Stream, StreamExt, TryStreamExt};
use log::{debug, info, warn};
use log_derive::logfn;
use tokio::{
    fs::File,
    io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt},
    sync::Mutex,
};

use crate::{
    complete_multipart_upload,
    compression::compress_to_tempfile,
    config::Config,
    create_multipart_upload,
    errors::{Error, Result},
    opts::*,
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
    pub async fn abort(&self, s3: &Client) -> Result<()> {
        info!(
            "abort: bucket={}, key={}, upload_id={}",
            self.bucket, self.key, self.upload_id
        );
        s3.abort_multipart_upload()
            .bucket(self.bucket.clone())
            .key(self.key.clone())
            .upload_id(self.upload_id.clone())
            .send()
            .await
            .map_err(|e| Error::AbortMultipartUploadFailed(e.to_string()))?;
        Ok(())
    }

    /// Abort this multipart upload
    #[cfg(feature = "blocking")]
    #[tokio::main]
    pub async fn abort_blocking<T>(self, s3: &Client) -> Result<()> {
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
pub async fn upload(
    s3: &Client,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    file: impl AsRef<Path>,
    opts: EsthriPutOptParams,
) -> Result<()> {
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
pub async fn upload_from_reader<R>(
    s3: &Client,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    reader: R,
    file_size: u64,
    metadata: Option<HashMap<String, String>>,
) -> Result<()>
where
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
pub async fn upload_from_reader_with_storage_class<R>(
    s3: &Client,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    reader: R,
    file_size: u64,
    metadata: Option<HashMap<String, String>>,
    storage_class: StorageClass,
) -> Result<()>
where
    R: AsyncRead + AsyncSeek + Unpin + Send + 'static,
{
    let (bucket, key) = (bucket.as_ref(), key.as_ref());
    info!(
        "put: bucket={}, key={}, file_size={}, storage_class={:?}",
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
pub async fn upload_file_helper(
    s3: &Client,
    bucket: &str,
    key: &str,
    path: &Path,
    compressed: bool,
    storage_class: StorageClass,
) -> Result<()> {
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

async fn empty_upload(
    s3: &Client,
    bucket: &str,
    key: &str,
    metadata: Option<HashMap<String, String>>,
    storage_class: StorageClass,
) -> Result<()> {
    s3.put_object()
        .bucket(bucket)
        .key(key)
        .acl(ObjectCannedAcl::BucketOwnerFullControl)
        .set_metadata(metadata)
        .storage_class(storage_class)
        .send()
        .await
        .map_err(|e| Error::PutObjectFailed(e.to_string()))?;
    Ok(())
}

async fn singlepart_upload<R>(
    s3: &Client,
    bucket: &str,
    key: &str,
    reader: R,
    file_size: u64,
    metadata: Option<HashMap<String, String>>,
    storage_class: StorageClass,
) -> Result<()>
where
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
    s3.put_object()
        .bucket(bucket)
        .key(key)
        .content_length(body.len() as i64)
        .body(into_byte_stream(body.clone()))
        .acl(ObjectCannedAcl::BucketOwnerFullControl)
        .set_metadata(metadata)
        .storage_class(storage_class)
        .send()
        .await
        .map_err(|e| Error::PutObjectFailed(e.to_string()))?;
    Ok(())
}

async fn multipart_upload<R>(
    s3: &Client,
    bucket: &str,
    key: &str,
    reader: R,
    file_size: u64,
    metadata: Option<HashMap<String, String>>,
    storage_class: StorageClass,
) -> Result<()>
where
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
async fn create_stream_for_chunk<R>(
    reader: Arc<Mutex<R>>,
    file_size: u64,
    chunk_number: u64,
) -> Result<ByteStream>
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
    let mut stream = stream.map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err));

    // stream needs to be collected into Bytes for ByteStream to be retryable
    let mut collected_bytes = Vec::new();

    while let Some(bytes) = stream.try_next().await? {
        collected_bytes.extend_from_slice(&bytes);
    }
    Ok(ByteStream::from(collected_bytes))
}

async fn upload_request_stream<'a, R>(
    s3: &'a Client,
    bucket: &'a str,
    key: &'a str,
    upload_id: &'a str,
    reader: R,
    file_size: u64,
    part_size: u64,
) -> impl Stream<Item = impl Future<Output = Result<CompletedPart>> + 'a> + 'a
where
    R: AsyncRead + AsyncSeek + Unpin + Send + 'static,
{
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
    let shared_reader = Arc::new(Mutex::new(reader));

    stream::iter(sizes)
        .map(move |(part_number, part_size)| {
            let reader = shared_reader.clone();
            (reader, part_number, part_size)
        })
        .map(move |(reader, part_number, part_size)| async move {
            let body = create_stream_for_chunk(reader.clone(), file_size, part_number - 1).await?;
            let res = s3
                .upload_part()
                .bucket(bucket)
                .key(key)
                .upload_id(upload_id)
                .part_number(part_number as i32)
                .content_length(part_size as i64)
                .body(body)
                .send()
                .await
                .map_err(|e| Error::UploadPartFailed(e.to_string()))?;

            if res.e_tag.is_none() {
                warn!(
                    "upload_part e_tag was not present (part_number: {})",
                    part_number,
                )
            }
            let compeleted_part = CompletedPart::builder()
                .set_e_tag(res.e_tag)
                .part_number(part_number as i32)
                .build();
            Ok(compeleted_part)
        })
}

fn into_byte_stream(chunk: Bytes) -> ByteStream {
    ByteStream::from(chunk)
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
