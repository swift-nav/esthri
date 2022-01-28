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

#![cfg_attr(feature = "aggressive_lint", deny(warnings))]

use std::borrow::Cow;
use std::convert::TryInto;
use std::io::SeekFrom;
use std::sync::{Arc, Mutex};
use std::{collections::HashMap, path::Path};

use bytes::{Bytes, BytesMut};
use futures::{stream, Future, Stream, StreamExt, TryStreamExt};
use log::{debug, info, warn};
use log_derive::logfn;
use once_cell::sync::Lazy;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::{
    fs,
    io::{AsyncRead, AsyncSeek, BufReader},
};

use crate::{
    config::Config,
    errors::{Error, Result},
    handle_dispatch_error,
    rusoto::*,
    types::GlobalData,
};

const EXPECT_GLOBAL_DATA: &str = "failed to lock global data";

static GLOBAL_DATA: Lazy<Mutex<GlobalData>> = Lazy::new(|| {
    Mutex::new(GlobalData {
        bucket: None,
        key: None,
        upload_id: None,
    })
});

#[logfn(err = "ERROR")]
pub async fn abort_upload<T>(
    s3: &T,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    upload_id: impl AsRef<str>,
) -> Result<()>
where
    T: S3 + Send,
{
    let (bucket, key, upload_id) = (bucket.as_ref(), key.as_ref(), upload_id.as_ref());

    info!(
        "abort: bucket={}, key={}, upload_id={}",
        bucket, key, upload_id
    );

    handle_dispatch_error(|| async {
        let amur = AbortMultipartUploadRequest {
            bucket: bucket.into(),
            key: key.into(),
            upload_id: upload_id.into(),
            ..Default::default()
        };

        s3.abort_multipart_upload(amur).await
    })
    .await?;

    Ok(())
}

async fn upload_helper<T>(
    s3: &T,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    path: impl AsRef<Path>,
    compressed: bool,
) -> Result<()>
where
    T: S3 + Send + Clone,
{
    let (bucket, key, path) = (bucket.as_ref(), key.as_ref(), path.as_ref().to_owned());

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
    let key = if !key.ends_with('/') {
        Cow::Borrowed(key)
    } else {
        let filename = path
            .file_name()
            .ok_or(Error::CouldNotParseS3Filename)?
            .to_str()
            .ok_or(Error::CouldNotParseS3Filename)?;

        Cow::Owned(format!("{}{}", key, filename))
    };

    if path.exists() {
        let stat = fs::metadata(&path).await?;
        if compressed {
            use crate::compression::compress_to_tempfile;
            let (mut compressed, size) = compress_to_tempfile(&path).await?;
            upload_from_reader(
                s3,
                bucket,
                key,
                compressed.file_mut(),
                size,
                Some(crate::compression::compressed_file_metadata()),
            )
            .await
        } else {
            let size = stat.len();
            debug!("upload: file size: {}", size);
            let f = File::open(path).await?;
            let reader = BufReader::new(f);
            upload_from_reader(s3, bucket, key, reader, size, None).await
        }
    } else {
        Err(Error::InvalidSourceFile(path))
    }
}

#[logfn(err = "ERROR")]
pub async fn upload<T>(
    s3: &T,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    file: impl AsRef<Path>,
) -> Result<()>
where
    T: S3 + Send + Clone,
{
    info!(
        "put: bucket={}, key={}, file={}",
        bucket.as_ref(),
        key.as_ref(),
        file.as_ref().display()
    );

    let compressed = false;
    upload_helper(s3, bucket, key, file, compressed).await
}

#[logfn(err = "ERROR")]
pub async fn upload_compressed<T>(
    s3: &T,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    file: impl AsRef<Path>,
) -> Result<()>
where
    T: S3 + Send + Clone,
{
    info!(
        "put(compressed): bucket={}, key={}, file={}",
        bucket.as_ref(),
        key.as_ref(),
        file.as_ref().display()
    );

    let compressed = true;
    upload_helper(s3, bucket, key, file, compressed).await
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
) -> (u64, impl Stream<Item = Result<Bytes>>)
where
    R: AsyncRead + AsyncSeek + Unpin + Send + 'static,
{
    let chunk_size = Config::global().upload_part_size();
    let read_size = Config::global().upload_read_size();

    // The total amount to read for this chunk
    let to_read = u64::min(file_size - (chunk_size * chunk_number), chunk_size);

    let init_state = (to_read, /* part_number = */ 0, Some(reader));

    let stream = Box::pin(stream::try_unfold(
        init_state,
        move |(remaining, part_number, reader)| async move {
            if remaining == 0 {
                Ok(None)
            } else {
                let read_size = u64::min(remaining, read_size);

                let mut buf = BytesMut::with_capacity(read_size as usize);
                buf.resize(read_size as usize, 0);

                let reader = reader.unwrap();

                let result: Result<Bytes> = {
                    let mut reader = reader.lock().unwrap();

                    let seek = chunk_size * chunk_number + part_number * read_size;
                    reader.seek(SeekFrom::Start(seek)).await?;

                    let slice = buf.as_mut();
                    reader.read_exact(&mut slice[..read_size as usize]).await?;

                    let body = buf.freeze();
                    Ok(body)
                };

                match result {
                    Ok(buf) => {
                        if read_size == 0 {
                            Err(Error::ReadZero)
                        } else {
                            Ok(Some((
                                buf,
                                (remaining - read_size, part_number + 1, Some(reader)),
                            )))
                        }
                    }
                    Err(err) => Err(err),
                }
            }
        },
    ));

    (to_read, stream)
}

fn get_chunk_count(file_size: u64) -> u64 {
    let chunk_size = Config::global().upload_part_size();

    // Rounds up as the last chunk may be smaller than the chunk size
    (file_size + (chunk_size - 1)) / chunk_size
}

async fn create_chunk_upload_stream<ClientT, R>(
    s3: ClientT,
    upload_id: impl Into<String> + Clone,
    bucket: impl Into<String> + Clone,
    key: impl Into<String> + Clone,
    reader: Arc<Mutex<R>>,
    file_size: u64,
) -> impl Stream<Item = impl Future<Output = Result<CompletedPart>>>
where
    ClientT: S3 + Send + Clone,
    R: AsyncRead + AsyncSeek  + Unpin + Send + 'static,
{
    stream::iter(0..get_chunk_count(file_size))
        .map(move |chunk_number| {
            let s3 = s3.clone();
            let bucket: String = bucket.clone().into();
            let key: String = key.clone().into();
            let upload_id: String = upload_id.clone().into();
            let reader = reader.clone();
            (s3, bucket, key, upload_id, chunk_number, reader)
        })
        .map(
            move |(s3, bucket, key, upload_id, chunk_number, reader)| async move {
                // AWS part numbers are 1 indexed
                let part_number: i64 = (chunk_number + 1).try_into().expect("part_number too big");

                let cp = handle_dispatch_error(|| async {
                    let (chunk_size, chunk_stream) =
                        create_stream_for_chunk(reader.clone(), file_size, chunk_number);

                    let chunk_stream = chunk_stream
                        .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err));

                    let body: StreamingBody =
                        ByteStream::new_with_size(chunk_stream, chunk_size as usize);

                    let upr = UploadPartRequest {
                        bucket: bucket.clone(),
                        key: key.clone(),
                        part_number,
                        upload_id: upload_id.clone(),
                        body: Some(body),
                        ..Default::default()
                    };
                    s3.upload_part(upr).await
                })
                .await
                .map(|upo| {
                    if upo.e_tag.is_none() {
                        warn!(
                            "upload_part e_tag was not present (part_number: {})",
                            part_number
                        );
                    }
                    CompletedPart {
                        e_tag: upo.e_tag,
                        part_number: Some(part_number),
                    }
                })
                .map_err(Error::UploadPartFailed)?;

                Ok(cp)
            },
        )
}

#[logfn(err = "ERROR")]
pub async fn upload_from_reader<T, R>(
    s3: &T,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    mut reader: R,
    file_size: u64,
    metadata: Option<HashMap<String, String>>,
) -> Result<()>
where
    T: S3 + Send + Clone,
    R: AsyncRead + AsyncSeek + Send,
{
    let upload_part_size = Config::global().upload_part_size();
    let (bucket, key) = (bucket.as_ref(), key.as_ref());

    info!(
        "put: bucket={}, key={}, file_size={}",
        bucket, key, file_size
    );

    if file_size >= upload_part_size {
        let cmuo = handle_dispatch_error(|| async {
            let cmur = CreateMultipartUploadRequest {
                bucket: bucket.into(),
                key: key.into(),
                acl: Some("bucket-owner-full-control".into()),
                metadata: metadata.as_ref().cloned(),
                ..Default::default()
            };

            s3.create_multipart_upload(cmur).await
        })
        .await
        .map_err(Error::CreateMultipartUploadFailed)?;

        let upload_id = cmuo.upload_id.ok_or(Error::UploadIdNone)?;

        debug!("upload_id: {}", upload_id);

        // Load into global data so it can be cancelled for CTRL-C / SIGTERM
        {
            let mut global_data = GLOBAL_DATA.lock().expect(EXPECT_GLOBAL_DATA);
            global_data.bucket = Some(bucket.into());
            global_data.key = Some(key.into());
            global_data.upload_id = Some(upload_id.clone());
        }

        let shared_reader = Arc::new(Mutex::new(reader));
        let upload_stream = create_chunk_upload_stream(
            s3.clone(),
            upload_id.clone(),
            bucket,
            key,
            shared_reader,
            file_size,
        )
        .await;

        let uploaders_count = Config::global().concurrent_upload_tasks();

        let mut completed_parts: Vec<CompletedPart> = upload_stream
            .buffer_unordered(uploaders_count)
            .try_collect()
            .await?;

        completed_parts.sort_unstable_by_key(|a| a.part_number);

        handle_dispatch_error(|| async {
            let cmpu = CompletedMultipartUpload {
                parts: Some(completed_parts.clone()),
            };

            let cmur = CompleteMultipartUploadRequest {
                bucket: bucket.into(),
                key: key.into(),
                upload_id: upload_id.clone(),
                multipart_upload: Some(cmpu),
                ..Default::default()
            };

            s3.complete_multipart_upload(cmur).await
        })
        .await
        .map_err(Error::CompletedMultipartUploadFailed)?;

        // Clear multi-part upload
        {
            let mut global_data = GLOBAL_DATA.lock().expect(EXPECT_GLOBAL_DATA);
            global_data.bucket = None;
            global_data.key = None;
            global_data.upload_id = None;
        }
    } else {
        let mut buffer = vec![0u8; file_size as usize];
        futures::pin_mut!(reader);
        let read_size = reader.read(&mut buffer).await?;
        if read_size == 0 && file_size != 0 {
            return Err(Error::ReadZero);
        }
        handle_dispatch_error(|| async {
            let body: StreamingBody = buffer.clone().into();

            let por = PutObjectRequest {
                bucket: bucket.into(),
                key: key.into(),
                body: Some(body),
                acl: Some("bucket-owner-full-control".into()),
                metadata: metadata.as_ref().cloned(),
                ..Default::default()
            };

            s3.put_object(por).await
        })
        .await
        .map_err(Error::PutObjectFailed)?;
    }

    Ok(())
}

/// Since large uploads require us to create a multi-part upload request
/// we need to tell AWS that we're aborting the upload, otherwise the
/// unfinished could stick around indefinitely.
#[cfg(feature = "cli")]
pub fn setup_upload_termination_handler() {
    use std::process;
    ctrlc::set_handler(move || {
        let global_data = GLOBAL_DATA.lock().expect(EXPECT_GLOBAL_DATA);
        if global_data.bucket.is_none()
            || global_data.key.is_none()
            || global_data.upload_id.is_none()
        {
            info!("\ncancelled");
        } else if let Some(bucket) = &global_data.bucket {
            if let Some(key) = &global_data.key {
                if let Some(upload_id) = &global_data.upload_id {
                    info!("\ncancelling...");
                    let region = Region::default();
                    let s3 = S3Client::new(region);
                    let res = crate::blocking::abort_upload(&s3, &bucket, &key, &upload_id);
                    if let Err(e) = res {
                        log::error!("cancelling failed: {}", e);
                    }
                }
            }
        }
        process::exit(0);
    })
    .expect("Error setting Ctrl-C handler");
}
