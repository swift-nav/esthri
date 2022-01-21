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

use std::borrow::Cow;
use std::sync::Mutex;
use std::{collections::HashMap, path::Path};

use futures::{stream, Future, Stream, StreamExt, TryStreamExt};

use log::{debug, info, warn};
use log_derive::logfn;
use once_cell::sync::Lazy;
use tokio::task;

use crate::{
    config::Config,
    errors::{Error, Result},
    handle_dispatch_error,
    rusoto::*,
    types::GlobalData,
    EXPECT_SPAWN_BLOCKING,
};

/// Internal module used to call out operations that may block.
mod bio {
    pub(super) use std::fs;
    pub(super) use std::fs::File;
    pub(super) use std::io::prelude::*;
    pub(super) use std::io::BufReader;
}

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
    use bio::*;
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
        let stat = fs::metadata(&path)?;
        if compressed {
            use crate::compression::compress_to_tempfile;
            let (compressed, size) = compress_to_tempfile(path.clone()).await?;
            upload_from_reader(
                s3,
                bucket,
                key,
                compressed,
                size,
                Some(crate::compression::compressed_file_metadata()),
            )
            .await
        } else {
            let size = stat.len();
            debug!("upload: file size: {}", size);
            let reader = BufReader::new(File::open(path)?);
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

async fn create_file_chunk_stream<R>(
    reader: R,
    file_size: u64,
) -> impl Stream<Item = Result<(i64, Vec<u8>)>>
where
    R: bio::Read + Send + 'static,
{
    let upload_part_size = Config::global().upload_part_size();
    let init_state = (file_size, /* part_number = */ 1, Some(reader));
    Box::pin(stream::unfold(
        init_state,
        move |(remaining, part_number, reader)| async move {
            let done_state = (0, 0, None);
            if remaining == 0 {
                None
            } else {
                let upload_part_size = u64::min(remaining, upload_part_size);
                let mut buf = vec![0u8; upload_part_size as usize];
                let result: Result<(Vec<u8>, usize, R)> = task::spawn_blocking(move || {
                    let mut reader = reader.unwrap();
                    let read_size = reader.read(&mut buf)?;
                    Ok((buf, read_size, reader))
                })
                .await
                .expect(EXPECT_SPAWN_BLOCKING);
                match result {
                    Ok((buf, read_size, reader)) => {
                        if read_size == 0 {
                            let err = Err(Error::ReadZero);
                            Some((err, done_state))
                        } else {
                            let result = Ok((part_number, buf));
                            Some((
                                result,
                                (remaining - upload_part_size, part_number + 1, Some(reader)),
                            ))
                        }
                    }
                    Err(err) => Some((Err(err), done_state)),
                }
            }
        },
    ))
}

async fn create_chunk_upload_stream<StreamT, ClientT>(
    source_stream: StreamT,
    s3: ClientT,
    upload_id: impl Into<String> + Clone,
    bucket: impl Into<String> + Clone,
    key: impl Into<String> + Clone,
) -> impl Stream<Item = impl Future<Output = Result<CompletedPart>>>
where
    StreamT: Stream<Item = Result<(i64, Vec<u8>)>>,
    ClientT: S3 + Send + Clone,
{
    source_stream
        .map(move |value| {
            let s3 = s3.clone();
            let bucket: String = bucket.clone().into();
            let key: String = key.clone().into();
            let upload_id: String = upload_id.clone().into();
            (s3, bucket, key, upload_id, value)
        })
        .map(|(s3, bucket, key, upload_id, value)| async move {
            let (part_number, buf) = value?;
            let cp = handle_dispatch_error(|| async {
                let body: StreamingBody = buf.clone().into();
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
        })
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
    R: bio::Read + Send + 'static,
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

        let chunk_stream = create_file_chunk_stream(reader, file_size).await;
        let upload_stream =
            create_chunk_upload_stream(chunk_stream, s3.clone(), upload_id.clone(), bucket, key)
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
        let buffer = task::spawn_blocking(move || {
            let mut buffer = vec![0u8; file_size as usize];
            let read_size = reader.read(&mut buffer)?;
            if read_size == 0 && file_size != 0 {
                Err(Error::ReadZero)
            } else {
                Ok(buffer)
            }
        })
        .await
        .expect(EXPECT_SPAWN_BLOCKING)?;
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
