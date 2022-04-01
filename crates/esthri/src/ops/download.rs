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

use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use async_compression::tokio::bufread::GzipDecoder as GzipDecoderReader;
use async_compression::tokio::write::GzipDecoder;
use bytes::Bytes;
use futures::{stream, Stream, StreamExt, TryStreamExt};
use log::info;
use log_derive::logfn;
use tokio::io;
use tokio_util::io::{ReaderStream, StreamReader};

use crate::config::Config;
use crate::errors::{Error, Result};
use crate::head_object_request;
use crate::rusoto::*;
use crate::tempfile::TempFile;

#[logfn(err = "ERROR")]
pub async fn download<T>(
    s3: &T,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    file: impl AsRef<Path>,
) -> Result<()>
where
    T: S3 + Sync + Send + Clone,
{
    info!(
        "get: bucket={}, key={}, file={}",
        bucket.as_ref(),
        key.as_ref(),
        file.as_ref().display()
    );

    download_file(s3, bucket.as_ref(), key.as_ref(), file.as_ref(), false).await
}

#[logfn(err = "ERROR")]
pub async fn download_with_transparent_decompression<T>(
    s3: &T,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    file: impl AsRef<Path>,
) -> Result<()>
where
    T: S3 + Sync + Send + Clone,
{
    info!(
        "get(decompress): bucket={}, key={}, file={}",
        bucket.as_ref(),
        key.as_ref(),
        file.as_ref().display()
    );

    download_file(s3, bucket.as_ref(), key.as_ref(), file.as_ref(), true).await
}

pub async fn download_streaming<'a, T>(
    s3: &'a T,
    bucket: &'a str,
    key: &'a str,
    transparent_decompression: bool,
) -> Result<Pin<Box<dyn Stream<Item = Result<Bytes>> + Send + 'a>>>
where
    T: S3 + std::marker::Sync,
{
    let obj_info = head_object_request(s3, bucket, key, Some(1))
        .await?
        .ok_or_else(|| Error::GetObjectInvalidKey(key.to_owned()))?;
    let stream = download_streaming_helper(s3, bucket, key, obj_info.parts);

    if obj_info.is_esthri_compressed() && transparent_decompression {
        let src = StreamReader::new(stream);
        let dest = GzipDecoderReader::new(src);
        let reader = ReaderStream::new(dest);
        Ok(Box::pin(futures::TryStreamExt::map_err(
            reader,
            Error::IoError,
        )))
    } else {
        Ok(Box::pin(stream))
    }
}

async fn download_file<T>(
    s3: &T,
    bucket: &str,
    key: &str,
    download_path: &Path,
    transparent_decompression: bool,
) -> Result<()>
where
    T: S3 + Send + Sync + Clone,
{
    let obj_info = head_object_request(s3, bucket, key, Some(1))
        .await?
        .ok_or_else(|| Error::GetObjectInvalidKey(key.into()))?;
    info!(
        "parts={} part_size={} total_size={}",
        obj_info.parts,
        obj_info.size,
        obj_info.parts * obj_info.size as u64
    );
    let dir = init_download_dir(download_path).await?;
    let mut dest = TempFile::new(dir, None).await?;

    if transparent_decompression && obj_info.is_esthri_compressed() {
        let stream = download_streaming_helper(s3, bucket, key, obj_info.parts);
        let mut src = StreamReader::new(stream);
        let mut dest = GzipDecoder::new(dest.file_mut());
        io::copy(&mut src, &mut dest).await?;
    } else {
        let dest = &Arc::new(dest.take_std_file().await);
        let part_size = obj_info.size;
        let stream = download_unordered_streaming_helper(s3, bucket, key, obj_info.parts)
            .map_ok(|(part, mut chunks)| async move {
                let mut offset = (part - 1) * part_size;
                while let Some(buf) = chunks.try_next().await? {
                    let len = buf.len();
                    write_all_at(Arc::clone(dest), buf, offset as u64).await?;
                    offset += len as i64;
                }
                Result::Ok(())
            })
            .try_buffer_unordered(Config::global().concurrent_writer_tasks());
        let _: () = stream.try_collect().await?;
    };

    // If we're trying to download into a directory, assemble the path for the user
    if download_path.is_dir() {
        let s3filename = key
            .split('/')
            .next_back()
            .ok_or(Error::CouldNotParseS3Filename)?;
        dest.persist(download_path.join(s3filename)).await?;
    } else {
        dest.persist(download_path.into()).await?;
    }

    Ok(())
}

/// Fetches an object as a stream of `Byte`s
fn download_streaming_helper<'a, T>(
    s3: &'a T,
    bucket: &'a str,
    key: &'a str,
    parts: u64,
) -> impl Stream<Item = Result<Bytes>> + 'a
where
    T: S3,
{
    stream::iter(1..=parts)
        .map(move |part| get_object_part_request(s3, bucket, key, part as i64))
        .buffered(Config::global().concurrent_downloader_tasks())
        .map_ok(GetObjectResponse::into_stream)
        .try_flatten()
}

/// like download_streaming_helper but the requests are not in order and the part
/// number is returned with each request
fn download_unordered_streaming_helper<'a, T>(
    s3: &'a T,
    bucket: &'a str,
    key: &'a str,
    parts: u64,
) -> impl Stream<Item = Result<(i64, impl Stream<Item = Result<Bytes>> + 'a)>> + 'a
where
    T: S3,
{
    stream::iter(1..=parts)
        .map(move |part| get_object_part_request(s3, bucket, key, part as i64))
        .buffer_unordered(Config::global().concurrent_downloader_tasks())
        .map_ok(|res| (res.part, res.into_stream()))
}

async fn init_download_dir(path: &Path) -> Result<PathBuf> {
    let mut path = path.to_owned();
    tokio::task::spawn_blocking(move || {
        if !path.is_file() && !path.is_dir() {
            // If the specified destination directory doesn't already exist,
            // see if the directory structure needs to be made
            let dir = if !path.to_string_lossy().ends_with('/') {
                path.pop();
                path
            } else {
                path
            };
            if !dir.exists() && !dir.as_os_str().is_empty() {
                info!("Creating directory path {:?}", dir.as_os_str());
                std::fs::create_dir_all(&dir)?;
            }
            Ok(dir)
        } else {
            path.pop();
            Ok(path)
        }
    })
    .await?
}

#[cfg(unix)]
async fn write_all_at(file: Arc<std::fs::File>, buf: Bytes, offset: u64) -> Result<()> {
    use std::os::unix::prelude::FileExt;

    tokio::task::spawn_blocking(move || {
        file.write_all_at(&buf, offset)?;
        Result::Ok(())
    })
    .await??;
    Ok(())
}

#[cfg(windows)]
async fn write_all_at(file: Arc<std::fs::File>, buf: Bytes, offset: u64) -> Result<()> {
    use std::os::windows::prelude::FileExt;

    tokio::task::spawn_blocking(move || {
        let (mut offset, mut length) = (offset, buf.len());
        let mut buffer_offset = 0;
        while length > 0 {
            let write_size = file
                .seek_write(&buf[buffer_offset..length], offset)
                .map_err(Error::from)?;
            length -= write_size;
            offset += write_size as u64;
            buffer_offset += write_size;
        }
        Result::Ok(())
    })
    .await??;
    Ok(())
}
