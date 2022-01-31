/*
 * Copyright (C) 2020 Swift Navigation Inc.
 * Contact: Swift Navigation <dev@swiftnav.com>
 *
 * This source is subject to the license found in the file 'LICENSE' which must
 * be be distributed together with this source. All other rights reserved.
 *
 * THIS CODE AND INFORMATION IS PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND,
 * EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A PARTICULAR PURPOSE.
 */

use std::path::Path;

use crate::ops::sync::GlobFilter;
use crate::rusoto::*;
use crate::HeadObjectInfo;
use crate::Result;
use crate::S3PathParam;

#[tokio::main]
pub async fn head_object<T>(
    s3: &T,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
) -> Result<Option<HeadObjectInfo>>
where
    T: S3 + Send,
{
    crate::head_object(s3, bucket, key).await
}

#[tokio::main]
pub async fn upload<T>(
    s3: &T,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    file: impl AsRef<Path>,
) -> Result<()>
where
    T: S3 + Send + Clone,
{
    crate::upload(s3, bucket, key, file).await
}

// #[tokio::main]
// pub async fn upload_from_reader<T, R>(
//     s3: &T,
//     bucket: impl AsRef<str>,
//     key: impl AsRef<str>,
//     reader: R,
//     file_size: u64,
// ) -> Result<()>
// where
//     T: S3 + Send + Clone,
//     R: Read + Send + 'static,
// {
//     crate::upload_from_reader(
//         s3,
//         bucket,
//         key,
//         reader,
//         file_size,
//         None,
//     )
//     .await
// }

#[tokio::main]
pub async fn upload_compressed<T>(
    s3: &T,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    file: impl AsRef<Path>,
) -> Result<()>
where
    T: S3 + Send + Clone,
{
    crate::upload_compressed(s3, bucket, key, file).await
}

#[tokio::main]
pub async fn download<T>(
    s3: &T,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    file: impl AsRef<Path>,
) -> Result<()>
where
    T: S3 + Sync + Send + Clone,
{
    crate::download(s3, bucket, key, file).await
}

#[tokio::main]
pub async fn download_with_transparent_decompression<T>(
    s3: &T,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    file: impl AsRef<Path>,
) -> Result<()>
where
    T: S3 + Sync + Send + Clone,
{
    crate::download_with_transparent_decompression(s3, bucket, key, file).await
}

#[tokio::main]
pub async fn sync<T>(
    s3: &T,
    source: S3PathParam,
    destination: S3PathParam,
    filters: Option<&[GlobFilter]>,
    transparent_compression: bool,
) -> Result<()>
where
    T: S3 + Sync + Send + Clone,
{
    crate::sync(s3, source, destination, filters, transparent_compression).await
}

#[tokio::main]
pub async fn list_objects<T>(
    s3: &T,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
) -> Result<Vec<String>>
where
    T: S3 + Send,
{
    crate::list_objects(s3, bucket, key).await
}

#[tokio::main]
pub async fn compute_etag(path: impl AsRef<Path>) -> Result<String> {
    crate::compute_etag(path).await
}