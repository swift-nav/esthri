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

use crate::{ops::sync::GlobFilter, opts::*, rusoto::*, HeadObjectInfo, Result, S3PathParam};

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
    opts: EsthriPutOptParams,
) -> Result<()>
where
    T: S3 + Send + Clone,
{
    crate::upload(s3, bucket, key, file, opts).await
}

#[tokio::main]
pub async fn download<T>(
    s3: &T,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    file: impl AsRef<Path>,
    opts: EsthriGetOptParams,
) -> Result<()>
where
    T: S3 + Sync + Send + Clone,
{
    crate::download(s3, bucket, key, file, opts).await
}

#[tokio::main]
pub async fn sync<T>(
    s3: &T,
    source: S3PathParam,
    destination: S3PathParam,
    filters: Option<&[GlobFilter]>,
    opts: SharedSyncOptParams,
) -> Result<()>
where
    T: S3 + Sync + Send + Clone,
{
    crate::sync(s3, source, destination, filters, opts).await
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

#[tokio::main]
pub async fn delete<T>(s3: &T, bucket: impl AsRef<str>, keys: &[impl AsRef<str>]) -> Result<()>
where
    T: S3 + Sync + Send + Clone,
{
    crate::delete(s3, bucket, keys).await
}
