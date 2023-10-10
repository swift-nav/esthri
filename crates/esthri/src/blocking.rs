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

use aws_sdk_s3::Client;
use std::path::Path;

use crate::{ops::sync::GlobFilter, opts::*, HeadObjectInfo, Result, S3PathParam};

#[tokio::main]
pub async fn build_s3_client(region: Option<impl AsRef<str>>) -> Client {
    crate::init_default_s3client_with_region(region).await
}

#[tokio::main]
pub async fn head_object(
    s3: &Client,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
) -> Result<Option<HeadObjectInfo>> {
    crate::head_object(s3, bucket, key).await
}

#[tokio::main]
pub async fn upload(
    s3: &Client,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    file: impl AsRef<Path>,
    opts: EsthriPutOptParams,
) -> Result<()> {
    crate::upload(s3, bucket, key, file, opts).await
}

#[tokio::main]
pub async fn download(
    s3: &Client,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    file: impl AsRef<Path>,
    opts: EsthriGetOptParams,
) -> Result<()> {
    crate::download(s3, bucket, key, file, opts).await
}

#[tokio::main]
pub async fn sync(
    s3: &Client,
    source: S3PathParam,
    destination: S3PathParam,
    filters: Option<&[GlobFilter]>,
    opts: SharedSyncOptParams,
) -> Result<()> {
    crate::sync(s3, source, destination, filters, opts).await
}

#[tokio::main]
pub async fn list_objects(
    s3: &Client,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
) -> Result<Vec<String>> {
    crate::list_objects(s3, bucket, key).await
}

#[tokio::main]
pub async fn compute_etag(path: impl AsRef<Path>) -> Result<String> {
    crate::compute_etag(path).await
}

#[tokio::main]
pub async fn delete(s3: &Client, bucket: impl AsRef<str>, keys: &[impl AsRef<str>]) -> Result<()> {
    crate::delete(s3, bucket, keys).await
}
