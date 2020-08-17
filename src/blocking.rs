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

use std::io::prelude::*;

use rusoto_s3::S3;

use super::types::SyncDirection;
use super::Result;

#[tokio::main]
pub async fn head_object<T>(s3: &T, bucket: &str, key: &str) -> Result<Option<String>>
where
    T: S3 + Send,
{
    super::head_object(s3, bucket, key).await
}

#[tokio::main]
pub async fn abort_upload<T>(s3: &T, bucket: &str, key: &str, upload_id: &str) -> Result<()>
where
    T: S3 + Send,
{
    super::abort_upload(s3, bucket, key, upload_id).await
}

#[tokio::main]
pub async fn upload<T>(s3: &T, bucket: &str, key: &str, file: &str) -> Result<()>
where
    T: S3 + Send,
{
    super::upload(s3, bucket, key, file).await
}

#[tokio::main]
pub async fn upload_from_reader<T>(
    s3: &T,
    bucket: &str,
    key: &str,
    reader: &mut dyn Read,
    file_size: u64,
) -> Result<()>
where
    T: S3 + Send,
{
    super::upload_from_reader(s3, bucket, key, reader, file_size).await
}

#[tokio::main]
pub async fn download<T>(s3: &T, bucket: &str, key: &str, file: &str) -> Result<()>
where
    T: S3 + Send,
{
    super::download(s3, bucket, key, file).await
}

#[tokio::main]
pub async fn sync<T>(
    s3: &T,
    direction: SyncDirection,
    bucket: &str,
    key: &str,
    directory: &str,
    includes: &Option<Vec<String>>,
    excludes: &Option<Vec<String>>,
) -> Result<()>
where
    T: S3 + Send,
{
    super::sync(s3, direction, bucket, key, directory, includes, excludes).await
}

#[tokio::main]
pub async fn list_objects<T>(s3: &T, bucket: &str, key: &str) -> Result<Vec<String>>
where
    T: S3 + Send,
{
    super::list_objects(s3, bucket, key).await
}
