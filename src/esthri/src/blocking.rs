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
use std::path::Path;

use super::rusoto::*;

use super::ObjectInfo;
use super::Result;
use super::SyncParam;

#[tokio::main]
pub async fn head_object<T, SR0, SR1>(s3: &T, bucket: SR0, key: SR1) -> Result<Option<ObjectInfo>>
where
    T: S3 + Send,
    SR0: AsRef<str>,
    SR1: AsRef<str>,
{
    super::head_object(s3, bucket, key).await
}

#[tokio::main]
pub async fn abort_upload<T, SR0, SR1, SR2>(
    s3: &T,
    bucket: SR0,
    key: SR1,
    upload_id: SR2,
) -> Result<()>
where
    T: S3 + Send,
    SR0: AsRef<str>,
    SR1: AsRef<str>,
    SR2: AsRef<str>,
{
    super::abort_upload(s3, bucket, key, upload_id).await
}

#[tokio::main]
pub async fn upload<T, P, SR0, SR1>(s3: &T, bucket: SR0, key: SR1, file: P) -> Result<()>
where
    T: S3 + Send + Clone,
    P: AsRef<Path>,
    SR0: AsRef<str>,
    SR1: AsRef<str>,
{
    super::upload(s3, bucket, key, file).await
}

#[tokio::main]
pub async fn upload_from_reader<T, SR0, SR1>(
    s3: &T,
    bucket: SR0,
    key: SR1,
    reader: &mut dyn Read,
    file_size: u64,
) -> Result<()>
where
    T: S3 + Send + Clone,
    SR0: AsRef<str>,
    SR1: AsRef<str>,
{
    super::upload_from_reader(s3, bucket, key, reader, file_size).await
}

#[tokio::main]
pub async fn download<T, P, SR0, SR1>(s3: &T, bucket: SR0, key: SR1, file: P) -> Result<()>
where
    T: S3 + Send + Clone,
    P: AsRef<Path>,
    SR0: AsRef<str>,
    SR1: AsRef<str>,
{
    super::download(s3, bucket, key, file).await
}

#[tokio::main]
pub async fn sync<T, SR0, SR1>(
    s3: &T,
    source: SyncParam,
    destination: SyncParam,
    includes: Option<&[SR0]>,
    excludes: Option<&[SR1]>,
) -> Result<()>
where
    T: S3 + Send + Clone,
    SR0: AsRef<str>,
    SR1: AsRef<str>,
{
    super::sync(s3, source, destination, includes, excludes).await
}

#[tokio::main]
pub async fn list_objects<T, SR0, SR1>(s3: &T, bucket: SR0, key: SR1) -> Result<Vec<String>>
where
    T: S3 + Send,
    SR0: AsRef<str>,
    SR1: AsRef<str>,
{
    super::list_objects(s3, bucket, key).await
}
