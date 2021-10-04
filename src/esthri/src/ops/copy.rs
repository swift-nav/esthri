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

use crate::errors::{Error, Result};
use crate::{download, upload, SyncParam};
use log_derive::logfn;
use rusoto_s3::S3;

#[logfn(err = "ERROR")]
pub async fn copy<T>(s3: &T, source: SyncParam, destination: SyncParam) -> Result<()>
where
    T: S3 + Sync + Send + Clone,
{
    match source {
        SyncParam::Bucket { bucket, key } => match destination {
            SyncParam::Local { path } => download(s3, bucket, key, path).await,
            SyncParam::Bucket { bucket: _, key: _ } => {
                Err(Error::BucketToBucketCpNotImplementedError)
            }
        },
        SyncParam::Local { path } => match destination {
            SyncParam::Bucket { bucket, key } => upload(s3, bucket, key, path).await,
            SyncParam::Local { path: _ } => Err(Error::LocalToLocalCpNotImplementedError),
        },
    }
}
