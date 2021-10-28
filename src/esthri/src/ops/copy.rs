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
use crate::{download, upload, S3PathParam};
use log_derive::logfn;
use rusoto_s3::S3;

#[logfn(err = "ERROR")]
pub async fn copy<T>(s3: &T, source: S3PathParam, destination: S3PathParam) -> Result<()>
where
    T: S3 + Sync + Send + Clone,
{
    match source {
        S3PathParam::Bucket { bucket, key } => match destination {
            S3PathParam::Local { path } => download(s3, bucket, key, path).await,
            S3PathParam::Bucket { bucket: _, key: _ } => {
                Err(Error::BucketToBucketCpNotImplementedError)
            }
        },
        S3PathParam::Local { path } => match destination {
            S3PathParam::Bucket { bucket, key } => upload(s3, bucket, key, path).await,
            S3PathParam::Local { path: _ } => Err(Error::LocalToLocalCpNotImplementedError),
        },
    }
}