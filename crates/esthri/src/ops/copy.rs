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

use aws_sdk_s3::Client;
use log_derive::logfn;

use crate::{
    download,
    errors::{Error, Result},
    opts::*,
    upload, S3PathParam,
};

#[logfn(err = "ERROR")]
pub async fn copy(
    s3: &Client,
    source: S3PathParam,
    destination: S3PathParam,
    opts: AwsCopyOptParams,
) -> Result<()> {
    match source {
        S3PathParam::Bucket { bucket, key } => match destination {
            S3PathParam::Local { path } => download(s3, bucket, key, path, opts.into()).await,
            S3PathParam::Bucket { bucket: _, key: _ } => {
                Err(Error::BucketToBucketCpNotImplementedError)
            }
        },
        S3PathParam::Local { path } => match destination {
            S3PathParam::Bucket { bucket, key } => upload(s3, bucket, key, path, opts.into()).await,
            S3PathParam::Local { path: _ } => Err(Error::LocalToLocalCpNotImplementedError),
        },
    }
}
