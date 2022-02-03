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

use log_derive::logfn;

use crate::errors::{Error, Result};
use crate::rusoto::S3;
use crate::{
    download, download_with_transparent_decompression, upload, upload_compressed, S3PathParam,
};

#[logfn(err = "ERROR")]
pub async fn copy<T>(
    s3: &T,
    source: S3PathParam,
    destination: S3PathParam,
    transparent_compression: bool,
) -> Result<()>
where
    T: S3 + Sync + Send + Clone,
{
    match source {
        S3PathParam::Bucket { bucket, key } => match destination {
            S3PathParam::Local { path } => {
                if transparent_compression {
                    download_with_transparent_decompression(s3, bucket, key, path).await
                } else {
                    download(s3, bucket, key, path).await
                }
            }
            S3PathParam::Bucket { bucket: _, key: _ } => {
                Err(Error::BucketToBucketCpNotImplementedError)
            }
        },
        S3PathParam::Local { path } => match destination {
            S3PathParam::Bucket { bucket, key } => {
                if transparent_compression {
                    upload_compressed(s3, bucket, key, path).await
                } else {
                    upload(s3, bucket, key, path).await
                }
            }
            S3PathParam::Local { path: _ } => Err(Error::LocalToLocalCpNotImplementedError),
        },
    }
}
