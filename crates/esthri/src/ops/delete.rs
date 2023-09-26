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

use futures::{future, stream::Stream, Future, StreamExt, TryFutureExt};

use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use aws_sdk_s3::Client;
use log::{debug, error, info};
use log_derive::logfn;

use crate::errors::Result;
use crate::Error;

const DELETE_BATCH_SIZE: usize = 50;

/// Delete a remote object given a bucket and a key.
///
/// # Arguments
///
/// * `s3` - The S3 client object, see [crate::rusoto::S3]
/// * `bucket` - The bucket to delete from
/// * `keys` - The keys within the bucket to delete
///
/// # Errors
///
/// Will transparently pass failures from [crate::rusoto::S3::delete_object] via [crate::errors::Error].
///
#[logfn(err = "ERROR")]
pub async fn delete(s3: &Client, bucket: impl AsRef<str>, keys: &[impl AsRef<str>]) -> Result<()> {
    info!(
        "delete: bucket={}, keys.len()={:?}",
        bucket.as_ref(),
        keys.len()
    );

    let delete = create_delete(keys, false);
    s3.delete_objects()
        .bucket(bucket.as_ref().to_string())
        .delete(delete)
        .send()
        .await
        .map_err(|e| match e {
            SdkError::ServiceError(error) => Error::DeleteObjectsFailed(Box::new(error.into_err())),
            _ => Error::SdkError(e.to_string()),
        })?;

    Ok(())
}

/// Delete from a bucket with a stream of keys.  Returns a stream of futures which can be forced with
/// [futures::stream::StreamExt::buffered] or [futures::stream::StreamExt::buffer_unordered].
///
/// # Arguments
///
/// * `s3` - The S3 client object, see [crate::rusoto::S3]
/// * `bucket` - The bucket to delete from
/// * `keys` - The stream which provides keys to delete
///
/// # Errors
///
/// Returns a stream of [Result] values.  Any S3 errors are mapped into the local [crate::errors::Error] types.
///
pub fn delete_streaming<'a>(
    s3: &'a Client,
    bucket: impl AsRef<str> + 'a,
    keys: impl Stream<Item = Result<String>> + Unpin + 'a,
) -> impl Stream<Item = impl Future<Output = Result<usize>> + 'a> + 'a {
    info!(
        "delete_streaming: bucket={}, batch_size={}",
        bucket.as_ref(),
        DELETE_BATCH_SIZE
    );

    let chunks = keys.chunks(DELETE_BATCH_SIZE);

    chunks.map(move |keys| {
        let keys: Result<Vec<String>> = keys.into_iter().collect();
        match keys {
            Ok(keys) => {
                debug!("delete_streaming: keys={:?}", keys);
                let len = keys.len();
                let delete = create_delete(&keys, false);
                let fut = s3
                    .delete_objects()
                    .bucket(bucket.as_ref().to_string())
                    .delete(delete)
                    .send();
                future::Either::Left(fut.map_ok(move |_| len).map_err(|e| match e {
                    SdkError::ServiceError(error) => {
                        Error::DeleteObjectsFailed(Box::new(error.into_err()))
                    }
                    _ => Error::SdkError(e.to_string()),
                }))
            }
            Err(err) => {
                error!("nothing found in delete_streaming keys");
                future::Either::Right(future::ready(Err(err)))
            }
        }
    })
}

fn create_delete(keys: &[impl AsRef<str>], quiet: bool) -> Delete {
    let objects = keys
        .iter()
        .map(|key| {
            ObjectIdentifier::builder()
                .key(key.as_ref().to_string())
                .build()
        })
        .collect::<Vec<_>>();
    Delete::builder()
        .set_objects(Some(objects))
        .quiet(quiet)
        .build()
}
