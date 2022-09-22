/*
 * Copyright (C) 2022 Swift Navigation Inc.
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

use futures::stream::Stream;
use futures::{future, Future, StreamExt, TryFutureExt};

use log::{debug, info};

use crate::errors::Result;
use crate::rusoto::{Delete, DeleteObjectsRequest, ObjectIdentifier, S3};

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
pub async fn delete<T>(s3: &T, bucket: impl AsRef<str>, keys: &[impl AsRef<str>]) -> Result<()>
where
    T: S3 + Sync + Send + Clone,
{
    info!(
        "delete: bucket={}, keys.len()={:?}",
        bucket.as_ref(),
        keys.len()
    );

    let dor = create_delete_request(bucket, keys);
    s3.delete_objects(dor).await?;

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
pub fn delete_streaming<'a, T>(
    s3: &'a T,
    bucket: impl AsRef<str> + 'a,
    keys: impl Stream<Item = Result<String>> + Unpin + 'a,
) -> impl Stream<Item = impl Future<Output = Result<usize>> + 'a> + 'a
where
    T: S3 + Sync + Send + Clone,
{
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
                let dor = create_delete_request(&bucket, &keys);
                let fut = s3.delete_objects(dor);
                future::Either::Left(fut.map_ok(move |_| len).map_err(|e| e.into()))
            }
            Err(err) => {
                println!("nothing found in delete_streaming keys");
                future::Either::Right(future::ready(Err(err)))
            }
        }
    })
}

fn create_delete_request(
    bucket: impl AsRef<str>,
    keys: &[impl AsRef<str>],
) -> DeleteObjectsRequest {
    let objects = keys
        .iter()
        .map(|key| ObjectIdentifier {
            key: key.as_ref().into(),
            ..Default::default()
        })
        .collect::<Vec<_>>();
    let del = Delete {
        objects,
        ..Default::default()
    };
    DeleteObjectsRequest {
        bucket: bucket.as_ref().into(),
        delete: del,
        ..Default::default()
    }
}
