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

#![cfg_attr(feature = "aggressive_lint", deny(warnings))]

//! Blocking version of the [esthri](crate) crate.  All functions are annotated with
//! `[tokio::main]` and block until completion.

use std::io::prelude::*;
use std::path::Path;

use super::rusoto::*;

use super::ObjectInfo;
use super::Result;
use super::SyncParam;

/// (Blocking) version of [esthri::head_object](crate::head_object) -- fetch metadata about an S3 object.
///
/// # Arguments
///
/// * `s3` - S3 client reference, see [rusoto_s3::S3]
/// * `bucket` - [String] or [&str] name of an S3 bucket
/// * `key` - [String] or [&str] key of the object for which to fetch metdata
///
/// # Errors
///
/// - [HeadObjectUnexpected](crate::errors::Error::HeadObjectUnexpected)
/// - [HeadObjectFailure](crate::errors::Error::HeadObjectFailure)
/// - [HeadObjectFailedParseError](crate::errors::Error::HeadObjectFailedParseError)
///
/// # Examples
///
/// ```
/// # use std::sync::Arc;
/// # use esthri::rusoto::*;
/// # use esthri::blocking::*;
/// # let s3 = Arc::new(S3Client::new(Region::default()));
/// # let s3 = s3.as_ref();
/// head_object(s3, "esthri-test", "foo.txt").unwrap();
/// ```
#[tokio::main]
pub async fn head_object<T>(
    s3: &T,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
) -> Result<Option<ObjectInfo>>
where
    T: S3 + Send,
{
    super::head_object(s3, bucket, key).await
}

/// (Blocking) version of [esthri::abort_upload](crate::abort_upload) -- abort a multi-part upload.
///
/// # Arguments
///
/// * `s3` - S3 client reference, see [rusoto_s3::S3]
/// * `bucket` - [String] or [&str] name of an S3 bucket
/// * `key` - [String] or [&str] key of the object for which to fetch metdata
/// * `upload_id` - The ID of the multipart upload to cancel. See awscli docs on [s3api
///                 abort-multipart-upload](https://awscli.amazonaws.com/v2/documentation/api/latest/reference/s3api/abort-multipart-upload.html) or (rusoto_s3::S3::abort_multipart_upload)[https://rusoto.github.io/rusoto/rusoto_s3/trait.S3.html#tymethod.abort_multipart_upload]
///
/// # Errors
///
/// * [AbortMultipartUploadFailed](crate::errors::Error::AbortMultipartUploadFailed)
///
/// # Examples
///
/// ```
/// # use std::sync::Arc;
/// # use esthri::rusoto::*;
/// # use esthri::blocking::*;
/// # let s3 = Arc::new(S3Client::new(Region::default()));
/// # let s3 = s3.as_ref();
/// # let bucket = "esthri-test";
/// # let key = "foo-multipart.bin";
/// # let cmur = CreateMultipartUploadRequest {
/// #               bucket: bucket.into(),
/// #               key: key.into(),
/// #               acl: Some("bucket-owner-full-control".into()),
/// #               ..Default::default()
/// #           };
/// # let cmuo = tokio_test::block_on(async {
/// #         s3.create_multipart_upload(cmur).await
/// # });
/// # let upload_id = cmuo.unwrap().upload_id.unwrap();
/// abort_upload(s3, "esthri-test", "foo-multipart.bin", upload_id).unwrap();
/// ```
#[tokio::main]
pub async fn abort_upload<T>(
    s3: &T,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    upload_id: impl AsRef<str>,
) -> Result<()>
where
    T: S3 + Send,
{
    super::abort_upload(s3, bucket, key, upload_id).await
}

/// (Blocking) version of [esthri::upload](crate::upload) -- upload a file to S3.
///
/// # Arguments
///
/// * `s3` - S3 client reference, see [rusoto_s3::S3]
/// * `bucket` - [String] or [&str] name of an S3 bucket
/// * `key` - Destination key ([String] or [&str]) for the S3 object
/// * `file` - The path of the file upload
///
/// # Errors
///
/// * [PutObjectFailed](crate::errors::Error::PutObjectFailed)
///
/// # Examples
///
/// ```
/// # use std::sync::Arc;
/// # use esthri::rusoto::*;
/// # use esthri::blocking::*;
/// # let s3 = Arc::new(S3Client::new(Region::default()));
/// # let s3 = s3.as_ref();
/// upload(s3, "esthri-test", "foo.txt", "tests/data/test1mb.bin").unwrap();
/// ```
#[tokio::main]
pub async fn upload<T>(
    s3: &T,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    file: impl AsRef<Path>,
) -> Result<()>
where
    T: S3 + Send + Clone,
{
    super::upload(s3, bucket, key, file).await
}

#[tokio::main]
pub async fn upload_from_reader<T, R>(
    s3: &T,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    reader: R,
    file_size: u64,
) -> Result<()>
where
    T: S3 + Send + Clone,
    R: Read + Send + 'static,
{
    super::upload_from_reader(s3, bucket, key, reader, file_size).await
}

#[cfg(feature = "compression")]
#[tokio::main]
pub async fn upload_compressed<T>(
    s3: &T,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    file: impl AsRef<Path>,
) -> Result<()>
where
    T: S3 + Send + Clone,
{
    super::upload_compressed(s3, bucket, key, file).await
}

#[tokio::main]
pub async fn download<T>(
    s3: &T,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    file: impl AsRef<Path>,
) -> Result<()>
where
    T: S3 + Sync + Send + Clone,
{
    super::download(s3, bucket, key, file).await
}

#[cfg(feature = "compression")]
#[tokio::main]
pub async fn download_decompressed<T>(
    s3: &T,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    file: impl AsRef<Path>,
) -> Result<()>
where
    T: S3 + Sync + Send + Clone,
{
    super::download_decompressed(s3, bucket, key, file).await
}

#[tokio::main]
pub async fn sync<T>(
    s3: &T,
    source: SyncParam,
    destination: SyncParam,
    includes: Option<&[impl AsRef<str>]>,
    excludes: Option<&[impl AsRef<str>]>,
    #[cfg(feature = "compression")] compressed: bool,
) -> Result<()>
where
    T: S3 + Sync + Send + Clone,
{
    super::sync(
        s3,
        source,
        destination,
        includes,
        excludes,
        #[cfg(feature = "compression")]
        compressed,
    )
    .await
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
    super::list_objects(s3, bucket, key).await
}

#[tokio::main]
pub async fn compute_etag(path: impl AsRef<Path>) -> Result<String> {
    super::compute_etag(path).await
}
