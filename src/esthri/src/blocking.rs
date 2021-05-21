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
/// - [crate::errors::Error::HeadObjectUnexpected]
/// - [crate::errors::Error::HeadObjectFailure]
/// - [crate::errors::Error::HeadObjectFailedParseError]
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
pub async fn head_object<T, SR0, SR1>(s3: &T, bucket: SR0, key: SR1) -> Result<Option<ObjectInfo>>
where
    T: S3 + Send,
    SR0: AsRef<str>,
    SR1: AsRef<str>,
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
/// - [crate::errors::Error::AbortMultipartUploadError]
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
    T: S3 + Send,
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
    T: S3 + Send,
    SR0: AsRef<str>,
    SR1: AsRef<str>,
{
    super::upload_from_reader(s3, bucket, key, reader, file_size).await
}

#[tokio::main]
pub async fn download<T, P, SR0, SR1>(s3: &T, bucket: SR0, key: SR1, file: P) -> Result<()>
where
    T: S3 + Send,
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
    T: S3 + Send,
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
