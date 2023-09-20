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

use std::{path::Path, time::Duration};

use esthri_internals::hyper::HeaderMap;

use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::types::ObjectCannedAcl;
use aws_sdk_s3::Client as S3Client;
use reqwest::{header::CONTENT_LENGTH, Body, Client as HttpClient};
use tokio_util::codec::{BytesCodec, FramedRead};

use crate::{compression::compressed_meta_value, opts::EsthriPutOptParams, Error, Result};

use super::{file_maybe_compressed, DEAFULT_EXPIRATION};

const COMPRESS_HEADER: &str = "x-amz-meta-esthri_compress_version";

/// Generate a presigned URL for a client to use to upload a file.
/// The file can be deleted using an HTTP PUT on this URL.
/// Note that the headers `Content-Length`, `x-amz-acl`, `x-amz-storage-class`
/// and `x-amz-meta-esthri_compress_version` may need to be set.
pub async fn presign_put(
    s3: &S3Client,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    expiration: Option<Duration>,
    opts: EsthriPutOptParams,
) -> Result<String> {
    let presigning_config = PresigningConfig::builder()
        .expires_in(expiration.unwrap_or(DEAFULT_EXPIRATION))
        .build()
        .map_err(Error::PresigningConfigError)?;

    let presigned_req = s3
        .put_object()
        .bucket(bucket.as_ref().to_string())
        .key(key.as_ref().to_string())
        .acl(ObjectCannedAcl::BucketOwnerFullControl)
        .set_storage_class(opts.storage_class)
        .presigned(presigning_config)
        .await
        .map_err(|e| match e {
            SdkError::ServiceError(error) => Error::PutObjectFailed(Box::new(error.into_err())),
            _ => Error::SdkError(e.to_string()),
        })?;

    Ok(presigned_req.uri().to_string())
}

/// Helper to download a file using a presigned URL, setting appropriate
/// headers.
pub async fn upload_file_presigned(
    client: &HttpClient,
    presigned_url: &str,
    filepath: &Path,
    opts: EsthriPutOptParams,
) -> Result<()> {
    let file = file_maybe_compressed(filepath, &opts).await?;
    let file_size = file.metadata().await?.len();
    let stream = FramedRead::new(file, BytesCodec::new());
    let body = Body::wrap_stream(stream);
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_LENGTH, file_size.into());
    headers.insert("x-amz-acl", "bucket-owner-full-control".parse().unwrap());
    if let Some(class) = opts.storage_class {
        headers.insert(
            "x-amz-storage-class",
            class.as_str().to_string().parse().unwrap(),
        );
    }
    if opts.transparent_compression {
        headers.insert(COMPRESS_HEADER, compressed_meta_value().parse().unwrap());
    }
    client
        .put(presigned_url)
        .headers(headers)
        .body(body)
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}
