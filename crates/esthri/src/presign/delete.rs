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
use std::time::Duration;

use crate::{Error, Result};
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::Client as S3Client;
use reqwest::Client as HttpClient;

use super::DEAFULT_EXPIRATION;

/// Generate a presigned URL for a client to use to delete a file.
/// The file can be deleted using an HTTP DELETE on this URL.
pub async fn presign_delete(
    s3: &S3Client,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    expiration: Option<Duration>,
) -> Result<String> {
    let presigning_config = PresigningConfig::builder()
        .expires_in(expiration.unwrap_or(DEAFULT_EXPIRATION))
        .build()
        .map_err(Error::PresigningConfigError)?;

    let presigned_req = s3
        .delete_object()
        .bucket(bucket.as_ref().to_string())
        .key(key.as_ref().to_string())
        .presigned(presigning_config)
        .await
        .map_err(|e| match e {
            SdkError::ServiceError(error) => Error::DeleteObjectFailed(Box::new(error.into_err())),
            _ => Error::SdkError(e.to_string()),
        })?;

    Ok(presigned_req.uri().to_string())
}

/// Helper to delete a file using a presigned URL.
pub async fn delete_file_presigned(
    client: &HttpClient,
    presigned_url: impl AsRef<str>,
) -> Result<()> {
    client
        .delete(presigned_url.as_ref())
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}
