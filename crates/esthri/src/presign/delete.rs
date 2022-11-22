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

use crate::Result;
use esthri_internals::rusoto::{
    util::{PreSignedRequest, PreSignedRequestOption},
    AwsCredentials, DeleteObjectRequest, Region,
};
use reqwest::Client;

use super::DEAFULT_EXPIRATION;

/// Generate a presigned URL for a client to use to delete a file.
/// The file can be deleted using an HTTP DELETE on this URL.
pub fn presign_delete(
    credentials: &AwsCredentials,
    region: &Region,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    expiration: Option<Duration>,
) -> String {
    let options = PreSignedRequestOption {
        expires_in: expiration.unwrap_or(DEAFULT_EXPIRATION),
    };
    DeleteObjectRequest {
        bucket: bucket.as_ref().to_owned(),
        key: key.as_ref().to_owned(),
        ..Default::default()
    }
    .get_presigned_url(region, credentials, &options)
}

/// Helper to delete a file using a presigned URL.
pub async fn delete_file_presigned(client: &Client, presigned_url: impl AsRef<str>) -> Result<()> {
    client
        .delete(presigned_url.as_ref())
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}
