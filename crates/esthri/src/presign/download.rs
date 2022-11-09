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

use async_compression::tokio::write::GzipDecoder;
use esthri_internals::rusoto::{
    util::{PreSignedRequest, PreSignedRequestOption},
    AwsCredentials, GetObjectRequest, Region,
};
use futures::TryStreamExt;
use reqwest::Client;
use tokio::{
    fs::File,
    io::{copy, AsyncWriteExt},
};
use tokio_util::io::StreamReader;

use crate::{opts::EsthriGetOptParams, Result};

use super::DEAFULT_EXPIRATION;

/// Generate a presigned URL for a client to use to download a file.
/// The file can be downloaded using an HTTP GET on this URL.
pub fn presign_get(
    credentials: &AwsCredentials,
    region: &Region,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    expiration: Option<Duration>,
) -> String {
    let options = PreSignedRequestOption {
        expires_in: expiration.unwrap_or(DEAFULT_EXPIRATION),
    };
    GetObjectRequest {
        bucket: bucket.as_ref().to_owned(),
        key: key.as_ref().to_owned(),
        ..Default::default()
    }
    .get_presigned_url(region, credentials, &options)
}

/// Helper to download a file using a presigned URL, taking care of transparent
/// compression.
pub async fn download_file_presigned(
    client: &Client,
    presigned_url: &str,
    filepath: &Path,
    opts: &EsthriGetOptParams,
) -> Result<()> {
    let mut file = File::create(filepath).await?;
    let mut resp = client.get(presigned_url).send().await?.error_for_status()?;
    if opts.transparent_compression {
        let st = resp
            .bytes_stream()
            .map_err(|e| futures::io::Error::new(futures::io::ErrorKind::Other, e));
        let mut src = StreamReader::new(st);
        let mut dest = GzipDecoder::new(file);
        copy(&mut src, &mut dest).await?;
    } else {
        while let Some(bytes) = resp.chunk().await? {
            file.write_all(&bytes).await?;
        }
    }
    Ok(())
}
