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
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::Client as S3Client;
use futures::TryStreamExt;
use reqwest::Client as HttpClient;
use tokio::{
    fs::File,
    io::{copy, AsyncWriteExt},
};
use tokio_util::io::StreamReader;

use crate::{opts::EsthriGetOptParams, Error, Result};

use super::DEAFULT_EXPIRATION;

/// Generate a presigned URL for a client to use to download a file.
/// The file can be downloaded using an HTTP GET on this URL.
pub async fn presign_get(
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
        .get_object()
        .bucket(bucket.as_ref().to_string())
        .key(key.as_ref().to_string())
        .presigned(presigning_config)
        .await
        .map_err(|e| match e {
            SdkError::ServiceError(error) => Error::GetObjectFailed(Box::new(error.into_err())),
            _ => Error::SdkError(e.to_string()),
        })?;

    Ok(presigned_req.uri().to_string())
}

/// Helper to download a file using a presigned URL, taking care of transparent
/// compression.
pub async fn download_file_presigned(
    client: &HttpClient,
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
            file.flush().await?;
        }
    }
    Ok(())
}
