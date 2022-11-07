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

use std::{collections::HashMap, path::PathBuf, time::Duration};

use esthri_internals::rusoto::{
    util::{PreSignedRequest, PreSignedRequestOption},
    AwsCredentials, DeleteObjectRequest, GetObjectRequest, PutObjectRequest, Region, S3Client,
    UploadPartRequest,
};
use reqwest::Client;
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, AsyncReadExt, BufReader},
};

use crate::rusoto::{create_multipart_upload, S3StorageClass};
use crate::Result;

pub const DEAFULT_EXPIRATION: Duration = Duration::from_secs(60 * 60);

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
        bucket: bucket.as_ref().to_string(),
        key: key.as_ref().to_string(),
        ..Default::default()
    }
    .get_presigned_url(region, credentials, &options)
}

pub fn presign_put(
    credentials: &AwsCredentials,
    region: &Region,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    expiration: Option<Duration>,
) -> String {
    let options = PreSignedRequestOption {
        expires_in: expiration.unwrap_or(DEAFULT_EXPIRATION),
    };
    PutObjectRequest {
        bucket: bucket.as_ref().to_string(),
        key: key.as_ref().to_string(),
        ..Default::default()
    }
    .get_presigned_url(region, credentials, &options)
}

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
        bucket: bucket.as_ref().to_string(),
        key: key.as_ref().to_string(),
        ..Default::default()
    }
    .get_presigned_url(region, credentials, &options)
}

/// Set up a presigned multipart upload.
/// This function should be used on the Server.
///
/// 1. Client tells server it wants to upload a file of a given size.
/// 2. Server creates a multipart upload.
/// 3: Server generates a presigned URL for each part and returns urls to client.
/// 4. Client uploads each part to the presigned URL.
/// 5. Client tells server it has uploaded all parts.
/// 6. Server completes the multipart upload.
/// 7. Server needs to fail the multipart upload after a timeout.
pub async fn setup_presigned_multipart_upload(
    client: &S3Client,
    credentials: &AwsCredentials,
    region: &Region,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    part_size: usize,
    file_size: usize,
    expiration: Option<Duration>,
    metadata: Option<HashMap<String, String>>,
    storage_class: S3StorageClass,
) -> Result<(Option<String>, Map<usize, String>)> {
    let upload = create_multipart_upload(
        client,
        bucket.as_ref(),
        key.as_ref(),
        metadata,
        storage_class,
    )
    .await?;
    let urls = (0..n_parts(file_size, part_size))
        .map(|part| {
            (
                part,
                presign_multipart_upload(
                    credentials,
                    region,
                    bucket.as_ref(),
                    key.as_ref(),
                    part as i64 + 1,
                    expiration,
                ),
            )
        })
        .collect();
    Ok((upload.upload_id, urls))
}

pub fn presign_multipart_upload(
    credentials: &AwsCredentials,
    region: &Region,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    part: i64,
    expiration: Option<Duration>,
) -> String {
    let options = PreSignedRequestOption {
        expires_in: expiration.unwrap_or(DEAFULT_EXPIRATION),
    };
    UploadPartRequest {
        bucket: bucket.as_ref().to_string(),
        key: key.as_ref().to_string(),
        part_number: part,
        ..Default::default()
    }
    .get_presigned_url(region, credentials, &options)
}

pub async fn upload_file_presigned_multipart_upload(
    client: &Client,
    urls: HashMap<usize, String>,
    file: &PathBuf,
    part_size: usize,
) -> Result<()> {
    let file = File::open(file).await?;
    assert!(urls.len() == n_parts(file.metadata().await?.len() as usize, part_size));
    let mut reader = BufReader::with_capacity(part_size, file);

    for url in urls {
        let mut buf = vec![0u8; part_size];
        let bytes_read = reader.read_exact(&mut buf).await?;
        client
            .put(url)
            .header("Content-Length", bytes_read.to_string())
            .body(buf)
            .send()
            .await?;
    }
    Ok(())
}

pub async fn complete_presigned_multipart_upload(
    s3: &S3Client,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    upload_id: impl AsRef<str>,
) {
    complete_multipart_upload(s3, bucket, key, &upload_id, &completed_parts).await?
}

fn n_parts(file_size: usize, chunk_size: usize) -> usize {
    let mut n = file_size / chunk_size;
    if file_size % chunk_size != 0 {
        n += 1;
    }
    n
}
