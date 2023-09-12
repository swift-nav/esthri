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

//! Functions for presigning multipart upload requests.
//! These can be used to allow a client to upload a large file over 100Mb to S3
//! without giving them complete access.
//!
//! 1. Client requests a multipart upload and tells the server how many parts it
//!    requires.  Note that the minimum size for a part is 5Mb.
//! 2. Server calls `setup_presigned_multipart_upload` and sends resulting
//!    `PresignedMultipartUpload` to client.  This contains the upload_id as
//!    well as a presigned url for each part.
//! 3. Client calls `upload_file_presigned_multipart_upload` to upload the file.
//!    This returns a different `PresignedMultipartUpload` struct which now
//!    needs to be sent back to the server.
//! 4. Server calls `complete_presigned_multipart_upload` to complete the upload.
//! 5. If the client disapears after 2, the server should call
//!    `abort_presigned_multipart_upload` to abort the upload.
use std::{path::PathBuf, time::Duration};

use crate::{
    aws_sdk::{complete_multipart_upload, create_multipart_upload},
    opts::EsthriPutOptParams,
    presign::n_parts,
    Error, PendingUpload, Result,
};
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::{presigning::PresigningConfig, types::CompletedPart};
use reqwest::Client as HttpClient;
use serde::{Deserialize, Serialize};
use tokio::{
    fs::File,
    io::{AsyncReadExt, BufReader},
};

use super::DEAFULT_EXPIRATION;

#[derive(Debug, Serialize, Deserialize)]
pub struct PresignedMultipartUpload {
    pub upload_id: String,
    #[serde(with = "tuple_vec_map")]
    pub parts: Vec<(usize, String)>,
}

/// Begin a multipart upload and presign the urls for each part.
#[allow(clippy::too_many_arguments)]
pub async fn setup_presigned_multipart_upload(
    s3: &S3Client,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    n_parts: usize,
    expiration: Option<Duration>,
    opts: EsthriPutOptParams,
) -> Result<PresignedMultipartUpload> {
    let upload_id = create_multipart_upload(
        s3,
        bucket.as_ref(),
        key.as_ref(),
        None,
        opts.storage_class.unwrap(),
    )
    .await?
    .upload_id
    .unwrap();

    let parts = (1..n_parts + 1)
        .map(|part| async {
            (
                part,
                presign_multipart_upload(
                    s3,
                    bucket.as_ref(),
                    key.as_ref(),
                    part as i64,
                    upload_id.clone(),
                    expiration,
                )
                .await
                .unwrap(),
            )
        })
        .collect();
    Ok(PresignedMultipartUpload { upload_id, parts })
}

/// Upload a file using a presigned multipart upload.
pub async fn upload_file_presigned_multipart_upload(
    client: &HttpClient,
    presigned_multipart_upload: PresignedMultipartUpload,
    file: &PathBuf,
    part_size: usize,
) -> Result<PresignedMultipartUpload> {
    let file = File::open(file).await?;
    assert!(
        presigned_multipart_upload.parts.len()
            == n_parts(file.metadata().await?.len() as usize, part_size)
    );
    let mut reader = BufReader::with_capacity(part_size, file);

    let mut upload = PresignedMultipartUpload {
        upload_id: presigned_multipart_upload.upload_id,
        parts: Vec::new(),
    };

    for (n, url) in presigned_multipart_upload.parts.iter() {
        let mut buf = vec![0; part_size];
        let n_read = reader.read_exact(&mut buf).await?;
        buf.shrink_to(n_read);
        let e_tag = client
            .put(url)
            .header("Content-Length", n_read.to_string())
            .body(buf)
            .send()
            .await?
            .error_for_status()?
            .headers()
            .get("ETag")
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        upload.parts.push((*n, e_tag));
    }

    Ok(upload)
}

/// Complete a multipart upload.
pub async fn complete_presigned_multipart_upload(
    s3: &S3Client,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    presigned_multipart_upload: PresignedMultipartUpload,
) -> Result<()> {
    let parts: Vec<_> = presigned_multipart_upload
        .parts
        .into_iter()
        .map(|(part, etag)| {
            CompletedPart::builder()
                .e_tag(etag)
                .part_number(part as i32)
                .build()
        })
        .collect();
    complete_multipart_upload(
        s3,
        bucket.as_ref(),
        key.as_ref(),
        presigned_multipart_upload.upload_id.as_ref(),
        parts.as_slice(),
    )
    .await
}

/// Abort a multipart upload.
pub async fn abort_presigned_multipart_upload(
    s3: &S3Client,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    upload_id: impl AsRef<str>,
) -> Result<()> {
    PendingUpload::new(bucket.as_ref(), key.as_ref(), upload_id.as_ref())
        .abort(s3)
        .await
}

async fn presign_multipart_upload(
    s3: &S3Client,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    part: i64,
    upload_id: String,
    expiration: Option<Duration>,
) -> Result<String> {
    let presigning_config = PresigningConfig::builder()
        .expires_in(expiration.unwrap_or(DEAFULT_EXPIRATION))
        .build()
        .map_err(Error::PresigningConfigError)?;

    let presigned_req = s3
        .upload_part()
        .bucket(bucket.as_ref().to_string())
        .key(key.as_ref().to_string())
        .part_number(part as i32)
        .upload_id(upload_id)
        .presigned(presigning_config)
        .await
        .map_err(|e| Error::UploadPartFailed(e.to_string()))?;

    Ok(presigned_req.uri().to_string())
}
