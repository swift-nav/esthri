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

use std::{
    os::unix::prelude::MetadataExt,
    path::{Path, PathBuf},
    time::Duration,
};

use esthri_internals::{
    hyper::HeaderMap,
    rusoto::{
        util::{PreSignedRequest, PreSignedRequestOption},
        AwsCredentials, CompletedPart, DeleteObjectRequest, GetObjectRequest, PutObjectRequest,
        Region, S3Client, UploadPartRequest,
    },
};
use reqwest::{Body, Client};
use serde::{Deserialize, Serialize};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
};
use tokio_util::codec::{BytesCodec, FramedRead};

use crate::Result;
use crate::{
    opts::EsthriPutOptParams,
    rusoto::{complete_multipart_upload, create_multipart_upload},
};

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
        bucket: bucket.as_ref().to_owned(),
        key: key.as_ref().to_owned(),
        ..Default::default()
    }
    .get_presigned_url(region, credentials, &options)
}

pub async fn download_file_presigned(
    client: &Client,
    presigned_url: &str,
    filepath: &Path,
) -> Result<()> {
    let mut file = File::create(filepath).await?;
    let mut resp = client.get(presigned_url).send().await?.error_for_status()?;
    while let Some(chunk) = resp.chunk().await? {
        file.write_all(&chunk).await?;
    }
    Ok(())
}

pub fn presign_put(
    credentials: &AwsCredentials,
    region: &Region,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    expiration: Option<Duration>,
    opts: EsthriPutOptParams,
) -> String {
    let options = PreSignedRequestOption {
        expires_in: expiration.unwrap_or(DEAFULT_EXPIRATION),
    };
    PutObjectRequest {
        bucket: bucket.as_ref().to_owned(),
        key: key.as_ref().to_owned(),
        storage_class: opts.storage_class.map(|s| s.to_string()),
        acl: Some("bucket-owner-full-control".into()),
        ..Default::default()
    }
    .get_presigned_url(region, credentials, &options)
}

pub async fn upload_file_presigned(
    client: &Client,
    presigned_url: &str,
    filepath: &PathBuf,
    opts: EsthriPutOptParams,
) -> Result<()> {
    let file = File::open(filepath).await?;
    let file_size = file.metadata().await?.size();
    let stream = FramedRead::new(file, BytesCodec::new());
    let body = Body::wrap_stream(stream);
    let mut headers = HeaderMap::new();
    headers.insert("Content-Length", file_size.into());
    headers.insert("x-amz-acl", "bucket-owner-full-control".parse().unwrap());
    if let Some(class) = opts.storage_class {
        headers.insert("x-amz-storage-class", class.to_string().parse().unwrap());
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

pub async fn delete_file_presigned(client: &Client, presigned_url: impl AsRef<str>) -> Result<()> {
    client
        .delete(presigned_url.as_ref())
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PresignedMultipartUpload {
    pub upload_id: String,
    #[serde(with = "tuple_vec_map")]
    pub parts: Vec<(usize, String)>,
}

/// Set up a presigned multipart upload.
/// This function should be used on the Server.
///
/// 1. Client tells server it wants to upload a file of a given size.
/// 2. Server creates a multipart upload.
/// 3: Server generates a presigned URL for each part and returns urls to client.
/// 4. Client uploads each part to the corresponding presigned URL.
/// 5. Client tells server it has uploaded all parts.
/// 6. Server completes the multipart upload.
/// Note the server should fail the multipart upload after a timeout.
pub async fn setup_presigned_multipart_upload(
    client: &S3Client,
    credentials: &AwsCredentials,
    region: &Region,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    part_size: usize,
    file_size: usize,
    expiration: Option<Duration>,
    opts: EsthriPutOptParams,
) -> Result<PresignedMultipartUpload> {
    assert!(part_size >= 5242880);
    let upload_id = create_multipart_upload(
        client,
        bucket.as_ref(),
        key.as_ref(),
        None,
        opts.storage_class.unwrap(),
    )
    .await?
    .upload_id
    .unwrap();
    let parts = (1..n_parts(file_size, part_size) + 1)
        .map(|part| {
            (
                part,
                presign_multipart_upload(
                    credentials,
                    region,
                    bucket.as_ref(),
                    key.as_ref(),
                    part as i64 + 1,
                    upload_id.clone(),
                    expiration,
                ),
            )
        })
        .collect();
    Ok(PresignedMultipartUpload { upload_id, parts })
}

fn presign_multipart_upload(
    credentials: &AwsCredentials,
    region: &Region,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    part: i64,
    upload_id: String,
    expiration: Option<Duration>,
) -> String {
    let options = PreSignedRequestOption {
        expires_in: expiration.unwrap_or(DEAFULT_EXPIRATION),
    };
    UploadPartRequest {
        bucket: bucket.as_ref().to_owned(),
        key: key.as_ref().to_owned(),
        part_number: part,
        upload_id,
        ..Default::default()
    }
    .get_presigned_url(region, credentials, &options)
}

pub async fn upload_file_presigned_multipart_upload(
    client: &Client,
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

pub async fn complete_presigned_multipart_upload(
    s3: &S3Client,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    presigned_multipart_upload: PresignedMultipartUpload,
) -> Result<()> {
    let parts: Vec<_> = presigned_multipart_upload
        .parts
        .into_iter()
        .map(|(part, etag)| CompletedPart {
            e_tag: Some(etag),
            part_number: Some(part as i64 + 1),
        })
        .collect();
    complete_multipart_upload(
        s3,
        bucket.as_ref(),
        key.as_ref(),
        presigned_multipart_upload.upload_id.as_ref(),
        &parts,
    )
    .await
}

fn n_parts(file_size: usize, chunk_size: usize) -> usize {
    let mut n = file_size / chunk_size;
    if file_size % chunk_size != 0 {
        n += 1;
    }
    n
}
