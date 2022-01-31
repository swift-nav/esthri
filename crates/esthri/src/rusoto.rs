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

use std::collections::HashMap;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::Stream;

use crate::{retry::handle_dispatch_error, Error, Result};

pub use rusoto_core::{ByteStream, HttpClient, Region, RusotoError, RusotoResult};
pub use rusoto_credential::DefaultCredentialsProvider;
pub use rusoto_s3::{
    AbortMultipartUploadRequest, CompleteMultipartUploadRequest, CompletedMultipartUpload,
    CompletedPart, CopyObjectOutput, CopyObjectRequest, CreateMultipartUploadRequest,
    GetObjectError, GetObjectOutput, GetObjectRequest, HeadObjectOutput, HeadObjectRequest,
    ListObjectsV2Request, PutObjectRequest, S3Client, StreamingBody, UploadPartRequest, S3,
};

/// The data returned from a head object request
#[derive(Debug)]
pub struct HeadObjectInfo {
    pub e_tag: String,
    pub size: i64,
    pub last_modified: DateTime<Utc>,
    pub metadata: HashMap<String, String>,
    pub(crate) parts: u64,
}

impl HeadObjectInfo {
    pub fn is_esthri_compressed(&self) -> bool {
        self.metadata
            .contains_key(crate::compression::ESTHRI_METADATA_COMPRESS_KEY)
    }

    pub(crate) fn from_head_object_output(hoo: HeadObjectOutput) -> Result<Option<HeadObjectInfo>> {
        if let Some(true) = hoo.delete_marker {
            return Ok(None);
        }
        let e_tag = hoo
            .e_tag
            .ok_or_else(|| Error::HeadObjectUnexpected("no e_tag found".into()))?;
        let last_modified: DateTime<Utc> = hoo
            .last_modified
            .ok_or_else(|| Error::HeadObjectUnexpected("no last_modified found".into()))
            .map(|last| DateTime::parse_from_rfc2822(&last))??
            .into();
        let size = hoo
            .content_length
            .ok_or_else(|| Error::HeadObjectUnexpected("no content_length found".into()))?;
        let metadata = hoo
            .metadata
            .ok_or_else(|| Error::HeadObjectUnexpected("no metadata found".into()))?;
        let parts = hoo.parts_count.unwrap_or(1) as u64;
        Ok(Some(HeadObjectInfo {
            e_tag,
            size,
            last_modified,
            metadata,
            parts,
        }))
    }
}

pub async fn head_object_request<T>(
    s3: &T,
    bucket: &str,
    key: &str,
    part_number: Option<i64>,
) -> Result<Option<HeadObjectInfo>>
where
    T: S3,
{
    let res = handle_dispatch_error(|| async {
        s3.head_object(HeadObjectRequest {
            bucket: bucket.into(),
            key: key.into(),
            part_number,
            ..Default::default()
        })
        .await
    })
    .await;
    match res {
        Ok(hoo) => HeadObjectInfo::from_head_object_output(hoo),
        Err(RusotoError::Unknown(err)) if err.status == 404 => Ok(None),
        Err(err) => Err(Error::HeadObjectFailure(err)),
    }
}

/// The data returned from a get object request
#[derive(Debug)]
pub struct GetObjectResponse {
    pub stream: ByteStream,
    pub size: i64,
    pub part: i64,
}

impl GetObjectResponse {
    pub fn into_stream(self) -> impl Stream<Item = Result<Bytes>> {
        futures::TryStreamExt::map_err(self.stream, Error::IoError)
    }
}

pub async fn get_object_part_request<T>(
    s3: &T,
    bucket: &str,
    key: &str,
    part: i64,
) -> Result<GetObjectResponse>
where
    T: S3,
{
    log::debug!("get part={} bucket={} key={}", part, bucket, key);
    let goo = handle_dispatch_error(|| {
        s3.get_object(GetObjectRequest {
            bucket: bucket.into(),
            key: key.into(),
            part_number: Some(part),
            ..Default::default()
        })
    })
    .await
    .map_err(Error::GetObjectFailed)?;
    log::debug!("got part={} bucket={} key={}", part, bucket, key);
    Ok(GetObjectResponse {
        stream: goo.body.ok_or(Error::GetObjectOutputBodyNone)?,
        size: goo.content_length.ok_or(Error::GetObjectOutputBodyNone)?,
        part,
    })
}

pub async fn get_object_request<T>(
    s3: &T,
    bucket: &str,
    key: &str,
    range: Option<String>,
) -> Result<GetObjectOutput>
where
    T: S3,
{
    handle_dispatch_error(|| {
        s3.get_object(GetObjectRequest {
            bucket: bucket.into(),
            key: key.into(),
            range: range.clone(),
            ..Default::default()
        })
    })
    .await
    .map_err(Error::GetObjectFailed)
}

pub async fn complete_multipart_upload<T>(
    s3: &T,
    bucket: &str,
    key: &str,
    upload_id: &str,
    completed_parts: &[CompletedPart],
) -> Result<()>
where
    T: S3,
{
    handle_dispatch_error(|| async {
        let cmur = CompleteMultipartUploadRequest {
            bucket: bucket.into(),
            key: key.into(),
            upload_id: upload_id.into(),
            multipart_upload: Some(CompletedMultipartUpload {
                parts: Some(completed_parts.to_vec()),
            }),
            ..Default::default()
        };
        s3.complete_multipart_upload(cmur).await
    })
    .await
    .map_err(Error::CompletedMultipartUploadFailed)?;
    Ok(())
}

pub async fn create_multipart_upload<T>(
    s3: &T,
    bucket: &str,
    key: &str,
    metadata: Option<HashMap<String, String>>,
) -> Result<rusoto_s3::CreateMultipartUploadOutput>
where
    T: S3,
{
    handle_dispatch_error(|| async {
        let cmur = CreateMultipartUploadRequest {
            bucket: bucket.into(),
            key: key.into(),
            acl: Some("bucket-owner-full-control".into()),
            metadata: metadata.as_ref().cloned(),
            ..Default::default()
        };

        s3.create_multipart_upload(cmur).await
    })
    .await
    .map_err(Error::CreateMultipartUploadFailed)
}