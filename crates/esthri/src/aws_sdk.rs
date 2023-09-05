/*
 * Copyright (C) 2023 Swift Navigation Inc.
 * Contact: Swift Navigation <dev@swiftnav.com>
 *
 * This source is subject to the license found in the file 'LICENSE' which must
 * be be distributed together with this source. All other rights reserved.
 *
 * THIS CODE AND INFORMATION IS PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND,
 * EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A PARTICULAR PURPOSE.
 */

use std::{collections::HashMap, str::FromStr};

use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadOutput;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart, StorageClass};
use aws_sdk_s3::Client;
use aws_smithy_http::result::SdkError;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::Stream;

use crate::{retry::handle_dispatch_error, Error, Result};

/// The data returned from a head object request
#[derive(Debug)]
pub struct HeadObjectInfo {
    pub e_tag: String,
    pub size: i64,
    pub last_modified: DateTime<Utc>,
    pub metadata: HashMap<String, String>,
    pub storage_class: StorageClass,
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
        let storage_class = StorageClass::from_str(
            hoo.storage_class
                .unwrap_or_else(|| "Standard".into()) // AWS doesn't set header for STANDARD
                .as_str(),
        )
        .map_err(|e| Error::UnknownStorageClass(e.to_string()))?;
        let parts = hoo.parts_count.unwrap_or(1) as u64;
        Ok(Some(HeadObjectInfo {
            e_tag,
            size,
            last_modified,
            metadata,
            storage_class,
            parts,
        }))
    }
}

/// Fetches head object (metadata) of a S3 key
pub async fn head_object_request(
    s3: &Client,
    bucket: &str,
    key: &str,
    part_number: Option<i64>,
) -> Result<Option<HeadObjectOutput>> {
    let res =
        handle_dispatch_error(|| async { s3.head_object().bucket(bucket).key(key).send().await })
            .await;
    match res {
        Ok(hoo) => Ok(Some(hoo)),
        Err(SdkError::ServiceError(error)) => Err(Error::HeadObjectFailure(error.into_err())),
        Err(_) => Ok(None),
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

pub async fn get_object_part_request(
    s3: &Client,
    bucket: &str,
    key: &str,
    part: i64,
) -> Result<GetObjectResponse> {
    log::debug!("get part={} bucket={} key={}", part, bucket, key);
    let goo = handle_dispatch_error(|| async {
        s3.get_object()
            .bucket(bucket)
            .key(key)
            .part_number(part)
            .send()
            .await
    })
    .await
    .map_err(Error::GetObjectFailed)?;
    log::debug!("got part={} bucket={} key={}", part, bucket, key);
    Ok(GetObjectResponse {
        stream: goo.body,
        size: goo.content_length,
        part,
    })
}

pub async fn get_object_request(
    s3: &Client,
    bucket: &str,
    key: &str,
    range: Option<String>,
) -> Result<GetObjectOutput> {
    handle_dispatch_error(|| async { s3.get_object().bucket(bucket).key(key).send().await })
        .await
        .map_err(Error::GetObjectFailed)?
}

pub async fn complete_multipart_upload(
    s3: &Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    completed_parts: &[CompletedPart],
) -> Result<()> {
    handle_dispatch_error(|| async {
        s3.complete_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(CompletedMultipartUpload {
                parts: Some(completed_parts.to_vec()),
            })
            .await
    })
    .await
    .map_err(Error::CompletedMultipartUploadFailed)?;
    Ok(())
}

pub async fn create_multipart_upload<T>(
    s3: &Client,
    bucket: &str,
    key: &str,
    metadata: Option<HashMap<String, String>>,
    storage_class: StorageClass,
) -> Result<CreateMultipartUploadOutput> {
    handle_dispatch_error(|| async {
        s3.create_multipart_upload()
            .bucket(bucket)
            .key(key)
            .set_metadata(metadata)
            .storage_class(storage_class)
            .await
    })
    .await
    .map_err(Error::CreateMultipartUploadFailed)
}

pub async fn get_bucket_location(s3: &Client, bucket: &str) -> Result<String> {
    let region =
        handle_dispatch_error(|| async { s3.get_bucket_location().bucket(bucket).send().await })
            .await
            .map_err(Error::GetBucketLocationFailed)?
            .location_constraint
            .ok_or(Error::LocationConstraintNone)?;
    log::debug!("got region={}", region);
    Ok(region)
}
