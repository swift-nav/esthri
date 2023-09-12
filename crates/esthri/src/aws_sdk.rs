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
use aws_smithy_types_convert::date_time::DateTimeExt;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::Stream;

use crate::{Error, Result};

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
        if hoo.delete_marker {
            return Ok(None);
        }
        let e_tag = hoo
            .e_tag
            .ok_or_else(|| Error::HeadObjectUnexpected("no e_tag found".into()))?;
        let last_modified: DateTime<Utc> = hoo
            .last_modified
            .ok_or_else(|| Error::HeadObjectUnexpected("no last_modified found".into()))
            .map(|last| last.to_chrono_utc())?
            .map_err(|_| {
                Error::HeadObjectUnexpected(
                    "cannot convert last_modified to chrono time format".into(),
                )
            })?;

        let size = hoo.content_length;

        let metadata = hoo
            .metadata
            .ok_or_else(|| Error::HeadObjectUnexpected("no metadata found".into()))?;
        let storage_class = StorageClass::from_str(
            hoo.storage_class
                .unwrap_or_else(|| "Standard".into()) // AWS doesn't set header for STANDARD
                .as_str(),
        )
        .map_err(|e| Error::UnknownStorageClass(e.to_string()))?;
        let parts = hoo.parts_count as u64;

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
    part_number: Option<i32>,
) -> Result<Option<HeadObjectInfo>> {
    let res = s3
        .head_object()
        .bucket(bucket)
        .key(key)
        .set_part_number(part_number)
        .send()
        .await;

    match res {
        Ok(hoo) => {
            let info = HeadObjectInfo::from_head_object_output(hoo)?;
            Ok(info)
        }
        Err(SdkError::ServiceError(error)) => {
            Err(Error::HeadObjectFailure(error.into_err().to_string()))
        }
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
        futures::TryStreamExt::map_err(self.stream, |e| Error::ByteStreamError(e.to_string()))
    }
}

pub async fn get_object_part_request(
    s3: &Client,
    bucket: &str,
    key: &str,
    part: i64,
) -> Result<GetObjectResponse> {
    log::debug!("get part={} bucket={} key={}", part, bucket, key);
    let goo = s3
        .get_object()
        .bucket(bucket)
        .key(key)
        .part_number(part as i32)
        .send()
        .await
        .map_err(|e| Error::GetObjectFailed(e.to_string()))?;
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
    s3.get_object()
        .bucket(bucket)
        .key(key)
        .set_range(range)
        .send()
        .await
        .map_err(|e| Error::GetObjectFailed(e.to_string()))
}

pub async fn complete_multipart_upload(
    s3: &Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    completed_parts: &[CompletedPart],
) -> Result<()> {
    let completed_parts = CompletedMultipartUpload::builder()
        .set_parts(Some(completed_parts.to_vec()))
        .build();

    match s3
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(completed_parts)
        .send()
        .await
    {
        Ok(_) => Ok(()),
        Err(err) => Err(Error::CompletedMultipartUploadFailed(err.to_string())),
    }
}

pub async fn create_multipart_upload(
    s3: &Client,
    bucket: &str,
    key: &str,
    metadata: Option<HashMap<String, String>>,
    storage_class: StorageClass,
) -> Result<CreateMultipartUploadOutput> {
    s3.create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .set_metadata(metadata)
        .storage_class(storage_class)
        .send()
        .await
        .map_err(|e| Error::CreateMultipartUploadFailed(e.to_string()))
}

pub async fn get_bucket_location(s3: &Client, bucket: &str) -> Result<String> {
    let region = s3
        .get_bucket_location()
        .bucket(bucket)
        .send()
        .await
        .map_err(|e| Error::GetBucketLocationFailed(e.to_string()))?
        .location_constraint
        .ok_or(Error::LocationConstraintNone)?
        .as_str()
        .to_string();
    log::debug!("got region={:?}", region);
    Ok(region)
}
