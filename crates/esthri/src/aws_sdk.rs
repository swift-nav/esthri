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

use std::{
    collections::HashMap,
    convert::TryFrom,
    pin::Pin,
    str::FromStr,
    task::{Context, Poll},
    time::SystemTime,
};

use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadOutput;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::operation::head_object::{HeadObjectError, HeadObjectOutput};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::Stream;

use crate::{Error, Result};

pub use aws_sdk_s3::types::StorageClass;
pub use aws_sdk_s3::{config::Region, Client};

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
        if hoo.delete_marker.unwrap_or_default() {
            return Ok(None);
        }
        let e_tag = hoo
            .e_tag
            .ok_or_else(|| Error::HeadObjectUnexpected("no e_tag found".into()))?;
        let last_modified: DateTime<Utc> = {
            // convert the s3 date time string into chrono UTC format. FWIW the s3 date time string
            // is not actually guaranteed to be in UTC.
            let last_modified = hoo
                .last_modified
                .ok_or_else(|| Error::HeadObjectUnexpected("no last_modified found".into()))?;

            SystemTime::try_from(last_modified)
                .map(DateTime::from)
                .map_err(|_| {
                    Error::HeadObjectUnexpected(
                        "cannot convert last_modified to chrono time format".into(),
                    )
                })?
        };

        let size = hoo.content_length.unwrap_or_default();

        let metadata = hoo
            .metadata
            .ok_or_else(|| Error::HeadObjectUnexpected("no metadata found".into()))?;
        let storage_class = StorageClass::from_str(
            hoo.storage_class
                .unwrap_or_else(|| "STANDARD".into()) // AWS doesn't set header for STANDARD
                .as_str(),
        )
        .map_err(|e| Error::UnknownStorageClass(e.to_string()))?;

        // parts_count = 0 means it is not a multipart upload, default parts count to 1
        let parts = match hoo.parts_count {
            Some(0) | None => 1,
            Some(x) if x < 0 => {
                return Err(Error::HeadObjectUnexpected(
                    "parts_count is negative".into(),
                ))
            }
            Some(x) => x as u64,
        };

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
        Err(SdkError::ServiceError(error)) => match error.err() {
            HeadObjectError::NotFound(_) => Ok(None),
            _ => Err(Error::HeadObjectFailure {
                prefix: key.to_string(),
                source: Box::new(error.into_err()),
            }),
        },
        Err(error) => Err(Error::SdkError(error.to_string())),
    }
}

/// The data returned from a get object request
#[derive(Debug)]
pub struct GetObjectResponse {
    pub stream: ByteStream,
    pub size: i64,
    pub part: i64,
}
/// The AWS SDK removed future stream implementation of a Byte Stream Wrapper,
/// so we implement it here
struct FutureStreamByteStreamWrapper(ByteStream);

impl FutureStreamByteStreamWrapper {
    fn new(stream: ByteStream) -> Self {
        Self(stream)
    }
}

impl Stream for FutureStreamByteStreamWrapper {
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.0)
            .poll_next(cx)
            .map(|opt| opt.map(|res| res.map_err(|e| Error::ByteStreamError(e.to_string()))))
    }
}
impl GetObjectResponse {
    pub fn into_stream(self) -> impl Stream<Item = Result<Bytes>> {
        FutureStreamByteStreamWrapper::new(self.stream)
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
        .map_err(|e| match e {
            SdkError::ServiceError(error) => Error::GetObjectFailed(Box::new(error.into_err())),
            _ => Error::SdkError(e.to_string()),
        })?;
    log::debug!("got part={} bucket={} key={}", part, bucket, key);
    Ok(GetObjectResponse {
        stream: goo.body,
        size: goo.content_length.unwrap_or_default(),
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
        .map_err(|e| match e {
            SdkError::ServiceError(error) => Error::GetObjectFailed(Box::new(error.into_err())),
            _ => Error::SdkError(e.to_string()),
        })
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
        Err(err) => match err {
            SdkError::ServiceError(error) => Err(Error::CompletedMultipartUploadFailed(Box::new(
                error.into_err(),
            ))),
            _ => Err(Error::SdkError(err.to_string())),
        },
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
        .map_err(|e| match e {
            SdkError::ServiceError(error) => {
                Error::CreateMultipartUploadFailed(Box::new(error.into_err()))
            }
            _ => Error::SdkError(e.to_string()),
        })
}

pub async fn get_bucket_location(s3: &Client, bucket: &str) -> Result<String> {
    let region = s3
        .get_bucket_location()
        .bucket(bucket)
        .send()
        .await
        .map_err(|e| match e {
            SdkError::ServiceError(error) => {
                Error::GetBucketLocationFailed(Box::new(error.into_err()))
            }
            _ => Error::SdkError(e.to_string()),
        })?
        .location_constraint
        .ok_or(Error::LocationConstraintNone)?
        .as_str()
        .to_string();
    log::debug!("got region={:?}", region);
    Ok(region)
}
