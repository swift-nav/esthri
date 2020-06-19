use std::io::prelude::*;

use rusoto_s3::S3;

use super::types::SyncDirection;
use super::Result;

#[tokio::main]
pub async fn s3_head_object(s3: &dyn S3, bucket: &str, key: &str) -> Result<Option<String>> {
    super::s3_head_object(s3, bucket, key).await
}

#[tokio::main]
pub async fn s3_abort_upload(s3: &dyn S3, bucket: &str, key: &str, upload_id: &str) -> Result<()> {
    super::s3_abort_upload(s3, bucket, key, upload_id).await
}

#[tokio::main]
pub async fn s3_upload(s3: &dyn S3, bucket: &str, key: &str, file: &str) -> Result<()> {
    super::s3_upload(s3, bucket, key, file).await
}

#[tokio::main]
pub async fn s3_upload_from_reader(
    s3: &dyn S3,
    bucket: &str,
    key: &str,
    reader: &mut dyn Read,
    file_size: u64,
) -> Result<()> {
    super::s3_upload_from_reader(s3, bucket, key, reader, file_size).await
}

#[tokio::main]
pub async fn s3_download(s3: &dyn S3, bucket: &str, key: &str, file: &str) -> Result<()> {
    super::s3_download(s3, bucket, key, file).await
}

#[tokio::main]
pub async fn s3_sync(
    s3: &dyn S3,
    direction: SyncDirection,
    bucket: &str,
    key: &str,
    directory: &str,
    includes: &Option<Vec<String>>,
    excludes: &Option<Vec<String>>,
) -> Result<()> {
    super::s3_sync(s3, direction, bucket, key, directory, includes, excludes).await
}

#[tokio::main]
pub async fn s3_list_objects(s3: &dyn S3, bucket: &str, key: &str) -> Result<Vec<String>> {
    super::s3_list_objects(s3, bucket, key).await
}
