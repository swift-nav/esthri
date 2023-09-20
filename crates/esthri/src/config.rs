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

//! Configuration module for the library, allows sizing of internal concurrent task counts,
//! multipart upload sizes and read buffer sizes, among other things.

use std::{path::PathBuf, str::FromStr};

use aws_sdk_s3::types::StorageClass as S3StorageClass;
use once_cell::sync::OnceCell;
use serde::{de, Deserialize, Deserializer};

/// The default size of parts in a multipart upload to S3.  8 MiB is the default chunk
/// size from awscli, changing this size will affect the calculation of ETags.
pub const UPLOAD_PART_SIZE: u64 = 8 * 1024 * 1024;
/// The default amount of data to read out of a file at a time.
pub const UPLOAD_READ_SIZE: u64 = 1024 * 1024;
/// The default number of concurrent tasks run when sending data to S3.
pub const CONCURRENT_UPLOAD_TASKS: u16 = 8;
/// The default number of concurrent tasks run when receiving data from S3.  Each task
/// represents a connection to S3.
pub const CONCURRENT_DOWNLOADER_TASKS: u16 = 8;
/// The default number of concurrent tasks run when running a sync operation
pub const CONCURRENT_SYNC_TASKS: u16 = 4;
/// The default number of times to retry a request if it fails with a transient error.
pub const REQUEST_RETRIES: u16 = 5;
/// The default number of concurrent tasks run when writing download data to disk.
pub const CONCURRENT_WRITER_TASKS: u16 = 1;
/// Holds configuration information for the library.
#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(default, deserialize_with = "deserialize_storage_class")]
    storage_class: StorageClass,
    #[serde(default)]
    upload_part_size: UploadPartSize,
    #[serde(default)]
    upload_read_size: UploadReadSize,
    #[serde(default)]
    concurrent_upload_tasks: ConcurrentUploadTasks,
    #[serde(default)]
    concurrent_downloader_tasks: ConcurrentDownloaderTasks,
    #[serde(default)]
    concurrent_sync_tasks: ConcurrentSyncTasks,
    #[serde(default)]
    request_retries: RequestRetries,
    #[serde(default)]
    concurrent_writer_tasks: ConcurrentWriterTasks,
    #[serde(default)]
    temp_dir_path: TempDirPath,
}

/// Wrapper type for [TEMP_DIR_PATH] which allows [Config::temp_dir_path()] to bind a default
/// value.
#[derive(Debug, Deserialize, Default)]
#[serde(transparent)]
struct TempDirPath(Option<PathBuf>);

/// Wrapper type for [STORAGE_CLASS] which allows [Config::storage_class()] to bind a default
/// value.
#[derive(Debug)]
struct StorageClass(S3StorageClass);

impl Default for StorageClass {
    fn default() -> Self {
        StorageClass(S3StorageClass::Standard)
    }
}

fn deserialize_storage_class<'de, D>(deserializer: D) -> Result<StorageClass, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    S3StorageClass::from_str(&s.to_uppercase())
        .map(StorageClass)
        .map_err(de::Error::custom)
}

/// Wrapper type for [UPLOAD_PART_SIZE] which allows [Config::upload_part_size()] to bind a default
/// value.
#[derive(Debug, Deserialize)]
#[serde(transparent)]
struct UploadPartSize(u64);

impl Default for UploadPartSize {
    fn default() -> Self {
        UploadPartSize(UPLOAD_PART_SIZE)
    }
}

/// Wrapper type for [UPLOAD_READ_SIZE] which allows [Config::upload_read_size()] to bind a default
/// value.
#[derive(Debug, Deserialize)]
#[serde(transparent)]
struct UploadReadSize(u64);

impl Default for UploadReadSize {
    fn default() -> Self {
        UploadReadSize(UPLOAD_READ_SIZE)
    }
}

/// Wrapper type for [CONCURRENT_UPLOAD_TASKS] which allows [Config::concurrent_upload_tasks()] to
/// bind a default value.
#[derive(Debug, Deserialize)]
#[serde(transparent)]
struct ConcurrentUploadTasks(u16);

impl Default for ConcurrentUploadTasks {
    fn default() -> Self {
        ConcurrentUploadTasks(CONCURRENT_UPLOAD_TASKS)
    }
}

/// Wrapper type for [CONCURRENT_DOWNLOADER_TASKS] which allows
/// [Config::concurrent_downloader_tasks()] to bind a default value.
#[derive(Debug, Deserialize)]
#[serde(transparent)]
struct ConcurrentDownloaderTasks(u16);

impl Default for ConcurrentDownloaderTasks {
    fn default() -> Self {
        ConcurrentDownloaderTasks(CONCURRENT_DOWNLOADER_TASKS)
    }
}

/// Wrapper type for [CONCURRENT_SYNC_TASKS] which allows [Config::concurrent_sync_tasks()] to bind
/// a default value.
#[derive(Debug, Deserialize)]
#[serde(transparent)]
struct ConcurrentSyncTasks(u16);

impl Default for ConcurrentSyncTasks {
    fn default() -> Self {
        ConcurrentSyncTasks(CONCURRENT_SYNC_TASKS)
    }
}

/// Wrapper type for [REQUEST_RETRIES] which allows [Config::request_retries()] to bind
/// a default value.
#[derive(Debug, Deserialize)]
#[serde(transparent)]
struct RequestRetries(u16);

impl Default for RequestRetries {
    fn default() -> Self {
        RequestRetries(REQUEST_RETRIES)
    }
}

/// Wrapper type for [CONCURRENT_WRITER_TASKS] which allows [Config::concurrent_writer_tasks()] to
/// bind a default value.
#[derive(Debug, Deserialize)]
#[serde(transparent)]
struct ConcurrentWriterTasks(u16);

impl Default for ConcurrentWriterTasks {
    fn default() -> Self {
        ConcurrentWriterTasks(CONCURRENT_WRITER_TASKS)
    }
}

static CONFIG: OnceCell<Config> = OnceCell::new();

const EXPECT_GLOBAL_CONFIG: &str = "failed to parse config from environment";

impl Config {
    /// Fetches the global config object, values are either defaulted or populated
    /// from the environment:
    ///
    /// - `ESTHRI_STORAGE_CLASS` - [Config::storage_class()]
    /// - `ESTHRI_UPLOAD_PART_SIZE` - [Config::upload_part_size()]
    /// - `ESTHRI_UPLOAD_READ_SIZE` - [Config::upload_read_size()]
    /// - `ESTHRI_CONCURRENT_UPLOAD_TASKS` - [Config::concurrent_upload_tasks()]
    /// - `ESTHRI_CONCURRENT_DOWNLOADER_TASKS` - [Config::concurrent_downloader_tasks()]
    /// - `ESTHRI_CONCURRENT_SYNC_TASKS` - [Config::concurrent_sync_tasks()]
    /// - `ESTHRI_REQUEST_RETRIES` - [Config::request_retries()]
    /// - `ESTHRI_CONCURRENT_WRITER_TASKS` - [Config::concurrent_writer_tasks(
    pub fn global() -> &'static Config {
        CONFIG.get_or_init(|| {
            envy::prefixed("ESTHRI_")
                .from_env::<Config>()
                .expect(EXPECT_GLOBAL_CONFIG)
        })
    }

    /// The default storage class for put requests to S3. STANDARD is the default storage class
    /// unless specified. View AWS documentation for more specifications on other storage classes.
    /// Defaults to [STORAGE_CLASS].
    pub fn storage_class(&self) -> S3StorageClass {
        self.storage_class.0.clone()
    }

    /// The default path for new temp files. Uses to current directory if unset. Defaults to
    /// [TEMP_DIR_PATH].
    pub fn temp_dir_path(&self) -> Option<PathBuf> {
        self.temp_dir_path.0.clone()
    }

    /// The default size of parts in a multipart upload to S3.  8 MiB is the default chunk size
    /// from awscli, changing this size will affect the calculation of ETags.  Defaults to
    /// [UPLOAD_PART_SIZE].
    pub fn upload_part_size(&self) -> u64 {
        self.upload_part_size.0
    }

    /// The amount of data to read at a time from files being uploaded. This is set separately
    /// to [UPLOAD_PART_SIZE] to lower memory usage, as having lots of 8MiB chunks in flight
    /// can cause high memory usage. Defaults to [UPLOAD_READ_SIZE].
    pub fn upload_read_size(&self) -> u64 {
        self.upload_read_size.0
    }

    /// The number of concurrent tasks run when sending data to S3.  Defaults to
    /// [CONCURRENT_UPLOAD_TASKS].
    pub fn concurrent_upload_tasks(&self) -> usize {
        self.concurrent_upload_tasks.0 as usize
    }

    /// The number of concurrent tasks run when receiving data from S3.  Each task represents a
    /// connection to S3.  Defaults to [CONCURRENT_DOWNLOADER_TASKS].
    pub fn concurrent_downloader_tasks(&self) -> usize {
        self.concurrent_downloader_tasks.0 as usize
    }

    /// The number of concurrent tasks run when running a sync operation.  Defaults to
    /// [CONCURRENT_SYNC_TASKS].
    pub fn concurrent_sync_tasks(&self) -> usize {
        self.concurrent_sync_tasks.0 as usize
    }

    /// The number of times to retry a request before giving up.  Defaults to
    /// [REQUEST_RETRIES].
    pub fn request_retries(&self) -> usize {
        self.request_retries.0 as usize
    }

    /// The number of concurrent tasks run when writing download data to disk.  Defaults to
    /// [CONCURRENT_WRITER_TASKS].
    pub fn concurrent_writer_tasks(&self) -> usize {
        self.concurrent_writer_tasks.0 as usize
    }
}
