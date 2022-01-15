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

#![cfg_attr(feature = "aggressive_lint", deny(warnings))]

//! Configuration module for the library, allows sizing of internal concurrent task counts,
//! multipart upload sizes and read buffer sizes, among other things.

use once_cell::sync::OnceCell;
use serde::Deserialize;

/// Default redis server url. Fallback if REDIS_SERVER_URL env var not found.
/// Expecting url to include `redis://` prefix.
pub const DEFAULT_REDIS_SERVER_URL: &str = "redis://127.0.0.1";
/// The multipart stream default size. 4 kiB is the default chunk.
pub const STREAM_PART_SIZE: usize = 4 * 1024;
/// The default size of parts in a multipart upload to S3.  8 MiB is the default chunk
/// size from awscli, changing this size will affect the calculation of ETags.
pub const UPLOAD_PART_SIZE: u64 = 8 * 1024 * 1024;
/// The default number of concurrent tasks run when sending data to S3.
pub const CONCURRENT_UPLOAD_TASKS: u16 = 16;
/// When downloading or receiving data from S3, this sizes a task's download buffer.
pub const DOWNLOAD_BUFFER_SIZE: usize = 8 * 1024 * 1024;
/// The default number of concurrent tasks run when receiving data from S3.  Each task
/// represents a connection to S3.
pub const CONCURRENT_DOWNLOADER_TASKS: u16 = 32;
/// The default number of concurrent tasks run when receiving compressed data
/// from S3.  Each task represents a connection to S3.
pub const CONCURRENT_COMPRESSED_DOWNLOADER_TASKS: u16 = 2;
/// The default number of concurrent tasks run when writing download data to disk.
pub const CONCURRENT_WRITER_TASKS: u16 = 64;
/// The default number of concurrent tasks run when running a sync operation
pub const CONCURRENT_SYNC_TASKS: u16 = 16;

/// Holds configuration information for the library.
#[derive(Deserialize)]
pub struct Config {
    #[serde(default)]
    default_redis_server_url: DefaultRedisServerUrl,
    #[serde(default)]
    stream_part_size: StreamPartSize,
    #[serde(default)]
    upload_part_size: UploadPartSize,
    #[serde(default)]
    concurrent_upload_tasks: ConcurrentUploadTasks,
    #[serde(default)]
    download_buffer_size: DownloadBufferSize,
    #[serde(default)]
    concurrent_downloader_tasks: ConcurrentDownloaderTasks,
    #[serde(default)]
    concurrent_compressed_downloader_tasks: ConcurrentCompressedDownloaderTasks,
    #[serde(default)]
    concurrent_writer_tasks: ConcurrentWriterTasks,
    #[serde(default)]
    concurrent_sync_tasks: ConcurrentSyncTasks,
}

/// Wrapper type for [DEFAULT_REDIS_SERVER_URL] which allows [Config::default_redis_server_url()] to bind a default
/// value.
#[derive(Deserialize)]
#[serde(transparent)]
struct DefaultRedisServerUrl(String);

impl Default for DefaultRedisServerUrl {
    fn default() -> Self {
        DefaultRedisServerUrl(DEFAULT_REDIS_SERVER_URL.to_string())
    }
}

/// Wrapper type for [STREAM_PART_SIZE] which allows [Config::stream_part_size()] to bind a default
/// value.
#[derive(Deserialize)]
#[serde(transparent)]
struct StreamPartSize(usize);

impl Default for StreamPartSize {
    fn default() -> Self {
        StreamPartSize(STREAM_PART_SIZE)
    }
}

/// Wrapper type for [UPLOAD_PART_SIZE] which allows [Config::upload_part_size()] to bind a default
/// value.
#[derive(Deserialize)]
#[serde(transparent)]
struct UploadPartSize(u64);

impl Default for UploadPartSize {
    fn default() -> Self {
        UploadPartSize(UPLOAD_PART_SIZE)
    }
}

/// Wrapper type for [CONCURRENT_UPLOAD_TASKS] which allows [Config::concurrent_upload_tasks()] to
/// bind a default value.
#[derive(Deserialize)]
#[serde(transparent)]
struct ConcurrentUploadTasks(u16);

impl Default for ConcurrentUploadTasks {
    fn default() -> Self {
        ConcurrentUploadTasks(CONCURRENT_UPLOAD_TASKS)
    }
}

/// Wrapper type for [CONCURRENT_DOWNLOADER_TASKS] which allows
/// [Config::concurrent_downloader_tasks()] to bind a default value.
#[derive(Deserialize)]
#[serde(transparent)]
struct ConcurrentDownloaderTasks(u16);

impl Default for ConcurrentDownloaderTasks {
    fn default() -> Self {
        ConcurrentDownloaderTasks(CONCURRENT_DOWNLOADER_TASKS)
    }
}

/// Wrapper type for [CONCURRENT_COMPRESSED_DOWNLOADER_TASKS] which allows
/// [Config::concurrent_compressed_downloader_tasks()] to bind a default value.
#[derive(Deserialize)]
#[serde(transparent)]
struct ConcurrentCompressedDownloaderTasks(u16);

impl Default for ConcurrentCompressedDownloaderTasks {
    fn default() -> Self {
        ConcurrentCompressedDownloaderTasks(CONCURRENT_COMPRESSED_DOWNLOADER_TASKS)
    }
}

/// Wrapper type for [DOWNLOAD_BUFFER_SIZE] which allows [Config::download_buffer_size()] to bind a
/// default value.
#[derive(Deserialize)]
#[serde(transparent)]
struct DownloadBufferSize(usize);

impl Default for DownloadBufferSize {
    fn default() -> Self {
        DownloadBufferSize(DOWNLOAD_BUFFER_SIZE)
    }
}

/// Wrapper type for [CONCURRENT_WRITER_TASKS] which allows [Config::concurrent_writer_tasks()] to
/// bind a default value.
#[derive(Deserialize)]
#[serde(transparent)]
struct ConcurrentWriterTasks(u16);

impl Default for ConcurrentWriterTasks {
    fn default() -> Self {
        ConcurrentWriterTasks(CONCURRENT_WRITER_TASKS)
    }
}

/// Wrapper type for [CONCURRENT_SYNC_TASKS] which allows [Config::concurrent_sync_tasks()] to bind
/// a default value.
#[derive(Deserialize)]
#[serde(transparent)]
struct ConcurrentSyncTasks(u16);

impl Default for ConcurrentSyncTasks {
    fn default() -> Self {
        ConcurrentSyncTasks(CONCURRENT_SYNC_TASKS)
    }
}

static CONFIG: OnceCell<Config> = OnceCell::new();

const EXPECT_GLOBAL_CONFIG: &str = "failed to parse config from environment";

impl Config {
    /// Fetches the global config object, values are either defaulted or populated
    /// from the environment:
    ///
    /// - `ESTHRI_STREAM_PART_SIZE` - [Config::stream_part_size()]
    /// - `ESTHRI_UPLOAD_PART_SIZE` - [Config::upload_part_size()]
    /// - `ESTHRI_CONCURRENT_DOWNLOADER_TASKS` - [Config::concurrent_downloader_tasks()]
    /// - `ESTHRI_CONCURRENT_COMPRESSED_DOWNLOADER_TASKS` - [Config::concurrent_compressed_downloader_tasks()]
    /// - `ESTHRI_DOWNLOAD_BUFFER_SIZE` - [Config::download_buffer_size()]
    /// - `ESTHRI_CONCURRENT_DOWNLOADER_TASKS` - [Config::concurrent_downloader_tasks()]
    /// - `ESTHRI_CONCURRENT_WRITER_TASKS` - [Config::concurrent_writer_tasks()]
    pub fn global() -> &'static Config {
        CONFIG.get_or_init(|| {
            envy::prefixed("ESTHRI_")
                .from_env::<Config>()
                .expect(EXPECT_GLOBAL_CONFIG)
        })
    }

    /// The default redis server url. Defaults to DEFAULT_REDIS_SERVER_URL.
    /// [DEFAULT_REDIS_SERVER_URL].
    pub fn default_redis_server_url(&self) -> String {
        self.default_redis_server_url.0.to_string()
    }

    /// The multipart stream default size. Defaults to
    /// [STREAM_PART_SIZE].
    pub fn stream_part_size(&self) -> usize {
        self.stream_part_size.0
    }

    /// The default size of parts in a multipart upload to S3.  8 MiB is the default chunk size
    /// from awscli, changing this size will affect the calculation of ETags.  Defaults to
    /// [UPLOAD_PART_SIZE].
    pub fn upload_part_size(&self) -> u64 {
        self.upload_part_size.0
    }

    /// The number of concurrent tasks run when sending data to S3.  Defautls to
    /// [CONCURRENT_UPLOAD_TASKS].
    pub fn concurrent_upload_tasks(&self) -> usize {
        self.concurrent_upload_tasks.0 as usize
    }

    /// When downloading or receiving data from S3, this sizes a task's download buffer.  Defaults
    /// to [DOWNLOAD_BUFFER_SIZE].
    pub fn download_buffer_size(&self) -> usize {
        self.download_buffer_size.0
    }

    /// The number of concurrent tasks run when receiving data from S3.  Each task represents a
    /// connection to S3.  Defaults to [CONCURRENT_DOWNLOADER_TASKS].
    pub fn concurrent_downloader_tasks(&self) -> usize {
        self.concurrent_downloader_tasks.0 as usize
    }

    /// The number of concurrent tasks run when receiving compressed data from S3.  Each task
    /// represents a connection to S3.  Defaults to [CONCURRENT_COMPRESSED_DOWNLOADER_TASKS].
    pub fn concurrent_compressed_downloader_tasks(&self) -> usize {
        self.concurrent_compressed_downloader_tasks.0 as usize
    }

    /// The number of concurrent tasks run when writing download data to disk.  Defaults to
    /// [CONCURRENT_WRITER_TASKS].
    pub fn concurrent_writer_tasks(&self) -> usize {
        self.concurrent_writer_tasks.0 as usize
    }

    /// The number of concurrent tasks run when running a sync operation.  Defaults to
    /// [CONCURRENT_SYNC_TASKS].
    pub fn concurrent_sync_tasks(&self) -> usize {
        self.concurrent_sync_tasks.0 as usize
    }
}
