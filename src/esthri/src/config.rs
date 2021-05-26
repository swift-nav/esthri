//! Configuration module for the library, allows sizing of internal worker pool sizes, multipart
//! upload sizes and read buffer sizes, among other things.

use once_cell::sync::OnceCell;
use serde::Deserialize;

/// The default size of chunks or parts in a multipart upload to S3.  8 MiB is the default chunk
/// size from awscli.
pub const CHUNK_SIZE: u64 = 8 * 1024 * 1024;
/// The default number of workers to use in the when transferring files or running a sync operation.
pub const WORKER_COUNT: u16 = 16;
/// The default size of internal buffers used to for file reads.
pub const READ_SIZE: usize = 8 * 1024 * 1024;

/// Holds configuration information for the library.
#[derive(Deserialize)]
pub struct Config {
    /// The size of chunks or parts in a multipart upload to S3.  Default value is 8 MiB.
    #[serde(default)]
    chunk_size: ChunkSize,
    /// The number of workers to use in the when transferring files or running a sync operation.
    #[serde(default)]
    worker_count: WorkerCount,
    /// The size of internal buffers used to for file reads.
    #[serde(default)]
    read_size: ReadSize,
}

/// Wrapper type for [CHUNK_SIZE] and [Config::chunk_size()] to bind a default value.
#[derive(Deserialize)]
#[serde(transparent)]
struct ChunkSize(u64);

impl Default for ChunkSize {
    fn default() -> Self {
        ChunkSize(CHUNK_SIZE)
    }
}

/// Wrapper type for [WORKER_COUNT] and [Config::worker_count()] to bind a default value.
#[derive(Deserialize)]
#[serde(transparent)]
struct WorkerCount(u16);

impl Default for WorkerCount {
    fn default() -> Self {
        WorkerCount(WORKER_COUNT)
    }
}

/// Wrapper type for [READ_SIZE] and [Config::read_size()] to bind a default value.
#[derive(Deserialize)]
#[serde(transparent)]
struct ReadSize(usize);

impl Default for ReadSize {
    fn default() -> Self {
        ReadSize(READ_SIZE)
    }
}

static CONFIG: OnceCell<Config> = OnceCell::new();

const EXPECT_GLOBAL_CONFIG: &str = "failed to parse config from environment";

impl Config {
    /// Fetches the global config object, values are either defaulted or populated
    /// from the environment:
    ///
    /// - `ESTHRI_READ_SIZE` - [Config::read_size()]
    /// - `ESTHRI_CHUNK_SIZE` - [Config::chunk_size()]
    /// - `ESTHRI_WORKER_COUNT` - [Config::worker_count()]
    pub fn global() -> &'static Config {
        CONFIG.get_or_init(|| {
            envy::prefixed("ESTHRI_")
                .from_env::<Config>()
                .expect(EXPECT_GLOBAL_CONFIG)
        })
    }

    /// The size of internal buffers used to for file reads. See [READ_SIZE].
    pub fn read_size(&self) -> usize {
        self.read_size.0
    }

    /// The size of chunks or parts in a multipart upload to S3.  Default value is 8 MiB.
    /// See [CHUNK_SIZE].
    pub fn chunk_size(&self) -> u64 {
        self.chunk_size.0
    }

    /// The number of workers to use in the when transferring files or running a sync operation.
    /// See [WORKER_COUNT].
    pub fn worker_count(&self, multiplier: f64) -> usize {
        let worker_count = self.worker_count.0 as f64;
        usize::max(1, (worker_count * multiplier) as usize)
    }
}
