use once_cell::sync::OnceCell;
use serde::Deserialize;

// This is the default chunk size from awscli
const CHUNK_SIZE: u64 = 8 * 1024 * 1024;
const XFER_COUNT: usize = 16;
const READ_SIZE: usize = 4096;

#[derive(Deserialize)]
pub struct Config {
    #[serde(default)]
    pub chunk_size: ChunkSize,
    #[serde(default)]
    pub xfer_count: XferCount,
    #[serde(default)]
    pub read_size: ReadSize,
}

#[derive(Deserialize)]
#[serde(transparent)]
pub struct ChunkSize(u64);

impl Default for ChunkSize {
    fn default() -> Self {
        ChunkSize(CHUNK_SIZE)
    }
}

#[derive(Deserialize)]
#[serde(transparent)]
pub struct XferCount(usize);

impl Default for XferCount {
    fn default() -> Self {
        XferCount(XFER_COUNT)
    }
}

#[derive(Deserialize)]
#[serde(transparent)]
pub struct ReadSize(usize);

impl Default for ReadSize {
    fn default() -> Self {
        ReadSize(READ_SIZE)
    }
}

pub static CONFIG: OnceCell<Config> = OnceCell::new();

const EXPECT_GLOBAL_CONFIG: &str = "failed to parse config from environment";

impl Config {
    pub fn global() -> &'static Config {
        CONFIG.get_or_init(|| {
            envy::prefixed("ESTHRI_")
                .from_env::<Config>()
                .expect(EXPECT_GLOBAL_CONFIG)
        })
    }

    pub fn read_size(&self) -> usize {
        self.read_size.0
    }

    pub fn chunk_size(&self) -> u64 {
        self.chunk_size.0
    }

    pub fn xfer_count(&self) -> usize {
        self.xfer_count.0
    }
}
