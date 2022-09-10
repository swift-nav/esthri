#![cfg_attr(feature = "aggressive_lint", deny(warnings))]

use hyper::Client;
use md5::{Digest, Md5};
use once_cell::sync::Lazy;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use tempdir::TempDir;

use uuid::Uuid;

use esthri_internals::new_https_connector;
use esthri_internals::rusoto::*;

pub struct TestGlobal {
    s3client: Option<Arc<S3Client>>,
}

static TEST_GLOBAL: Lazy<Mutex<TestGlobal>> =
    Lazy::new(|| Mutex::new(TestGlobal { s3client: None }));

const TEST_GLOBAL_LOCK_FAILED: &str = "locking global test data failed";

pub const TEST_BUCKET: &str = "esthri-test";

pub type DateTime = chrono::DateTime<chrono::Utc>;

pub fn test_data(name: &str) -> PathBuf {
    test_data_dir().join(name)
}

pub fn randomised_name(name: &str) -> String {
    format!("{}-{}", Uuid::new_v4(), name)
}

pub fn test_data_dir() -> PathBuf {
    let workspace = std::env::var("WORKSPACE").expect("WORKSPACE not set");
    PathBuf::from(workspace).join("crates/esthri-test/data")
}

pub fn validate_key_hash_pairs(local_directory: impl AsRef<Path>, key_hash_pairs: &[KeyHashPair]) {
    let local_directory = local_directory.as_ref();
    for key_hash_pair in key_hash_pairs {
        let path = local_directory.join(key_hash_pair.0);
        let data = fs::read(&path).unwrap();
        let digest = Md5::new().chain_update(data).finalize();
        let digest = format!("{:x}", digest);
        assert_eq!(
            digest,
            key_hash_pair.1,
            "md5 digest did not match: {}",
            path.display(),
        );
    }

    // Ensure there aren't any extra unexpected files in the directory
    let paths = fs::read_dir(local_directory).unwrap();
    assert_eq!(paths.count(), key_hash_pairs.len());
}

pub fn get_s3client() -> Arc<S3Client> {
    init_s3client();
    let test_global = TEST_GLOBAL.lock().expect(TEST_GLOBAL_LOCK_FAILED);
    test_global.s3client.as_ref().unwrap().clone()
}

pub struct KeyHashPair(pub &'static str, pub &'static str);

pub struct EphemeralTempDir {
    pub temp_dir: TempDir,
    pub old_dir: std::path::PathBuf,
}

impl EphemeralTempDir {
    pub fn pushd() -> EphemeralTempDir {
        let temp_dir = TempDir::new("esthri_tmp").expect("creating temporary directory");
        let old_dir = std::env::current_dir().expect("getting current directory");
        std::env::set_current_dir(temp_dir.path()).expect("changing to new temporary directory");
        EphemeralTempDir { temp_dir, old_dir }
    }
}

impl Drop for EphemeralTempDir {
    fn drop(&mut self) {
        std::env::set_current_dir(&self.old_dir).expect("setting CWD to previous directory");
    }
}

fn init_s3client() {
    let mut test_global = TEST_GLOBAL.lock().expect(TEST_GLOBAL_LOCK_FAILED);

    match test_global.s3client {
        None => {
            env_logger::init();

            let mut hyper_builder = Client::builder();

            // Prevent hyper from pooling open connections
            hyper_builder.pool_idle_timeout(None);
            hyper_builder.pool_max_idle_per_host(0);

            let https_connector = new_https_connector();
            let http_client = HttpClient::from_builder(hyper_builder, https_connector);

            let credentials_provider = DefaultCredentialsProvider::new().unwrap();
            let s3 = S3Client::new_with(http_client, credentials_provider, Region::default());

            let s3 = Arc::new(s3);
            test_global.s3client = Some(s3);
        }
        Some(_) => {}
    }
}
