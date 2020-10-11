#![cfg_attr(feature = "aggressive_lint", deny(warnings))]

mod download_test;
mod http_server;
mod list_object_test;
mod sync_test;
mod tail_test;
mod upload_test;

use std::fs;

use std::sync::Arc;
use std::sync::Mutex;

use hyper::Client;
use hyper_tls::HttpsConnector;

use tempdir::TempDir;

use rusoto_core::{HttpClient, Region};
use rusoto_credential::DefaultCredentialsProvider;
use rusoto_s3::S3Client;

use once_cell::sync::Lazy;

pub struct TestGlobal {
    s3client: Option<Arc<S3Client>>,
}

static TEST_GLOBAL: Lazy<Mutex<TestGlobal>> =
    Lazy::new(|| Mutex::new(TestGlobal { s3client: None }));

static TEST_GLOBAL_LOCK_FAILED: &str = "locking global test data failed";

pub static TEST_BUCKET: &str = "esthri-test";

fn init_s3client() {
    let mut test_global = TEST_GLOBAL.lock().expect(TEST_GLOBAL_LOCK_FAILED);

    match test_global.s3client {
        None => {
            env_logger::init();

            let mut hyper_builder = Client::builder();

            // Prevent hyper from pooling open connections
            hyper_builder.pool_idle_timeout(None);
            hyper_builder.pool_max_idle_per_host(0);

            let https_connector = HttpsConnector::new();
            let http_client = HttpClient::from_builder(hyper_builder, https_connector);

            let credentials_provider = DefaultCredentialsProvider::new().unwrap();
            let s3 = S3Client::new_with(http_client, credentials_provider, Region::default());

            let s3 = Arc::new(s3);
            test_global.s3client = Some(s3);
        }
        Some(_) => {}
    }
}

pub fn get_s3client() -> Arc<S3Client> {
    init_s3client();
    let test_global = TEST_GLOBAL.lock().expect(TEST_GLOBAL_LOCK_FAILED);
    test_global.s3client.as_ref().unwrap().clone()
}

pub struct KeyHashPair(pub &'static str, pub &'static str);

pub struct EphemeralTempDir {
    _temp_dir: TempDir,
    old_dir: std::path::PathBuf,
}

impl EphemeralTempDir {
    pub fn pushd() -> EphemeralTempDir {
        let temp_dir = TempDir::new("esthri_tmp").expect("creating temporary directory");
        let old_dir = std::env::current_dir().expect("getting current directory");
        std::env::set_current_dir(temp_dir.path()).expect("changing to new temporary directory");
        EphemeralTempDir {
            _temp_dir: temp_dir,
            old_dir,
        }
    }
}

impl Drop for EphemeralTempDir {
    fn drop(&mut self) {
        std::env::set_current_dir(&self.old_dir).expect("setting CWD to previous directory");
    }
}

pub fn validate_key_hash_pairs(local_directory: &str, key_hash_pairs: &[KeyHashPair]) {
    for key_hash_pair in &key_hash_pairs[..] {
        let path = format!("{}/{}", local_directory, key_hash_pair.0);
        let data = fs::read(&path).unwrap();
        let digest = md5::compute(data);
        let digest = format!("{:x}", digest);
        assert_eq!(
            digest, key_hash_pair.1,
            "md5 digest did not match: {}",
            path
        );
    }
}
