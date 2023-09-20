#![cfg_attr(feature = "aggressive_lint", deny(warnings))]

use aws_sdk_s3::Client as S3Client;
use aws_smithy_client::hyper_ext;
use esthri_internals::new_https_connector;
use fs_extra::dir;
use fs_extra::dir::CopyOptions;
use md5::{Digest, Md5};
use once_cell::sync::Lazy;
use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};
use tempdir::TempDir;
use tokio::sync::Mutex;
use uuid::Uuid;

pub struct TestGlobal {
    s3client: Option<Arc<S3Client>>,
}

static TEST_GLOBAL: Lazy<Mutex<TestGlobal>> =
    Lazy::new(|| Mutex::new(TestGlobal { s3client: None }));

pub const TEST_BUCKET: &str = "esthri-test";

pub type DateTime = chrono::DateTime<chrono::Utc>;

pub fn test_data(name: &str) -> PathBuf {
    test_data_dir().join(name)
}

pub fn copy_test_data(name: &str) -> PathBuf {
    let randomized_name = randomised_name(name);

    let source_data_dir = test_data_dir().join(name);
    let target_data_dir = test_data_dir().join(randomized_name);

    println!("copy_test_data: {source_data_dir:?} {target_data_dir:?}");

    let mut opts = CopyOptions::new();
    opts.copy_inside = true;

    dir::copy(source_data_dir, &target_data_dir, &opts).expect("failed to copy test data dir");

    target_data_dir
}

pub fn randomised_name(name: &str) -> String {
    format!("{}-{}", Uuid::new_v4(), name)
}

// add prefix that s3 lifecycle policy would match to do deletion
pub fn randomised_lifecycled_prefix(name: &str) -> String {
    format!("test_runs_to_be_lifecycled/{}-{}", Uuid::new_v4(), name)
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
    let rt = tokio::runtime::Runtime::new().unwrap();
    let test_global = rt.block_on(async {
        init_s3client().await;
        TEST_GLOBAL.lock().await
    });
    test_global.s3client.as_ref().unwrap().clone()
}

pub async fn get_s3client_async() -> Arc<S3Client> {
    init_s3client().await;
    let test_global = TEST_GLOBAL.lock().await;
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

async fn init_s3client() {
    let mut test_global = TEST_GLOBAL.lock().await;

    match test_global.s3client {
        None => {
            env_logger::init();

            let env_config = aws_config::load_from_env().await;
            let https_connector = new_https_connector();
            let smithy_connector = hyper_ext::Adapter::builder().build(https_connector);

            let config = aws_sdk_s3::config::Builder::from(&env_config)
                .http_connector(smithy_connector)
                .build();

            let s3 = aws_sdk_s3::Client::from_conf(config);
            let s3 = Arc::new(s3);
            test_global.s3client = Some(s3);
        }
        Some(_) => {}
    }
}
