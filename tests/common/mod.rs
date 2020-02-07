use rusoto_core::Region;
use rusoto_s3::S3Client;

use once_cell::sync::Lazy;

use std::sync::Arc;
use std::sync::Mutex;

pub struct TestGlobal {
    s3client: Option<Arc<S3Client>>,
}

static TEST_GLOBAL: Lazy<Mutex<TestGlobal>> =
    Lazy::new(|| Mutex::new(TestGlobal { s3client: None }));

static TEST_GLOBAL_LOCK_FAILED: &'static str = "locking global test data failed";

pub static TEST_BUCKET: &'static str = "esthri-test";

fn init_s3client() {
    let mut test_global = TEST_GLOBAL.lock().expect(TEST_GLOBAL_LOCK_FAILED);

    match test_global.s3client {
        None => {
            let region = Region::default();
            let s3 = Arc::new(S3Client::new(region));
            test_global.s3client = Some(s3);
        }
        Some(_) => {
            return;
        }
    }
}

pub fn get_s3client() -> Arc<S3Client> {
    init_s3client();
    let test_global = TEST_GLOBAL.lock().expect(TEST_GLOBAL_LOCK_FAILED);

    test_global.s3client.as_ref().unwrap().clone()
}
