#![cfg(feature = "http_server")]
#![cfg_attr(feature = "aggressive_lint", deny(warnings))]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use flate2::read::GzDecoder;
use tar::Archive;

use tokio::runtime::Runtime;

use esthri::blocking;
use esthri::http_server::esthri_filter;
use esthri::upload;

use crate::{validate_key_hash_pairs, DateTime, KeyHashPair};

#[tokio::test]
async fn test_fetch_object() {
    let filename = "tests/data/test_file.txt".to_owned();

    let s3client = crate::get_s3client();
    let bucket = crate::TEST_BUCKET;

    let s3_key = "test_file.txt".to_owned();

    let res = upload(s3client.as_ref(), crate::TEST_BUCKET, &s3_key, &filename).await;
    assert!(res.is_ok());

    let filter = esthri_filter((*s3client).clone(), bucket);

    let mut result = warp::test::request()
        .path("/test_file.txt")
        .filter(&filter)
        .await
        .unwrap();

    let body = hyper::body::to_bytes(result.body_mut()).await.unwrap();
    assert_eq!(body, "this file has contents\n");
}

fn upload_test_data() -> anyhow::Result<()> {
    let s3client = crate::get_s3client();
    let local_directory = "tests/data/sync_up";
    let s3_key = "test_fetch_archive";
    let filenames = ["1-one.data", "2-two.bin", "3-three.junk"];
    for filename in &filenames {
        let filepath = std::path::Path::new(local_directory).join(filename);
        let s3_key = format!("{}/{}", s3_key, filename);
        blocking::upload(
            s3client.as_ref(),
            crate::TEST_BUCKET,
            &s3_key,
            filepath.to_str().unwrap(),
        )?;
    }
    Ok(())
}

fn validate_fetch_archive(
    body: bytes::Bytes,
    local_dir: &str,
    key_hash_pairs: &[KeyHashPair],
    upload_time: DateTime,
) {
    use std::path::Path;
    let tar = GzDecoder::new(&body[..]);
    let mut archive = Archive::new(tar);
    let _tmp_dir = crate::EphemeralTempDir::pushd();
    archive.unpack(".").unwrap();
    validate_key_hash_pairs(local_dir, key_hash_pairs);
    for key_hash_pair in key_hash_pairs {
        let filename = Path::new(key_hash_pair.0);
        let filepath = Path::new(local_dir).join(filename);
        let file_last_mod = std::fs::metadata(&filepath)
            .expect("stat'ing test file path")
            .modified()
            .expect("last modified timestamp");
        let mod_time: DateTime = file_last_mod.into();
        let time_diff = mod_time - upload_time;
        // Hopefully it never takes more than an hour to upload the files
        assert_eq!(time_diff.num_hours(), 0);
    }
}

async fn fetch_archive_and_validate(
    request_path: &str,
    success: Arc<AtomicBool>,
    local_dir: &str,
    key_hash_pairs: &[KeyHashPair],
    upload_time: DateTime,
) {
    let s3client = crate::get_s3client();
    let filter = esthri_filter((*s3client).clone(), crate::TEST_BUCKET);
    let mut body = warp::test::request()
        .path(request_path)
        .filter(&filter)
        .await
        .unwrap();
    let body_bytes = hyper::body::to_bytes(body.body_mut()).await.unwrap();
    validate_fetch_archive(body_bytes, local_dir, key_hash_pairs, upload_time);
    success.store(true, Ordering::Release);
}

fn test_fetch_archive_helper(
    request_path: &str,
    local_dir: &str,
    key_hash_pairs: Vec<KeyHashPair>,
) {
    const TIMEOUT_MILLIS: u64 = 20_000;
    const SLEEP_MILLIS: u64 = 50;

    let success = Arc::new(AtomicBool::new(false));
    let thread_success = success.clone();
    let panic_success = success.clone();

    let done = Arc::new(AtomicBool::new(false));
    let thread_done = done.clone();
    let panic_done = done.clone();

    let request_path = request_path.to_owned();
    let local_dir = local_dir.to_owned();

    let upload_time: DateTime = std::time::SystemTime::now().into();
    upload_test_data().unwrap();

    std::panic::set_hook(Box::new(move |panic_info| {
        panic_success.store(false, Ordering::Release);
        panic_done.store(true, Ordering::Relaxed);
        eprint!("{:?}", backtrace::Backtrace::new());
        eprint!("\n{}\n", panic_info);
    }));

    std::thread::spawn(move || {
        let runtime = Runtime::new().unwrap();
        runtime.block_on(fetch_archive_and_validate(
            &request_path,
            thread_success,
            &local_dir,
            &key_hash_pairs,
            upload_time,
        ));
        thread_done.store(true, Ordering::Relaxed);
    });

    let mut timeout_count: i64 = (TIMEOUT_MILLIS / SLEEP_MILLIS) as i64;

    while !done.load(Ordering::Relaxed) && timeout_count > 0 {
        std::thread::sleep(std::time::Duration::from_millis(SLEEP_MILLIS));
        timeout_count -= 1;
    }

    if timeout_count == 0 {
        println!("****************");
        println!("*** TIME OUT ***");
        println!("****************");
    }

    let _ = std::panic::take_hook();
    assert!(success.load(Ordering::Acquire) || timeout_count == 0);
}

#[test]
fn test_fetch_archive() {
    let key_hash_pairs: Vec<KeyHashPair> = vec![
        KeyHashPair("1-one.data", "827aa1b392c93cb25d2348bdc9b907b0"),
        KeyHashPair("2-two.bin", "35500e07a35b413fc5f434397a4c6bfa"),
        KeyHashPair("3-three.junk", "388f9763d78cecece332459baecb4b85"),
    ];
    test_fetch_archive_helper(
        "/test_fetch_archive/?archive=true",
        "test_fetch_archive",
        key_hash_pairs,
    );
}

#[test]
fn test_fetch_archive_prefixes() {
    let key_hash_pairs: Vec<KeyHashPair> = vec![
        KeyHashPair("1-one.data", "827aa1b392c93cb25d2348bdc9b907b0"),
        KeyHashPair("2-two.bin", "35500e07a35b413fc5f434397a4c6bfa"),
    ];
    test_fetch_archive_helper(
        "/?prefixes=test_fetch_archive/1-one.data|test_fetch_archive/2-two.bin&archive=true",
        "test_fetch_archive",
        key_hash_pairs,
    );
}
