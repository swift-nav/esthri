#![cfg_attr(feature = "aggressive_lint", deny(warnings))]

use esthri::{blocking, download};
use tempdir::TempDir;

use crate::{validate_key_hash_pairs, KeyHashPair};

#[test]
fn test_download() {
    let s3client = crate::get_s3client();
    let filename = "test_file.txt";
    let filepath = format!("tests/data/{}", filename);
    let s3_key = format!("test_folder/{}", filename);

    let res = blocking::download(s3client.as_ref(), crate::TEST_BUCKET, &s3_key, &filepath);
    assert!(res.is_ok());
}

#[test]
fn test_download_to_nonexistent_path() {
    let s3client = crate::get_s3client();
    let filename = "test_file.txt";
    let tmpdir = TempDir::new("esthri_tmp").expect("creating temporary directory");
    let filepath = tmpdir
        .path()
        .join("some/extra/directories/that/dont/exist/");
    let s3_key = format!("test_folder/{}", filename);

    let res = blocking::download(s3client.as_ref(), crate::TEST_BUCKET, &s3_key, &filepath);
    assert!(res.is_ok());
    assert!(filepath.join(filename).exists());
}

#[test]
fn test_download_to_current_directory() {
    let s3client = crate::get_s3client();
    let filename = "test_file.txt";
    let _tmp_dir = crate::EphemeralTempDir::pushd();
    let filepath = format!(".");
    let s3_key = format!("test_folder/{}", filename);

    let res = blocking::download(s3client.as_ref(), crate::TEST_BUCKET, &s3_key, &filepath);
    assert!(res.is_ok());
    assert!(_tmp_dir.temp_dir.path().join(filename).exists());
}

#[tokio::test]
async fn test_download_async() {
    let s3client = crate::get_s3client();
    let filename = "test_file.txt";
    let filepath = format!("tests/data/{}", filename);
    let s3_key = format!("test_folder/{}", filename);

    let res = download(s3client.as_ref(), crate::TEST_BUCKET, &s3_key, &filepath).await;
    assert!(res.is_ok());
}

#[tokio::test]
async fn test_download_zero_size() {
    let s3client = crate::get_s3client();
    let _tmp_dir = crate::EphemeralTempDir::pushd();

    // Test object `test_download/test0b.bin` must be prepopulated in the S3 bucket
    let res = download(
        s3client.as_ref(),
        crate::TEST_BUCKET,
        "test_download/test0b.bin",
        "test0b.download",
    )
    .await;
    assert!(res.is_ok());

    let stat = std::fs::metadata("test0b.download");
    assert!(stat.is_ok());

    let stat = stat.unwrap();
    assert_eq!(stat.len(), 0);
}

#[test]
fn test_download_decompressed() {
    let s3client = crate::get_s3client();
    let _tmp_dir = crate::EphemeralTempDir::pushd();

    // Test object `test_download/27-185232-msg.csv.gz` must be prepopulated in the S3 bucket
    let filename = "27-185232-msg.csv";
    let s3_key = format!("test_download/{}.gz", filename);

    let res = blocking::download_with_transparent_decompression(
        s3client.as_ref(),
        crate::TEST_BUCKET,
        &s3_key,
        &filename,
    );
    assert!(res.is_ok());

    let etag = blocking::compute_etag(filename).unwrap();
    assert_eq!(etag, "\"6dbb4258fa16030c2daf6f1eac93dddd-8\"");
}

#[test]
fn test_download_decompressed_to_directory() {
    let s3client = crate::get_s3client();
    let _tmp_dir = crate::EphemeralTempDir::pushd();

    // Test object `test_download/27-185232-msg.csv.gz` must be prepopulated in the S3 bucket
    let filename = "27-185232-msg.csv.gz";
    let s3_key = format!("test_download/{}", filename);

    let res = blocking::download_with_transparent_decompression(
        s3client.as_ref(),
        crate::TEST_BUCKET,
        &s3_key,
        ".",
    );
    assert!(res.is_ok());

    let etag = blocking::compute_etag(filename).unwrap();
    assert_eq!(etag, "\"6dbb4258fa16030c2daf6f1eac93dddd-8\"");
}

#[test]
fn test_download_transparent_with_non_compressed() {
    let s3client = crate::get_s3client();

    let local_dir = TempDir::new("esthri_cli").unwrap();
    let local_dir_path = local_dir.path().as_os_str().to_str().unwrap();
    let filename = "test_file.txt";
    let s3_key = format!("test_folder/{}", filename);

    let res = blocking::download_with_transparent_decompression(
        s3client.as_ref(),
        crate::TEST_BUCKET,
        &s3_key,
        local_dir_path,
    );
    assert!(res.is_ok());

    let key_hash_pairs = [KeyHashPair(
        "test_file.txt",
        "8fd41740698064016b7daaddddd3531a",
    )];

    validate_key_hash_pairs(local_dir_path, &key_hash_pairs);
}
