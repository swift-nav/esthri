#![cfg_attr(feature = "aggressive_lint", deny(warnings))]

use esthri_lib::{blocking, download};

mod common;

#[test]
fn test_download() {
    let s3client = common::get_s3client();
    let filename = "test_file.txt";
    let filepath = format!("tests/data/{}", filename);
    let s3_key = format!("test_folder/{}", filename);

    let res = blocking::download(s3client.as_ref(), common::TEST_BUCKET, &s3_key, &filepath);
    assert!(res.is_ok());
}

#[tokio::test]
async fn test_download_async() {
    let s3client = common::get_s3client();
    let filename = "test_file.txt";
    let filepath = format!("tests/data/{}", filename);
    let s3_key = format!("test_folder/{}", filename);

    let res = download(s3client.as_ref(), common::TEST_BUCKET, &s3_key, &filepath).await;
    assert!(res.is_ok());
}
