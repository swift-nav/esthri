#![cfg_attr(feature = "aggressive_lint", deny(warnings))]

use esthri::{blocking, download};

#[test]
fn test_download() {
    let s3client = crate::get_s3client();
    let filename = "test_file.txt";
    let filepath = format!("tests/data/{}", filename);
    let s3_key = format!("test_folder/{}", filename);

    let res = blocking::download(s3client.as_ref(), crate::TEST_BUCKET, &s3_key, &filepath);
    assert!(res.is_ok());
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
