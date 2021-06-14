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

#[cfg(feature = "compression")]
#[test]
fn test_download_decompressed() {
    let s3client = crate::get_s3client();
    let _tmp_dir = crate::EphemeralTempDir::pushd();

    // Test object `test_download/27-185232-msg.csv.gz` must be prepopulated in the S3 bucket
    let filename = "27-185232-msg.csv";
    let s3_key = format!("test_download/{}.gz", filename);

    let res =
        blocking::download_decompressed(s3client.as_ref(), crate::TEST_BUCKET, &s3_key, &filename);
    assert!(res.is_ok());

    let etag = blocking::compute_etag(filename).unwrap();
    assert_eq!(etag, "\"6dbb4258fa16030c2daf6f1eac93dddd-8\"");
}
