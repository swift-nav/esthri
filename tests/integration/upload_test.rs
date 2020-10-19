#![cfg_attr(feature = "aggressive_lint", deny(warnings))]

use std::io::Cursor;

use esthri_lib::blocking;
use esthri_lib::upload;

#[test]
fn test_upload() {
    let s3client = crate::get_s3client();
    let filename = "test5mb.bin";
    let filepath = format!("tests/data/{}", filename);
    let s3_key = format!("test_upload/{}", filename);

    let res = blocking::upload(s3client.as_ref(), crate::TEST_BUCKET, &s3_key, &filepath);
    assert!(res.is_ok());
}

#[tokio::test]
async fn test_upload_async() {
    let s3client = crate::get_s3client();
    let filename = "test5mb.bin";
    let filepath = format!("tests/data/{}", filename);
    let s3_key = format!("test_upload/{}", filename);

    let res = upload(s3client.as_ref(), crate::TEST_BUCKET, &s3_key, &filepath).await;
    assert!(res.is_ok());
}

#[test]
fn test_upload_reader() {
    let s3client = crate::get_s3client();
    let filename = "test_reader_upload.bin";
    let filepath = format!("test_upload_reader/{}", filename);
    let contents = "file contents";
    let mut reader = Cursor::new(contents);

    let res = blocking::upload_from_reader(
        s3client.as_ref(),
        crate::TEST_BUCKET,
        &filepath,
        &mut reader,
        contents.len() as u64,
    );
    assert!(res.is_ok());
}

#[test]
fn test_upload_zero_size() {
    let s3client = crate::get_s3client();
    let filename = "test0b.bin";
    let filepath = format!("tests/data/{}", filename);
    let s3_key = format!("test_upload_zero_size/{}", filename);

    let res = blocking::upload(s3client.as_ref(), crate::TEST_BUCKET, &s3_key, &filepath);
    assert!(res.is_ok());
}