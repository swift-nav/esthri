use std::io::Cursor;

use esthri_lib::s3_upload;
use esthri_lib::s3_upload_reader;

mod common;

#[test]
fn test_upload() {
    let s3client = common::get_s3client();

    let filename = "test5mb.bin";
    let filepath = format!("tests/data/{}", filename);

    let res = s3_upload(s3client.as_ref(), common::TEST_BUCKET, filename, &filepath);

    assert!(res.is_ok());
}

#[test]
fn test_upload_reader() {
    let s3client = common::get_s3client();
    let filename = "test_reader_upload.bin";

    let contents = "file contents";
    let mut reader = Cursor::new(contents);

    let res = s3_upload_reader(
        s3client.as_ref(),
        common::TEST_BUCKET,
        filename,
        &mut reader,
        contents.len() as u64,
    );

    assert!(res.is_ok());
}
