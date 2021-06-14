#![cfg_attr(feature = "aggressive_lint", deny(warnings))]

use std::io::Cursor;

use esthri::blocking;
use esthri::upload;
#[allow(unused_imports)]
use esthri::ObjectInfo;

#[test]
fn test_upload() {
    let s3client = crate::get_s3client();
    let filename = "test5mb.bin";
    let filepath = format!("tests/data/{}", filename);
    let s3_key = format!("test_upload/{}", filename);

    let res = blocking::upload(s3client.as_ref(), crate::TEST_BUCKET, &s3_key, &filepath);
    assert!(res.is_ok());
}

#[cfg(feature = "compression")]
#[test]
fn test_upload_compressed() {
    let s3client = crate::get_s3client();
    let filename = "27-185232-msg.csv";
    let filepath = format!("tests/data/{}", filename);
    let s3_key = format!("test_upload/{}.gz", filename);

    let res =
        blocking::upload_compressed(s3client.as_ref(), crate::TEST_BUCKET, &s3_key, &filepath);
    assert!(res.is_ok());

    let res = blocking::head_object(s3client.as_ref(), crate::TEST_BUCKET, &s3_key);
    let obj_info: Option<ObjectInfo> = res.unwrap();

    assert!(obj_info.is_some());

    let obj_info: ObjectInfo = obj_info.unwrap();

    assert_eq!(obj_info.size, 3344161);
    assert_eq!(obj_info.e_tag, "\"4a57bdf6ed65bc7e9ed34a4796561f06\"");
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
    let reader = Cursor::new(contents);

    let res = blocking::upload_from_reader(
        s3client.as_ref(),
        crate::TEST_BUCKET,
        &filepath,
        reader,
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
