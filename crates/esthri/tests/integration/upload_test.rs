use std::io::Cursor;

use esthri::blocking;
use esthri::rusoto::S3StorageClass;
use esthri::upload;
use esthri::upload_from_reader;
use esthri::HeadObjectInfo;

#[test]
fn test_upload() {
    let s3client = esthri_test::get_s3client();
    let filename = "test5mb.bin";
    let filepath = esthri_test::test_data(filename);
    let s3_key = esthri_test::randomised_name(&format!("test_upload/{}", filename));

    let res = esthri::blocking::upload(
        s3client.as_ref(),
        esthri_test::TEST_BUCKET,
        &s3_key,
        &filepath,
    );
    assert!(res.is_ok());

    let res = esthri::blocking::head_object(s3client.as_ref(), esthri_test::TEST_BUCKET, &s3_key);
    let obj_info: Option<HeadObjectInfo> = res.unwrap();
    assert!(obj_info.is_some());
    let obj_info: HeadObjectInfo = obj_info.unwrap();

    assert_eq!(obj_info.size, 5242880);
    assert_eq!(obj_info.e_tag, "\"8542c49db935a57bb8c26ec68d39aaea\"");
    assert_eq!(obj_info.storage_class, S3StorageClass::Standard);
    assert!(!obj_info.metadata.contains_key("esthri_compress_version"));
}

#[test]
fn test_upload_compressed() {
    let s3client = esthri_test::get_s3client();
    let filename = "27-185232-msg.csv";
    let filepath = esthri_test::test_data(filename);
    let s3_key = esthri_test::randomised_name(&format!("test_upload/{}", filename));

    let res = esthri::blocking::upload_compressed(
        s3client.as_ref(),
        esthri_test::TEST_BUCKET,
        &s3_key,
        &filepath,
    );
    assert!(res.is_ok());

    let res = esthri::blocking::head_object(s3client.as_ref(), esthri_test::TEST_BUCKET, &s3_key);
    let obj_info: Option<HeadObjectInfo> = res.unwrap();

    assert!(obj_info.is_some());

    let obj_info: HeadObjectInfo = obj_info.unwrap();

    assert_eq!(obj_info.size, 3344161);
    assert_eq!(obj_info.e_tag, "\"4a57bdf6ed65bc7e9ed34a4796561f06\"");
    assert_eq!(obj_info.storage_class, S3StorageClass::Standard);
    assert_eq!(
        obj_info.metadata.get("esthri_compress_version").unwrap(),
        env!("CARGO_PKG_VERSION")
    );
}

#[tokio::test]
async fn test_upload_async() {
    let s3client = esthri_test::get_s3client();
    let filename = "test5mb.bin";
    let filepath = esthri_test::test_data(filename);
    let s3_key = esthri_test::randomised_name(&format!("test_upload/{}", filename));

    let res = upload(
        s3client.as_ref(),
        esthri_test::TEST_BUCKET,
        &s3_key,
        &filepath,
    )
    .await;
    assert!(res.is_ok());
}

#[tokio::test]
async fn test_upload_reader() {
    let s3client = esthri_test::get_s3client();
    let filename = "test_reader_upload.bin";
    let filepath = esthri_test::randomised_name(&format!("test_upload_reader/{}", filename));
    let contents = "file contents";
    let reader = Cursor::new(contents);

    let res = upload_from_reader(
        s3client.as_ref(),
        esthri_test::TEST_BUCKET,
        &filepath,
        reader,
        contents.len() as u64,
        None,
    )
    .await;
    assert!(res.is_ok());
}

#[test]
fn test_upload_zero_size() {
    let s3client = esthri_test::get_s3client();
    let filename = "test0b.bin";
    let filepath = esthri_test::test_data(filename);
    let s3_key = esthri_test::randomised_name(&format!("test_upload_zero_size/{}", filename));

    let res = blocking::upload(
        s3client.as_ref(),
        esthri_test::TEST_BUCKET,
        &s3_key,
        &filepath,
    );
    assert!(res.is_ok());
}

#[test]
fn test_upload_storage_class_rrs() {
    let s3client = esthri_test::get_s3client();
    let filename = "test5mb.bin";
    let filepath = esthri_test::test_data(filename);
    let s3_key = esthri_test::randomised_name(&format!("test_upload/{}", filename));

    let res = esthri::blocking::upload_with_storage_class(
        s3client.as_ref(),
        esthri_test::TEST_BUCKET,
        &s3_key,
        &filepath,
        S3StorageClass::RRS,
    );
    assert!(res.is_ok());

    let res = esthri::blocking::head_object(s3client.as_ref(), esthri_test::TEST_BUCKET, &s3_key);
    let obj_info: Option<HeadObjectInfo> = res.unwrap();
    assert!(obj_info.is_some());
    let obj_info: HeadObjectInfo = obj_info.unwrap();
    assert_eq!(obj_info.storage_class, S3StorageClass::RRS);
}

#[test]
fn test_upload_storage_class_standard() {
    let s3client = esthri_test::get_s3client();
    let filename = "test5mb.bin";
    let filepath = esthri_test::test_data(filename);
    let s3_key = esthri_test::randomised_name(&format!("test_upload/{}", filename));

    let res = esthri::blocking::upload_with_storage_class(
        s3client.as_ref(),
        esthri_test::TEST_BUCKET,
        &s3_key,
        &filepath,
        S3StorageClass::Standard,
    );
    assert!(res.is_ok());

    let res = esthri::blocking::head_object(s3client.as_ref(), esthri_test::TEST_BUCKET, &s3_key);
    let obj_info: Option<HeadObjectInfo> = res.unwrap();
    assert!(obj_info.is_some());
    let obj_info: HeadObjectInfo = obj_info.unwrap();
    assert_eq!(obj_info.storage_class, S3StorageClass::Standard);
}
