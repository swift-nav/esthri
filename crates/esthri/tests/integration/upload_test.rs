use std::io::Cursor;

use esthri::{blocking, opts::*, upload, upload_from_reader, HeadObjectInfo};

use aws_sdk_s3::types::StorageClass as S3StorageClass;

#[test]
fn test_upload() {
    let s3client = esthri_test::get_s3client();
    let filename = "test5mb.bin";
    let filepath = esthri_test::test_data(filename);
    let s3_key = esthri_test::randomised_lifecycled_prefix(&format!("test_upload/{}", filename));
    let opts = EsthriPutOptParamsBuilder::default().build().unwrap();

    let res = blocking::upload(
        s3client.as_ref(),
        esthri_test::TEST_BUCKET,
        &s3_key,
        filepath,
        opts,
    );
    assert!(res.is_ok());

    let res = blocking::head_object(s3client.as_ref(), esthri_test::TEST_BUCKET, &s3_key);
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
    let s3_key = esthri_test::randomised_lifecycled_prefix(&format!("test_upload/{}", filename));
    let opts = EsthriPutOptParamsBuilder::default()
        .transparent_compression(true)
        .build()
        .unwrap();

    let res = esthri::blocking::upload(
        s3client.as_ref(),
        esthri_test::TEST_BUCKET,
        &s3_key,
        filepath,
        opts,
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
    let s3client = esthri_test::get_s3client_async().await;
    let filename = "test5mb.bin";
    let filepath = esthri_test::test_data(filename);
    let s3_key = esthri_test::randomised_lifecycled_prefix(&format!("test_upload/{}", filename));
    let opts = EsthriPutOptParamsBuilder::default().build().unwrap();

    let res = upload(
        s3client.as_ref(),
        esthri_test::TEST_BUCKET,
        &s3_key,
        &filepath,
        opts,
    )
    .await;
    assert!(res.is_ok());
}

#[tokio::test]
async fn test_upload_reader() {
    let s3client = esthri_test::get_s3client_async().await;
    let filename = "test_reader_upload.bin";
    let filepath =
        esthri_test::randomised_lifecycled_prefix(&format!("test_upload_reader/{}", filename));
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
    let s3_key =
        esthri_test::randomised_lifecycled_prefix(&format!("test_upload_zero_size/{}", filename));
    let opts = EsthriPutOptParamsBuilder::default().build().unwrap();

    let res = blocking::upload(
        s3client.as_ref(),
        esthri_test::TEST_BUCKET,
        s3_key,
        filepath,
        opts,
    );
    assert!(res.is_ok());
}

#[test]
fn test_upload_storage_class_all() {
    let s3client = esthri_test::get_s3client();
    let filename = "test5mb.bin";
    let filepath = &esthri_test::test_data(filename);
    let s3_key = esthri_test::randomised_lifecycled_prefix(&format!("test_upload/{}", filename));

    // 1. Glacier Class might take hours to populate metadata, skipping tests...
    // Reference: https://aws.amazon.com/s3/faqs/
    // 2. Uploading to S3 bucket in AWS region via OUTPOSTS is not supported, skipping test...
    // Reference: https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-class-intro.html#s3-outposts
    for class in [
        S3StorageClass::IntelligentTiering,
        S3StorageClass::OnezoneIa,
        S3StorageClass::ReducedRedundancy,
        S3StorageClass::Standard,
        S3StorageClass::StandardIa,
    ] {
        let opts = EsthriPutOptParamsBuilder::default()
            .storage_class(Some(class.clone()))
            .build()
            .unwrap();

        let res = esthri::blocking::upload(
            s3client.as_ref(),
            esthri_test::TEST_BUCKET,
            &s3_key,
            filepath,
            opts,
        );
        assert!(res.is_ok());

        let res =
            esthri::blocking::head_object(s3client.as_ref(), esthri_test::TEST_BUCKET, &s3_key);
        let obj_info: Option<HeadObjectInfo> = res.unwrap();
        assert!(obj_info.is_some());
        let obj_info: HeadObjectInfo = obj_info.unwrap();
        assert_eq!(obj_info.storage_class, class);
    }
}
