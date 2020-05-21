use esthri_lib::s3_head_object;
use esthri_lib::s3_list_objects;
use esthri_lib::s3_upload;

mod common;

#[test]
fn test_handle_head_object() {
    let s3client = common::get_s3client();

    let filename = "test1mb.bin";
    let filepath = format!("tests/data/{}", filename);

    let s3_key = format!("test_handle_head_object/{}", filename);

    let res = s3_upload(s3client.as_ref(), common::TEST_BUCKET, &s3_key, &filepath);

    assert!(res.is_ok());

    let res = s3_head_object(s3client.as_ref(), common::TEST_BUCKET, &s3_key);

    let e_tag: Option<String> = res.unwrap();

    assert!(e_tag.is_some());
}

#[test]
fn test_handle_list_objects() {
    let s3client = common::get_s3client();

    let filename = "test1mb.bin";
    let filepath = format!("tests/data/{}", filename);

    let not_empty_folder = "test_handle_list_objects/not_empty_folder";
    let not_empty_s3_key = format!("{}/{}", not_empty_folder, filename);

    let empty_folder = "test_handle_list_objects/empty_folder";

    let res = s3_upload(s3client.as_ref(), common::TEST_BUCKET, &not_empty_s3_key, &filepath);

    assert!(res.is_ok());

    let res = s3_list_objects(s3client.as_ref(), common::TEST_BUCKET, &not_empty_folder);

    let bucket_contents = res.unwrap();

    assert_eq!(1, bucket_contents.len());
    assert_eq!("test_handle_list_objects/not_empty_folder/test1mb.bin", bucket_contents[0]);

    let res = s3_list_objects(s3client.as_ref(), common::TEST_BUCKET, &empty_folder);

    let bucket_contents = res.unwrap();

    assert!(bucket_contents.is_empty())
}
