use esthri_lib::s3_head_object;
use esthri_lib::s3_list_objects;
use esthri_lib::s3_upload;

mod common;

#[test]
fn test_handle_head_object() {
    let s3client = common::get_s3client();

    let filename = "test5mb.bin";
    let filepath = format!("tests/data/{}", filename);

    let res = s3_upload(s3client.as_ref(), common::TEST_BUCKET, filename, &filepath);

    assert!(res.is_ok());

    let res = s3_head_object(s3client.as_ref(), common::TEST_BUCKET, &filename);

    let e_tag: Option<String> = res.unwrap();

    assert!(e_tag.is_some());
}

#[test]
fn test_handle_list_objects() {
    let s3client = common::get_s3client();

    let filename = "test5mb.bin";
    let filepath = format!("tests/data/{}", filename);
    let folder = "test_folder";
    let empty_folder = "test_folder_empty";
    let bucket = format!("{}/{}", common::TEST_BUCKET, folder);

    let res = s3_upload(s3client.as_ref(), &bucket, filename, &filepath);

    assert!(res.is_ok());

    let res = s3_list_objects(s3client.as_ref(), common::TEST_BUCKET, &folder);

    let bucket_contents = res.unwrap();

    assert_eq!(1, bucket_contents.len());
    assert_eq!("test_folder/test5mb.bin", bucket_contents[0]);

    let res = s3_list_objects(s3client.as_ref(), common::TEST_BUCKET, &empty_folder);

    let bucket_contents = res.unwrap();

    assert!(bucket_contents.is_empty())
}
