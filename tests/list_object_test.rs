use futures::TryStreamExt;

use esthri_lib::blocking;
use esthri_lib::head_object;
use esthri_lib::list_objects;
use esthri_lib::list_objects_stream;
use esthri_lib::upload;

mod common;

#[test]
fn test_handle_head_object() {
    let s3client = common::get_s3client();
    let filename = "test1mb.bin";
    let filepath = format!("tests/data/{}", filename);
    let s3_key = format!("test_handle_head_object/{}", filename);

    let res = blocking::upload(s3client.as_ref(), common::TEST_BUCKET, &s3_key, &filepath);
    assert!(res.is_ok());

    let res = blocking::head_object(s3client.as_ref(), common::TEST_BUCKET, &s3_key);
    let e_tag: Option<String> = res.unwrap();
    assert!(e_tag.is_some());
}

#[tokio::test]
async fn test_handle_head_object_async() {
    let s3client = common::get_s3client();
    let filename = "test1mb.bin";
    let filepath = format!("tests/data/{}", filename);
    let s3_key = format!("test_handle_head_object/{}", filename);

    let res = upload(s3client.as_ref(), common::TEST_BUCKET, &s3_key, &filepath).await;
    assert!(res.is_ok());

    let res = head_object(s3client.as_ref(), common::TEST_BUCKET, &s3_key).await;
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

    let res = blocking::upload(
        s3client.as_ref(),
        common::TEST_BUCKET,
        &not_empty_s3_key,
        &filepath,
    );
    assert!(res.is_ok());

    let res = blocking::list_objects(s3client.as_ref(), common::TEST_BUCKET, &not_empty_folder);
    let bucket_contents = res.unwrap();
    assert_eq!(1, bucket_contents.len());
    assert_eq!(
        "test_handle_list_objects/not_empty_folder/test1mb.bin",
        bucket_contents[0]
    );

    let res = blocking::list_objects(s3client.as_ref(), common::TEST_BUCKET, &empty_folder);
    let bucket_contents = res.unwrap();
    assert!(bucket_contents.is_empty())
}

#[tokio::test]
async fn test_handle_list_objects_async() {
    let s3client = common::get_s3client();
    let filename = "test1mb.bin";
    let filepath = format!("tests/data/{}", filename);
    let not_empty_folder = "test_handle_list_objects/not_empty_folder";
    let not_empty_s3_key = format!("{}/{}", not_empty_folder, filename);
    let empty_folder = "test_handle_list_objects/empty_folder";

    let res = upload(
        s3client.as_ref(),
        common::TEST_BUCKET,
        &not_empty_s3_key,
        &filepath,
    )
    .await;
    assert!(res.is_ok());

    let res = list_objects(s3client.as_ref(), common::TEST_BUCKET, &not_empty_folder);
    let bucket_contents = res.await.unwrap();
    assert_eq!(1, bucket_contents.len());
    assert_eq!(
        "test_handle_list_objects/not_empty_folder/test1mb.bin",
        bucket_contents[0]
    );

    let res = list_objects(s3client.as_ref(), common::TEST_BUCKET, &empty_folder);
    let bucket_contents = res.await.unwrap();
    assert!(bucket_contents.is_empty())
}

#[tokio::test]
async fn test_handle_stream_objects() {
    let s3client = common::get_s3client();
    // Test data created with ./tests/scripts/populate_stream_obj_test_data.bash
    let folder = "test_handle_stream_objects";

    let mut stream = list_objects_stream(s3client.as_ref(), common::TEST_BUCKET, folder);

    let objs = stream.try_next().await.unwrap().unwrap();
    assert_eq!(objs.len(), 1000);

    let objs = stream.try_next().await.unwrap().unwrap();
    assert_eq!(objs.len(), 10);
}
