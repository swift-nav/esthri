#![cfg_attr(feature = "aggressive_lint", deny(warnings))]

mod delete_test;
mod download_test;
mod list_object_test;
mod presign;
mod send_test;
mod sync_test;
mod upload_test;

#[tokio::test]
async fn test_get_bucket_location() {
    let s3client = esthri_test::get_s3client_async().await;
    let bucket = esthri_test::TEST_BUCKET;
    let location = esthri::aws_sdk::get_bucket_location(s3client.as_ref(), bucket)
        .await
        .expect("get_bucket_location failed");
    assert_eq!(location, "us-west-2");
}
