#![cfg(feature = "s3serve")]

use esthri_lib::upload;
use esthri_lib::http_server::esthri_filter;

mod common;

#[tokio::test]
async fn test_fetch_object() {
 
    let filename = "tests/data/test_file.txt".to_owned();

    let s3client = common::get_s3client();
    let bucket= common::TEST_BUCKET;

    let s3_key = "test_file.txt".to_owned();

    let res = upload(s3client.as_ref(), common::TEST_BUCKET, &s3_key, &filename).await;
    assert!(res.is_ok());

    let filter = esthri_filter((*s3client).clone(), bucket);

    let mut result = warp::test::request()
        .path("/test_file.txt")
        .filter(&filter)
        .await
        .unwrap();

    let body = hyper::body::to_bytes(result.body_mut()).await.unwrap();
    assert_eq!(body, "this file has contents\n");
}