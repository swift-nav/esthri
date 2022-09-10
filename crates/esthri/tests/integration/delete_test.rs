use std::sync::Arc;

use futures::{stream, StreamExt};

use esthri::Result;
use esthri_internals::rusoto::S3Client;

#[tokio::test]
async fn test_delete() {
    let (s3client, s3_key, bucket, s3_key2) = upload_test_data().await;
    let s3client = s3client.as_ref();
    let result = esthri::delete(s3client, &bucket, &vec![&s3_key, &s3_key2]).await;
    assert!(result.is_ok());
    let result = esthri::head_object(s3client, &bucket, &s3_key).await;
    assert!(result.unwrap().is_none());
    let result = esthri::head_object(s3client, &bucket, &s3_key2).await;
    assert!(result.unwrap().is_none());
}

#[tokio::test]
async fn test_delete_streaming() {
    let (s3client, s3_key, bucket, s3_key2) = upload_test_data().await;
    let s3client = s3client.as_ref();
    let stream = stream::iter(vec![s3_key.clone(), s3_key2.clone()]);
    let result = esthri::delete_streaming(s3client, bucket, stream).await;
    let result: Vec<Result<usize>> = result.buffered(1).collect().await;
    assert_eq!(*result[0].as_ref().unwrap(), 2);
}

async fn upload_test_data() -> (Arc<S3Client>, String, String, String) {
    let s3client_owned = esthri_test::get_s3client();
    let s3client = s3client_owned.as_ref();
    let filepath = "test_file.txt";
    let filepath = esthri_test::test_data(filepath);
    let s3_key = "delete_me.txt";
    let bucket = esthri_test::TEST_BUCKET;
    let res = esthri::upload(s3client, &bucket, &s3_key, &filepath).await;
    assert!(res.is_ok());
    let s3_key2 = "delete_me2.txt";
    let res = esthri::upload(s3client, &bucket, &s3_key, &filepath).await;
    assert!(res.is_ok());
    (s3client_owned, s3_key.into(), bucket.into(), s3_key2.into())
}
