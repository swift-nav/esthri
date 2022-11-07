use std::{os::unix::prelude::MetadataExt, path::PathBuf};

use esthri::{
    presign_delete, presign_get, presign_put,
    rusoto::{
        AwsCredentials, DefaultCredentialsProvider, ProvideAwsCredentials, Region, S3StorageClass,
    },
    setup_presigned_multipart_upload, upload_file_presigned_multipart_upload,
};
use reqwest::{Body, Client, Response};
use tokio::fs::File;
use tokio_util::codec::{BytesCodec, FramedRead};

#[tokio::test]
async fn test_presign_get() {
    let region = Region::default();
    let filename = "test_file.txt";
    let bucket = esthri_test::TEST_BUCKET;
    let s3_key = format!("test_folder/{}", filename);

    let creds = creds().await;

    let presigned_url = presign_get(&creds, &region, &bucket, &s3_key, None);
    let resp = reqwest::get(&presigned_url).await.unwrap();
    assert!(resp.error_for_status().is_ok());
}

#[tokio::test]
async fn test_presign_put() {
    let filename = "test5mb.bin";
    let filepath = esthri_test::test_data(filename);
    let s3_key = esthri_test::randomised_name(&format!("test_upload/{}", filename));
    let region = Region::default();
    let bucket = esthri_test::TEST_BUCKET;

    let creds = creds().await;

    let presigned_url = presign_put(&creds, &region, &bucket, &s3_key, None);
    let resp = put_file(&filepath, &presigned_url).await;
    assert!(resp.error_for_status().is_ok());
}

#[tokio::test]
async fn test_presign_delete() {
    let s3client_owned = esthri_test::get_s3client();
    let s3 = s3client_owned.as_ref();
    let filepath = esthri_test::test_data("test_file.txt");
    let s3_key = "delete_me.txt";
    let bucket = esthri_test::TEST_BUCKET;
    esthri::upload(s3, &bucket, &s3_key, &filepath)
        .await
        .unwrap();
    assert!(esthri::head_object(s3, bucket.to_owned(), s3_key.clone())
        .await
        .unwrap()
        .is_some());

    let region = Region::default();
    let creds = creds().await;

    let presigned_url = presign_delete(&creds, &region, &bucket, &s3_key, None);
    let resp = Client::new().delete(&presigned_url).send().await.unwrap();
    assert!(resp.error_for_status().is_ok());

    assert!(esthri::head_object(s3, bucket.to_owned(), s3_key)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn test_presign_multipart_upload() {
    let s3client_owned = esthri_test::get_s3client();
    let s3 = s3client_owned.as_ref();
    let filename = "test5mb.bin";
    let filepath = esthri_test::test_data(filename);
    let s3_key = esthri_test::randomised_name(&format!("test_upload/{}", filename));
    let region = Region::default();
    let bucket = esthri_test::TEST_BUCKET;

    let size = 5242880;
    let part_size = 5242880 / 2;

    let creds = creds().await;

    let urls = setup_presigned_multipart_upload(
        s3,
        &creds,
        &region,
        &bucket,
        &s3_key,
        part_size,
        size,
        None,
        None,
        S3StorageClass::Standard,
    )
    .await
    .unwrap();

    upload_file_presigned_multipart_upload(&Client::new(), urls, &filepath, part_size)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_presign_multipart() {
    let region = Region::default();
    let filename = "test_file.txt";
    let bucket = esthri_test::TEST_BUCKET;
    let s3_key = format!("test_folder/{}", filename);

    let creds = creds().await;

    let presigned_url = presign_get(&creds, &region, &bucket, &s3_key, None);
    let response = reqwest::get(&presigned_url).await.unwrap();
    assert_eq!(response.status(), 200);
}

async fn creds() -> AwsCredentials {
    DefaultCredentialsProvider::new()
        .unwrap()
        .credentials()
        .await
        .unwrap()
}

async fn put_file(filepath: &PathBuf, url: &str) -> Response {
    let file = File::open(filepath).await.unwrap();
    let file_size = file.metadata().await.unwrap().size();
    let stream = FramedRead::new(file, BytesCodec::new());
    let body = Body::wrap_stream(stream);
    let client = Client::new();
    client
        .put(url)
        .header("Content-Length", file_size.to_string())
        .body(body)
        .send()
        .await
        .unwrap()
}

async fn put_chunk(filepath: &PathBuf, url: &str) -> Response {
    let file = File::open(filepath).await.unwrap();
    let file_size = file.metadata().await.unwrap().size();
    let stream = FramedRead::new(file, BytesCodec::new());
    let body = Body::wrap_stream(stream);
    let client = Client::new();
    client
        .put(url)
        .header("Content-Length", file_size.to_string())
        .body(body)
        .send()
        .await
        .unwrap()
}
