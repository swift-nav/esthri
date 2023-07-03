use esthri::{
    complete_presigned_multipart_upload, delete_file_presigned, download_file_presigned,
    opts::{EsthriGetOptParamsBuilder, EsthriPutOptParamsBuilder, SharedSyncOptParamsBuilder},
    presign_delete, presign_get, presign_put,
    rusoto::{AwsCredentials, DefaultCredentialsProvider, ProvideAwsCredentials, Region},
    setup_presigned_multipart_upload, upload_file_presigned,
    upload_file_presigned_multipart_upload,
};
use reqwest::Client;
use tempdir::TempDir;

#[tokio::test]
async fn test_presign_get() {
    let filename = "test_file.txt";
    let bucket = esthri_test::TEST_BUCKET;
    let s3key = esthri_test::randomised_name(&format!("test_presigned_get/{}", filename));
    let s3client = esthri_test::get_s3client();
    let opts_compress = SharedSyncOptParamsBuilder::default().build().unwrap();
    esthri::upload(
        s3client.as_ref(),
        esthri_test::TEST_BUCKET,
        &s3key,
        esthri_test::test_data(filename),
        opts_compress.into(),
    )
    .await
    .unwrap();
    let region = Region::default();
    let creds = creds().await;
    let presigned_url = presign_get(&creds, &region, bucket, &s3key, None);
    let tmpdir = TempDir::new("esthri_tmp").expect("creating temporary directory");
    let download_file_path = tmpdir.path().join(filename);
    download_file_presigned(
        &Client::new(),
        &presigned_url,
        &download_file_path,
        &EsthriGetOptParamsBuilder::default().build().unwrap(),
    )
    .await
    .unwrap();
    let file_contents = std::fs::read_to_string(download_file_path).unwrap();
    assert_eq!(file_contents, "this file has contents\n");
}

#[tokio::test]
async fn test_presign_put() {
    let s3client_owned = esthri_test::get_s3client();
    let s3 = s3client_owned.as_ref();
    let filename = "test5mb.bin";
    let filepath = esthri_test::test_data(filename);
    let s3_key = esthri_test::randomised_name(&format!("test_upload/{}", filename));
    let region = Region::default();
    let bucket = esthri_test::TEST_BUCKET;
    let creds = creds().await;
    let opts = EsthriPutOptParamsBuilder::default().build().unwrap();
    let presigned_url = presign_put(&creds, &region, bucket, &s3_key, None, opts);
    upload_file_presigned(&Client::new(), &presigned_url, &filepath, opts)
        .await
        .unwrap();
    assert_eq!(
        esthri::head_object(s3, bucket.to_owned(), s3_key.clone())
            .await
            .unwrap()
            .unwrap()
            .size,
        5242880
    );
}

#[tokio::test]
async fn test_presign_delete() {
    let s3client_owned = esthri_test::get_s3client();
    let s3 = s3client_owned.as_ref();
    let filepath = esthri_test::test_data("test_file.txt");
    let s3_key = "delete_me.txt";
    let bucket = esthri_test::TEST_BUCKET;
    esthri::upload(
        s3,
        &bucket,
        &s3_key,
        &filepath,
        EsthriPutOptParamsBuilder::default().build().unwrap(),
    )
    .await
    .unwrap();
    assert!(esthri::head_object(s3, bucket.to_owned(), s3_key)
        .await
        .unwrap()
        .is_some());
    let region = Region::default();
    let creds = creds().await;
    let presigned_url = presign_delete(&creds, &region, bucket, s3_key, None);
    delete_file_presigned(&Client::new(), &presigned_url)
        .await
        .unwrap();
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
    let part_size = size;
    let creds = creds().await;
    let upload = setup_presigned_multipart_upload(
        s3,
        &creds,
        &region,
        &bucket,
        &s3_key,
        1,
        None,
        EsthriPutOptParamsBuilder::default().build().unwrap(),
    )
    .await
    .unwrap();
    let upload =
        upload_file_presigned_multipart_upload(&Client::new(), upload, &filepath, part_size)
            .await
            .unwrap();
    complete_presigned_multipart_upload(s3, &bucket, &s3_key, upload)
        .await
        .unwrap();
    let res = esthri::head_object(s3, bucket.to_owned(), s3_key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(res.size, size as i64);
}

async fn creds() -> AwsCredentials {
    DefaultCredentialsProvider::new()
        .unwrap()
        .credentials()
        .await
        .unwrap()
}
