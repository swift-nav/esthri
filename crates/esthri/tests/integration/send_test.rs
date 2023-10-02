use aws_sdk_s3::types::StorageClass;
use esthri::opts::{
    EsthriGetOptParamsBuilder, EsthriPutOptParamsBuilder, SharedSyncOptParamsBuilder,
};
use esthri::{GlobFilter, HeadObjectInfo, S3PathParam};
use glob::Pattern;

#[tokio::test]
async fn test_send_download() {
    let s3client = esthri_test::get_s3client_async().await;
    let filename = "test_file.txt";
    let filepath = esthri_test::test_data(filename);
    let s3_bucket = esthri_test::TEST_BUCKET;
    let s3_key = format!("test_folder/{}", filename);
    let opts = EsthriGetOptParamsBuilder::default().build().unwrap();
    tokio::spawn(async move {
        let res = esthri::download(&s3client, s3_bucket, s3_key, filepath, opts).await;
        assert!(res.is_ok());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_send_upload() {
    let s3client = esthri_test::get_s3client_async().await;
    let filename = "test5mb.bin";
    let filepath = esthri_test::test_data(filename);
    let s3_bucket = esthri_test::TEST_BUCKET;
    let s3_key = esthri_test::randomised_lifecycled_prefix(&format!("test_upload/{}", filename));
    let opts = EsthriPutOptParamsBuilder::default().build().unwrap();
    tokio::spawn(async move {
        let res = esthri::upload(&s3client, s3_bucket, &s3_key, filepath, opts).await;
        assert!(res.is_ok());

        let res = esthri::head_object(&s3client, s3_bucket, &s3_key);
        let obj_info: Option<HeadObjectInfo> = res.await.unwrap();
        assert!(obj_info.is_some());
        let obj_info: HeadObjectInfo = obj_info.unwrap();

        assert_eq!(obj_info.size, 5242880);
        assert_eq!(obj_info.e_tag, "\"8542c49db935a57bb8c26ec68d39aaea\"");
        assert_eq!(obj_info.storage_class, StorageClass::Standard);
        assert!(!obj_info.metadata.contains_key("esthri_compress_version"));
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_send_sync_down() {
    let s3client = esthri_test::get_s3client_async().await;
    let local_directory = esthri_test::test_data_dir();
    let s3_key = "test_folder/";
    let filters: Option<Vec<GlobFilter>> = Some(vec![
        GlobFilter::Include(Pattern::new("*.txt").unwrap()),
        GlobFilter::Exclude(Pattern::new("*").unwrap()),
    ]);
    let source = S3PathParam::new_bucket(esthri_test::TEST_BUCKET, s3_key);
    let destination = S3PathParam::new_local(local_directory);
    let opts = SharedSyncOptParamsBuilder::default().build().unwrap();
    tokio::spawn(async move {
        let res = esthri::sync(&s3client, source, destination, filters.as_deref(), opts).await;
        assert!(res.is_ok(), "s3_sync result: {:?}", res);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_send_sync_up() {
    let s3client = esthri_test::get_s3client_async().await;
    let local_directory = esthri_test::test_data_dir();
    let s3_key = esthri_test::randomised_lifecycled_prefix("test_folder/");
    let filters: Option<Vec<GlobFilter>> = Some(vec![
        GlobFilter::Include(Pattern::new("*.txt").unwrap()),
        GlobFilter::Exclude(Pattern::new("*").unwrap()),
    ]);
    let source = S3PathParam::new_local(&local_directory);
    let destination = S3PathParam::new_bucket(esthri_test::TEST_BUCKET, s3_key);
    let opts = SharedSyncOptParamsBuilder::default().build().unwrap();
    tokio::spawn(async move {
        let res = esthri::sync(&s3client, source, destination, filters.as_deref(), opts).await;
        assert!(res.is_ok());
    })
    .await
    .unwrap();
}
