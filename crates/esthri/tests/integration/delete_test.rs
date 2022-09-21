use rusoto_s3::S3Client;
use tempdir::TempDir;

use esthri::blocking;

#[tokio::test]
async fn test_delete_aaaaa() {
    let s3client = esthri_test::get_s3client();

    let filename = "test_file.txt";
    let filepath = esthri_test::test_data(filename);

    let s3_key = "delete_me.txt";

    let bucket = esthri_test::TEST_BUCKET;

    let res = esthri::upload(s3client.as_ref(), &bucket, s3_key, filepath).await;
    assert!(res.is_ok());

    let result = esthri::delete(s3client.as_ref(), &bucket, &s3_key).await;
    assert!(result.is_ok());
}
