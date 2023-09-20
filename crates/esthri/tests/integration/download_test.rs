use tempdir::TempDir;

use esthri::{blocking, download, opts::*};
use esthri_test::{validate_key_hash_pairs, KeyHashPair};

#[test]
fn test_download() {
    let s3client = esthri_test::get_s3client();
    let filename = "test_file.txt";
    let filepath = esthri_test::test_data(filename);
    let s3_key = format!("test_folder/{}", filename);
    let opts = EsthriGetOptParamsBuilder::default().build().unwrap();

    let res = blocking::download(
        s3client.as_ref(),
        esthri_test::TEST_BUCKET,
        s3_key,
        filepath,
        opts,
    );
    assert!(res.is_ok());
}

#[test]
fn test_download_to_nonexistent_path() {
    let s3client = esthri_test::get_s3client();
    let filename = "test_file.txt";
    let tmpdir = TempDir::new("esthri_tmp").expect("creating temporary directory");
    let filepath = tmpdir
        .path()
        .join("some/extra/directories/that/dont/exist/");
    let s3_key = format!("test_folder/{}", filename);
    let opts = EsthriGetOptParamsBuilder::default().build().unwrap();

    let res = blocking::download(
        s3client.as_ref(),
        esthri_test::TEST_BUCKET,
        s3_key,
        &filepath,
        opts,
    );
    assert!(res.is_ok());
    assert!(filepath.join(filename).exists());
}

#[test]
fn test_download_to_current_directory() {
    let s3client = esthri_test::get_s3client();
    let filename = "test_file.txt";
    let _tmp_dir = esthri_test::EphemeralTempDir::pushd();
    let filepath = ".";
    let s3_key = format!("test_folder/{}", filename);
    let opts = EsthriGetOptParamsBuilder::default().build().unwrap();

    let res = blocking::download(
        s3client.as_ref(),
        esthri_test::TEST_BUCKET,
        s3_key,
        filepath,
        opts,
    );
    assert!(res.is_ok());
    assert!(_tmp_dir.temp_dir.path().join(filename).exists());
}

#[tokio::test]
async fn test_download_async() {
    let s3client = esthri_test::get_s3client_async().await;
    let filename = "test_file.txt";
    let filepath = esthri_test::test_data(filename);
    let s3_key = format!("test_folder/{}", filename);
    let opts = EsthriGetOptParamsBuilder::default().build().unwrap();

    let res = download(
        s3client.as_ref(),
        esthri_test::TEST_BUCKET,
        &s3_key,
        &filepath,
        opts,
    )
    .await;
    assert!(res.is_ok());
}

#[tokio::test]
async fn test_download_zero_size() {
    let s3client = esthri_test::get_s3client_async().await;
    let _tmp_dir = esthri_test::EphemeralTempDir::pushd();
    let opts = EsthriGetOptParamsBuilder::default().build().unwrap();

    // Test object `test_download/test0b.bin` must be prepopulated in the S3 bucket
    let res = download(
        s3client.as_ref(),
        esthri_test::TEST_BUCKET,
        "test_download/test0b.bin",
        "test0b.download",
        opts,
    )
    .await;
    assert!(res.is_ok());

    let stat = std::fs::metadata("test0b.download");
    assert!(stat.is_ok());

    let stat = stat.unwrap();
    assert_eq!(stat.len(), 0);
}

#[test]
fn test_download_decompressed() {
    let s3client = esthri_test::get_s3client();
    let _tmp_dir = esthri_test::EphemeralTempDir::pushd();

    // Test object `test_download/27-185232-msg.csv.gz` must be prepopulated in the S3 bucket
    let filename = "27-185232-msg.csv";
    let s3_key = format!("test_download/{}.gz", filename);
    let opts = EsthriGetOptParamsBuilder::default()
        .transparent_compression(true)
        .build()
        .unwrap();

    let res = blocking::download(
        s3client.as_ref(),
        esthri_test::TEST_BUCKET,
        s3_key,
        filename,
        opts,
    );
    assert!(res.is_ok());

    let etag = blocking::compute_etag(filename).unwrap();
    assert_eq!(etag, "\"6dbb4258fa16030c2daf6f1eac93dddd-8\"");
}

#[test]
fn test_download_decompressed_to_directory() {
    let s3client = esthri_test::get_s3client();
    let _tmp_dir = esthri_test::EphemeralTempDir::pushd();

    // Test object `test_download/27-185232-msg.csv.gz` must be prepopulated in the S3 bucket
    let filename = "27-185232-msg.csv.gz";
    let s3_key = format!("test_download/{}", filename);
    let opts = EsthriGetOptParamsBuilder::default()
        .transparent_compression(true)
        .build()
        .unwrap();

    let res = blocking::download(
        s3client.as_ref(),
        esthri_test::TEST_BUCKET,
        s3_key,
        ".",
        opts,
    );
    assert!(res.is_ok());

    let etag = blocking::compute_etag(filename).unwrap();
    assert_eq!(etag, "\"6dbb4258fa16030c2daf6f1eac93dddd-8\"");
}

#[test]
fn test_download_transparent_with_non_compressed() {
    let s3client = esthri_test::get_s3client();

    let local_dir = TempDir::new("esthri_cli").unwrap();
    let local_dir_path = local_dir.path().as_os_str().to_str().unwrap();
    let filename = "test_file.txt";
    let s3_key = format!("test_folder/{}", filename);
    let opts = EsthriGetOptParamsBuilder::default()
        .transparent_compression(true)
        .build()
        .unwrap();

    let res = blocking::download(
        s3client.as_ref(),
        esthri_test::TEST_BUCKET,
        s3_key,
        local_dir_path,
        opts,
    );
    assert!(res.is_ok());

    let key_hash_pairs = [KeyHashPair(
        "test_file.txt",
        "8fd41740698064016b7daaddddd3531a",
    )];

    validate_key_hash_pairs(local_dir_path, &key_hash_pairs);
}
