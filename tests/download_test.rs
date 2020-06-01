#![cfg_attr(feature = "aggressive_lint", deny(warnings))]

use esthri_lib::s3_download;
use esthri_lib::s3_sync;
use esthri_lib::types::SyncDirection;

mod common;

#[test]
fn test_download() {
    let s3client = common::get_s3client();

    let filename = "test_file.txt";
    let filepath = format!("tests/data/{}", filename);

    let s3_key = format!("test_folder/{}", filename);

    let res = s3_download(s3client.as_ref(), common::TEST_BUCKET, &s3_key, &filepath);

    assert!(res.is_ok());
}

#[test]
fn test_sync_down() {
    let s3client = common::get_s3client();

    let local_directory = "tests/data/";

    let s3_key = "test_folder/";
    let includes: Option<Vec<String>> = Some(vec!["*.txt".to_string()]);
    let excludes: Option<Vec<String>> = Some(vec!["*".to_string()]);
    let res = s3_sync(
        s3client.as_ref(),
        SyncDirection::down,
        common::TEST_BUCKET,
        &s3_key,
        &local_directory,
        &includes,
        &excludes,
    );
    assert!(res.is_ok(), format!("s3_sync result: {:?}", res));
}


#[test]
fn test_sync_down_fail() {
    let s3client = common::get_s3client();
    let local_directory = "tests/data/";
    let s3_key = "test_folder";
    let res = s3_sync(
        s3client.as_ref(),
        SyncDirection::down,
        common::TEST_BUCKET,
        &s3_key,
        &local_directory,
        &None,
        &None,
    );
    assert!(res.is_err());
}

#[test]
fn test_sync_up_fail() {
    let s3client = common::get_s3client();
    let local_directory = "tests/data/";
    let s3_key = "test_folder";
    let res = s3_sync(
        s3client.as_ref(),
        SyncDirection::up,
        common::TEST_BUCKET,
        &local_directory,
        &s3_key,
        &None,
        &None,
    );
    assert!(res.is_err());
}

#[test]
fn test_sync_up() {
    let s3client = common::get_s3client();

    let local_directory = "tests/data/";

    let s3_key = "test_folder/";
    let includes: Option<Vec<String>> = Some(vec!["*.txt".to_string()]);
    let excludes: Option<Vec<String>> = Some(vec!["*".to_string()]);
    let res = s3_sync(
        s3client.as_ref(),
        SyncDirection::up,
        common::TEST_BUCKET,
        &s3_key,
        &local_directory,
        &includes,
        &excludes,
    );

    assert!(res.is_ok());
}
