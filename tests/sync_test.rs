#![cfg_attr(feature = "aggressive_lint", deny(warnings))]

use std::fs;

use esthri_lib::types::SyncDirection;
use esthri_lib::{s3_head_object, s3_sync};

mod common;

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

#[test]
fn test_sync_up_default() {
    let s3client = common::get_s3client();
    let local_directory = "tests/data/sync_up";

    let s3_key = "test_sync_up_default/";

    let res = s3_sync(
        s3client.as_ref(),
        SyncDirection::up,
        common::TEST_BUCKET,
        &s3_key,
        &local_directory,
        &None,
        &None,
    );

    assert!(res.is_ok());

    struct KeyHashPair(&'static str, &'static str);

    let key_hash_pairs = [
        KeyHashPair("1-one.data", "\"827aa1b392c93cb25d2348bdc9b907b0\""),
        KeyHashPair("2-two.bin", "\"35500e07a35b413fc5f434397a4c6bfa\""),
        KeyHashPair("3-three.junk", "\"388f9763d78cecece332459baecb4b85\""),
    ];

    for key_hash_pair in &key_hash_pairs[..] {
        let key = format!("{}{}", s3_key, key_hash_pair.0);
        let res = s3_head_object(s3client.as_ref(), common::TEST_BUCKET, &key);

        assert!(res.is_ok(), "fetching s3 etag failed for: {}", key);

        let res = res.unwrap();
        assert!(res.is_some(), "s3 etag returned was nil for: {}", key);

        assert_eq!(res.unwrap(), key_hash_pair.1, "invalid hash: {}", key);
    }
}

#[test]
fn test_sync_down_default() {
    let s3client = common::get_s3client();
    let local_directory = "tests/data/sync_down/d";

    let sync_dir_meta = fs::metadata(local_directory);

    if let Ok(sync_dir_meta) = sync_dir_meta {
        assert!(sync_dir_meta.is_dir());
        assert!(fs::remove_dir_all(local_directory).is_ok());
    }

    // Test was data populated with the following command:
    //
    //     aws s3 cp --recursive test/data/sync_up s3://esthri-test/test_sync_down_default/
    //
    let s3_key = "test_sync_down_default/";

    let res = s3_sync(
        s3client.as_ref(),
        SyncDirection::down,
        common::TEST_BUCKET,
        &s3_key,
        &local_directory,
        &None,
        &None,
    );

    assert!(res.is_ok());

    struct KeyHashPair(&'static str, &'static str);

    let key_hash_pairs = [
        KeyHashPair("1-one.data", "827aa1b392c93cb25d2348bdc9b907b0"),
        KeyHashPair("2-two.bin", "35500e07a35b413fc5f434397a4c6bfa"),
        KeyHashPair("3-three.junk", "388f9763d78cecece332459baecb4b85"),
    ];

    for key_hash_pair in &key_hash_pairs[..] {
        let path = format!("{}/{}", local_directory, key_hash_pair.0);
        let data = fs::read(&path);

        assert!(data.is_ok());
        let data = data.unwrap();

        let digest = md5::compute(data);
        let digest = format!("{:x}", digest);

        assert_eq!(
            digest, key_hash_pair.1,
            "md5 digest did not match: {}",
            path
        );
    }
}
