#![cfg_attr(feature = "aggressive_lint", deny(warnings))]

use std::fs;

use esthri::{blocking, sync, SyncParam, EXCLUDE_EMPTY, INCLUDE_EMPTY};

use crate::{validate_key_hash_pairs, KeyHashPair};

#[test]
fn test_sync_down() {
    let s3client = crate::get_s3client();
    let local_directory = "tests/data/";
    let s3_key = "test_folder/";
    let includes: Option<Vec<String>> = Some(vec!["*.txt".to_string()]);
    let excludes: Option<Vec<String>> = Some(vec!["*".to_string()]);

    let source = SyncParam::new_bucket(crate::TEST_BUCKET, s3_key);
    let destination = SyncParam::new_local(local_directory);

    let res = blocking::sync(
        s3client.as_ref(),
        source,
        destination,
        includes.as_deref(),
        excludes.as_deref(),
        #[cfg(feature = "compression")]
        false,
    );
    assert!(res.is_ok(), "s3_sync result: {:?}", res);
}

#[tokio::test]
async fn test_sync_down_async() {
    let s3client = crate::get_s3client();
    let local_directory = "tests/data/";
    let s3_key = "test_folder/";
    let includes: Option<Vec<String>> = Some(vec!["*.txt".to_string()]);
    let excludes: Option<Vec<String>> = Some(vec!["*".to_string()]);

    let source = SyncParam::new_bucket(crate::TEST_BUCKET, s3_key);
    let destination = SyncParam::new_local(local_directory);

    let res = sync(
        s3client.as_ref(),
        source,
        destination,
        includes.as_deref(),
        excludes.as_deref(),
        #[cfg(feature = "compression")]
        false,
    )
    .await;
    assert!(res.is_ok(), "s3_sync result: {:?}", res);
}

#[test]
fn test_sync_down_fail() {
    let s3client = crate::get_s3client();
    let local_directory = "tests/data/";
    let s3_key = "test_folder";

    let source = SyncParam::new_bucket(crate::TEST_BUCKET, s3_key);
    let destination = SyncParam::new_local(local_directory);

    let res = blocking::sync(
        s3client.as_ref(),
        source,
        destination,
        INCLUDE_EMPTY,
        EXCLUDE_EMPTY,
        #[cfg(feature = "compression")]
        false,
    );
    assert!(res.is_err());
}

#[test]
fn test_sync_up_fail() {
    let s3client = crate::get_s3client();
    let local_directory = "tests/data/";
    let s3_key = "test_folder";

    let source = SyncParam::new_local(local_directory);
    let destination = SyncParam::new_bucket(crate::TEST_BUCKET, s3_key);

    let res = blocking::sync(
        s3client.as_ref(),
        source,
        destination,
        INCLUDE_EMPTY,
        EXCLUDE_EMPTY,
        #[cfg(feature = "compression")]
        false,
    );
    assert!(res.is_err());
}

#[test]
fn test_sync_up() {
    let s3client = crate::get_s3client();
    let local_directory = "tests/data/";
    let s3_key = "test_folder/";
    let includes: Option<Vec<String>> = Some(vec!["*.txt".to_string()]);
    let excludes: Option<Vec<String>> = Some(vec!["*".to_string()]);

    let source = SyncParam::new_local(local_directory);
    let destination = SyncParam::new_bucket(crate::TEST_BUCKET, s3_key);

    let res = blocking::sync(
        s3client.as_ref(),
        source,
        destination,
        includes.as_deref(),
        excludes.as_deref(),
        #[cfg(feature = "compression")]
        false,
    );
    assert!(res.is_ok());
}

#[tokio::test]
async fn test_sync_up_async() {
    let s3client = crate::get_s3client();
    let local_directory = "tests/data/";
    let s3_key = "test_folder/";
    let includes: Option<Vec<String>> = Some(vec!["*.txt".to_string()]);
    let excludes: Option<Vec<String>> = Some(vec!["*".to_string()]);

    let source = SyncParam::new_local(local_directory);
    let destination = SyncParam::new_bucket(crate::TEST_BUCKET, s3_key);

    let res = sync(
        s3client.as_ref(),
        source,
        destination,
        includes.as_deref(),
        excludes.as_deref(),
        #[cfg(feature = "compression")]
        false,
    )
    .await;
    assert!(res.is_ok());
}

#[test]
fn test_sync_up_default() {
    let s3client = crate::get_s3client();
    let local_directory = "tests/data/sync_up";
    let s3_key = "test_sync_up_default/";

    let source = SyncParam::new_local(local_directory);
    let destination = SyncParam::new_bucket(crate::TEST_BUCKET, s3_key);

    let res = blocking::sync(
        s3client.as_ref(),
        source,
        destination,
        INCLUDE_EMPTY,
        EXCLUDE_EMPTY,
        #[cfg(feature = "compression")]
        false,
    );
    assert!(res.is_ok());

    let key_hash_pairs = [
        KeyHashPair("1-one.data", "\"827aa1b392c93cb25d2348bdc9b907b0\""),
        KeyHashPair("2-two.bin", "\"35500e07a35b413fc5f434397a4c6bfa\""),
        KeyHashPair("3-three.junk", "\"388f9763d78cecece332459baecb4b85\""),
        KeyHashPair("nested/2MiB.bin", "\"64a2635e42ef61c69d62feebdbf118d4\""),
    ];

    for key_hash_pair in &key_hash_pairs[..] {
        let key = format!("{}{}", s3_key, key_hash_pair.0);
        let res = blocking::head_object(s3client.as_ref(), crate::TEST_BUCKET, &key);
        assert!(res.is_ok(), "fetching s3 etag failed for: {}", key);
        let res = res.unwrap();
        assert!(res.is_some(), "s3 etag returned was nil for: {}", key);
        assert_eq!(res.unwrap().e_tag, key_hash_pair.1, "invalid hash: {}", key);
    }
}

#[test]
fn test_sync_down_default() {
    let s3client = crate::get_s3client();
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

    let source = SyncParam::new_bucket(crate::TEST_BUCKET, s3_key);
    let destination = SyncParam::new_local(local_directory);

    let res = blocking::sync(
        s3client.as_ref(),
        source,
        destination,
        INCLUDE_EMPTY,
        EXCLUDE_EMPTY,
        #[cfg(feature = "compression")]
        false,
    );
    assert!(res.is_ok());

    let key_hash_pairs = [
        KeyHashPair("1-one.data", "827aa1b392c93cb25d2348bdc9b907b0"),
        KeyHashPair("2-two.bin", "35500e07a35b413fc5f434397a4c6bfa"),
        KeyHashPair("3-three.junk", "388f9763d78cecece332459baecb4b85"),
        KeyHashPair("nested/2MiB.bin", "64a2635e42ef61c69d62feebdbf118d4"),
    ];

    validate_key_hash_pairs(local_directory, &key_hash_pairs);
}

#[tokio::test]
async fn test_sync_across() {
    let s3client = crate::get_s3client();
    let source_prefix = "test_sync_folder1/";
    let dest_prefix = "test_sync_folder2/";

    let includes: Option<Vec<String>> = Some(vec!["*.txt".to_string()]);
    let excludes: Option<Vec<String>> = None;

    let source = SyncParam::new_bucket(crate::TEST_BUCKET, source_prefix);
    let destination = SyncParam::new_bucket(crate::TEST_BUCKET, dest_prefix);

    let res = sync(
        s3client.as_ref(),
        source,
        destination,
        includes.as_deref(),
        excludes.as_deref(),
        #[cfg(feature = "compression")]
        false,
    )
    .await;
    assert!(res.is_ok(), "s3_sync result: {:?}", res);
}

#[cfg(feature = "compression")]
#[test]
fn test_sync_up_compressed() {
    let s3client = crate::get_s3client();
    let data_dir = "tests/data/sync_up/";
    let temp_data_dir = "sync_up_compressed/";
    let s3_key = "test_sync_up_compressed/";

    let old_cwd = std::env::current_dir().unwrap();
    let data_dir_fp = old_cwd.join(data_dir);

    eprintln!("{:?}", old_cwd);

    let tmp_dir = crate::EphemeralTempDir::pushd();
    let temp_data_dir_fp = tmp_dir.temp_dir.path().join(temp_data_dir);

    eprintln!("temp_data_dir: {:?}", temp_data_dir_fp);

    let mut opts = fs_extra::dir::CopyOptions::new();
    opts.copy_inside = true;

    fs_extra::dir::copy(data_dir_fp, temp_data_dir, &opts).unwrap();

    let source = SyncParam::new_local(temp_data_dir);
    let destination = SyncParam::new_bucket(crate::TEST_BUCKET, s3_key);

    let res = blocking::sync(
        s3client.as_ref(),
        source,
        destination,
        INCLUDE_EMPTY,
        EXCLUDE_EMPTY,
        true,
    );
    assert!(res.is_ok());

    let key_hash_pairs = [
        KeyHashPair("1-one.data.gz", "\"276ebe187bb53cb68e484e8c0c0fef68\""),
        KeyHashPair("2-two.bin.gz", "\"2b08f95817755fc00c1cc1e528dc7db8\""),
        KeyHashPair("3-three.junk.gz", "\"12bc292b0d53b61203b839588213a9a1\""),
        KeyHashPair("nested/2MiB.bin.gz", "\"da4b426cae11741846271040d9b4dc71\""),
    ];

    for key_hash_pair in &key_hash_pairs[..] {
        let key = format!("{}{}", s3_key, key_hash_pair.0);
        let res = blocking::head_object(s3client.as_ref(), crate::TEST_BUCKET, &key);
        assert!(res.is_ok(), "fetching s3 etag failed for: {}", key);
        let res = res.unwrap();
        assert!(res.is_some(), "s3 etag returned was nil for: {}", key);
        assert_eq!(res.unwrap().e_tag, key_hash_pair.1, "invalid hash: {}", key);
    }
}
