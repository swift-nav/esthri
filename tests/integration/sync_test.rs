#![cfg_attr(feature = "aggressive_lint", deny(warnings))]

use std::fs;

use esthri_lib::{blocking, sync, SyncParam};

use crate::{validate_key_hash_pairs, KeyHashPair};

#[test]
fn test_sync_down() {
    let s3client = crate::get_s3client();
    let local_directory = "tests/data/";
    let s3_key = "test_folder/";
    let includes: Option<Vec<String>> = Some(vec!["*.txt".to_string()]);
    let excludes: Option<Vec<String>> = Some(vec!["*".to_string()]);

    let source = SyncParam::Local {
        path: local_directory.to_owned(),
    };
    let destination = SyncParam::Bucket {
        bucket: crate::TEST_BUCKET.to_owned(),
        path: s3_key.to_owned(),
    };

    let res = blocking::sync(s3client.as_ref(), source, destination, &includes, &excludes);
    assert!(res.is_ok(), format!("s3_sync result: {:?}", res));
}

#[tokio::test]
async fn test_sync_down_async() {
    let s3client = crate::get_s3client();
    let local_directory = "tests/data/";
    let s3_key = "test_folder/";
    let includes: Option<Vec<String>> = Some(vec!["*.txt".to_string()]);
    let excludes: Option<Vec<String>> = Some(vec!["*".to_string()]);

    let source = SyncParam::Local {
        path: local_directory.to_owned(),
    };
    let destination = SyncParam::Bucket {
        bucket: crate::TEST_BUCKET.to_owned(),
        path: s3_key.to_owned(),
    };

    let res = sync(s3client.as_ref(), source, destination, &includes, &excludes).await;
    assert!(res.is_ok(), format!("s3_sync result: {:?}", res));
}

#[test]
fn test_sync_down_fail() {
    let s3client = crate::get_s3client();
    let local_directory = "tests/data/";
    let s3_key = "test_folder";

    let source = SyncParam::Local {
        path: local_directory.to_owned(),
    };
    let destination = SyncParam::Bucket {
        bucket: crate::TEST_BUCKET.to_owned(),
        path: s3_key.to_owned(),
    };

    let res = blocking::sync(s3client.as_ref(), source, destination, &None, &None);
    assert!(res.is_err());
}

#[test]
fn test_sync_up_fail() {
    let s3client = crate::get_s3client();
    let local_directory = "tests/data/";
    let s3_key = "test_folder";

    let source = SyncParam::Local {
        path: local_directory.to_owned(),
    };
    let destination = SyncParam::Bucket {
        bucket: crate::TEST_BUCKET.to_owned(),
        path: s3_key.to_owned(),
    };

    let res = blocking::sync(s3client.as_ref(), source, destination, &None, &None);
    assert!(res.is_err());
}

#[test]
fn test_sync_up() {
    let s3client = crate::get_s3client();
    let local_directory = "tests/data/";
    let s3_key = "test_folder/";
    let includes: Option<Vec<String>> = Some(vec!["*.txt".to_string()]);
    let excludes: Option<Vec<String>> = Some(vec!["*".to_string()]);

    let source = SyncParam::Local {
        path: local_directory.to_owned(),
    };
    let destination = SyncParam::Bucket {
        bucket: crate::TEST_BUCKET.to_owned(),
        path: s3_key.to_owned(),
    };

    let res = blocking::sync(s3client.as_ref(), source, destination, &includes, &excludes);
    assert!(res.is_ok());
}

#[tokio::test]
async fn test_sync_up_async() {
    let s3client = crate::get_s3client();
    let local_directory = "tests/data/";
    let s3_key = "test_folder/";
    let includes: Option<Vec<String>> = Some(vec!["*.txt".to_string()]);
    let excludes: Option<Vec<String>> = Some(vec!["*".to_string()]);

    let source = SyncParam::Local {
        path: local_directory.to_owned(),
    };
    let destination = SyncParam::Bucket {
        bucket: crate::TEST_BUCKET.to_owned(),
        path: s3_key.to_owned(),
    };

    let res = sync(s3client.as_ref(), source, destination, &includes, &excludes).await;
    assert!(res.is_ok());
}

#[test]
fn test_sync_up_default() {
    let s3client = crate::get_s3client();
    let local_directory = "tests/data/sync_up";
    let s3_key = "test_sync_up_default/";

    let source = SyncParam::Local {
        path: local_directory.to_owned(),
    };
    let destination = SyncParam::Bucket {
        bucket: crate::TEST_BUCKET.to_owned(),
        path: s3_key.to_owned(),
    };

    let res = blocking::sync(s3client.as_ref(), source, destination, &None, &None);
    assert!(res.is_ok());

    let key_hash_pairs = [
        KeyHashPair("1-one.data", "\"827aa1b392c93cb25d2348bdc9b907b0\""),
        KeyHashPair("2-two.bin", "\"35500e07a35b413fc5f434397a4c6bfa\""),
        KeyHashPair("3-three.junk", "\"388f9763d78cecece332459baecb4b85\""),
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

    let source = SyncParam::Local {
        path: local_directory.to_owned(),
    };
    let destination = SyncParam::Bucket {
        bucket: crate::TEST_BUCKET.to_owned(),
        path: s3_key.to_owned(),
    };

    let res = blocking::sync(s3client.as_ref(), source, destination, &None, &None);
    assert!(res.is_ok());

    let key_hash_pairs = [
        KeyHashPair("1-one.data", "827aa1b392c93cb25d2348bdc9b907b0"),
        KeyHashPair("2-two.bin", "35500e07a35b413fc5f434397a4c6bfa"),
        KeyHashPair("3-three.junk", "388f9763d78cecece332459baecb4b85"),
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

    let source = SyncParam::Bucket {
        bucket: crate::TEST_BUCKET.to_owned(),
        path: source_prefix.to_owned(),
    };
    let destination = SyncParam::Bucket {
        bucket: crate::TEST_BUCKET.to_owned(),
        path: dest_prefix.to_owned(),
    };

    let res = sync(s3client.as_ref(), source, destination, &includes, &excludes).await;
    assert!(res.is_ok(), format!("s3_sync result: {:?}", res));
}
