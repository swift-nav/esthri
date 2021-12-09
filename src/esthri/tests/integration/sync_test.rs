#![cfg_attr(feature = "aggressive_lint", deny(warnings))]

use std::fs;

use esthri::{blocking, sync, GlobFilter, S3PathParam, FILTER_EMPTY};
use glob::Pattern;
use tempdir::TempDir;

use crate::{validate_key_hash_pairs, KeyHashPair};

#[test]
fn test_sync_down() {
    let s3client = crate::get_s3client();
    let local_directory = "tests/data/";
    let s3_key = "test_folder/";

    let filters: Option<Vec<GlobFilter>> = Some(vec![
        GlobFilter::Include(Pattern::new("*.txt").unwrap()),
        GlobFilter::Exclude(Pattern::new("*").unwrap()),
    ]);

    let source = S3PathParam::new_bucket(crate::TEST_BUCKET, s3_key);
    let destination = S3PathParam::new_local(local_directory);

    let res = blocking::sync(
        s3client.as_ref(),
        source,
        destination,
        filters.as_deref(),
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
    let filters: Option<Vec<GlobFilter>> = Some(vec![
        GlobFilter::Include(Pattern::new("*.txt").unwrap()),
        GlobFilter::Exclude(Pattern::new("*").unwrap()),
    ]);

    let source = S3PathParam::new_bucket(crate::TEST_BUCKET, s3_key);
    let destination = S3PathParam::new_local(local_directory);

    let res = sync(
        s3client.as_ref(),
        source,
        destination,
        filters.as_deref(),
        #[cfg(feature = "compression")]
        false,
    )
    .await;
    assert!(res.is_ok(), "s3_sync result: {:?}", res);
}

#[test]
fn test_sync_down_without_slash() {
    let s3client = crate::get_s3client();
    let local_directory = "tests/data/";
    let s3_key = "test_folder";
    let filters: Option<Vec<GlobFilter>> = Some(vec![
        GlobFilter::Include(Pattern::new("*.txt").unwrap()),
        GlobFilter::Exclude(Pattern::new("*").unwrap()),
    ]);

    let source = S3PathParam::new_bucket(crate::TEST_BUCKET, s3_key);
    let destination = S3PathParam::new_local(local_directory);

    let res = blocking::sync(
        s3client.as_ref(),
        source,
        destination,
        filters.as_deref(),
        #[cfg(feature = "compression")]
        false,
    );
    assert!(res.is_ok());
}

#[test]
fn test_sync_up_without_slash() {
    let s3client = crate::get_s3client();
    let local_directory = "tests/data/";
    let s3_key = "test_folder";
    let filters: Option<Vec<GlobFilter>> = Some(vec![
        GlobFilter::Include(Pattern::new("*.txt").unwrap()),
        GlobFilter::Exclude(Pattern::new("*").unwrap()),
    ]);

    let source = S3PathParam::new_local(local_directory);
    let destination = S3PathParam::new_bucket(crate::TEST_BUCKET, s3_key);

    let res = blocking::sync(
        s3client.as_ref(),
        source,
        destination,
        filters.as_deref(),
        #[cfg(feature = "compression")]
        false,
    );
    assert!(res.is_ok());
}

#[test]
fn test_sync_up() {
    let s3client = crate::get_s3client();
    let local_directory = "tests/data/";
    let s3_key = "test_folder/";
    let filters: Option<Vec<GlobFilter>> = Some(vec![
        GlobFilter::Include(Pattern::new("*.txt").unwrap()),
        GlobFilter::Exclude(Pattern::new("*").unwrap()),
    ]);

    let source = S3PathParam::new_local(local_directory);
    let destination = S3PathParam::new_bucket(crate::TEST_BUCKET, s3_key);

    let res = blocking::sync(
        s3client.as_ref(),
        source,
        destination,
        filters.as_deref(),
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
    let filters: Option<Vec<GlobFilter>> = Some(vec![
        GlobFilter::Include(Pattern::new("*.txt").unwrap()),
        GlobFilter::Exclude(Pattern::new("*").unwrap()),
    ]);

    let source = S3PathParam::new_local(local_directory);
    let destination = S3PathParam::new_bucket(crate::TEST_BUCKET, s3_key);

    let res = sync(
        s3client.as_ref(),
        source,
        destination,
        filters.as_deref(),
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

    let source = S3PathParam::new_local(local_directory);
    let destination = S3PathParam::new_bucket(crate::TEST_BUCKET, s3_key);

    let res = blocking::sync(
        s3client.as_ref(),
        source,
        destination,
        FILTER_EMPTY,
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

    let source = S3PathParam::new_bucket(crate::TEST_BUCKET, s3_key);
    let destination = S3PathParam::new_local(local_directory);

    let res = blocking::sync(
        s3client.as_ref(),
        source,
        destination,
        FILTER_EMPTY,
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

#[test]
fn test_sync_down_filter() {
    let s3client = crate::get_s3client();
    let local_dir = TempDir::new("esthri_cli").unwrap();

    let s3_key = "test_sync_down_default/";

    let filters: Option<Vec<GlobFilter>> =
        Some(vec![GlobFilter::Exclude(Pattern::new("*.bin").unwrap())]);

    let source = S3PathParam::new_bucket(crate::TEST_BUCKET, s3_key);
    let destination = S3PathParam::new_local(&local_dir);

    let res = blocking::sync(
        s3client.as_ref(),
        source,
        destination,
        filters.as_deref(),
        #[cfg(feature = "compression")]
        false,
    );
    assert!(res.is_ok());

    let key_hash_pairs = [
        KeyHashPair("1-one.data", "827aa1b392c93cb25d2348bdc9b907b0"),
        KeyHashPair("3-three.junk", "388f9763d78cecece332459baecb4b85"),
    ];

    validate_key_hash_pairs(&local_dir.path().to_string_lossy(), &key_hash_pairs);
}

#[tokio::test]
async fn test_sync_across() {
    let s3client = crate::get_s3client();
    let source_prefix = "test_sync_folder1/";
    let dest_prefix = "test_sync_folder2/";

    let filters: Option<Vec<GlobFilter>> =
        Some(vec![GlobFilter::Include(Pattern::new("*.txt").unwrap())]);

    let source = S3PathParam::new_bucket(crate::TEST_BUCKET, source_prefix);
    let destination = S3PathParam::new_bucket(crate::TEST_BUCKET, dest_prefix);

    let res = sync(
        s3client.as_ref(),
        source,
        destination,
        filters.as_deref(),
        #[cfg(feature = "compression")]
        false,
    )
    .await;
    assert!(res.is_ok(), "s3_sync result: {:?}", res);
}

#[cfg(feature = "compression")]
fn sync_test_files_up_compressed(s3client: &rusoto_s3::S3Client, s3_key: &str) -> String {
    let data_dir = "tests/data/sync_up/";
    let temp_data_dir = "sync_up_compressed/";
    let old_cwd = std::env::current_dir().unwrap();
    let data_dir_fp = old_cwd.join(data_dir);
    eprintln!("{:?}", old_cwd);
    let tmp_dir = crate::EphemeralTempDir::pushd();
    let temp_data_dir_fp = tmp_dir.temp_dir.path().join(temp_data_dir);
    eprintln!("temp_data_dir: {:?}", temp_data_dir_fp);
    let mut opts = fs_extra::dir::CopyOptions::new();
    opts.copy_inside = true;
    fs_extra::dir::copy(data_dir_fp, temp_data_dir, &opts).unwrap();
    let source = S3PathParam::new_local(temp_data_dir);
    let destination = S3PathParam::new_bucket(crate::TEST_BUCKET, s3_key);
    let res = blocking::sync(s3client, source, destination, FILTER_EMPTY, true);
    assert!(res.is_ok());
    s3_key.to_string()
}

#[cfg(feature = "compression")]
#[test]
fn test_sync_up_compressed() {
    let s3client = crate::get_s3client();
    let s3_key = sync_test_files_up_compressed(s3client.as_ref(), "test_sync_up_compressed/");

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

/// Test downloading files that were synced up with compression enabled
/// These should download as the original, uncompressed file
#[cfg(feature = "compression")]
#[test]
fn test_sync_down_compressed() {
    let s3client = crate::get_s3client();
    let s3_key = "test_sync_down_compressed/";

    sync_test_files_up_compressed(s3client.as_ref(), s3_key);

    let local_directory = "tests/data/sync_down/d";
    let sync_dir_meta = fs::metadata(local_directory);

    if let Ok(sync_dir_meta) = sync_dir_meta {
        assert!(sync_dir_meta.is_dir());
        assert!(fs::remove_dir_all(local_directory).is_ok());
    }
    let destination = S3PathParam::new_local(local_directory);

    let res = blocking::sync(
        s3client.as_ref(),
        S3PathParam::new_bucket(crate::TEST_BUCKET, s3_key),
        destination,
        FILTER_EMPTY,
        #[cfg(feature = "compression")]
        true,
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

/// Test syncing down a mix of compressed and non-compressed files
#[cfg(feature = "compression")]
#[test]
fn test_sync_down_compressed_mixed() {
    let s3client = crate::get_s3client();

    let local_directory = "tests/data/sync_down/d";
    let sync_dir_meta = fs::metadata(local_directory);

    if let Ok(sync_dir_meta) = sync_dir_meta {
        assert!(sync_dir_meta.is_dir());
        assert!(fs::remove_dir_all(local_directory).is_ok());
    }
    let destination = S3PathParam::new_local(local_directory);

    // Test was data populated with the following command:
    //
    //     aws s3 cp compressed.html.gz s3://esthri-test/test_sync_down_mixed_compressed/
    //     aws s3 cp test_file.txt      s3://esthri-test/test_sync_down_mixed_compressed/
    //
    let res = blocking::sync(
        s3client.as_ref(),
        S3PathParam::new_bucket(crate::TEST_BUCKET, "test_sync_down_mixed_compressed"),
        destination,
        FILTER_EMPTY,
        #[cfg(feature = "compression")]
        true,
    );
    assert!(res.is_ok());

    let key_hash_pairs = [
        KeyHashPair("compressed.html", "b4e3f354e8575e2fa5f489ab6078917c"),
        KeyHashPair("test_file.txt", "8fd41740698064016b7daaddddd3531a"),
    ];

    validate_key_hash_pairs(local_directory, &key_hash_pairs);
}
