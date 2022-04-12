use std::fs;

use glob::Pattern;
use tempdir::TempDir;

use esthri::{blocking, sync, GlobFilter, S3PathParam, FILTER_EMPTY};
use esthri_test::{validate_key_hash_pairs, KeyHashPair};

#[test]
fn test_sync_down() {
    let s3client = esthri_test::get_s3client();
    let local_directory = esthri_test::test_data_dir();
    let s3_key = "test_folder/";
    let s3_storage_class = "STANDARD";

    let filters: Option<Vec<GlobFilter>> = Some(vec![
        GlobFilter::Include(Pattern::new("*.txt").unwrap()),
        GlobFilter::Exclude(Pattern::new("*").unwrap()),
    ]);

    let source = S3PathParam::new_bucket(esthri_test::TEST_BUCKET, s3_key);
    let destination = S3PathParam::new_local(&local_directory);

    let res = esthri::blocking::sync(
        s3client.as_ref(),
        source,
        destination,
        filters.as_deref(),
        false,
        s3_storage_class
    );
    assert!(res.is_ok(), "s3_sync result: {:?}", res);
}

#[tokio::test]
async fn test_sync_down_async() {
    let s3client = esthri_test::get_s3client();
    let local_directory = esthri_test::test_data_dir();
    let s3_key = "test_folder/";
    let s3_storage_class = "STANDARD";

    let filters: Option<Vec<GlobFilter>> = Some(vec![
        GlobFilter::Include(Pattern::new("*.txt").unwrap()),
        GlobFilter::Exclude(Pattern::new("*").unwrap()),
    ]);

    let source = S3PathParam::new_bucket(esthri_test::TEST_BUCKET, s3_key);
    let destination = S3PathParam::new_local(&local_directory);

    let res = sync(
        s3client.as_ref(),
        source,
        destination,
        filters.as_deref(),
        false,
        s3_storage_class
    )
    .await;
    assert!(res.is_ok(), "s3_sync result: {:?}", res);
}

#[test]
fn test_sync_down_without_slash() {
    let s3client = esthri_test::get_s3client();
    let local_directory = esthri_test::test_data_dir();
    let s3_key = esthri_test::randomised_name("test_folder");
    let s3_storage_class = "STANDARD";
    let filters: Option<Vec<GlobFilter>> = Some(vec![
        GlobFilter::Include(Pattern::new("*.txt").unwrap()),
        GlobFilter::Exclude(Pattern::new("*").unwrap()),
    ]);

    let source = S3PathParam::new_bucket(esthri_test::TEST_BUCKET, s3_key);
    let destination = S3PathParam::new_local(&local_directory);

    let res = blocking::sync(
        s3client.as_ref(),
        source,
        destination,
        filters.as_deref(),
        false,
        s3_storage_class
    );
    assert!(res.is_ok());
}

#[test]
fn test_sync_up_without_slash() {
    let s3client = esthri_test::get_s3client();
    let local_directory = esthri_test::test_data_dir();
    let s3_key = esthri_test::randomised_name("test_folder");
    let s3_storage_class = "STANDARD";

    let filters: Option<Vec<GlobFilter>> = Some(vec![
        GlobFilter::Include(Pattern::new("*.txt").unwrap()),
        GlobFilter::Exclude(Pattern::new("*").unwrap()),
    ]);

    let source = S3PathParam::new_local(&local_directory);
    let destination = S3PathParam::new_bucket(esthri_test::TEST_BUCKET, s3_key);

    let res = blocking::sync(
        s3client.as_ref(),
        source,
        destination,
        filters.as_deref(),
        false,
        s3_storage_class
    );
    assert!(res.is_ok());
}

#[test]
fn test_sync_up() {
    let s3client = esthri_test::get_s3client();
    let local_directory = esthri_test::test_data_dir();
    let s3_key = esthri_test::randomised_name("test_folder/");
    let s3_storage_class = "STANDARD";

    let filters: Option<Vec<GlobFilter>> = Some(vec![
        GlobFilter::Include(Pattern::new("*.txt").unwrap()),
        GlobFilter::Exclude(Pattern::new("*").unwrap()),
    ]);

    let source = S3PathParam::new_local(&local_directory);
    let destination = S3PathParam::new_bucket(esthri_test::TEST_BUCKET, s3_key);

    let res = blocking::sync(
        s3client.as_ref(),
        source,
        destination,
        filters.as_deref(),
        false,
        s3_storage_class
    );
    assert!(res.is_ok());
}

#[tokio::test]
async fn test_sync_up_async() {
    let s3client = esthri_test::get_s3client();
    let local_directory = esthri_test::test_data_dir();
    let s3_key = esthri_test::randomised_name("test_folder/");
    let s3_storage_class = "STANDARD";
    let filters: Option<Vec<GlobFilter>> = Some(vec![
        GlobFilter::Include(Pattern::new("*.txt").unwrap()),
        GlobFilter::Exclude(Pattern::new("*").unwrap()),
    ]);

    let source = S3PathParam::new_local(&local_directory);
    let destination = S3PathParam::new_bucket(esthri_test::TEST_BUCKET, s3_key);

    let res = sync(
        s3client.as_ref(),
        source,
        destination,
        filters.as_deref(),
        false,
        s3_storage_class
    )
    .await;
    assert!(res.is_ok());
}

#[test]
fn test_sync_up_default() {
    let s3client = esthri_test::get_s3client();
    let local_directory = esthri_test::test_data("sync_up");
    let s3_key = esthri_test::randomised_name("test_sync_up_default/");
    let s3_storage_class = "STANDARD";

    let source = S3PathParam::new_local(&local_directory);
    let destination = S3PathParam::new_bucket(esthri_test::TEST_BUCKET, &s3_key);

    let res = blocking::sync(
        s3client.as_ref(),
        source,
        destination,
        FILTER_EMPTY,
        false,
        s3_storage_class
    );
    assert!(res.is_ok());

    let key_hash_pairs = [
        KeyHashPair("1-one.data", "\"827aa1b392c93cb25d2348bdc9b907b0\""),
        KeyHashPair("2-two.bin", "\"35500e07a35b413fc5f434397a4c6bfa\""),
        KeyHashPair("3-three.junk", "\"388f9763d78cecece332459baecb4b85\""),
        KeyHashPair("nested/2MiB.bin", "\"64a2635e42ef61c69d62feebdbf118d4\""),
    ];

    for key_hash_pair in &key_hash_pairs[..] {
        let key = format!("{}{}", &s3_key, key_hash_pair.0);
        let res = blocking::head_object(s3client.as_ref(), esthri_test::TEST_BUCKET, &key);
        assert!(res.is_ok(), "fetching s3 etag failed for: {}", key);
        let res = res.unwrap();
        assert!(res.is_some(), "s3 etag returned was nil for: {}", key);
        assert_eq!(res.unwrap().e_tag, key_hash_pair.1, "invalid hash: {}", key);
    }
}

#[test]
fn test_sync_down_default() {
    let s3client = esthri_test::get_s3client();
    let local_directory = "tests/data/sync_down/d";
    let sync_dir_meta = fs::metadata(&local_directory);

    if let Ok(sync_dir_meta) = sync_dir_meta {
        assert!(sync_dir_meta.is_dir());
        assert!(fs::remove_dir_all(&local_directory).is_ok());
    }

    // Test was data populated with the following command:
    //
    //     aws s3 cp --recursive test/data/sync_up s3://esthri-test/test_sync_down_default/
    //
    let s3_key = "test_sync_down_default/";
    let s3_storage_class = "STANDARD";

    let source = S3PathParam::new_bucket(esthri_test::TEST_BUCKET, s3_key);
    let destination = S3PathParam::new_local(&local_directory);

    let res = blocking::sync(
        s3client.as_ref(),
        source,
        destination,
        FILTER_EMPTY,
        false,
        s3_storage_class
    );
    assert!(res.is_ok());

    let key_hash_pairs = [
        KeyHashPair("1-one.data", "827aa1b392c93cb25d2348bdc9b907b0"),
        KeyHashPair("2-two.bin", "35500e07a35b413fc5f434397a4c6bfa"),
        KeyHashPair("3-three.junk", "388f9763d78cecece332459baecb4b85"),
        KeyHashPair("nested/2MiB.bin", "64a2635e42ef61c69d62feebdbf118d4"),
    ];

    validate_key_hash_pairs(&local_directory, &key_hash_pairs);
}

#[test]
fn test_sync_down_filter() {
    let s3client = esthri_test::get_s3client();
    let local_dir = TempDir::new("esthri_cli").unwrap();

    let s3_key = "test_sync_down_default/";
    let s3_storage_class = "STANDARD";

    let filters: Option<Vec<GlobFilter>> =
        Some(vec![GlobFilter::Exclude(Pattern::new("*.bin").unwrap())]);

    let source = S3PathParam::new_bucket(esthri_test::TEST_BUCKET, s3_key);
    let destination = S3PathParam::new_local(&local_dir);

    let res = blocking::sync(
        s3client.as_ref(),
        source,
        destination,
        filters.as_deref(),
        false,
        s3_storage_class
    );
    assert!(res.is_ok());

    let key_hash_pairs = [
        KeyHashPair("1-one.data", "827aa1b392c93cb25d2348bdc9b907b0"),
        KeyHashPair("3-three.junk", "388f9763d78cecece332459baecb4b85"),
    ];

    validate_key_hash_pairs(&local_dir, &key_hash_pairs);
}

#[tokio::test]
async fn test_sync_across() {
    let s3client = esthri_test::get_s3client();
    let source_prefix = "test_sync_folder1/";
    let dest_prefix = esthri_test::randomised_name("test_sync_folder2/");

    let filters: Option<Vec<GlobFilter>> =
        Some(vec![GlobFilter::Include(Pattern::new("*.txt").unwrap())]);

    let source = S3PathParam::new_bucket(esthri_test::TEST_BUCKET, source_prefix);
    let destination = S3PathParam::new_bucket(esthri_test::TEST_BUCKET, dest_prefix);
    let s3_storage_class = "STANDARD";

    let res = sync(
        s3client.as_ref(),
        source,
        destination,
        filters.as_deref(),
        false,
        s3_storage_class
    )
    .await;
    assert!(res.is_ok(), "s3_sync result: {:?}", res);
}

fn sync_test_files_up_compressed(s3client: &rusoto_s3::S3Client, s3_key: &str) -> String {
    let data_dir = esthri_test::test_data("sync_up/");
    let temp_data_dir = "sync_up_compressed/";
    let s3_storage_class = "STANDARD";
    let old_cwd = std::env::current_dir().unwrap();
    let data_dir_fp = old_cwd.join(data_dir);
    eprintln!("{:?}", old_cwd);
    let tmp_dir = esthri_test::EphemeralTempDir::pushd();
    let temp_data_dir_fp = tmp_dir.temp_dir.path().join(temp_data_dir);
    eprintln!("temp_data_dir: {:?}", temp_data_dir_fp);
    let mut opts = fs_extra::dir::CopyOptions::new();
    opts.copy_inside = true;
    fs_extra::dir::copy(data_dir_fp, temp_data_dir, &opts).unwrap();
    let source = S3PathParam::new_local(temp_data_dir);
    let destination = S3PathParam::new_bucket(esthri_test::TEST_BUCKET, s3_key);
    let res = blocking::sync(
        s3client,
        source,
        destination,
        FILTER_EMPTY,
        true,
        s3_storage_class
    );
    assert!(res.is_ok());
    s3_key.to_string()
}

#[test]
fn test_sync_up_compressed() {
    let s3client = esthri_test::get_s3client();
    let key = esthri_test::randomised_name("test_sync_up_compressed/");
    let s3_key = sync_test_files_up_compressed(s3client.as_ref(), &key);

    let key_hash_pairs = [
        KeyHashPair("1-one.data", "\"276ebe187bb53cb68e484e8c0c0fef68\""),
        KeyHashPair("2-two.bin", "\"2b08f95817755fc00c1cc1e528dc7db8\""),
        KeyHashPair("3-three.junk", "\"12bc292b0d53b61203b839588213a9a1\""),
        KeyHashPair("nested/2MiB.bin", "\"da4b426cae11741846271040d9b4dc71\""),
    ];

    for key_hash_pair in &key_hash_pairs[..] {
        let key = format!("{}{}", s3_key, key_hash_pair.0);
        let res = blocking::head_object(s3client.as_ref(), esthri_test::TEST_BUCKET, &key);
        assert!(res.is_ok(), "fetching s3 etag failed for: {}", key);
        let res = res.unwrap();
        assert!(res.is_some(), "s3 etag returned was nil for: {}", key);
        assert_eq!(res.unwrap().e_tag, key_hash_pair.1, "invalid hash: {}", key);
    }
}

/// Test downloading files that were synced up with compression enabled
/// These should download as the original, uncompressed file

#[test]
fn test_sync_down_compressed() {
    let s3client = esthri_test::get_s3client();
    let s3_key = esthri_test::randomised_name("test_sync_down_compressed_v7/");
    let s3_storage_class = "STANDARD";

    sync_test_files_up_compressed(s3client.as_ref(), &s3_key);

    let local_directory = esthri_test::test_data("sync_down/d");
    let sync_dir_meta = fs::metadata(&local_directory);

    if let Ok(sync_dir_meta) = sync_dir_meta {
        assert!(sync_dir_meta.is_dir());
        assert!(fs::remove_dir_all(&local_directory).is_ok());
    }
    let destination = S3PathParam::new_local(&local_directory);

    let res = blocking::sync(
        s3client.as_ref(),
        S3PathParam::new_bucket(esthri_test::TEST_BUCKET, s3_key),
        destination,
        FILTER_EMPTY,
        true,
        s3_storage_class
    );
    assert!(res.is_ok());

    let key_hash_pairs = [
        KeyHashPair("1-one.data", "827aa1b392c93cb25d2348bdc9b907b0"),
        KeyHashPair("2-two.bin", "35500e07a35b413fc5f434397a4c6bfa"),
        KeyHashPair("3-three.junk", "388f9763d78cecece332459baecb4b85"),
        KeyHashPair("nested/2MiB.bin", "64a2635e42ef61c69d62feebdbf118d4"),
    ];

    validate_key_hash_pairs(&local_directory, &key_hash_pairs);
}

/// Test syncing down a mix of compressed and non-compressed files

#[test]
fn test_sync_down_compressed_mixed() {
    let s3client = esthri_test::get_s3client();
    let s3_key = esthri_test::randomised_name("test_sync_down_compressed_mixed_v7/");
    let s3_storage_class = "STANDARD";

    blocking::upload(
        s3client.as_ref(),
        esthri_test::TEST_BUCKET,
        &s3_key,
        esthri_test::test_data("index.html"),
        s3_storage_class
    )
    .unwrap();
    blocking::upload_compressed(
        s3client.as_ref(),
        esthri_test::TEST_BUCKET,
        &s3_key,
        esthri_test::test_data("test_file.txt"),
        s3_storage_class
    )
    .unwrap();

    let local_directory = esthri_test::test_data("sync_down/d");

    // Syncing down without compression should give the files as they are on S3
    // ie, "index.html" uncompressed and "test_file.txt" compressed
    {
        let sync_dir_meta = fs::metadata(&local_directory);

        if let Ok(sync_dir_meta) = sync_dir_meta {
            assert!(sync_dir_meta.is_dir());
            assert!(fs::remove_dir_all(&local_directory).is_ok());
        }
        let destination = S3PathParam::new_local(&local_directory);

        let res = blocking::sync(
            s3client.as_ref(),
            S3PathParam::new_bucket(esthri_test::TEST_BUCKET, &s3_key),
            destination,
            FILTER_EMPTY,
            false,
            s3_storage_class
        );
        assert!(res.is_ok());

        let key_hash_pairs = [
            KeyHashPair("index.html", "b4e3f354e8575e2fa5f489ab6078917c"),
            KeyHashPair("test_file.txt", "9648fc00e6a6c4b68127ca6547f15eb6"),
        ];

        validate_key_hash_pairs(&local_directory, &key_hash_pairs);
    }

    // Syncing down with transparent compression should get the uncompressed version
    // of both files
    {
        let sync_dir_meta = fs::metadata(&local_directory);

        if let Ok(sync_dir_meta) = sync_dir_meta {
            assert!(sync_dir_meta.is_dir());
            assert!(fs::remove_dir_all(&local_directory).is_ok());
        }
        let destination = S3PathParam::new_local(&local_directory);

        let res = blocking::sync(
            s3client.as_ref(),
            S3PathParam::new_bucket(esthri_test::TEST_BUCKET, &s3_key),
            destination,
            FILTER_EMPTY,
            true,
            s3_storage_class
        );
        assert!(res.is_ok());

        let key_hash_pairs = [
            KeyHashPair("index.html", "b4e3f354e8575e2fa5f489ab6078917c"),
            KeyHashPair("test_file.txt", "8fd41740698064016b7daaddddd3531a"),
        ];

        validate_key_hash_pairs(&local_directory, &key_hash_pairs);
    }
}
