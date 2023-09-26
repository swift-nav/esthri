use std::{fs, path::Path};

use glob::Pattern;
use tempdir::TempDir;
use tempfile::tempdir;

use esthri::{blocking, opts::*, sync, sync_streaming, GlobFilter, S3PathParam, FILTER_EMPTY};
use esthri_test::{validate_key_hash_pairs, KeyHashPair};
use tokio_stream::StreamExt;

use aws_sdk_s3::Client as S3Client;

#[test]
fn test_sync_down() {
    let s3client = esthri_test::get_s3client();
    let local_directory = esthri_test::test_data_dir();
    let s3_key = "test_folder/";

    let filters: Option<Vec<GlobFilter>> = Some(vec![
        GlobFilter::Include(Pattern::new("*.txt").unwrap()),
        GlobFilter::Exclude(Pattern::new("*").unwrap()),
    ]);

    let source = S3PathParam::new_bucket(esthri_test::TEST_BUCKET, s3_key);
    let destination = S3PathParam::new_local(local_directory);
    let opts = SharedSyncOptParamsBuilder::default().build().unwrap();

    let res = esthri::blocking::sync(
        s3client.as_ref(),
        source,
        destination,
        filters.as_deref(),
        opts,
    );
    assert!(res.is_ok(), "s3_sync result: {:?}", res);
}

#[tokio::test]
async fn test_sync_down_async() {
    let s3client = esthri_test::get_s3client_async().await;
    let local_directory = esthri_test::test_data_dir();
    let s3_key = "test_folder/";
    let filters: Option<Vec<GlobFilter>> = Some(vec![
        GlobFilter::Include(Pattern::new("*.txt").unwrap()),
        GlobFilter::Exclude(Pattern::new("*").unwrap()),
    ]);

    let source = S3PathParam::new_bucket(esthri_test::TEST_BUCKET, s3_key);
    let destination = S3PathParam::new_local(&local_directory);
    let opts = SharedSyncOptParamsBuilder::default().build().unwrap();

    let res = sync(
        s3client.as_ref(),
        source,
        destination,
        filters.as_deref(),
        opts,
    )
    .await;
    assert!(res.is_ok(), "s3_sync result: {:?}", res);
}

#[test]
fn test_sync_down_without_slash() {
    let s3client = esthri_test::get_s3client();
    let local_directory = esthri_test::test_data_dir();
    let s3_key = esthri_test::randomised_lifecycled_prefix("test_folder");
    let filters: Option<Vec<GlobFilter>> = Some(vec![
        GlobFilter::Include(Pattern::new("*.txt").unwrap()),
        GlobFilter::Exclude(Pattern::new("*").unwrap()),
    ]);

    let source = S3PathParam::new_bucket(esthri_test::TEST_BUCKET, s3_key);
    let destination = S3PathParam::new_local(local_directory);
    let opts = SharedSyncOptParamsBuilder::default().build().unwrap();

    let res = blocking::sync(
        s3client.as_ref(),
        source,
        destination,
        filters.as_deref(),
        opts,
    );
    assert!(res.is_ok());
}

#[test]
fn test_sync_up_without_slash() {
    let s3client = esthri_test::get_s3client();
    let local_directory = esthri_test::test_data_dir();
    let s3_key = esthri_test::randomised_lifecycled_prefix("test_folder");
    let filters: Option<Vec<GlobFilter>> = Some(vec![
        GlobFilter::Include(Pattern::new("*.txt").unwrap()),
        GlobFilter::Exclude(Pattern::new("*").unwrap()),
    ]);

    let source = S3PathParam::new_local(local_directory);
    let destination = S3PathParam::new_bucket(esthri_test::TEST_BUCKET, s3_key);
    let opts = SharedSyncOptParamsBuilder::default().build().unwrap();

    let res = blocking::sync(
        s3client.as_ref(),
        source,
        destination,
        filters.as_deref(),
        opts,
    );
    assert!(res.is_ok());
}

#[test]
fn test_sync_up() {
    let s3client = esthri_test::get_s3client();
    let local_directory = esthri_test::test_data_dir();
    let s3_key = esthri_test::randomised_lifecycled_prefix("test_folder/");
    let filters: Option<Vec<GlobFilter>> = Some(vec![
        GlobFilter::Include(Pattern::new("*.txt").unwrap()),
        GlobFilter::Exclude(Pattern::new("*").unwrap()),
    ]);

    let source = S3PathParam::new_local(local_directory);
    let destination = S3PathParam::new_bucket(esthri_test::TEST_BUCKET, s3_key);
    let opts = SharedSyncOptParamsBuilder::default().build().unwrap();

    let res = blocking::sync(
        s3client.as_ref(),
        source,
        destination,
        filters.as_deref(),
        opts,
    );
    assert!(res.is_ok());
}

#[tokio::test]
async fn test_sync_up_async() {
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

    let res = sync(
        s3client.as_ref(),
        source,
        destination,
        filters.as_deref(),
        opts,
    )
    .await;
    assert!(res.is_ok());
}

#[test]
fn test_sync_up_default() {
    let s3client = esthri_test::get_s3client();
    let local_directory = esthri_test::test_data("sync_up");
    let s3_key = esthri_test::randomised_lifecycled_prefix("test_sync_up_default/");

    let source = S3PathParam::new_local(local_directory);
    let destination = S3PathParam::new_bucket(esthri_test::TEST_BUCKET, &s3_key);
    let opts = SharedSyncOptParamsBuilder::default().build().unwrap();

    let res = blocking::sync(s3client.as_ref(), source, destination, FILTER_EMPTY, opts);
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

// Validate local to remote sync with delete flag set
#[test]
fn test_sync_up_delete() {
    let s3client = esthri_test::get_s3client();
    let local_directory = esthri_test::copy_test_data("sync_up");
    let s3_key_prefix = esthri_test::randomised_lifecycled_prefix("test_sync_up_delete/");

    let source = S3PathParam::new_local(&local_directory);
    let destination = S3PathParam::new_bucket(esthri_test::TEST_BUCKET, &s3_key_prefix);
    let opts = SharedSyncOptParamsBuilder::default()
        .transparent_compression(false)
        .delete(true)
        .build()
        .unwrap();

    let res = blocking::sync(
        s3client.as_ref(),
        source.clone(),
        destination.clone(),
        FILTER_EMPTY,
        opts,
    );
    assert!(res.is_ok());

    let keys = ["1-one.data", "2-two.bin", "3-three.junk", "nested/2MiB.bin"];

    for key in &keys[..] {
        let key = format!("{}{}", &s3_key_prefix, key);
        let res = blocking::head_object(s3client.as_ref(), esthri_test::TEST_BUCKET, &key);
        assert!(res.is_ok(), "head object failed for: {}", key);
        let res = res.unwrap();
        assert!(
            res.is_some(),
            "head object info returned was nil for: {}",
            key
        );
    }

    let remove_path_target = "nested/2MiB.bin";
    let remove_path_target = local_directory.join(remove_path_target);

    fs::remove_file(&remove_path_target).expect("could not remove file");
    assert!(fs::metadata(remove_path_target).is_err());
    let opts = SharedSyncOptParamsBuilder::default()
        .transparent_compression(false)
        .delete(true)
        .build()
        .unwrap();

    let res = blocking::sync(s3client.as_ref(), source, destination, FILTER_EMPTY, opts);
    assert!(res.is_ok());

    let keyexists_pairs = [
        ("1-one.data", true),
        ("2-two.bin", true),
        ("3-three.junk", true),
        ("nested/2MiB.bin", false),
    ];

    for (key, exists) in &keyexists_pairs[..] {
        let key = format!("{}{}", &s3_key_prefix, key);
        let res = blocking::head_object(s3client.as_ref(), esthri_test::TEST_BUCKET, &key);
        assert!(res.is_ok(), "head object failed for: {}", key);
        let res = res.unwrap();
        if *exists {
            assert!(
                res.is_some(),
                "head object info returned was nil for: {}",
                key
            );
        } else {
            assert!(
                res.is_none(),
                "expected head object to fail for key: {}",
                key
            );
        }
    }
}

// Validate remote to local sync with delete flag set
#[test]
fn test_sync_down_delete() {
    let s3client = esthri_test::get_s3client();
    // Get path to some directory and populate it.
    let local_directory = esthri_test::copy_test_data("sync_up");
    // Create a key that correspods to non-existant data in an S3 bucket
    let s3_key_prefix = esthri_test::randomised_lifecycled_prefix("test_sync_down_delete/");

    let src = S3PathParam::new_bucket(esthri_test::TEST_BUCKET, s3_key_prefix);
    let dst = S3PathParam::new_local(&local_directory);

    // Expect contents to have been copied to local directory
    assert!(local_directory
        .read_dir()
        .expect("unable to read from directory")
        .next()
        .is_some());

    let opts = SharedSyncOptParamsBuilder::default()
        .delete(true)
        .build()
        .unwrap();

    // Perform sync with delete flag set. Because the target S3 key is empty, all contents in local directory should be deleted.
    let res = blocking::sync(s3client.as_ref(), src, dst, FILTER_EMPTY, opts);
    assert!(res.is_ok());

    // Expect no files to exist within local_directory. Metadata such as directories are permissible.
    let no_files = fs::read_dir(&local_directory)
        .unwrap()
        .all(|path| path.unwrap().file_type().unwrap().is_dir());

    assert!(no_files);
}

#[test]
fn test_sync_across_delete() {
    let s3client = esthri_test::get_s3client();
    // Indicate two empty S3 keys to perform opperations on.
    let s3_key_src_prefix =
        esthri_test::randomised_lifecycled_prefix("test_sync_across_delete_src/");
    let s3_key_dst_prefix =
        esthri_test::randomised_lifecycled_prefix("test_sync_across_delete_dst/");

    // Create a dummy file. Deletion of this file will be indicative of test success
    let temp_directory = tempdir().expect("Unable to create temp directory");
    let file_pathbuf = temp_directory.path().join("delete-me.txt");
    let _to_be_delete_file =
        fs::File::create(&file_pathbuf).expect("Error encountered while creating file");

    let temp_directory_as_pathbuf = temp_directory.as_ref().to_path_buf();

    // Create 2 empty buckets
    let source = S3PathParam::new_bucket(esthri_test::TEST_BUCKET, s3_key_src_prefix);
    let destination = S3PathParam::new_bucket(esthri_test::TEST_BUCKET, &s3_key_dst_prefix);

    let local_source = S3PathParam::new_local(temp_directory_as_pathbuf.as_path());
    let opts = SharedSyncOptParamsBuilder::default().build().unwrap();

    // Copy the dummy file to one of the test buckets
    let res = blocking::sync(
        s3client.as_ref(),
        local_source.clone(),
        destination.clone(),
        FILTER_EMPTY,
        opts,
    );
    println!("{}", local_source.clone());
    assert!(res.is_ok());

    // Expect bucket to have been populated
    let keys = ["delete-me.txt"];
    for key in &keys[..] {
        let key = format!("{}{}", &s3_key_dst_prefix, key);
        let res = blocking::head_object(s3client.as_ref(), esthri_test::TEST_BUCKET, &key);
        assert!(res.is_ok(), "head object failed for: {}", key);
        let res = res.unwrap();
        assert!(
            res.is_some(),
            "head object info returned was nil for: {}",
            key
        );
    }

    // Local cleanup
    fs::remove_file(file_pathbuf).expect("Unable to remove file");

    let opts = SharedSyncOptParamsBuilder::default()
        .transparent_compression(false)
        .delete(true)
        .build()
        .unwrap();

    // At this point in execution, two buckets exist, one (dst) with a single file, one (src) which is empty

    // Perform a sync with delete flag set between the two buckets
    let res = blocking::sync(s3client.as_ref(), source, destination, FILTER_EMPTY, opts);
    assert!(res.is_ok());

    // Expect destination bucket to have no contents
    let keys = ["delete-me.txt"];
    for key in &keys[..] {
        let key = format!("{}{}", &s3_key_dst_prefix, key);
        let res = blocking::head_object(s3client.as_ref(), esthri_test::TEST_BUCKET, &key);
        assert!(res.is_ok(), "head object failed for: {}", key);
        let res = res.unwrap();
        assert!(
            res.is_some(),
            "head object info returned was nil for: {}",
            key
        );
    }
}

#[test]
fn test_sync_down_default() {
    let s3client = esthri_test::get_s3client();
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

    let source = S3PathParam::new_bucket(esthri_test::TEST_BUCKET, s3_key);
    let destination = S3PathParam::new_local(local_directory);
    let opts = SharedSyncOptParamsBuilder::default().build().unwrap();

    let res = blocking::sync(s3client.as_ref(), source, destination, FILTER_EMPTY, opts);
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
    let s3client = esthri_test::get_s3client();
    let local_dir = TempDir::new("esthri_cli").unwrap();

    let s3_key = "test_sync_down_default/";

    let filters: Option<Vec<GlobFilter>> =
        Some(vec![GlobFilter::Exclude(Pattern::new("*.bin").unwrap())]);

    let source = S3PathParam::new_bucket(esthri_test::TEST_BUCKET, s3_key);
    let destination = S3PathParam::new_local(&local_dir);
    let opts = SharedSyncOptParamsBuilder::default().build().unwrap();

    let res = blocking::sync(
        s3client.as_ref(),
        source,
        destination,
        filters.as_deref(),
        opts,
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
    let s3client = esthri_test::get_s3client_async().await;
    let source_prefix = "test_sync_folder1/";
    let dest_prefix = esthri_test::randomised_lifecycled_prefix("test_sync_folder2/");

    let filters: Option<Vec<GlobFilter>> =
        Some(vec![GlobFilter::Include(Pattern::new("*.txt").unwrap())]);

    let source = S3PathParam::new_bucket(esthri_test::TEST_BUCKET, source_prefix);
    let destination = S3PathParam::new_bucket(esthri_test::TEST_BUCKET, dest_prefix);
    let opts = SharedSyncOptParamsBuilder::default().build().unwrap();

    let res = sync(
        s3client.as_ref(),
        source,
        destination,
        filters.as_deref(),
        opts,
    )
    .await;
    assert!(res.is_ok(), "s3_sync result: {:?}", res);
}

fn sync_test_files_up_compressed(s3client: &S3Client, s3_key: &str) -> String {
    let data_dir = esthri_test::test_data("sync_up/");
    let temp_data_dir = "sync_up_compressed/";
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
    let opts = SharedSyncOptParamsBuilder::default()
        .transparent_compression(true)
        .delete(false)
        .build()
        .unwrap();
    let res = blocking::sync(s3client, source, destination, FILTER_EMPTY, opts);
    assert!(res.is_ok());
    s3_key.to_string()
}

#[test]
fn test_sync_up_compressed() {
    let s3client = esthri_test::get_s3client();
    let key = esthri_test::randomised_lifecycled_prefix("test_sync_up_compressed/");
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
    let s3_key = esthri_test::randomised_lifecycled_prefix("test_sync_down_compressed_v7/");

    sync_test_files_up_compressed(s3client.as_ref(), &s3_key);

    let local_directory = esthri_test::test_data("sync_down/d");
    let sync_dir_meta = fs::metadata(&local_directory);

    if let Ok(sync_dir_meta) = sync_dir_meta {
        assert!(sync_dir_meta.is_dir());
        assert!(fs::remove_dir_all(&local_directory).is_ok());
    }
    let destination = S3PathParam::new_local(&local_directory);
    let opts = SharedSyncOptParamsBuilder::default()
        .transparent_compression(true)
        .delete(false)
        .build()
        .unwrap();

    let res = blocking::sync(
        s3client.as_ref(),
        S3PathParam::new_bucket(esthri_test::TEST_BUCKET, s3_key),
        destination,
        FILTER_EMPTY,
        opts,
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
    let s3_key = esthri_test::randomised_lifecycled_prefix("test_sync_down_compressed_mixed_v7/");
    let opts = SharedSyncOptParamsBuilder::default().build().unwrap();
    let opts_compress = SharedSyncOptParamsBuilder::default()
        .transparent_compression(true)
        .build()
        .unwrap();

    blocking::upload(
        s3client.as_ref(),
        esthri_test::TEST_BUCKET,
        &s3_key,
        esthri_test::test_data("index.html"),
        opts.into(),
    )
    .unwrap();
    blocking::upload(
        s3client.as_ref(),
        esthri_test::TEST_BUCKET,
        &s3_key,
        esthri_test::test_data("test_file.txt"),
        opts_compress.into(),
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
        let opts = SharedSyncOptParamsBuilder::default().build().unwrap();

        let res = blocking::sync(
            s3client.as_ref(),
            S3PathParam::new_bucket(esthri_test::TEST_BUCKET, &s3_key),
            destination,
            FILTER_EMPTY,
            opts,
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
        let opts = SharedSyncOptParamsBuilder::default()
            .transparent_compression(true)
            .delete(false)
            .build()
            .unwrap();

        let res = blocking::sync(
            s3client.as_ref(),
            S3PathParam::new_bucket(esthri_test::TEST_BUCKET, &s3_key),
            destination,
            FILTER_EMPTY,
            opts,
        );
        assert!(res.is_ok());

        let key_hash_pairs = [
            KeyHashPair("index.html", "b4e3f354e8575e2fa5f489ab6078917c"),
            KeyHashPair("test_file.txt", "8fd41740698064016b7daaddddd3531a"),
        ];

        validate_key_hash_pairs(&local_directory, &key_hash_pairs);
    }
}

#[tokio::test]
async fn test_sync_down_async_streaming() {
    let s3client = esthri_test::get_s3client_async().await;
    let local_directory = esthri_test::test_data_dir();
    let s3_key = "test_folder/";
    let filters: Vec<GlobFilter> = vec![
        GlobFilter::Include(Pattern::new("*.txt").unwrap()),
        GlobFilter::Exclude(Pattern::new("*").unwrap()),
    ];

    let source = S3PathParam::new_bucket(esthri_test::TEST_BUCKET, s3_key);
    let destination = S3PathParam::new_local(&local_directory);
    let opts = SharedSyncOptParamsBuilder::default().build().unwrap();

    let mut stream = sync_streaming(s3client.as_ref(), &source, &destination, &filters, opts)
        .await
        .unwrap();

    while let Some(res) = stream.next().await {
        assert!(Path::new(&res.unwrap().dest_path()).exists());
    }
}
