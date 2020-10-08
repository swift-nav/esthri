#![cfg(feature = "http_server")]
#![cfg_attr(feature = "aggressive_lint", deny(warnings))]

use flate2::read::GzDecoder;
use tar::Archive;

use esthri_lib::http_server::esthri_filter;
use esthri_lib::sync;
use esthri_lib::types::SyncDirection;
use esthri_lib::upload;

mod common;

use common::{validate_key_hash_pairs, KeyHashPair};

#[tokio::test]
async fn test_fetch_object() {
    let filename = "tests/data/test_file.txt".to_owned();

    let s3client = common::get_s3client();
    let bucket = common::TEST_BUCKET;

    let s3_key = "test_file.txt".to_owned();

    let res = upload(s3client.as_ref(), common::TEST_BUCKET, &s3_key, &filename).await;
    assert!(res.is_ok());

    let filter = esthri_filter((*s3client).clone(), bucket);

    let mut result = warp::test::request()
        .path("/test_file.txt")
        .filter(&filter)
        .await
        .unwrap();

    let body = hyper::body::to_bytes(result.body_mut()).await.unwrap();
    assert_eq!(body, "this file has contents\n");
}

#[tokio::test]
async fn test_fetch_archive() {
    let s3client = common::get_s3client();
    let local_directory = "tests/data/sync_up";
    let s3_key = "test_fetch_archive/";
    let res = sync(
        s3client.as_ref(),
        SyncDirection::up,
        common::TEST_BUCKET,
        &s3_key,
        &local_directory,
        &None,
        &None,
    )
    .await;
    assert!(res.is_ok());

    let filter = esthri_filter((*s3client).clone(), common::TEST_BUCKET);

    let mut result = warp::test::request()
        .path("/test_fetch_archive/?archive=true")
        .filter(&filter)
        .await
        .unwrap();

    let body = hyper::body::to_bytes(result.body_mut()).await.unwrap();

    let tar = GzDecoder::new(&body[..]);
    let mut archive = Archive::new(tar);

    let _tmp_dir = common::EphemeralTempDir::pushd();
    archive.unpack(".").unwrap();

    let key_hash_pairs = [
        KeyHashPair("1-one.data", "827aa1b392c93cb25d2348bdc9b907b0"),
        KeyHashPair("2-two.bin", "35500e07a35b413fc5f434397a4c6bfa"),
        KeyHashPair("3-three.junk", "388f9763d78cecece332459baecb4b85"),
    ];

    validate_key_hash_pairs("test_fetch_archive", &key_hash_pairs);
}
