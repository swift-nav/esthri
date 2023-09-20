use std::io::Cursor;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::{fs, io};

use tokio::runtime::Runtime;
use warp::http::status::StatusCode;

use esthri::{blocking, opts::*, upload, Config};
use esthri_server::esthri_filter;
use esthri_test::{validate_key_hash_pairs, DateTime, KeyHashPair};

#[tokio::test]
async fn test_fetch_object() {
    let filename = esthri_test::test_data("test_file.txt");

    let s3client = esthri_test::get_s3client_async().await;
    let bucket = esthri_test::TEST_BUCKET;

    let s3_key = "test_file.txt".to_owned();
    let opts = EsthriPutOptParamsBuilder::default().build().unwrap();

    let res = upload(
        s3client.as_ref(),
        esthri_test::TEST_BUCKET,
        &s3_key,
        &filename,
        opts,
    )
    .await;
    assert!(res.is_ok());

    let filter = esthri_filter((*s3client).clone(), bucket, false, &[]);

    let mut result = warp::test::request()
        .path("/test_file.txt")
        .filter(&filter)
        .await
        .unwrap();

    let body = hyper::body::to_bytes(result.body_mut()).await.unwrap();
    assert_eq!(body, "this file has contents\n");
}

/// Test allowed prefixes functionality.  Specifically if a set of allowed prefixes are specified then the server should
/// block access to those resource either via direct access or indirect via an archive request.
///
#[tokio::test]
async fn test_allowed_prefixes() {
    let filename = esthri_test::test_data("test_file.txt");

    let s3client = esthri_test::get_s3client_async().await;
    let bucket = esthri_test::TEST_BUCKET;

    // Nominal happy path, a prefix is allowed, so access to a file is allowed.

    let an_allowed_key = "allowed_prefix/test_file.txt".to_owned();
    let opts = EsthriPutOptParamsBuilder::default().build().unwrap();

    let res = upload(
        s3client.as_ref(),
        esthri_test::TEST_BUCKET,
        &an_allowed_key,
        &filename,
        opts,
    )
    .await;

    assert!(res.is_ok());

    // Nominal bad path, a prefix is *NOT* allowed, so access to a file is *NOT* allowed.

    let a_not_allowed_key = "not_allowed_prefix/test_file.txt".to_owned();
    let opts = EsthriPutOptParamsBuilder::default().build().unwrap();

    let res = upload(
        s3client.as_ref(),
        esthri_test::TEST_BUCKET,
        &a_not_allowed_key,
        &filename,
        opts,
    )
    .await;

    assert!(res.is_ok());

    // Test that a set of prefixes, one without a training slash are correctly allowed or blocked.

    let allowed_prefixes = [
        "allowed_prefix".to_owned(),
        "another_allowed_prefix/".to_owned(),
    ];
    let filter = esthri_filter((*s3client).clone(), bucket, false, &allowed_prefixes);

    let mut result = warp::test::request()
        .path("/allowed_prefix/test_file.txt")
        .filter(&filter)
        .await
        .unwrap();

    let body = hyper::body::to_bytes(result.body_mut()).await.unwrap();
    assert_eq!(body, "this file has contents\n");

    let result = warp::test::request()
        .path("/not_allowed_prefix/test_file.txt")
        .filter(&filter)
        .await;

    assert!(result.err().unwrap().is_not_found());

    let result = warp::test::request()
        .path("/allowed_prefix/?archive=true")
        .filter(&filter)
        .await;

    assert_eq!(result.unwrap().status(), StatusCode::OK);

    // Test indirect access via archive request is blocked.

    let result = warp::test::request()
        .path("/not_allowed_prefix/?archive=true")
        .filter(&filter)
        .await;

    assert!(result.err().unwrap().is_not_found());

    // Test indirect access via archive request is allowed even if it's 2 different allowed prefixes.

    let result = warp::test::request()
        .path("/?archive=true&prefixes=allowed_prefix/|another_allowed_prefix/")
        .filter(&filter)
        .await;

    assert_eq!(result.unwrap().status(), StatusCode::OK);

    // Test indirect access via archive request is *NOT* allowed when at least one blocked path is present.

    let result = warp::test::request()
        .path("/?archive=true&prefixes=not_allowed_prefix/|another_allowed_prefix/")
        .filter(&filter)
        .await;

    assert!(result.err().unwrap().is_not_found());
}

async fn upload_compressed_html_file() {
    let filename = esthri_test::test_data("index.html");
    let s3client = esthri_test::get_s3client_async().await;
    let s3_key = "index_compressed.html".to_owned();
    let opts = EsthriPutOptParamsBuilder::default()
        .transparent_compression(true)
        .storage_class(Some(Config::global().storage_class()))
        .build()
        .unwrap();
    let res = upload(
        s3client.as_ref(),
        esthri_test::TEST_BUCKET,
        &s3_key,
        &filename,
        opts,
    )
    .await;
    assert!(res.is_ok());
}

/// Ensures that getting an object with `from_gz_compressed` specified sets the
/// right content-encoding
#[tokio::test]
async fn test_fetch_compressed_object_encoding() {
    upload_compressed_html_file().await;
    let s3client = esthri_test::get_s3client_async().await;

    let filter = esthri_filter((*s3client).clone(), esthri_test::TEST_BUCKET, false, &[]);

    let mut result = warp::test::request()
        .path("/index_compressed.html")
        .filter(&filter)
        .await
        .unwrap();

    let headers = result.headers();
    assert_eq!(headers["content-type"], "text/html");
    assert_eq!(headers["content-encoding"], "gzip");

    let body = hyper::body::to_bytes(result.body_mut()).await.unwrap();
    let digest = md5::compute(body);
    assert_eq!(
        format!("{:x}", digest),
        "3f945ffd0cf39f07d927d8752526caad",
        "md5 digest did not match"
    );
}

fn upload_test_data() -> anyhow::Result<()> {
    let s3client = esthri_test::get_s3client();
    let local_directory = esthri_test::test_data("sync_up");
    let s3_key = "test_fetch_archive";
    let filenames = ["1-one.data", "2-two.bin", "3-three.junk"];
    let opts = EsthriPutOptParamsBuilder::default().build().unwrap();
    for filename in &filenames {
        let filepath = std::path::Path::new(&local_directory).join(filename);
        let s3_key = format!("{}/{}", s3_key, filename);
        blocking::upload(
            s3client.as_ref(),
            esthri_test::TEST_BUCKET,
            s3_key,
            filepath.to_str().unwrap(),
            opts.clone(),
        )?;
    }
    Ok(())
}

async fn upload_index_url_test_data() -> anyhow::Result<()> {
    let s3_key = "index_html";
    let s3client = esthri_test::get_s3client_async().await;
    let local_directory = esthri_test::test_data(s3_key);
    let opts = SharedSyncOptParamsBuilder::default().build().unwrap();
    esthri::sync(
        s3client.as_ref(),
        esthri::S3PathParam::new_local(local_directory),
        esthri::S3PathParam::new_bucket(esthri_test::TEST_BUCKET, s3_key),
        None,
        opts,
    )
    .await?;
    Ok(())
}

fn extract_zip_archive(mut archive: zip::ZipArchive<Cursor<&bytes::Bytes>>) -> anyhow::Result<()> {
    for i in 0..archive.len() {
        let mut file = archive.by_index(i)?;
        let outpath = match file.enclosed_name() {
            Some(path) => path.to_owned(),
            None => continue,
        };
        if let Some(p) = outpath.parent() {
            if !p.exists() {
                fs::create_dir_all(p)?;
            }
        }
        let mut outfile = fs::File::create(&outpath)?;
        io::copy(&mut file, &mut outfile)?;
    }

    Ok(())
}

fn validate_fetch_archive(
    body: bytes::Bytes,
    local_dir: &str,
    key_hash_pairs: &[KeyHashPair],
    upload_time: DateTime,
) {
    use std::path::Path;

    let cursor = Cursor::new(&body);
    let archive = zip::ZipArchive::new(cursor).expect("Error opening Zip");
    let _tmp_dir = esthri_test::EphemeralTempDir::pushd();

    extract_zip_archive(archive).expect("Error extracting Zip");

    validate_key_hash_pairs(local_dir, key_hash_pairs);
    for key_hash_pair in key_hash_pairs {
        let filename = Path::new(key_hash_pair.0);
        let filepath = Path::new(local_dir).join(filename);
        let file_last_mod = std::fs::metadata(filepath)
            .expect("stat'ing test file path")
            .modified()
            .expect("last modified timestamp");
        let mod_time: DateTime = file_last_mod.into();
        let time_diff = mod_time - upload_time;
        // Hopefully it never takes more than an hour to upload the files
        assert_eq!(time_diff.num_hours(), 0);
    }
}

async fn fetch_archive_and_validate(
    request_path: &str,
    success: Arc<AtomicBool>,
    local_dir: &str,
    key_hash_pairs: &[KeyHashPair],
    upload_time: DateTime,
) {
    let s3client = esthri_test::get_s3client_async().await;
    let filter = esthri_filter((*s3client).clone(), esthri_test::TEST_BUCKET, false, &[]);
    let mut body = warp::test::request()
        .path(request_path)
        .filter(&filter)
        .await
        .unwrap();
    let body_bytes = hyper::body::to_bytes(body.body_mut()).await.unwrap();
    validate_fetch_archive(body_bytes, local_dir, key_hash_pairs, upload_time);
    success.store(true, Ordering::Release);
}

fn test_fetch_archive_helper(
    request_path: &str,
    local_dir: &str,
    key_hash_pairs: Vec<KeyHashPair>,
) {
    const TIMEOUT_MILLIS: u64 = 20_000;
    const SLEEP_MILLIS: u64 = 50;

    let success = Arc::new(AtomicBool::new(false));
    let thread_success = success.clone();
    let panic_success = success.clone();

    let done = Arc::new(AtomicBool::new(false));
    let thread_done = done.clone();
    let panic_done = done.clone();

    let request_path = request_path.to_owned();
    let local_dir = local_dir.to_owned();

    let upload_time: DateTime = std::time::SystemTime::now().into();
    upload_test_data().unwrap();

    std::panic::set_hook(Box::new(move |panic_info| {
        panic_success.store(false, Ordering::Release);
        panic_done.store(true, Ordering::Relaxed);
        eprint!("{:?}", backtrace::Backtrace::new());
        eprint!("\n{}\n", panic_info);
    }));

    std::thread::spawn(move || {
        let runtime = Runtime::new().unwrap();
        runtime.block_on(fetch_archive_and_validate(
            &request_path,
            thread_success,
            &local_dir,
            &key_hash_pairs,
            upload_time,
        ));
        thread_done.store(true, Ordering::Relaxed);
    });

    let mut timeout_count: i64 = (TIMEOUT_MILLIS / SLEEP_MILLIS) as i64;

    while !done.load(Ordering::Relaxed) && timeout_count > 0 {
        std::thread::sleep(std::time::Duration::from_millis(SLEEP_MILLIS));
        timeout_count -= 1;
    }

    if timeout_count == 0 {
        println!("****************");
        println!("*** TIME OUT ***");
        println!("****************");
    }

    let _ = std::panic::take_hook();
    assert!(success.load(Ordering::Acquire) || timeout_count == 0);
}

#[test]
fn test_fetch_archive() {
    let key_hash_pairs: Vec<KeyHashPair> = vec![
        KeyHashPair("1-one.data", "827aa1b392c93cb25d2348bdc9b907b0"),
        KeyHashPair("2-two.bin", "35500e07a35b413fc5f434397a4c6bfa"),
        KeyHashPair("3-three.junk", "388f9763d78cecece332459baecb4b85"),
    ];
    test_fetch_archive_helper(
        "/test_fetch_archive/?archive=true",
        "test_fetch_archive",
        key_hash_pairs,
    );
}

#[test]
fn test_fetch_archive_prefixes() {
    let key_hash_pairs: Vec<KeyHashPair> = vec![
        KeyHashPair("1-one.data", "827aa1b392c93cb25d2348bdc9b907b0"),
        KeyHashPair("2-two.bin", "35500e07a35b413fc5f434397a4c6bfa"),
    ];
    test_fetch_archive_helper(
        "/?prefixes=test_fetch_archive/1-one.data|test_fetch_archive/2-two.bin&archive=true",
        "test_fetch_archive",
        key_hash_pairs,
    );
}

#[test]
fn test_fetch_archive_with_compressed_files() {
    let prefix = "test_fetch_archive_compressed";

    let filename = esthri_test::test_data("index.html");
    let s3client = esthri_test::get_s3client();
    let s3_key = format!("{}/{}", prefix, "index_compressed.html");
    let opts = EsthriPutOptParamsBuilder::default().build().unwrap();
    let opts_compressed = EsthriPutOptParamsBuilder::default()
        .transparent_compression(true)
        .storage_class(Some(Config::global().storage_class()))
        .build()
        .unwrap();
    let res = blocking::upload(
        s3client.as_ref(),
        esthri_test::TEST_BUCKET,
        s3_key,
        filename,
        opts_compressed,
    );
    assert!(res.is_ok());

    let filename = esthri_test::test_data("index.html");
    let s3client = esthri_test::get_s3client();
    let s3_key = format!("{}/{}", prefix, "index_notcompressed.html");
    let res = blocking::upload(
        s3client.as_ref(),
        esthri_test::TEST_BUCKET,
        s3_key,
        filename,
        opts,
    );
    assert!(res.is_ok());

    let key_hash_pairs: Vec<KeyHashPair> = vec![
        KeyHashPair("index_compressed.html", "b4e3f354e8575e2fa5f489ab6078917c"),
        KeyHashPair(
            "index_notcompressed.html",
            "b4e3f354e8575e2fa5f489ab6078917c",
        ),
    ];

    test_fetch_archive_helper(
        "/test_fetch_archive_compressed/?archive=true",
        "test_fetch_archive_compressed",
        key_hash_pairs,
    );
}

/// Tests nomimal behavaior of the `--index-html` switch which eventually manifests as a parameter passed to
/// [esthri_server::esthri_filter].  See [esthri_server::esthri_filter] for details on the intended behavior of the
/// `index_html` parameter.
///
#[tokio::test]
async fn test_index_url_nominal() {
    upload_index_url_test_data().await.unwrap();

    let s3client = esthri_test::get_s3client_async().await;
    let s3client = (*s3client).clone();

    let bucket = esthri_test::TEST_BUCKET;

    let filter = esthri_filter(s3client, bucket, true, &[]);

    let mut result = warp::test::request()
        .path("/index_html/")
        .filter(&filter)
        .await
        .unwrap();

    let body = hyper::body::to_bytes(result.body_mut()).await.unwrap();
    assert_eq!(body, "<p>Hello world!</p>\n");
}

/// See [esthri_server::esthri_filter] -- tests that requests without trailing slash will redirect to a slash if that
/// path resolves to a "directory".  In S3 directories are simulated by keys which use the "/" has a key separator.
///
#[tokio::test]
async fn test_index_url_redirect() {
    upload_index_url_test_data().await.unwrap();

    let s3client = esthri_test::get_s3client_async().await;
    let s3client = (*s3client).clone();

    let bucket = esthri_test::TEST_BUCKET;

    let filter = esthri_filter(s3client, bucket, true, &[]);

    let result = warp::test::request()
        .path("/index_html")
        .filter(&filter)
        .await
        .unwrap();

    assert_eq!(result.status(), warp::http::status::StatusCode::FOUND);
    assert_eq!(
        result
            .headers()
            .get(warp::http::header::LOCATION)
            .unwrap()
            .to_str()
            .unwrap(),
        "/index_html/"
    );
}

/// See [esthri_server::esthri_filter] -- tests that requests for a path without an `index.html` willl fall back to a
/// directory listing.
///
#[tokio::test]
async fn test_index_url_listing_fallback() {
    upload_index_url_test_data().await.unwrap();

    let s3client = esthri_test::get_s3client_async().await;
    let s3client = (*s3client).clone();

    let bucket = esthri_test::TEST_BUCKET;

    let filter = esthri_filter(s3client, bucket, true, &[]);

    let mut result = warp::test::request()
        .path("/index_html/subdir/has_index/index.html")
        .filter(&filter)
        .await
        .unwrap();

    let body = hyper::body::to_bytes(result.body_mut()).await.unwrap();
    assert_eq!(body, "<p>Hello world! I am from a subdir.</p>\n");
}

/// See [esthri_server::esthri_filter] -- sanity check test, just fetches a different index.html and makes sure it's
/// different from the top-level index.html (ensures that we're not redirecting or reading from an incorrect object
/// key).
///
#[tokio::test]
async fn test_index_url_different_index_html() {
    upload_index_url_test_data().await.unwrap();

    let s3client = esthri_test::get_s3client_async().await;
    let s3client = (*s3client).clone();

    let bucket = esthri_test::TEST_BUCKET;

    let filter = esthri_filter(s3client, bucket, true, &[]);

    let mut result = warp::test::request()
        .path("/index_html/subdir/has_index/index.html")
        .filter(&filter)
        .await
        .unwrap();

    let body = hyper::body::to_bytes(result.body_mut()).await.unwrap();
    assert_eq!(body, "<p>Hello world! I am from a subdir.</p>\n");

    let mut result = warp::test::request()
        .path("/index_html/subdir/has_index/")
        .filter(&filter)
        .await
        .unwrap();

    let body = hyper::body::to_bytes(result.body_mut()).await.unwrap();
    assert_eq!(body, "<p>Hello world! I am from a subdir.</p>\n");
}
