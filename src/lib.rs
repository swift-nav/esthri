#![cfg_attr(feature = "aggressive_lint", deny(warnings))]

use std::fs;
use std::fs::File;
use std::io::prelude::*;
use std::io::ErrorKind;
use std::io::{BufReader, BufWriter};
use std::path::Path;
use std::process;
use std::sync::Mutex;

use anyhow::{anyhow, ensure, Context};
use crypto::digest::Digest;
use crypto::md5::Md5;
use glob::Pattern;
use log::*;
use log_derive::logfn;
use once_cell::sync::Lazy;
use tokio::io::AsyncReadExt;
use walkdir::WalkDir;

pub mod errors;
pub mod types;

use crate::errors::EsthriError;
use crate::types::SyncDirection;

use rusoto_s3::{
    AbortMultipartUploadRequest, CompleteMultipartUploadRequest, CompletedMultipartUpload,
    CompletedPart, CreateMultipartUploadRequest, GetObjectRequest, HeadObjectRequest,
    ListObjectsV2Request, PutObjectRequest, S3Client, StreamingBody, UploadPartRequest, S3,
};

use rusoto_core::{Region, RusotoError};

struct GlobalData {
    bucket: Option<String>,
    key: Option<String>,
    upload_id: Option<String>,
}

const EXPECT_GLOBAL_DATA: &str = "failed to lock global data";

const FORWARD_SLASH: char = '/';

static GLOBAL_DATA: Lazy<Mutex<GlobalData>> = Lazy::new(|| {
    Mutex::new(GlobalData {
        bucket: None,
        key: None,
        upload_id: None,
    })
});

// This is the default chunk size from awscli
const CHUNK_SIZE: u64 = 8 * 1024 * 1024;

const READ_SIZE: usize = 4096;

pub use anyhow::Result;

#[logfn(err = "ERROR")]
pub fn s3_head_object(s3: &dyn S3, bucket: &str, key: &str) -> Result<Option<String>> {
    info!("head-object: buckey={}, key={}", bucket, key);
    let e_tag = head_object(s3, bucket, key)?;
    debug!("etag: e_tag={:?}", e_tag);
    Ok(e_tag)
}

#[deprecated(since = "0.2.1", note = "use s3_head_object instead")]
pub fn handle_head_object(s3: &dyn S3, bucket: &str, key: &str) -> Result<Option<String>> {
    s3_head_object(s3, bucket, key)
}

#[logfn(err = "ERROR")]
pub fn s3_log_etag(path: &str) -> Result<String> {
    info!("s3etag: path={}", path);
    let etag = s3_compute_etag(path)?;
    debug!("s3etag: file={}, etag={}", path, etag);
    Ok(etag)
}

#[deprecated(since = "0.2.1", note = "use s3_log_etag instead")]
pub fn handle_s3etag(path: &str) -> Result<()> {
    let _etag = s3_log_etag(path)?;
    Ok(())
}

#[tokio::main]
#[logfn(err = "ERROR")]
pub async fn s3_abort_upload(s3: &dyn S3, bucket: &str, key: &str, upload_id: &str) -> Result<()> {
    info!(
        "abort: bucket={}, key={}, upload_id={}",
        bucket, key, upload_id
    );

    let amur = AbortMultipartUploadRequest {
        bucket: bucket.into(),
        key: key.into(),
        upload_id: upload_id.into(),
        ..Default::default()
    };

    let res = s3.abort_multipart_upload(amur).await;
    let _ = res.context("abort_multipart_upload failed")?;

    Ok(())
}

#[deprecated(since = "0.2.1", note = "use s3_abort_upload instead")]
pub fn handle_abort(s3: &dyn S3, bucket: &str, key: &str, upload_id: &str) -> Result<()> {
    s3_abort_upload(s3, bucket, key, upload_id)
}

pub fn s3_upload(s3: &dyn S3, bucket: &str, key: &str, file: &str) -> Result<()> {
    info!("put: bucket={}, key={}, file={}", bucket, key, file);

    ensure!(
        Path::new(&file).exists(),
        anyhow!("source file does not exist: {}", file)
    );

    let stat = fs::metadata(&file)?;
    let file_size = stat.len();

    debug!("file_size: {}", file_size);

    let f = File::open(file)?;
    let mut reader = BufReader::new(f);

    s3_upload_from_reader(s3, bucket, key, &mut reader, file_size)
}

#[deprecated(since = "0.2.1", note = "use s3_upload_from_reader instead")]
pub fn s3_upload_reader(
    s3: &dyn S3,
    bucket: &str,
    key: &str,
    reader: &mut dyn Read,
    file_size: u64,
) -> Result<()> {
    s3_upload_from_reader(s3, bucket, key, reader, file_size)
}

#[tokio::main]
#[logfn(err = "ERROR")]
pub async fn s3_upload_from_reader(
    s3: &dyn S3,
    bucket: &str,
    key: &str,
    reader: &mut dyn Read,
    file_size: u64,
) -> Result<()> {
    info!(
        "put: bucket={}, key={}, file_size={}",
        bucket, key, file_size
    );

    if file_size >= CHUNK_SIZE {
        let cmur = CreateMultipartUploadRequest {
            bucket: bucket.into(),
            key: key.into(),
            ..Default::default()
        };

        let res = s3.create_multipart_upload(cmur).await;

        let cmuo = res.context("create_multipart_upload failed")?;

        let upload_id = cmuo
            .upload_id
            .ok_or_else(|| anyhow!("create_multipart_upload upload_id was none"))?;

        debug!("upload_id: {}", upload_id);

        // Load into global data so it can be cancelled for CTRL-C / SIGTERM
        {
            let mut global_data = GLOBAL_DATA.lock().expect(EXPECT_GLOBAL_DATA);
            global_data.bucket = Some(bucket.into());
            global_data.key = Some(key.into());
            global_data.upload_id = Some(upload_id.clone());
        }

        let mut remaining = file_size;
        let mut part_number = 1;
        let mut completed_parts: Vec<CompletedPart> = vec![];

        while remaining != 0 {
            let chunk_size = if remaining >= CHUNK_SIZE {
                CHUNK_SIZE
            } else {
                remaining
            };
            let mut buf = vec![0u8; chunk_size as usize];

            let res = reader.read(&mut buf);
            let read_count = res.context("read call returned error")?;

            if read_count == 0 {
                return Err(anyhow!("read size zero"));
            }

            let body: StreamingBody = buf.into();

            let upr = UploadPartRequest {
                bucket: bucket.into(),
                key: key.into(),
                part_number,
                upload_id: upload_id.clone(),
                body: Some(body),
                ..Default::default()
            };

            let res = s3.upload_part(upr).await;
            let upo = res.context("upload_part failed")?;

            if upo.e_tag.is_none() {
                warn!("upload_part e_tag was not present");
            }

            let cp = CompletedPart {
                e_tag: upo.e_tag,
                part_number: Some(part_number),
            };

            completed_parts.push(cp);

            remaining -= chunk_size;
            part_number += 1;
        }

        let cmpu = CompletedMultipartUpload {
            parts: Some(completed_parts),
        };

        let cmur = CompleteMultipartUploadRequest {
            bucket: bucket.into(),
            key: key.into(),
            upload_id,
            multipart_upload: Some(cmpu),
            ..Default::default()
        };

        let res = s3.complete_multipart_upload(cmur).await;
        let _ = res.context("complete_multipart_upload failed")?;

        // Clear multi-part upload
        {
            let mut global_data = GLOBAL_DATA.lock().expect(EXPECT_GLOBAL_DATA);
            global_data.bucket = None;
            global_data.key = None;
            global_data.upload_id = None;
        }
    } else {
        let mut buf = vec![0u8; file_size as usize];
        let read_size = reader.read(&mut buf).context("read returned failure")?;

        if read_size == 0 && file_size != 0 {
            return Err(anyhow!("read size zero"));
        }

        let body: StreamingBody = buf.into();

        let por = PutObjectRequest {
            bucket: bucket.into(),
            key: key.into(),
            body: Some(body),
            acl: Some("bucket-owner-full-control".into()),
            ..Default::default()
        };

        let res = s3.put_object(por).await;
        let _ = res.context("put_object failed")?;
    }

    Ok(())
}

#[deprecated(since = "0.2.1", note = "use s3_download instead")]
pub fn handle_download(s3: &dyn S3, bucket: &str, key: &str, file: &str) -> Result<()> {
    s3_download(s3, bucket, key, file)
}

#[tokio::main]
#[logfn(err = "ERROR")]
pub async fn s3_download(s3: &dyn S3, bucket: &str, key: &str, file: &str) -> Result<()> {
    info!("get: bucket={}, key={}, file={}", bucket, key, file);

    let f = File::create(file)?;
    let mut writer = BufWriter::new(f);

    let gor = GetObjectRequest {
        bucket: bucket.into(),
        key: key.into(),
        ..Default::default()
    };

    let res = s3.get_object(gor).await;

    let goo = res.context("get_object failed")?;
    let body = goo
        .body
        .ok_or_else(|| anyhow!("did not expect body field of GetObjectOutput to be none"))?;

    let mut reader = body.into_async_read();

    loop {
        let mut blob = [0u8; READ_SIZE];
        let res = reader.read(&mut blob).await;

        if let Err(e) = res {
            if e.kind() == ErrorKind::Interrupted {
                continue;
            }
        } else {
            let read_size = res?;
            if read_size == 0 {
                break;
            }
            writer.write_all(&blob[..read_size])?;
        }
    }

    Ok(())
}

#[deprecated(since = "0.2.1", note = "use s3_sync instead")]
pub fn handle_sync(
    s3: &dyn S3,
    direction: SyncDirection,
    bucket: &str,
    key: &str,
    directory: &str,
    includes: &Option<Vec<String>>,
    excludes: &Option<Vec<String>>,
) -> Result<()> {
    s3_sync(s3, direction, bucket, key, directory, includes, excludes)
}

#[logfn(err = "ERROR")]
pub fn s3_sync(
    s3: &dyn S3,
    direction: SyncDirection,
    bucket: &str,
    key: &str,
    directory: &str,
    includes: &Option<Vec<String>>,
    excludes: &Option<Vec<String>>,
) -> Result<()> {
    info!(
        "sync: direction={}, bucket={}, key={}, directory={}, include={:?}, exclude={:?}",
        direction, bucket, key, directory, includes, excludes
    );

    let mut glob_excludes: Vec<Pattern> = vec![];
    let mut glob_includes: Vec<Pattern> = vec![];

    if let Some(excludes) = excludes {
        for exclude in excludes {
            match Pattern::new(exclude) {
                Err(e) => {
                    return Err(anyhow!("exclude glob pattern error for {}: {}", exclude, e));
                }
                Ok(p) => {
                    glob_excludes.push(p);
                }
            }
        }
    }

    if let Some(includes) = includes {
        for include in includes {
            match Pattern::new(include) {
                Err(e) => {
                    return Err(anyhow!("include glob pattern error for {}: {}", include, e));
                }
                Ok(p) => {
                    glob_includes.push(p);
                }
            }
        }
    } else {
        glob_includes.push(Pattern::new("*")?);
    }

    match direction {
        SyncDirection::up => {
            sync_local_to_remote(s3, bucket, key, directory, &glob_includes, &glob_excludes)?;
        }
        SyncDirection::down => {
            sync_remote_to_local(s3, bucket, key, directory, &glob_includes, &glob_excludes)?;
        }
    }

    Ok(())
}

#[deprecated(since = "0.2.1", note = "use s3_list_objects instead")]
pub fn handle_list_objects(s3: &dyn S3, bucket: &str, key: &str) -> Result<Vec<String>> {
    s3_list_objects(s3, bucket, key)
}

#[logfn(err = "ERROR")]
pub fn s3_list_objects(s3: &dyn S3, bucket: &str, key: &str) -> Result<Vec<String>> {
    info!("list-objects: bucket={}, key={}", bucket, key);

    let mut bucket_contents = Vec::new();
    let mut continuation: Option<String> = None;
    loop {
        let listing = list_objects(s3, bucket, key, continuation)?;
        if !listing.objects.is_empty() {
            for entry in listing.objects {
                info!("key={}, etag={}", entry.key, entry.e_tag);
                bucket_contents.push(entry.key);
            }
        }
        if listing.continuation.is_none() {
            break;
        }
        continuation = listing.continuation;
    }

    Ok(bucket_contents)
}

pub fn setup_cancel_handler() {
    ctrlc::set_handler(move || {
        let global_data = GLOBAL_DATA.lock().expect(EXPECT_GLOBAL_DATA);
        if global_data.bucket.is_none()
            || global_data.key.is_none()
            || global_data.upload_id.is_none()
        {
            info!("\ncancelled");
        } else if let Some(bucket) = &global_data.bucket {
            if let Some(key) = &global_data.key {
                if let Some(upload_id) = &global_data.upload_id {
                    info!("\ncancelling...");
                    let region = Region::default();
                    let s3 = S3Client::new(region);
                    let res = s3_abort_upload(&s3, &bucket, &key, &upload_id);
                    if let Err(e) = res {
                        error!("cancelling failed: {}", e);
                    }
                }
            }
        }
        process::exit(0);
    })
    .expect("Error setting Ctrl-C handler");
}

fn s3_compute_etag(path: &str) -> Result<String> {
    if !Path::new(path).exists() {
        return Err(anyhow!(EsthriError::ETagNotPresent));
    }

    let f = File::open(path)?;
    let mut reader = BufReader::new(f);
    let mut hash = Md5::new();
    let stat = fs::metadata(&path)?;
    let file_size = stat.len();
    let mut digests: Vec<[u8; 16]> = vec![];
    let mut remaining = file_size;

    while remaining != 0 {
        let chunk_size: usize = (if remaining >= CHUNK_SIZE {
            CHUNK_SIZE
        } else {
            remaining
        }) as usize;
        hash.reset();
        let mut blob = vec![0u8; chunk_size];
        reader.read_exact(&mut blob)?;
        hash.input(&blob);
        let mut hash_bytes = [0u8; 16];
        hash.result(&mut hash_bytes);
        digests.push(hash_bytes);
        remaining -= chunk_size as u64;
    }

    if digests.is_empty() {
        let mut hash_bytes = [0u8; 16];
        hash.result(&mut hash_bytes);
        let hex_digest = hex::encode(hash_bytes);
        Ok(format!("\"{}\"", hex_digest))
    } else if digests.len() == 1 && file_size < CHUNK_SIZE {
        let hex_digest = hex::encode(digests[0]);
        Ok(format!("\"{}\"", hex_digest))
    } else {
        let count = digests.len();
        let mut etag_hash = Md5::new();
        for digest_bytes in digests {
            etag_hash.input(&digest_bytes);
        }
        let mut final_hash = [0u8; 16];
        etag_hash.result(&mut final_hash);
        let hex_digest = hex::encode(final_hash);
        Ok(format!("\"{}-{}\"", hex_digest, count))
    }
}

#[tokio::main]
#[logfn(err = "ERROR")]
async fn head_object(s3: &dyn S3, bucket: &str, key: &str) -> Result<Option<String>> {
    let hor = HeadObjectRequest {
        bucket: bucket.into(),
        key: key.into(),
        ..Default::default()
    };

    let res = s3.head_object(hor).await;

    match res {
        Ok(hoo) => {
            if let Some(delete_marker) = hoo.delete_marker {
                eprintln!("delete_marker: {}", delete_marker);
                if delete_marker {
                    return Ok(None);
                }
            }
            if let Some(e_tag) = hoo.e_tag {
                return Ok(Some(e_tag));
            }
        }
        Err(RusotoError::Unknown(e)) => {
            if e.status == 404 {
                return Ok(None);
            } else {
                return Err(anyhow!("head_object failed (1): {:?}", e));
            }
        }
        Err(e) => {
            return Err(anyhow!("head_object failed (2): {:?}", e));
        }
    }

    panic!("should NOT get here");
}

struct S3Listing {
    count: i64,
    continuation: Option<String>,
    objects: Vec<S3Obj>,
}

struct S3Obj {
    key: String,
    e_tag: String,
}

#[tokio::main]
async fn list_objects(
    s3: &dyn S3,
    bucket: &str,
    key: &str,
    continuation: Option<String>,
) -> Result<S3Listing> {
    let lov2r = ListObjectsV2Request {
        bucket: bucket.into(),
        prefix: Some(key.into()),
        continuation_token: continuation,
        ..Default::default()
    };

    let res = s3.list_objects_v2(lov2r).await;

    let lov2o = res.context("listing objects failed")?;
    let contents = if lov2o.contents.is_none() {
        warn!("listing returned no contents");
        return Ok(S3Listing {
            count: 0,
            continuation: None,
            objects: vec![],
        });
    } else {
        lov2o.contents.unwrap()
    };

    let count = lov2o
        .key_count
        .ok_or_else(|| anyhow!("unexpected: key count was none"))?;

    let mut listing = S3Listing {
        count,
        continuation: lov2o.next_continuation_token,
        objects: vec![],
    };

    for object in contents {
        let key = if object.key.is_some() {
            object.key.unwrap()
        } else {
            warn!("unexpected: object key was null");
            continue;
        };
        let e_tag = if object.e_tag.is_some() {
            object.e_tag.unwrap()
        } else {
            warn!("unexpected: object ETag was null");
            continue;
        };
        listing.objects.push(S3Obj { key, e_tag });
    }

    Ok(listing)
}

fn process_globs<'a>(
    path: &'a str,
    glob_includes: &[Pattern],
    glob_excludes: &[Pattern],
) -> Option<&'a str> {
    let mut excluded = false;
    let mut included = false;
    for pattern in glob_excludes {
        if pattern.matches(path) {
            excluded = true;
        }
    }
    for pattern in glob_includes {
        if pattern.matches(path) {
            included = true;
        }
    }
    if included && !excluded {
        Some(path)
    } else {
        None
    }
}

#[test]
fn test_process_globs() {
    let includes = vec![Pattern::new("*.csv").unwrap()];
    let excludes = vec![Pattern::new("*-blah.csv").unwrap()];

    assert!(process_globs("data.sbp", &includes[..], &excludes[..]).is_none());
    assert!(process_globs("yes.csv", &includes[..], &excludes[..]).is_some());
    assert!(process_globs("no-blah.csv", &includes[..], &excludes[..]).is_none());
}

#[test]
fn test_process_globs_exclude_all() {
    let includes = vec![Pattern::new("*.png").unwrap()];
    let excludes = vec![];

    assert!(process_globs("a-fancy-thing.png", &includes[..], &excludes[..]).is_some());
    assert!(process_globs("horse.gif", &includes[..], &excludes[..]).is_none());
}

fn download_with_dir(
    s3: &dyn S3,
    bucket: &str,
    s3_prefix: &str,
    s3_suffix: &str,
    local_dir: &str,
) -> Result<()> {
    let dest_path = Path::new(local_dir).join(s3_suffix);

    let parent_dir = dest_path
        .parent()
        .ok_or_else(|| anyhow!("unexpected: parent dir was null"))?;
    let parent_dir = format!("{}", parent_dir.display());

    fs::create_dir_all(parent_dir)?;

    let key = format!("{}", Path::new(s3_prefix).join(s3_suffix).display());
    let dest_path = format!("{}", dest_path.display());

    s3_download(s3, bucket, &key, &dest_path)?;

    Ok(())
}

fn sync_local_to_remote(
    s3: &dyn S3,
    bucket: &str,
    key: &str,
    directory: &str,
    glob_includes: &[Pattern],
    glob_excludes: &[Pattern],
) -> Result<()> {
    if !key.ends_with(FORWARD_SLASH) {
        return Err(EsthriError::DirlikePrefixRequired.into());
    }
    for entry in WalkDir::new(directory) {
        let entry = entry?;
        let stat = entry.metadata()?;
        if stat.is_dir() {
            continue;
        }
        // TODO: abort if symlink?
        let path = format!("{}", entry.path().display());
        debug!("local path={}", path);
        let path = process_globs(&path, glob_includes, glob_excludes);
        if let Some(path) = path {
            let remote_path = Path::new(key);
            let stripped_path = entry.path().strip_prefix(&directory);
            let stripped_path = match stripped_path {
                Err(e) => {
                    warn!("unexpected: failed to strip prefix: {}", e);
                    continue;
                }
                Ok(result) => result,
            };
            let stripped_path = format!("{}", stripped_path.display());
            let remote_path: String = format!("{}", remote_path.join(&stripped_path).display());
            debug!("checking remote: {}", remote_path);
            let remote_etag = head_object(s3, bucket, &remote_path)?;
            let local_etag = s3_compute_etag(&path)?;
            if let Some(remote_etag) = remote_etag {
                if remote_etag != local_etag {
                    info!(
                        "etag mis-match: {}, remote_etag={}, local_etag={}",
                        remote_path, remote_etag, local_etag
                    );
                    s3_upload(s3, bucket, &remote_path, &path)?;
                } else {
                    debug!(
                        "etags matched: {}, remote_etag={}, local_etag={}",
                        remote_path, remote_etag, local_etag
                    );
                }
            } else {
                info!("file did not exist remotely: {}", remote_path);
                s3_upload(s3, bucket, &remote_path, &path)?;
            }
        }
    }

    Ok(())
}

fn sync_remote_to_local(
    s3: &dyn S3,
    bucket: &str,
    key: &str,
    directory: &str,
    glob_includes: &[Pattern],
    glob_excludes: &[Pattern],
) -> Result<()> {
    if !key.ends_with(FORWARD_SLASH) {
        return Err(EsthriError::DirlikePrefixRequired.into());
    }
    let mut continuation: Option<String> = None;
    let dir_path = Path::new(directory);
    loop {
        let listing = list_objects(s3, bucket, key, continuation)?;
        debug!("syncing {} objects", listing.count);
        for entry in listing.objects {
            debug!("key={}", entry.key);
            let path = format!(
                "{}",
                Path::new(&entry.key)
                    .strip_prefix(key)
                    .with_context(|| format!("entry.key: {}, prefix: {}", &entry.key, &key))?
                    .display()
            );
            let path = process_globs(&path, glob_includes, glob_excludes);
            if let Some(path) = path {
                let local_path: String = format!("{}", dir_path.join(&path).display());
                debug!("checking {}", local_path);
                let local_etag = s3_compute_etag(&local_path);
                match local_etag {
                    Ok(local_etag) => {
                        if local_etag != entry.e_tag {
                            debug!(
                                "etag mismatch: {}, local etag={}, remote etag={}",
                                local_path, local_etag, entry.e_tag
                            );
                            download_with_dir(s3, bucket, &key, &path, &directory)?;
                        }
                    }
                    Err(err) => {
                        let not_present: Option<&EsthriError> = err.downcast_ref();
                        match not_present {
                            Some(EsthriError::ETagNotPresent) => {
                                debug!("file did not exist locally: {}", local_path);
                                download_with_dir(s3, bucket, &key, &path, &directory)?;
                            }
                            Some(_) | None => {
                                warn!("s3 etag error: {}", err);
                            }
                        }
                    }
                }
            }
        }
        if listing.continuation.is_none() {
            break;
        }
        continuation = listing.continuation;
    }

    Ok(())
}
