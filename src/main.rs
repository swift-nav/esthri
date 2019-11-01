extern crate anyhow;
extern crate clap;
extern crate ctrlc;
extern crate crypto;
extern crate glob;
extern crate hex;
extern crate once_cell;
extern crate structopt;
extern crate rusoto_core;
extern crate rusoto_s3;
extern crate thiserror;
extern crate walkdir;

use std::fs;
use std::error::Error;
use std::io::ErrorKind;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::io::prelude::*;
use std::path::Path;
use std::process;
use std::sync::Mutex;

use anyhow::{anyhow, Context};
use clap::arg_enum;
use crypto::digest::Digest;
use crypto::md5::Md5;
use glob::Pattern;
use thiserror::Error;
use once_cell::sync::Lazy;
use structopt::StructOpt;
use walkdir::WalkDir;

use rusoto_s3::{
    AbortMultipartUploadRequest,
    CreateMultipartUploadRequest,
    CompletedPart,
    CompleteMultipartUploadRequest,
    CompletedMultipartUpload,
    GetObjectRequest,
    HeadObjectRequest,
    ListObjectsV2Request,
    PutObjectRequest,
    S3,
    S3Client,
    StreamingBody,
    UploadPartRequest,
};

use rusoto_core::{
    Region,
    RusotoError,
};

struct GlobalData {
    bucket: Option<String>,
    key: Option<String>,
    upload_id: Option<String>,
}

const EXPECT_GLOBAL_DATA: &'static str = "failed to lock global data";

static GLOBAL_DATA: Lazy<Mutex<GlobalData>> = Lazy::new(|| {
    Mutex::new(GlobalData { bucket: None, key: None, upload_id: None })
});

// This is the default chunk size from awscli
const CHUNK_SIZE: u64 = 8 * 1024 * 1024;

const READ_SIZE: usize = 4096;

type Result<T> = std::result::Result<T, Box<dyn Error>>;

fn handle_abort(s3: &dyn S3, bucket: &str, key: &str, upload_id: &str) -> Result<()>
{
    let amur = AbortMultipartUploadRequest {
        bucket: bucket.into(),
        key: key.into(),
        upload_id: upload_id.into(),
        ..Default::default()
    };

    let fut = s3.abort_multipart_upload(amur);
    let res = fut.sync();

    if let Err(e) = res {
        eprintln!("abort_multipart_upload failed: {}", e);
        process::exit(1);
    }

    Ok(())
}

fn handle_upload(s3: &dyn S3, bucket: &str, key: &str, file: &str) -> Result<()>
{
    if ! Path::new(&file).exists() {
        eprintln!("source file does not exist: {}", file);
        process::exit(1);
    }

    let stat = fs::metadata(&file)?;
    let file_size = stat.len();

    eprintln!("file_size: {}", file_size);

    let f = File::open(file)?;
    let mut reader = BufReader::new(f);

    if file_size >= CHUNK_SIZE {

        let cmur = CreateMultipartUploadRequest {
            bucket: bucket.into(),
            key: key.into(),
            ..Default::default()
        };

        let fut = s3.create_multipart_upload(cmur);
        let res = fut.sync();

        if let Err(e) = res {

            eprintln!("create_multipart_upload failed: {}", e);
            process::exit(1);
        }

        let cmuo = res.unwrap();
        let upload_id = cmuo.upload_id;

        if upload_id.is_none() {

            eprintln!("create_multipart_upload upload_id was none");
            process::exit(1);
        }

        let upload_id = upload_id.unwrap();
        eprintln!("upload_id: {}", upload_id);

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

            let chunk_size = if remaining >= CHUNK_SIZE { CHUNK_SIZE } else { remaining };
            let mut buf = vec![0u8;chunk_size as usize];

            reader.read(&mut buf)?;

            let body: StreamingBody = buf.into();

            let upr = UploadPartRequest {
                bucket: bucket.into(),
                key: key.into(),
                part_number: part_number,
                upload_id: upload_id.clone(),
                body: Some(body),
                ..Default::default()
            };

            let fut = s3.upload_part(upr);
            let res = fut.sync();

            if let Err(e) = res {
                eprintln!("upload_part failed: {}", e);
                process::exit(1);
            }

            let upo = res.unwrap();

            if upo.e_tag.is_none() {
                eprintln!("WARNING: upload_part e_tag was not present");
            }

            let cp = CompletedPart { e_tag: upo.e_tag, part_number: Some(part_number) };

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
            upload_id: upload_id,
            multipart_upload: Some(cmpu),
            ..Default::default()
        };

        let fut = s3.complete_multipart_upload(cmur);
        let res = fut.sync();

        if let Err(e) = res {
            eprintln!("complete_multipart_upload failed: {}", e);
            process::exit(1);
        }

        // Clear multi-part upload
        {
            let mut global_data = GLOBAL_DATA.lock().expect(EXPECT_GLOBAL_DATA);
            global_data.bucket = None;
            global_data.key = None;
            global_data.upload_id = None;
        }

    } else {

        let mut buf = vec![0u8;file_size as usize];
        reader.read(&mut buf)?;

        let body: StreamingBody = buf.into();

        let por = PutObjectRequest {
            bucket: bucket.into(),
            key: key.into(),
            body: Some(body),
            ..Default::default()
        };

        let fut = s3.put_object(por);
        let res = fut.sync();

        if let Err(e) = res {

            eprintln!("put_object failed: {}", e);
            process::exit(1);
        }
    }

    Ok(())
}

fn handle_download(s3: &dyn S3, bucket: &str, key: &str, file: &str) -> Result<()>
{
    eprintln!("downloading: bucket={}, key={}, dest={}", bucket, key, file);

    let f = File::create(file)?;
    let mut writer = BufWriter::new(f);

    let gor = GetObjectRequest {
        bucket: bucket.into(),
        key: key.into(),
        ..Default::default()
    };

    let fut = s3.get_object(gor);
    let res = fut.sync();

    if let Err(e) = res {
        eprintln!("get_object failed: {}", e);
        process::exit(1);
    }

    let goo = res.unwrap();

    if goo.body.is_none() {
        eprintln!("did not expect body field of GetObjectOutput to be none");
        process::exit(1);
    }

    let body = goo.body.unwrap();
    let mut reader = body.into_blocking_read();

    loop {

        let mut blob = [0u8;READ_SIZE];
        let res = reader.read(&mut blob);

        if let Err(e) = res {
            if e.kind() == ErrorKind::Interrupted { continue; }
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

fn setup_cancel_handler() {
    ctrlc::set_handler(move || {
        let global_data = GLOBAL_DATA.lock().expect(EXPECT_GLOBAL_DATA);
        if global_data.bucket.is_none() || global_data.key.is_none() || global_data.upload_id.is_none()
        {
            eprintln!("\ncancelled");
        } else {
            if let Some(bucket) = &global_data.bucket {
                if let Some(key) = &global_data.key {
                    if let Some(upload_id) = &global_data.upload_id {
                        eprintln!("\ncancelling...");
                        let region = Region::default();
                        let s3 = S3Client::new(region);
                        let res = handle_abort(&s3, &bucket, &key, &upload_id);
                        if let Err(e) = res {
                            eprintln!("Error cancelling: {}", e);
                        }
                    }
                }
            }
        }
        process::exit(0);
    }).expect("Error setting Ctrl-C handler");
}

#[derive(Error, Debug)]
pub enum ETagErr {
    #[error("did not exist locally")]
    NotPresent,
}

fn s3_etag(path: &str) -> Result<String> {

    if ! Path::new(path).exists() {
        return Err(Box::new(ETagErr::NotPresent));
    }

    let f = File::open(path)?;
    let mut reader = BufReader::new(f);
    let mut hash = Md5::new();
    let stat = fs::metadata(&path)?;
    let file_size = stat.len();
    let mut digests: Vec<[u8;16]> = vec![];
    let mut remaining = file_size;

    while remaining != 0 {
        let chunk_size: usize = (if remaining >= CHUNK_SIZE { CHUNK_SIZE } else { remaining }) as usize;
        hash.reset();
        let mut blob = vec![0u8;chunk_size];
        let _ = reader.read_exact(&mut blob)?;
        hash.input(&blob);
        let mut hash_bytes = [0u8;16];
        hash.result(&mut hash_bytes);
        digests.push(hash_bytes);
        remaining -= chunk_size as u64;
    }

    if digests.len() == 0 {
        let mut hash_bytes = [0u8;16];
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
        let mut final_hash = [0u8;16];
        etag_hash.result(&mut final_hash);
        let hex_digest = hex::encode(final_hash);
        Ok(format!("\"{}-{}\"", hex_digest, count))
    }
}

fn handle_s3etag(path: &str) -> Result<()> {
    let etag = s3_etag(path)?;
    eprintln!("s3etag: file={}, etag={}", path, etag);
    Ok(())
}

fn head_object(s3: &dyn S3, bucket: &str, key: &str) -> Option<String>
{
    let hor = HeadObjectRequest {
        bucket: bucket.into(),
        key: key.into(),
        ..Default::default()
    };

    let fut = s3.head_object(hor);
    let res = fut.sync();

    match res {
        Ok(hoo) => {
            if let Some(delete_marker) = hoo.delete_marker {
                eprintln!("delete_marker: {}", delete_marker);
                if delete_marker {
                    return None;
                }
            }
            if let Some(e_tag) = hoo.e_tag {
                return Some(e_tag);
            }
        },
        Err(RusotoError::Unknown(e)) => {

            if e.status == 404 {
                return None;

            } else {
                eprintln!("head_object failed (1): {:?}", e);
                process::exit(1);
            }
        },
        Err(e) => {
            eprintln!("head_object failed (2): {:?}", e);
            process::exit(1);
        }
    }

    panic!("should NOT get here");
}

fn handle_head_object(s3: &dyn S3, bucket: &str, key: &str) -> Result<()>
{
    eprintln!("head-object: buckey={}, key={}", bucket, key);
    let e_tag = head_object(s3, bucket, key);

    eprintln!("etag: e_tag={:?}", e_tag);

    Ok(())
}

struct S3Listing
{
    count: i64,
    continuation: Option<String>,
    objects: Vec<S3Obj>,
}

struct S3Obj
{
    key: String,
    e_tag: String,
}

fn list_objects(s3: &dyn S3, bucket: &str, key: &str, continuation: Option<String>) -> Result<S3Listing> {

    let lov2r = ListObjectsV2Request {
        bucket: bucket.into(),
        prefix: Some(key.into()),
        continuation_token: continuation,
        ..Default::default()
    };

    let fut = s3.list_objects_v2(lov2r);
    let res = fut.sync();

    let lov2o = res.context(anyhow!("listing objects failed"))?;
    let contents = if lov2o.contents.is_none() {
        eprintln!("listing returned no contents");
        return Ok(S3Listing { count: 0, continuation: None, objects: vec![] });
    } else {
        lov2o.contents.unwrap()
    };

    let count = lov2o.key_count.ok_or(anyhow!("unexpected: key count was none"))?;

    let mut listing = S3Listing { count: count, continuation: lov2o.next_continuation_token, objects: vec![] };

    for object in contents {
        let key = if object.key.is_some() { object.key.unwrap() }
                  else { eprintln!("unexpected: object key was null"); continue; };
        let e_tag = if object.e_tag.is_some() { object.e_tag.unwrap() } 
                    else { eprintln!("unexpected: object ETag was null"); continue; };
        listing.objects.push(S3Obj { key: key, e_tag: e_tag });
    }

    Ok(listing)
}

fn process_globs<'a>(path: &'a str, glob_includes: &Vec<Pattern>, glob_excludes: &Vec<Pattern>) -> Option<&'a str>
{
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
    if included {
        Some(path)
    } else if !excluded {
        Some(path)
    } else {
        None
    }
}

fn download_with_dir(s3: &dyn S3, bucket: &str, s3_prefix: &str, s3_suffix: &str, local_dir: &str) -> Result<()>
{
    let dest_path = Path::new(local_dir).join(s3_suffix);

    let parent_dir = dest_path.parent().ok_or(anyhow!("unexpected: parent dir was null"))?;
    let parent_dir = format!("{}", parent_dir.display());

    /*
    eprintln!("bucket={}, s3_prefix={}, s3_suffix={}, local_dir={}, parent_dir={}",
              bucket, s3_prefix, s3_suffix, local_dir, parent_dir);
    */

    fs::create_dir_all(parent_dir)?;

    let key = format!("{}", Path::new(s3_prefix).join(s3_suffix).display());
    let dest_path = format!("{}", dest_path.display());

    handle_download(s3, bucket, &key, &dest_path)?;

    Ok(())
}

fn sync_local_to_remote(
        s3: &dyn S3,
        bucket: &str,
        key: &str,
        directory: &str,
        glob_includes: &Vec<Pattern>,
        glob_excludes: &Vec<Pattern>,
    ) -> Result<()> 
{
    for entry in WalkDir::new(directory) {
        let entry = entry?;
        let stat = entry.metadata()?;
        if stat.is_dir() {
            continue;
        }
        // TODO: abort if symlink?
        let path = format!("{}", entry.path().display());
        eprintln!("local path={}", path);
        let path = process_globs(&path, glob_includes, glob_excludes);
        if let Some(path) = path {
            let remote_path = Path::new(key);
            let stripped_path = entry.path().strip_prefix(&directory);
            let stripped_path = match stripped_path {
                Err(e) => { eprintln!("unexpected: failed to strip prefix: {}", e); continue; }
                Ok(result) => result,
            };
            let stripped_path = format!("{}", stripped_path.display());
            let remote_path: String = format!("{}", remote_path.join(&stripped_path).display());
            eprintln!("checking remote: {}", remote_path);
            let remote_etag = head_object(s3, bucket, &remote_path);
            let local_etag = s3_etag(&path)?;
            if remote_etag.is_none() {
                eprintln!("file did not exist remotely: {}", remote_path);
                handle_upload(s3, bucket, &remote_path, &path)?;
            } else {
                let remote_etag = remote_etag.unwrap();
                if remote_etag != local_etag {
                    eprintln!("file etag mistmatch: {}, remote_etag={}, local_etag={}", remote_path, remote_etag, local_etag);
                    handle_upload(s3, bucket, &remote_path, &path)?;
                } else {
                    eprintln!("etag match: {}, remote_etag={}, local_etag={}", remote_path, remote_etag, local_etag);
                }
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
        glob_includes: &Vec<Pattern>,
        glob_excludes: &Vec<Pattern>,
    ) -> Result<()>
{
    let mut continuation: Option<String> = None;
    let dir_path = Path::new(directory);

    loop {
        let listing = list_objects(s3, bucket, key, continuation)?;
        eprintln!("syncing {} objects", listing.count);
        for entry in listing.objects {
            eprintln!("key={}", entry.key);
            let path = format!("{}", Path::new(&entry.key).strip_prefix(key)?.display());
            let path = process_globs(&path, glob_includes, glob_excludes);
            if let Some(path) = path {
                let local_path: String = format!("{}", dir_path.join(&path).display());
                eprintln!("checking {}", local_path);
                let local_etag = s3_etag(&local_path);
                match local_etag {
                    Ok(local_etag) => { 
                        if local_etag != entry.e_tag {
                            eprintln!("etag mismatch: {}, local etag={}, remote etag={}", local_path, local_etag, entry.e_tag);
                            download_with_dir(s3, bucket, &key, &path, &directory)?;
                        }
                    },
                    Err(err) => {
                        let not_present: Option<&ETagErr> = err.downcast_ref();
                        match not_present {
                            Some(ETagErr::NotPresent) => { 
                                eprintln!("file did not exist locally: {}", local_path);
                                download_with_dir(s3, bucket, &key, &path, &directory)?;
                            },
                            None => { eprintln!("s3 etag error: {}", err); },
                        }
                    },
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

fn handle_sync(
        s3: &dyn S3,
        direction: SyncDirection,
        bucket: &str,
        key: &str,
        directory: &str,
        includes: &Option<Vec<String>>,
        excludes: &Option<Vec<String>>,
    ) -> Result<()> 
{
    eprintln!("sync: direction={}, bucket={}, key={}, directory={}, include={:?}, exclude={:?}",
              direction, bucket, key, directory, includes, excludes);

    let mut glob_excludes: Vec<Pattern> = vec![];
    let mut glob_includes: Vec<Pattern> = vec![];

    if let Some(excludes) = excludes {
        for exclude in excludes {
            match Pattern::new(exclude) {
                Err(e) => {
                    eprintln!("exclude glob pattern error for {}: {}", exclude, e);
                    process::exit(1);
                },
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
                    eprintln!("include glob pattern error for {}: {}", include, e);
                    process::exit(1);
                },
                Ok(p) => {
                    glob_includes.push(p);
                }
            }
        }
    }

    // TODO: Default excludes to '*' if excludes is empty and includes has something?

    match direction {
        SyncDirection::up => {
            sync_local_to_remote(s3, bucket, key, directory, &glob_includes, &glob_excludes)?;
        },
        SyncDirection::down => {
            sync_remote_to_local(s3, bucket, key, directory, &glob_includes, &glob_excludes)?;
        }
    }

    Ok(())
}

fn handle_list_objects(s3: &dyn S3, bucket: &str, key: &str) -> Result<()> {

    let mut continuation: Option<String> = None;
    loop {
        let listing = list_objects(s3, bucket, key, continuation)?;
        for entry in listing.objects {
            eprintln!("key={}, etag={}", entry.key, entry.e_tag);
        }
        if listing.continuation.is_none() {
            break;
        }
        continuation = listing.continuation;
    }

    Ok(())
}

#[derive(Debug, StructOpt)]
#[structopt(name = "esthri", about = "Simple S3 upload implementation.")]
struct Cli {
    #[structopt(subcommand)]
    cmd: Command
}

arg_enum! {
    #[derive(Debug)]
    #[allow(non_camel_case_types)]
    enum SyncDirection {
        up,
        down,
    }
}

#[derive(Debug, StructOpt)]
enum Command
{
    /// Upload an object to S3
    Put {
        #[structopt(long)]
        bucket: String,
        #[structopt(long)]
        key: String,
        /// The path of the local file to read
        file: String,
    },
    /// Download an object from S3
    Get {
        #[structopt(long)]
        bucket: String,
        #[structopt(long)]
        key: String,
        /// The path of the local file to write
        file: String,
    },
    /// Manually abort a multipart upload
    Abort {
        /// The bucket for the multipart upload
        #[structopt(long)]
        bucket: String,
        /// The key for the multipart upload
        #[structopt(long)]
        key: String,
        /// The upload_id for the multipart upload
        upload_id: String,
    },
    /// Compute and print the S3 ETag of the file
    S3Etag {
        file: String,
    },
    /// Sync a directory with S3
    #[structopt(name="sync")]
    SyncCmd {
        /// The direction of the sync: 'up' for local to remote, 'down' for remote to local
        #[structopt(long, default_value="up")]
        direction: SyncDirection, 
        #[structopt(long)]
        bucket: String, 
        #[structopt(long)]
        key: String, 
        /// The directory to use for up/down sync
        #[structopt(long)]
        directory: String, 
        /// Optional include glob pattern (see man 3 glob)
        #[structopt(long)]
        include: Option<Vec<String>>,
        /// Optional exclude glob pattern (see man 3 glob)
        #[structopt(long)]
        exclude: Option<Vec<String>>,
    },
    /// Retreive the ETag for a remote object
    HeadObject {
        /// The bucket to target
        #[structopt(long)]
        bucket: String, 
        /// The key to target
        #[structopt(long)]
        key: String, 
    },
    /// List remote objects in S3
    ListObjects {
        /// The bucket to target
        #[structopt(long)]
        bucket: String, 
        /// The key to target
        #[structopt(long)]
        key: String, 
    }
}

fn main() -> Result<()> {

    use Command::*;

    let cli = Cli::from_args();
   
    let region = Region::default();
    let s3 = S3Client::new(region);

    setup_cancel_handler();

    match cli.cmd {

        Put { bucket, key, file } => {
            eprintln!("put: bucket={}, key={}, file={}", bucket, key, file);
            handle_upload(&s3, &bucket, &key, &file)?;
        },

        Get { bucket, key, file } => {
            eprintln!("get: bucket={}, key={}, file={}", bucket, key, file);
            handle_download(&s3, &bucket, &key, &file)?;
        },

        Abort { bucket, key, upload_id } => {
            eprintln!("abort: bucket={}, key={}, upload_id={}", bucket, key, upload_id);
            handle_abort(&s3, &bucket, &key, &upload_id)?;
        },

        S3Etag { file } => {
            eprintln!("s3etag: file={}", file);
            handle_s3etag(&file)?;
        },

        SyncCmd { direction, bucket, key, directory, include, exclude } => {
            handle_sync(&s3, direction, &bucket, &key, &directory, &include, &exclude)?;
        },

        HeadObject {  bucket, key } => {
            handle_head_object(&s3, &bucket, &key)?;
        },

        ListObjects { bucket, key } => {
            handle_list_objects(&s3, &bucket, &key)?;
        },
    }

    Ok(())
}
