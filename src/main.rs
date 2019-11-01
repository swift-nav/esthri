extern crate clap;
extern crate ctrlc;
extern crate crypto;
extern crate hex;
extern crate once_cell;
extern crate structopt;
extern crate rusoto_core;
extern crate rusoto_s3;

use std::fs;
use std::error::Error;
use std::io::ErrorKind;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::io::prelude::*;
use std::path::Path;
use std::process;
use std::sync::Mutex;

use clap::arg_enum;
use crypto::digest::Digest;
use crypto::md5::Md5;
use once_cell::sync::Lazy;
use structopt::StructOpt;

use rusoto_s3::{
    AbortMultipartUploadRequest,
    CreateMultipartUploadRequest,
    CompletedPart,
    CompleteMultipartUploadRequest,
    CompletedMultipartUpload,
    GetObjectRequest,
    PutObjectRequest,
    S3,
    S3Client,
    StreamingBody,
    UploadPartRequest,
};

use rusoto_core::Region;

struct GlobalData {
    bucket: Option<String>,
    key: Option<String>,
    upload_id: Option<String>,
}

const EXPECT_GLOBAL_DATA: &'static str = "failed to lock global data";

static GLOBAL_DATA: Lazy<Mutex<GlobalData>> = Lazy::new(|| {
    Mutex::new(GlobalData { bucket: None, key: None, upload_id: None })
});

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
        eprintln!("source file does not exist");
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

fn s3_etag(path: &str) -> Result<String> {

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

    if digests.len() == 1 && file_size < CHUNK_SIZE {
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

fn handle_sync(_s3: &dyn S3, direction: SyncDirection, bucket: &str, key: &str, directory: &str) -> Result<()> {
    eprintln!("not implemented: direction={}, bucket={}, key={}, directory={}",
              direction, bucket, key, directory);
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
        /// The bucket to target
        bucket: String,
        /// The key to target
        key: String,
        /// The path of the file to read
        file: String,
    },
    /// Download an object from S3
    Get {
        /// The bucket to target
        bucket: String,
        /// The key to target
        key: String,
        /// The path of the file to write
        file: String,
    },
    /// Manually abort a multipart upload
    Abort {
        /// The bucket for the multipart upload
        bucket: String,
        /// The key for the multipart upload
        key: String,
        /// The upload_id for the multipart upload
        upload_id: String,
    },
    /// Compute and print the S3 ETag of the file
    S3etag {
        file: String,
    },
    /// Sync a directory with S3
    #[structopt(name="sync")]
    SyncCmd {
        /// The direction of the sync: 'up' for local to remote, 'down' for remote to local
        #[structopt(long, default_value="up")]
        direction: SyncDirection, 
        /// The bucket to target
        #[structopt(long)]
        bucket: String, 
        /// The key to target
        #[structopt(long)]
        key: String, 
        /// The directory to use for up/down sync
        #[structopt(long)]
        directory: String, 
    },
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

        S3etag { file } => {
            eprintln!("s3etag: file={}", file);
            handle_s3etag(&file)?;
        },

        SyncCmd { direction, bucket, key, directory } => {
            handle_sync(&s3, direction, &bucket, &key, &directory)?;
        },
    }

    Ok(())
}
