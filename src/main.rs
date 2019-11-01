extern crate ctrlc;
extern crate crypto;
extern crate hex;
extern crate once_cell;
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

use crypto::digest::Digest;
use crypto::md5::Md5;
use once_cell::sync::Lazy;

struct GlobalData {
    upload_id: Option<String>,
}

const EXPECT_GLOBAL_DATA: &'static str = "failed to lock global data";

static GLOBAL_DATA: Lazy<Mutex<GlobalData>> = Lazy::new(|| {
    Mutex::new(GlobalData { upload_id: None })
});

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

fn setup_cancel_handler(bucket: String, key: String) {
    ctrlc::set_handler(move || {
        let global_data = GLOBAL_DATA.lock().expect(EXPECT_GLOBAL_DATA);
        if let Some(upload_id) = &global_data.upload_id {
            eprintln!("\ncancelling...");
            let region = Region::default();
            let s3 = S3Client::new(region);
            let res = handle_abort(&s3, &bucket, &key, &upload_id);
            if let Err(e) = res {
                eprintln!("Error cancelling: {}", e);
            }
        } else {
            eprintln!("\ncancelled");
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

fn main() -> Result<()> {

    let region = Region::default();
    let s3 = S3Client::new(region);

    let mut argv: Vec<String> = std::env::args().collect();

    if argv.len() < 5 {
        eprintln!("usage: esthri <operation> <bucket> <key> <file>");
        process::exit(1);
    }

    let _ = argv.remove(0);
    let op = argv.remove(0);
    let bucket = argv.remove(0);
    let key = argv.remove(0);
    let file = argv.remove(0);

    setup_cancel_handler(bucket.clone(), key.clone());
   
    match op.as_str() {

        "put" => {
            eprintln!("put: bucket={}, key={}, file={}", bucket, key, file);
            handle_upload(&s3, &bucket, &key, &file)?;
        },

        "get" => {
            eprintln!("get: bucket={}, key={}, file={}", bucket, key, file);
            handle_download(&s3, &bucket, &key, &file)?;
        },

        "abort" => {
            eprintln!("abort: bucket={}, key={}, upload_id={}", bucket, key, file);
            handle_abort(&s3, &bucket, &key, &file)?;
        },

        "s3etag" => {
            eprintln!("s3etag: file={}", file);
            handle_s3etag(&file)?;
        },

        _  => {
            eprintln!("unknown operation");
            process::exit(1);
        }
    }

    Ok(())
}
