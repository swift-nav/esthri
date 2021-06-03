/*
* Copyright (C) 2020 Swift Navigation Inc.
* Contact: Swift Navigation <dev@swiftnav.com>
*
* This source is subject to the license found in the file 'LICENSE' which must
* be be distributed together with this source. All other rights reserved.
*
* THIS CODE AND INFORMATION IS PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND,
* EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED
* WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A PARTICULAR PURPOSE.
*/

#![cfg_attr(feature = "aggressive_lint", deny(warnings))]
#![recursion_limit = "256"]
extern crate regex;

use std::fs;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use std::io::ErrorKind;
use std::io::SeekFrom;
use std::marker::Unpin;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::result::Result as StdResult;
use std::sync::Mutex;

use crypto::digest::Digest;
use crypto::md5::Md5;
use futures::future::BoxFuture;
use futures::{stream, Future, Stream, StreamExt, TryStream, TryStreamExt};
use glob::Pattern;
use hyper::client::connect::HttpConnector;
use log::*;
use log_derive::logfn;
use once_cell::sync::Lazy;
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    task::{self, JoinError, JoinHandle},
};
use walkdir::WalkDir;

#[cfg(feature = "compression")]
use async_compression::tokio::bufread::GzipDecoder;

#[cfg(feature = "compression")]
use flate2::{read::GzEncoder, Compression};

pub use crate::errors::{Error, Result};

#[cfg(feature = "blocking")]
pub mod blocking;
pub mod config;
pub mod errors;
#[cfg(feature = "http_server")]
pub mod http_server;
pub mod retry;
pub mod types;

pub mod rusoto;

use crate::retry::handle_dispatch_error;
use crate::rusoto::*;

use crate::config::Config;
use crate::types::{GlobalData, ListingMetadata, MapEtagResult, ReadState, S3Listing};
pub use crate::types::{ObjectInfo, S3ListingItem, S3Object, SyncParam};

const EXPECT_GLOBAL_DATA: &str = "failed to lock global data";

const FORWARD_SLASH: char = '/';

static GLOBAL_DATA: Lazy<Mutex<GlobalData>> = Lazy::new(|| {
    Mutex::new(GlobalData {
        bucket: None,
        key: None,
        upload_id: None,
    })
});

pub const INCLUDE_EMPTY: Option<&[&str]> = None;
pub const EXCLUDE_EMPTY: Option<&[&str]> = None;

#[logfn(err = "ERROR")]
pub async fn head_object<T>(s3: &T, bucket: impl AsRef<str>, key: impl AsRef<str>) -> Result<Option<ObjectInfo>>
where
    T: S3 + Send,
{
    let (bucket, key) = (bucket.as_ref(), key.as_ref());
    info!("head-object: bucket={}, key={}", bucket, key);
    head_object_request(s3, bucket, key).await
}

#[logfn(err = "ERROR")]
pub async fn abort_upload<T>(
    s3: &T,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    upload_id: impl AsRef<str>,
) -> Result<()>
where
    T: S3 + Send,
{
    let (bucket, key, upload_id) = (bucket.as_ref(), key.as_ref(), upload_id.as_ref());

    info!(
        "abort: bucket={}, key={}, upload_id={}",
        bucket, key, upload_id
    );

    handle_dispatch_error(|| async {
        let amur = AbortMultipartUploadRequest {
            bucket: bucket.into(),
            key: key.into(),
            upload_id: upload_id.into(),
            ..Default::default()
        };

        s3.abort_multipart_upload(amur).await
    })
    .await?;

    Ok(())
}

async fn upload_helper<T>(
    s3: &T,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    file: impl AsRef<Path>,
    compressed: bool,
) -> Result<()>
where
    T: S3 + Send + Clone,
{
    let (bucket, key, file) = (bucket.as_ref(), key.as_ref(), file.as_ref());

    if file.exists() {
        let stat = fs::metadata(&file)?;
        let file_size = stat.len();
        let f = File::open(file)?;
        if compressed {
            #[cfg(feature = "compression")]
            {
                debug!("old file_size: {}", file_size);
                debug!("compressing: {}", file.display());
                let mut reader = GzEncoder::new(BufReader::new(f), Compression::default());
                let mut temp_compressed = tempfile::tempfile()?;
                std::io::copy(&mut reader, &mut temp_compressed)?;
                temp_compressed.flush()?;
                temp_compressed.seek(SeekFrom::Start(0))?;
                let file_size = temp_compressed.metadata()?.len();
                debug!("new file_size: {}", file_size);
                upload_from_reader(s3, bucket, key, &mut temp_compressed, file_size).await
            }
            #[cfg(not(feature = "compression"))]
            {
                panic!("compression feature not enabled");
            }
        } else {
            debug!("file_size: {}", file_size);
            let mut reader = BufReader::new(f);
            upload_from_reader(s3, bucket, key, &mut reader, file_size).await
        }
    } else {
        Err(Error::InvalidSourceFile(file.into()))
    }
}

#[logfn(err = "ERROR")]
pub async fn upload<T>(s3: &T, bucket: impl AsRef<str>, key: impl AsRef<str>, file: impl AsRef<Path>) -> Result<()>
where
    T: S3 + Send + Clone,
{
    info!(
        "put: bucket={}, key={}, file={}",
        bucket.as_ref(),
        key.as_ref(),
        file.as_ref().display()
    );

    let compressed = false;
    upload_helper(s3, bucket, key, file, compressed).await
}

#[cfg(feature = "compression")]
#[logfn(err = "ERROR")]
pub async fn upload_compressed<T>(s3: &T, bucket: impl AsRef<str>, key: impl AsRef<str>, file: impl AsRef<Path>) -> Result<()>
where
    T: S3 + Send + Clone,
{
    info!(
        "put(compressed): bucket={}, key={}, file={}",
        bucket.as_ref(),
        key.as_ref(),
        file.as_ref().display()
    );

    let compressed = true;
    upload_helper(s3, bucket, key, file, compressed).await
}

async fn create_file_chunk_stream<R: Read>(
    mut reader: R,
    file_size: u64,
) -> impl Stream<Item = Result<(i64, Vec<u8>)>> {
    let upload_part_size = Config::global().upload_part_size();
    async_stream::stream! {
        let mut remaining = file_size;
        let mut part_number: i64 = 1;
        while remaining != 0 {
            let upload_part_size = if remaining >= upload_part_size {
                upload_part_size
            } else {
                remaining
            };
            let mut buf = vec![0u8; upload_part_size as usize];
            if reader.read(&mut buf)? == 0 {
                yield Err(Error::ReadZero);
                return;
            } else {
                yield Ok((part_number, buf));
                remaining -= upload_part_size;
                part_number += 1;
            }
        }
    }
}

async fn create_chunk_upload_stream<StreamT, ClientT>(
    source_stream: StreamT,
    s3: ClientT,
    upload_id: impl Into<String> + Clone,
    bucket: impl Into<String> + Clone,
    key: impl Into<String> + Clone,
) -> impl Stream<Item = impl Future<Output = Result<CompletedPart>>>
where
    StreamT: Stream<Item = Result<(i64, Vec<u8>)>>,
    ClientT: S3 + Send + Clone,
{
    source_stream
        .map(move |value| {
            let s3 = s3.clone();
            let bucket: String = bucket.clone().into();
            let key: String = key.clone().into();
            let upload_id: String = upload_id.clone().into();
            (s3, bucket, key, upload_id, value)
        })
        .map(|(s3, bucket, key, upload_id, value)| async move {
            let (part_number, buf) = value?;
            let cp = handle_dispatch_error(|| async {
                let body: StreamingBody = buf.clone().into();
                let upr = UploadPartRequest {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    part_number,
                    upload_id: upload_id.clone(),
                    body: Some(body),
                    ..Default::default()
                };
                s3.upload_part(upr).await
            })
            .await
            .map(|upo| {
                if upo.e_tag.is_none() {
                    warn!(
                        "upload_part e_tag was not present (part_number: {})",
                        part_number
                    );
                }
                CompletedPart {
                    e_tag: upo.e_tag,
                    part_number: Some(part_number),
                }
            })
            .map_err(Error::UploadPartFailed)?;
            Ok(cp)
        })
}

#[logfn(err = "ERROR")]
pub async fn upload_from_reader<T, R>(
    s3: &T,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    mut reader: R,
    file_size: u64,
) -> Result<()>
where
    T: S3 + Send + Clone,
    R: Read,
{
    let upload_part_size = Config::global().upload_part_size();
    let (bucket, key) = (bucket.as_ref(), key.as_ref());

    info!(
        "put: bucket={}, key={}, file_size={}",
        bucket, key, file_size
    );

    if file_size >= upload_part_size {
        let cmuo = handle_dispatch_error(|| async {
            let cmur = CreateMultipartUploadRequest {
                bucket: bucket.into(),
                key: key.into(),
                acl: Some("bucket-owner-full-control".into()),
                ..Default::default()
            };

            s3.create_multipart_upload(cmur).await
        })
        .await
        .map_err(Error::CreateMultipartUploadFailed)?;

        let upload_id = cmuo.upload_id.ok_or(Error::UploadIdNone)?;

        debug!("upload_id: {}", upload_id);

        // Load into global data so it can be cancelled for CTRL-C / SIGTERM
        {
            let mut global_data = GLOBAL_DATA.lock().expect(EXPECT_GLOBAL_DATA);
            global_data.bucket = Some(bucket.into());
            global_data.key = Some(key.into());
            global_data.upload_id = Some(upload_id.clone());
        }

        let chunk_stream = create_file_chunk_stream(reader, file_size).await;
        let upload_stream =
            create_chunk_upload_stream(chunk_stream, s3.clone(), upload_id.clone(), bucket, key)
                .await;

        let downloaders_count = Config::global().concurrent_downloader_tasks();

        let mut completed_parts: Vec<CompletedPart> = upload_stream
            .buffer_unordered(downloaders_count)
            .try_collect()
            .await?;

        completed_parts.sort_unstable_by_key(|a| a.part_number);

        handle_dispatch_error(|| async {
            let cmpu = CompletedMultipartUpload {
                parts: Some(completed_parts.clone()),
            };

            let cmur = CompleteMultipartUploadRequest {
                bucket: bucket.into(),
                key: key.into(),
                upload_id: upload_id.clone(),
                multipart_upload: Some(cmpu),
                ..Default::default()
            };

            s3.complete_multipart_upload(cmur).await
        })
        .await
        .map_err(Error::CompletedMultipartUploadFailed)?;

        // Clear multi-part upload
        {
            let mut global_data = GLOBAL_DATA.lock().expect(EXPECT_GLOBAL_DATA);
            global_data.bucket = None;
            global_data.key = None;
            global_data.upload_id = None;
        }
    } else {
        let mut buf = vec![0u8; file_size as usize];
        let read_size = reader.read(&mut buf)?;

        if read_size == 0 && file_size != 0 {
            return Err(Error::ReadZero);
        }

        handle_dispatch_error(|| async {
            let body: StreamingBody = buf.clone().into();

            let por = PutObjectRequest {
                bucket: bucket.into(),
                key: key.into(),
                body: Some(body),
                acl: Some("bucket-owner-full-control".into()),
                ..Default::default()
            };

            s3.put_object(por).await
        })
        .await
        .map_err(Error::PutObjectFailed)?;
    }

    Ok(())
}

async fn download_streaming_range<T>(
    s3: &T,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    range: Option<ReadState>,
) -> Result<ByteStream>
where
    T: S3 + Send + Clone,
{
    let (bucket, key) = (bucket.as_ref(), key.as_ref());

    if let Some(range) = range {
        if range.total() == 0 {
            let empty = vec![0u8; 0];
            return Ok(empty.into());
        }
    }

    let range = range.map(|r| r.to_string());

    let goo = get_object_request(
        s3,
        &GetObjectRequest {
            bucket: bucket.into(),
            key: key.into(),
            range,
            ..Default::default()
        },
    )
    .await?;

    goo.body.ok_or(Error::GetObjectOutputBodyNone)
}

async fn read_all<ReaderT>(
    initial_buffer_size: usize,
    mut reader: ReaderT,
) -> Result<(usize, Vec<u8>)>
where
    ReaderT: AsyncReadExt + Unpin,
{
    let mut blob = vec![0u8; initial_buffer_size];
    loop {
        let result = reader.read_to_end(&mut blob).await;
        if let Err(e) = result {
            if e.kind() == ErrorKind::Interrupted {
                continue;
            }
        } else {
            return Ok((result?, blob));
        }
    }
}

async fn read_exact<ClientT, ReaderT>(
    s3: &ClientT,
    bucket: String,
    key: String,
    mut reader: ReaderT,
    range: &ReadState,
) -> Result<(usize, Vec<u8>)>
where
    ClientT: S3,
    ReaderT: AsyncReadExt + Unpin,
{
    let read_expected = range.read_size();
    let mut blob = vec![0u8; read_expected as usize];
    loop {
        let result = reader.read_exact(&mut blob).await;
        let stat = head_object_request(s3, bucket.clone(), key.clone())
            .await?
            .ok_or_else(|| Error::GetObjectInvalidKey(key.clone()))?;
        if stat.size as u64 != range.total() {
            break Err(Error::GetObjectSizeChanged);
        }
        if let Err(e) = result {
            if e.kind() == ErrorKind::Interrupted {
                continue;
            }
        } else {
            let read_actual = result?;
            if read_expected != read_actual {
                break Err(Error::GetObjectInvalidRead(read_expected, read_actual));
            } else {
                break Ok((read_actual, blob));
            }
        }
    }
}

fn create_reader(stream: ByteStream, decompress: bool) -> Pin<Box<dyn AsyncRead + Send + Sync>> {
    let async_reader = stream.into_async_read();
    let buf_reader = tokio::io::BufReader::new(async_reader);

    if decompress {
        #[cfg(feature = "compression")]
        {
            Box::pin(GzipDecoder::new(buf_reader))
        }
        #[cfg(not(feature = "compression"))]
        {
            panic!("compression feature not enabled");
        }
    } else {
        Box::pin(buf_reader)
    }
}

type DownloaderResult<'a, ClientT, ChunkT, SelfT> = (BoxFuture<'a, Result<ChunkT>>, (SelfT, ClientT));

trait Downloader {
    type Chunk: DownloaderChunk + Sized;

    fn create_future<'a, T>(
        self,
        client: T,
    ) -> Option<DownloaderResult<'a, T, Self::Chunk, Self>>
    where
        T: S3 + Send + Sync + Clone + Sized + 'a,
        Self: Sized;

    fn concurrent_downloader_tasks(&self) -> usize;

    fn concurrent_writer_tasks(&self) -> usize;
}

trait DownloaderChunk {
    fn write_chunk(self, writer: File) -> Result<()>;
}

struct DownloadMultipleChunk(u64, usize, Vec<u8>);

impl DownloaderChunk for DownloadMultipleChunk {
    fn write_chunk(self, writer: File) -> Result<()> {
        write_all_at(writer, self.0, self.2, self.1)
    }
}

struct DownloadMultiple {
    bucket: String,
    key: String,
    read_state: ReadState,
    read_size: usize,
}

impl DownloadMultiple {
    fn new(bucket: impl Into<String>, key: impl Into<String>, total_size: u64) -> Self {
        let download_buffer_size = Config::global().download_buffer_size();
        let read_state = ReadState::new(download_buffer_size, total_size);

        DownloadMultiple {
            key: key.into(),
            bucket: bucket.into(),
            read_state,
            read_size: read_state.read_size(),
        }
    }
}

impl Downloader for DownloadMultiple {
    type Chunk = DownloadMultipleChunk;

    fn create_future<'a, T>(
        mut self,
        client_in: T,
    ) -> Option<DownloaderResult<'a, T, Self::Chunk, Self>>
    where
        T: S3 + Send + Sync + Clone + 'a,
    {
        if self.read_state.complete() {
            None
        } else {
            let client = client_in.clone();
            let bucket = self.bucket.clone();
            let key = self.key.clone();
            let range = self.read_state;
            let fut = async move {
                let client = client.clone();
                let stream = download_streaming_range(&client, &bucket, &key, Some(range)).await?;
                let reader = create_reader(stream, false);
                let (read_size, buffer) = read_exact(&client, bucket, key, reader, &range).await?;
                Ok(DownloadMultipleChunk(range.offset(), read_size, buffer))
            };
            self.read_size = self.read_state.update(self.read_size);
            Some((Box::pin(fut), (self, client_in)))
        }
    }

    fn concurrent_downloader_tasks(&self) -> usize {
        Config::global().concurrent_downloader_tasks()
    }

    fn concurrent_writer_tasks(&self) -> usize {
        Config::global().concurrent_writer_tasks()
    }
}

struct DownloadCompressedChunk(usize, Vec<u8>);

impl DownloaderChunk for DownloadCompressedChunk {
    fn write_chunk(self, writer: File) -> Result<()> {
        write_all(writer, self.1, self.0)
    }
}
struct DownloadCompressed {
    bucket: String,
    key: String,
    read_state: ReadState,
    read_size: usize,
}

impl DownloadCompressed {
    fn new(bucket: impl Into<String>, key: impl Into<String>, total_size: u64) -> Self {
        let download_buffer_size = Config::global().download_buffer_size();
        let read_state = ReadState::new(download_buffer_size, total_size);

        DownloadCompressed {
            key: key.into(),
            bucket: bucket.into(),
            read_state,
            read_size: read_state.read_size(),
        }
    }
}

impl Downloader for DownloadCompressed {
    type Chunk = DownloadCompressedChunk;

    fn create_future<'a, T>(
        mut self,
        client_in: T,
    ) -> Option<DownloaderResult<'a, T, Self::Chunk, Self>>
    where
        T: S3 + Send + Sync + Clone + 'a,
    {
        if self.read_state.complete() {
            None
        } else {
            let client = client_in.clone();
            let bucket = self.bucket.clone();
            let key = self.key.clone();
            let range = self.read_state;
            let fut = async move {
                let client = client.clone();
                let stream = download_streaming_range(&client, &bucket, &key, Some(range)).await?;
                let reader = create_reader(stream, true);
                let initial_buffer_size = Config::global().download_buffer_size();
                let (read_size, buffer) = read_all(initial_buffer_size, reader).await?;
                Ok(DownloadCompressedChunk(read_size, buffer))
            };
            self.read_size = self.read_state.update(self.read_size);
            Some((Box::pin(fut), (self, client_in)))
        }
    }

    fn concurrent_downloader_tasks(&self) -> usize {
        2
    }

    fn concurrent_writer_tasks(&self) -> usize {
        1
    }
}

fn create_download_readers_stream<'a, ClientT, DownloaderT, ChunkT>(
    s3: ClientT,
    downloader: DownloaderT,
) -> impl Stream<Item = BoxFuture<'a, Result<ChunkT>>> + 'a
where
    ClientT: S3 + Sync + Send + Clone + 'a,
    ChunkT: DownloaderChunk,
    DownloaderT: Downloader<Chunk = ChunkT> + 'a,
{
    let state = (downloader, s3);

    Box::pin(stream::unfold(state, |(downloader, s3)| async move {
        downloader.create_future(s3.clone())
    }))
}

fn write_all(mut writer: File, buffer: Vec<u8>, length: usize) -> Result<()> {
    writer.write_all(&buffer[..length]).map_err(Error::from)
}

fn write_all_at(writer: File, file_offset: u64, buffer: Vec<u8>, length: usize) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::prelude::FileExt;
        writer
            .write_all_at(&buffer[..length], file_offset)
            .map_err(Error::from)
    }
    #[cfg(windows)]
    {
        use std::os::windows::prelude::FileExt;
        let (mut file_offset, mut length) = (file_offset, length);
        let mut buffer_offset = 0;
        while length > 0 {
            let write_size = writer
                .seek_write(&buffer[buffer_offset..length], file_offset)
                .map_err(Error::from)?;
            length -= write_size;
            file_offset += write_size as u64;
            buffer_offset += write_size;
        }
        Ok(())
    }
}

fn map_download_readers_to_writer<'a, StreamT, ChunkT>(
    chunk_stream: StreamT,
    writer: File,
) -> impl Stream<Item = impl Future<Output = Result<()>> + 'a> + 'a
where
    StreamT: Stream<Item = Result<ChunkT>> + 'a,
    ChunkT: DownloaderChunk + 'a,
{
    chunk_stream
        .map(move |chunk| {
            let writer = writer.try_clone()?;
            Ok((writer, chunk?))
        })
        .map(|value: Result<(File, ChunkT)>| async move {
            let (writer, chunk) = value?;
            chunk.write_chunk(writer)
        })
}

async fn download_helper<T>(
    s3: &T,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    file: impl AsRef<Path>,
    decompress: bool,
) -> Result<()>
where
    T: S3 + Send + Sync + Clone,
{
    let (bucket, key, file) = (bucket.as_ref(), key.as_ref(), file.as_ref());

    let file_output = File::create(file)?;

    let stat = head_object_request(s3, bucket, key).await?;
    let total_size = stat
        .ok_or_else(|| Error::GetObjectInvalidKey(key.into()))?
        .size as u64;

    fn run_downloader<'a, T: Downloader + 'a, ClientT>(
        s3: ClientT,
        downloader: T,
        file_output: File,
    ) -> impl Future<Output = Result<()>> + 'a
    where
        ClientT: S3 + Send + Sync + Clone + 'a,
    {
        let concurrent_downloader_tasks = downloader.concurrent_downloader_tasks();
        let concurrent_writer_tasks = downloader.concurrent_writer_tasks();

        let reader_chunk_stream = create_download_readers_stream(s3, downloader)
            .buffer_unordered(concurrent_downloader_tasks);

        let reader_result_stream = map_download_readers_to_writer(reader_chunk_stream, file_output);

        reader_result_stream
            .buffer_unordered(concurrent_writer_tasks)
            .try_collect()
    }

    if decompress {
        let downloader = DownloadCompressed::new(bucket, key, total_size);
        run_downloader(s3.clone(), downloader, file_output).await
    } else {
        let downloader = DownloadMultiple::new(bucket, key, total_size);
        run_downloader(s3.clone(), downloader, file_output).await
    }
}

pub async fn download_streaming<T>(s3: &T, bucket: impl AsRef<str>, key: impl AsRef<str>) -> Result<ByteStream>
where
    T: S3 + Send + Clone,
{
    let range = None;
    download_streaming_range(s3, bucket, key, range).await
}

#[logfn(err = "ERROR")]
pub async fn download<T>(s3: &T, bucket: impl AsRef<str>, key: impl AsRef<str>, file: impl AsRef<Path>) -> Result<()>
where
    T: S3 + Sync + Send + Clone,
{
    info!(
        "get: bucket={}, key={}, file={}",
        bucket.as_ref(),
        key.as_ref(),
        file.as_ref().display()
    );

    let decompress = false;
    download_helper(s3, bucket, key, file, decompress).await
}

#[cfg(feature = "compression")]
#[logfn(err = "ERROR")]
pub async fn download_decompressed<T>(
    s3: &T,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    file: impl AsRef<Path>,
) -> Result<()>
where
    T: S3 + Sync + Send + Clone,
{
    info!(
        "get(decompress): bucket={}, key={}, file={}",
        bucket.as_ref(),
        key.as_ref(),
        file.as_ref().display()
    );

    let decompress = true;
    download_helper(s3, bucket, key, file, decompress).await
}

#[logfn(err = "ERROR")]
pub async fn sync<T>(
    s3: &T,
    source: SyncParam,
    destination: SyncParam,
    includes: Option<&[impl AsRef<str>]>,
    excludes: Option<&[impl AsRef<str>]>,
) -> Result<()>
where
    T: S3 + Sync + Send + Clone,
{
    let mut glob_excludes: Vec<Pattern> = vec![];
    let mut glob_includes: Vec<Pattern> = vec![];

    if let Some(excludes) = excludes {
        for exclude in excludes {
            let exclude = exclude.as_ref();
            match Pattern::new(exclude) {
                Err(e) => {
                    return Err(Error::GlobPatternError(e));
                }
                Ok(p) => {
                    glob_excludes.push(p);
                }
            }
        }
    }

    if let Some(includes) = includes {
        for include in includes {
            let include = include.as_ref();
            match Pattern::new(include) {
                Err(e) => {
                    return Err(Error::GlobPatternError(e));
                }
                Ok(p) => {
                    glob_includes.push(p);
                }
            }
        }
    } else {
        glob_includes.push(Pattern::new("*")?);
    }

    match (source, destination) {
        (SyncParam::Local { path }, SyncParam::Bucket { bucket, key }) => {
            info!(
                "sync-up, local directory: {}, bucket: {}, key: {}",
                path.display(),
                bucket,
                key
            );

            sync_local_to_remote(s3, &bucket, &key, &path, &glob_includes, &glob_excludes).await?;
        }
        (SyncParam::Bucket { bucket, key }, SyncParam::Local { path }) => {
            info!(
                "sync-down, local directory: {}, bucket: {}, key: {}",
                path.display(),
                bucket,
                key
            );

            sync_remote_to_local(s3, &bucket, &key, &path, &glob_includes, &glob_excludes).await?;
        }
        (
            SyncParam::Bucket {
                bucket: source_bucket,
                key: source_key,
            },
            SyncParam::Bucket {
                bucket: destination_bucket,
                key: destination_key,
            },
        ) => {
            info!(
                "sync-across, bucket: {}, source_key: {}, bucket: {}, destination_key: {}",
                source_bucket, source_key, destination_bucket, destination_key
            );

            sync_across(
                s3,
                &source_bucket,
                &source_key,
                &destination_bucket,
                &destination_key,
                &glob_includes,
                &glob_excludes,
            )
            .await?;
        }
        _ => {
            warn!("Local to Local copy not implemented");
        }
    }

    Ok(())
}

#[logfn(err = "ERROR")]
pub async fn list_objects<T>(s3: &T, bucket: impl AsRef<str>, key: impl AsRef<str>) -> Result<Vec<String>>
where
    T: S3 + Send,
{
    let none: Option<&str> = None;
    list_objects_with_delim(s3, bucket, key, none).await
}

#[logfn(err = "ERROR")]
pub async fn list_directory<T>(s3: &T, bucket: impl AsRef<str>, dir_path: impl AsRef<str>) -> Result<Vec<String>>
where
    T: S3 + Send,
{
    list_objects_with_delim(s3, bucket, dir_path, Some("/")).await
}

async fn list_objects_with_delim<T, S0, S1, S2>(
    s3: &T,
    bucket: S0,
    key: S1,
    delim: Option<S2>,
) -> Result<Vec<String>>
where
    T: S3 + Send,
    S0: AsRef<str>,
    S1: AsRef<str>,
    S2: AsRef<str>,
{
    let (bucket, key, delim) = (bucket.as_ref(), key.as_ref(), delim.as_ref());

    let batches: Vec<_> = list_objects_stream_with_delim(s3, bucket, key, delim)
        .try_collect()
        .await?;

    let keys: Vec<_> = batches
        .into_iter()
        .flat_map(|batch| {
            batch.into_iter().map(|entry| {
                match &entry {
                    S3ListingItem::S3Object(obj) => info!("key={}, etag={}", obj.key, obj.e_tag),
                    S3ListingItem::S3CommonPrefix(cp) => info!("common_prefix={}", cp),
                }
                entry.prefix()
            })
        })
        .collect();

    Ok(keys)
}

pub fn list_objects_stream<'a, T>(
    s3: &'a T,
    bucket: impl AsRef<str> + 'a,
    key: impl AsRef<str> + 'a,
) -> impl TryStream<Ok = Vec<S3ListingItem>, Error = Error> + Unpin + 'a
where
    T: S3 + Send,
{
    let no_delim: Option<&str> = None;
    list_objects_stream_with_delim(s3, bucket, key, no_delim)
}

pub fn list_directory_stream<'a, T>(
    s3: &'a T,
    bucket: &'a str,
    key: &'a str,
) -> impl TryStream<Ok = Vec<S3ListingItem>, Error = Error> + Unpin + 'a
where
    T: S3 + Send,
{
    let slash_delim = Some("/");
    list_objects_stream_with_delim(s3, bucket, key, slash_delim)
}

fn list_objects_stream_with_delim<T>(
    s3: &'_ T,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    delimiter: Option<impl AsRef<str>>,
) -> impl TryStream<Ok = Vec<S3ListingItem>, Error = Error> + Unpin + '_
where
    T: S3 + Send,
{
    let (bucket, key) = (bucket.as_ref().to_owned(), key.as_ref().to_owned());

    info!("stream-objects: bucket={}, key={}", bucket, key);

    let continuation: Option<String> = None;
    let delimiter = delimiter.map(|s| s.as_ref().to_owned());

    let state = (s3, bucket, key, continuation, delimiter, false);

    Box::pin(stream::try_unfold(
        state,
        |(s3, bucket, key, prev_continuation, delimiter, done)| async move {
            // You can't yield a value and stop unfold at the same time, so do this
            if done {
                return Ok(None);
            }

            let listing =
                list_objects_request(s3, &bucket, &key, prev_continuation, delimiter.clone())
                    .await?;
            let continuation = listing.continuation.clone();

            info!("found count: {}", listing.count());

            if listing.continuation.is_some() {
                Ok(Some((
                    listing.combined(),
                    (s3, bucket, key, continuation, delimiter, false),
                )))
            } else if !listing.is_empty() {
                // Yield the last values, and exit on the next loop
                Ok(Some((
                    listing.combined(),
                    (s3, bucket, key, continuation, delimiter, true),
                )))
            } else {
                // Nothing to yield and we're done
                Ok(None)
            }
        },
    ))
}

/// Since large uploads require us to create a multi-part upload request
/// we need to tell AWS that we're aborting the upload, otherwise the
/// unfinished could stick around indefinitely.
#[cfg(feature = "cli")]
pub fn setup_upload_termination_handler() {
    use std::process;
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
                    let res = blocking::abort_upload(&s3, &bucket, &key, &upload_id);
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

pub fn s3_compute_etag(path: impl AsRef<Path>) -> Result<String>
{
    let path = path.as_ref();
    if !path.exists() {
        return Err(Error::ETagNotPresent);
    }

    let f = File::open(path)?;
    let mut reader = BufReader::new(f);
    let mut hash = Md5::new();
    let stat = fs::metadata(&path)?;
    let file_size = stat.len();
    let mut digests: Vec<[u8; 16]> = vec![];
    let mut remaining = file_size;

    let upload_part_size = Config::global().upload_part_size();

    while remaining != 0 {
        let upload_part_size: usize = (if remaining >= upload_part_size {
            upload_part_size
        } else {
            remaining
        }) as usize;
        hash.reset();
        let mut blob = vec![0u8; upload_part_size];
        reader.read_exact(&mut blob)?;
        hash.input(&blob);
        let mut hash_bytes = [0u8; 16];
        hash.result(&mut hash_bytes);
        digests.push(hash_bytes);
        remaining -= upload_part_size as u64;
    }

    if digests.is_empty() {
        let mut hash_bytes = [0u8; 16];
        hash.result(&mut hash_bytes);
        let hex_digest = hex::encode(hash_bytes);
        Ok(format!("\"{}\"", hex_digest))
    } else if digests.len() == 1 && file_size < upload_part_size {
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

async fn get_object_request<T>(
    s3: &T,
    gor: &GetObjectRequest,
) -> std::result::Result<GetObjectOutput, RusotoError<GetObjectError>>
where
    T: S3 + Send,
{
    handle_dispatch_error(|| s3.get_object(gor.clone())).await
}

fn process_head_obj_resp(hoo: HeadObjectOutput) -> Result<Option<ObjectInfo>> {
    if let Some(true) = hoo.delete_marker {
        return Ok(None);
    }

    let e_tag = if let Some(e_tag) = hoo.e_tag {
        e_tag
    } else {
        return Err(Error::HeadObjectUnexpected(format!(
            "no e_tag found: {:?}",
            hoo
        )));
    };

    let last_modified: String = if let Some(last_modified) = hoo.last_modified {
        last_modified
    } else {
        return Err(Error::HeadObjectUnexpected("no last_modified found".into()));
    };

    let last_modified: chrono::DateTime<chrono::Utc> =
        match chrono::DateTime::parse_from_rfc2822(&last_modified) {
            Ok(last_modified) => last_modified.into(),
            Err(err) => {
                return Err(Error::HeadObjectFailedParseError(err));
            }
        };

    let size = if let Some(content_length) = hoo.content_length {
        content_length
    } else {
        return Err(Error::HeadObjectUnexpected(
            "no content_length found".into(),
        ));
    };

    Ok(Some(ObjectInfo {
        e_tag,
        size,
        last_modified,
    }))
}

#[logfn(err = "ERROR")]
async fn head_object_request<T>(
    s3: &T,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
) -> Result<Option<ObjectInfo>>
where
    T: S3,
{
    let (bucket, key) = (bucket.as_ref(), key.as_ref());
    let mut res = Some(
        handle_dispatch_error(|| async {
            let hor = HeadObjectRequest {
                bucket: bucket.into(),
                key: key.into(),
                ..Default::default()
            };

            s3.head_object(hor).await
        })
        .await,
    );

    match res.as_mut() {
        Some(Ok(_)) => {
            let hoo = res.unwrap().unwrap();
            process_head_obj_resp(hoo)
        }
        Some(Err(RusotoError::Unknown(e))) => {
            if e.status == 404 {
                Ok(None)
            } else {
                let err = res.unwrap().err().unwrap();
                Err(Error::HeadObjectFailure(err))
            }
        }
        Some(Err(_)) => {
            let err = res.unwrap().err().unwrap();
            Err(Error::HeadObjectFailure(err))
        }
        _ => {
            panic!("impossible?");
        }
    }
}

async fn list_objects_request<T>(
    s3: &T,
    bucket: &str,
    key: &str,
    continuation: Option<String>,
    delimiter: Option<String>,
) -> Result<S3Listing>
where
    T: S3 + Send,
{
    let lov2o = handle_dispatch_error(|| async {
        let lov2r = ListObjectsV2Request {
            bucket: bucket.into(),
            prefix: Some(key.into()),
            continuation_token: continuation.clone(),
            delimiter: delimiter.clone(),
            ..Default::default()
        };

        s3.list_objects_v2(lov2r).await
    })
    .await?;

    let mut listing = S3Listing {
        continuation: lov2o.next_continuation_token,
        ..Default::default()
    };

    if let Some(contents) = lov2o.contents {
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
            listing.contents.push(S3Object { key, e_tag });
        }
    }

    if let Some(common_prefixes) = lov2o.common_prefixes {
        for common_prefix in common_prefixes {
            let prefix = if common_prefix.prefix.is_some() {
                common_prefix.prefix.unwrap()
            } else {
                warn!("unexpected: prefix was null");
                continue;
            };
            listing.common_prefixes.push(prefix);
        }
    }

    Ok(listing)
}

fn process_globs<'a, P: AsRef<Path> + 'a>(
    path: P,
    glob_includes: &[Pattern],
    glob_excludes: &[Pattern],
) -> Option<P> {
    let mut excluded = false;
    let mut included = false;
    {
        let path = path.as_ref();
        for pattern in glob_excludes {
            if pattern.matches(path.to_string_lossy().as_ref()) {
                excluded = true;
                break;
            }
        }
        for pattern in glob_includes {
            if pattern.matches(path.to_string_lossy().as_ref()) {
                included = true;
                break;
            }
        }
    }
    if included && !excluded {
        Some(path)
    } else {
        None
    }
}

async fn download_with_dir<T>(
    s3: &T,
    bucket: &str,
    s3_prefix: &str,
    s3_suffix: &str,
    local_dir: impl AsRef<Path>,
) -> Result<()>
where
    T: S3 + Sync + Send + Clone,
{
    let local_dir = local_dir.as_ref();
    let dest_path = local_dir.join(s3_suffix);

    let parent_dir = dest_path.parent().ok_or(Error::ParentDirNone)?;
    fs::create_dir_all(parent_dir)?;

    let key = format!("{}", Path::new(s3_prefix).join(s3_suffix).display());
    let dest_path = format!("{}", dest_path.display());

    download(s3, bucket, &key, &dest_path).await?;

    Ok(())
}

fn create_dirent_stream<'a>(
    directory: &'a Path,
    glob_includes: &'a [Pattern],
    glob_excludes: &'a [Pattern],
) -> impl Stream<Item = Result<(String, Option<ListingMetadata>)>> + 'a {
    async_stream::stream! {
        for entry in WalkDir::new(directory) {
            let entry = if let Ok(entry) = entry {
                entry
            } else {
                yield Err(entry.err().unwrap().into());
                return;
            };
            let metadata = entry.metadata();
            let stat = if let Ok(stat) = metadata {
                stat
            } else {
                yield Err(metadata.err().unwrap().into());
                return;
            };
            if stat.is_dir() {
                continue;
            }
            if entry.path_is_symlink() {
                warn!("symlinks are ignored");
                continue;
            }
            let path = entry.path();
            debug!("local path={}", path.display());
            if process_globs(&path, glob_includes, glob_excludes).is_some() {
                yield Ok((path.to_string_lossy().into(), ListingMetadata::none()));
            }
        }
    }
}

fn map_paths_to_etags<StreamT>(
    input_stream: StreamT,
) -> impl Stream<Item = JoinHandle<MapEtagResult>>
where
    StreamT: Stream<Item = Result<(String, Option<ListingMetadata>)>>,
{
    input_stream.map(|params| {
        task::spawn_blocking(move || {
            let (path, metadata) = params?;
            let local_etag = s3_compute_etag(&path);
            Ok((path, local_etag, metadata))
        })
    })
}

fn local_to_remote_sync_tasks<ClientT, StreamT>(
    s3: ClientT,
    bucket: String,
    key: String,
    directory: PathBuf,
    dirent_stream: StreamT,
) -> impl Stream<Item = impl Future<Output = Result<()>>>
where
    ClientT: S3 + Send + Clone,
    StreamT: Stream<Item = StdResult<MapEtagResult, JoinError>>,
{
    dirent_stream
        .map(move |entry| {
            (
                s3.clone(),
                bucket.clone(),
                key.clone(),
                directory.clone(),
                entry.unwrap(),
            )
        })
        .map(|clones| async move {
            let (s3, bucket, key, directory, entry) = clones;
            let (path, local_etag, _metadata) = entry?;
            let path = Path::new(&path);
            let remote_path = Path::new(&key);
            let stripped_path = path.strip_prefix(&directory);
            let stripped_path = match stripped_path {
                Err(e) => {
                    warn!("unexpected: failed to strip prefix: {}", e);
                    return Ok(());
                }
                Ok(result) => result,
            };
            let remote_path: String = format!("{}", remote_path.join(&stripped_path).display());
            debug!("checking remote: {}", remote_path);
            let local_etag = local_etag?;
            let object_info = head_object_request(&s3, &bucket, &remote_path).await?;
            if let Some(object_info) = object_info {
                let remote_etag = object_info.e_tag;
                if remote_etag != local_etag {
                    info!(
                        "etag mis-match: {}, remote_etag={}, local_etag={}",
                        remote_path, remote_etag, local_etag
                    );
                    upload(&s3, bucket, &remote_path, &path).await?;
                } else {
                    debug!(
                        "etags matched: {}, remote_etag={}, local_etag={}",
                        remote_path, remote_etag, local_etag
                    );
                }
            } else {
                info!("file did not exist remotely: {}", remote_path);
                upload(&s3, bucket, &remote_path, &path).await?;
            }
            Ok(())
        })
}

async fn sync_local_to_remote<T>(
    s3: &T,
    bucket: &str,
    key: &str,
    directory: impl AsRef<Path>,
    glob_includes: &[Pattern],
    glob_excludes: &[Pattern],
) -> Result<()>
where
    T: S3 + Send + Clone,
{
    let directory = directory.as_ref();

    if !key.ends_with(FORWARD_SLASH) {
        return Err(Error::DirlikePrefixRequired);
    }

    let task_count = Config::global().concurrent_sync_tasks();
    let dirent_stream = create_dirent_stream(directory, glob_includes, glob_excludes);
    let etag_stream = map_paths_to_etags(dirent_stream).buffer_unordered(task_count);
    let sync_tasks = local_to_remote_sync_tasks(
        s3.clone(),
        bucket.into(),
        key.into(),
        directory.into(),
        etag_stream,
    );

    sync_tasks
        .buffer_unordered(task_count)
        .try_collect()
        .await?;

    Ok(())
}

pub async fn sync_across<T>(
    s3: &T,
    source_bucket: &str,
    source_prefix: &str,
    dest_bucket: &str,
    destination_key: &str,
    glob_includes: &[Pattern],
    glob_excludes: &[Pattern],
) -> Result<()>
where
    T: S3 + Send,
{
    if !source_prefix.ends_with(FORWARD_SLASH) {
        return Err(Error::DirlikePrefixRequired);
    }

    if !destination_key.ends_with(FORWARD_SLASH) {
        return Err(Error::DirlikePrefixRequired);
    }

    let mut stream = list_objects_stream(s3, source_bucket, source_prefix);

    while let Some(from_entries) = stream.try_next().await? {
        for entry in from_entries {
            if let S3ListingItem::S3Object(src_object) = entry {
                let path = process_globs(&src_object.key, &glob_includes, &glob_excludes);

                if let Some(_accept) = path {
                    let mut should_copy_file: bool = true;
                    let new_file = src_object.key.replace(source_prefix, destination_key);
                    let dest_object_info = head_object_request(s3, dest_bucket, &new_file).await?;

                    if let Some(dest_object) = dest_object_info {
                        if dest_object.e_tag == src_object.e_tag {
                            should_copy_file = false;
                        }
                    }

                    if should_copy_file {
                        copy_object_request(
                            s3,
                            source_bucket,
                            source_prefix,
                            &src_object.key,
                            dest_bucket,
                            destination_key,
                        )
                        .await?;
                    }
                }
            }
        }
    }

    Ok(())
}

#[logfn(err = "ERROR")]
async fn copy_object_request<T>(
    s3: &T,
    source_bucket: &str,
    source_key: &str,
    file_name: &str,
    dest_bucket: &str,
    dest_key: &str,
) -> Result<CopyObjectOutput>
where
    T: S3 + Send,
{
    let res = handle_dispatch_error(|| async {
        let cor = CopyObjectRequest {
            bucket: dest_bucket.to_string(),
            copy_source: format!("{}/{}", source_bucket.to_string(), &file_name),
            key: file_name.replace(source_key, dest_key),
            ..Default::default()
        };

        s3.copy_object(cor).await
    })
    .await;

    Ok(res?)
}

pub fn create_globs(
    string_vector: &Option<Vec<String>>,
    includes_flag: bool,
) -> Result<Vec<Pattern>> {
    let mut globs: Vec<Pattern> = vec![];

    if let Some(filters) = string_vector {
        for filter in filters {
            match Pattern::new(filter) {
                Err(e) => {
                    return Err(Error::GlobPatternError(e));
                }
                Ok(p) => {
                    globs.push(p);
                }
            }
        }
    } else if includes_flag {
        globs.push(Pattern::new("*")?);
    }

    Ok(globs)
}

fn flattened_object_listing<'a, ClientT>(
    s3: &'a ClientT,
    bucket: &'a str,
    key: &'a str,
    directory: &'a Path,
    glob_includes: &'a [Pattern],
    glob_excludes: &'a [Pattern],
) -> impl Stream<Item = Result<(String, Option<ListingMetadata>)>> + 'a
where
    ClientT: S3 + Send + Clone,
{
    async_stream::stream! {
        let mut stream = list_objects_stream(s3, bucket, key);
        loop {
            let entries_result = stream.try_next().await;
            if let Ok(entries_option) = entries_result {
                if let Some(entries) = entries_option {
                    for entry in entries {
                        let entry = entry.unwrap_object();
                        debug!("key={}", entry.key);
                        let path_result = Path::new(&entry.key).strip_prefix(key);
                        if let Ok(s3_suffix) = path_result {
                            if process_globs(&s3_suffix, glob_includes, glob_excludes).is_some() {
                                let local_path: String = directory.join(&s3_suffix).to_string_lossy().into();
                                let s3_suffix = s3_suffix.to_string_lossy().into();
                                yield Ok((local_path, ListingMetadata::some(s3_suffix, entry.e_tag)));
                            }
                        } else {
                            yield Err(path_result.err().unwrap().into());
                            return;
                        }
                    }
                } else {
                    break;
                }
            } else {
                yield Err(entries_result.err().unwrap());
                return;
            }
        }
    }
}

fn remote_to_local_sync_tasks<ClientT, StreamT>(
    s3: ClientT,
    bucket: String,
    key: String,
    directory: PathBuf,
    input_stream: StreamT,
) -> impl Stream<Item = impl Future<Output = Result<()>>>
where
    ClientT: S3 + Sync + Send + Clone,
    StreamT: Stream<Item = StdResult<MapEtagResult, JoinError>>,
{
    input_stream
        .map(move |entry| {
            (
                s3.clone(),
                bucket.clone(),
                key.clone(),
                directory.clone(),
                entry.unwrap(),
            )
        })
        .map(|(s3, bucket, key, directory, entry)| async move {
            let (path, local_etag, metadata) = entry?;
            let metadata = metadata.unwrap();
            match local_etag {
                Ok(local_etag) => {
                    if local_etag != metadata.e_tag {
                        debug!(
                            "etag mismatch: {}, local etag={}, remote etag={}",
                            path, local_etag, metadata.e_tag
                        );
                        download_with_dir(&s3, &bucket, &key, &metadata.s3_suffix, &directory)
                            .await?;
                    }
                }
                Err(err) => match err {
                    Error::ETagNotPresent => {
                        debug!("file did not exist locally: {}", path);
                        download_with_dir(&s3, &bucket, &key, &metadata.s3_suffix, &directory)
                            .await?;
                    }
                    _ => {
                        warn!("s3 etag error: {}", err);
                    }
                },
            }
            Ok(())
        })
}

async fn sync_remote_to_local<T>(
    s3: &T,
    bucket: &str,
    key: &str,
    directory: impl AsRef<Path>,
    glob_includes: &[Pattern],
    glob_excludes: &[Pattern],
) -> Result<()>
where
    T: S3 + Sync + Send + Clone,
{
    let directory = directory.as_ref();
    if !key.ends_with(FORWARD_SLASH) {
        return Err(Error::DirlikePrefixRequired);
    }

    let task_count = Config::global().concurrent_sync_tasks();
    let object_listing =
        flattened_object_listing(s3, bucket, key, directory, glob_includes, glob_excludes);
    let etag_stream = map_paths_to_etags(object_listing).buffer_unordered(task_count);
    let sync_tasks = remote_to_local_sync_tasks(
        s3.clone(),
        bucket.into(),
        key.into(),
        directory.into(),
        etag_stream,
    );

    sync_tasks
        .buffer_unordered(task_count)
        .try_collect()
        .await?;

    Ok(())
}

#[cfg(feature = "rustls")]
pub fn new_https_connector() -> HttpsConnector<HttpConnector> {
    HttpsConnector::with_webpki_roots()
}

#[cfg(feature = "nativetls")]
pub fn new_https_connector() -> HttpsConnector<HttpConnector> {
    HttpsConnector::new()
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
