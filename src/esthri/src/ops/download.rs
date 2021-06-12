/*
* Copyright (C) 2021 Swift Navigation Inc.
* Contact: Swift Navigation <dev@swiftnav.com>
*
* This source is subject to the license found in the file 'LICENSE' which must
* be be distributed together with this source. All other rights reserved.
*
* THIS CODE AND INFORMATION IS PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND,
* EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED
* WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A PARTICULAR PURPOSE.
*/

use std::marker::Unpin;
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use futures::future::BoxFuture;
use futures::{stream, Future, Stream, StreamExt, TryStreamExt};
use log::info;
use log_derive::logfn;
use tokio::io::{AsyncRead, AsyncReadExt};

#[cfg(feature = "compression")]
use flate2::write::GzDecoder;

mod bio {
    pub(super) use std::fs::File;
    pub(super) use std::io::prelude::*;
    pub(super) use std::io::ErrorKind;
}

use crate::config::Config;
use crate::errors::{Error, Result};
use crate::rusoto::*;
use crate::types::ReadState;
use crate::{handle_dispatch_error, head_object_request};

/// Unique (locked) pointer to a type that implements the [std::io::Write] trait used here to hold a
/// pointer to an object that implements gzip decompression transparently.
type LockedBoxedWrite = Arc<Mutex<Box<dyn bio::Write + Unpin + Send + Sync>>>;

async fn get_object_request<T>(
    s3: &T,
    gor: &GetObjectRequest,
) -> std::result::Result<GetObjectOutput, RusotoError<GetObjectError>>
where
    T: S3 + Send,
{
    handle_dispatch_error(|| s3.get_object(gor.clone())).await
}

/// Reads from a source until an exact number of bytes are returned.
///
/// The object must not change size during the read operation.
///
/// The `bucket` and `key` parameters are used to validate that the object isn't change size during
/// the read.
///
/// Also see [ReadState].
///
/// # Arguments
///
/// * `s3` - The s3 interface
/// * `bucket` - The bucket to download from
/// * `key` - The key of the object to read from
/// * `reader` - A reader that will returns bytes from the specified object
/// * `range` - The range that's being read from, also stores the total size of the object
///
/// # Errors
///
/// - [Error::GetObjectInvalidKey]
/// - [Error::GetObjectInvalidRead]
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
            if e.kind() == bio::ErrorKind::Interrupted {
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

/// Transforms a [ByteStream] into an [AsyncRead] object suitable for use in a future.
fn create_reader(stream: ByteStream) -> Pin<Box<dyn AsyncRead + Send + Sync>> {
    let async_reader = stream.into_async_read();
    let buf_reader = tokio::io::BufReader::new(async_reader);
    Box::pin(buf_reader)
}

/// Represents one future returned by an implementor of the [Downloader] trait, and the next state
/// (or iteration) of the downloader.
type DownloaderResult<'a, ClientT, ChunkT, SelfT> =
    (BoxFuture<'a, Result<ChunkT>>, (SelfT, ClientT));

/// Implementors of this trait return a stream of futures and "themelves" in order to be easily
/// transformed into a stream object (which returns a stream of futures).  These futures can then
/// be passed to an executor or buffered in order to be run conrrently.
trait Downloader {
    type Chunk: DownloaderChunk + Sized + Unpin + Send + 'static;

    fn create_future<'a, T>(self, client: T) -> Option<DownloaderResult<'a, T, Self::Chunk, Self>>
    where
        T: S3 + Send + Sync + Clone + Sized + 'a,
        Self: Sized;

    fn concurrent_downloader_tasks(&self) -> usize;

    fn concurrent_writer_tasks(&self) -> usize;
}

/// Represents a download chunk that's waiting to be written.
trait DownloaderChunk {
    /// Initiate a (blocking) write of a chunk
    fn bio_write_chunk(self) -> Result<()>;
}

/// `DownloadMultipleChunk` uses multiple readers and writers concurrently to download uncompressed
/// files.
struct DownloadMultipleChunk {
    file: bio::File,
    offset: u64,
    buffer: Vec<u8>,
    length: usize,
}

impl DownloadMultipleChunk {
    fn new(file: bio::File, offset: u64, buffer: Vec<u8>, length: usize) -> Self {
        Self {
            file,
            offset,
            buffer,
            length,
        }
    }
}

impl DownloaderChunk for DownloadMultipleChunk {
    fn bio_write_chunk(self) -> Result<()> {
        write_all_at(self.file, self.offset, self.buffer, self.length)
    }
}

struct DownloadMultiple {
    file: bio::File,
    bucket: String,
    key: String,
    read_state: ReadState,
    read_size: usize,
}

impl DownloadMultiple {
    fn new(
        file: bio::File,
        bucket: impl Into<String>,
        key: impl Into<String>,
        total_size: u64,
    ) -> Self {
        let download_buffer_size = Config::global().download_buffer_size();
        let read_state = ReadState::new(download_buffer_size, total_size);
        Self {
            file,
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
            let file = self.file.try_clone();
            let fut = async move {
                let client = client.clone();
                let stream = download_streaming_range(&client, &bucket, &key, Some(range)).await?;
                let reader = create_reader(stream);
                let (read_size, buffer) = read_exact(&client, bucket, key, reader, &range).await?;
                Ok(DownloadMultipleChunk::new(
                    file?,
                    range.offset(),
                    buffer,
                    read_size,
                ))
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

/// `DownloadCompressedChunk` uses multiple readers and 1 writer concurrently to download compressed
/// files and uncompress them transparently.
struct DownloadCompressedChunk {
    writer: LockedBoxedWrite,
    buffer: Vec<u8>,
    length: usize,
}

impl DownloadCompressedChunk {
    fn new(writer: LockedBoxedWrite, buffer: Vec<u8>, length: usize) -> Self {
        Self {
            writer,
            buffer,
            length,
        }
    }
}

impl DownloaderChunk for DownloadCompressedChunk {
    fn bio_write_chunk(self) -> Result<()> {
        let mut writer = self.writer.lock().unwrap();
        let writer = writer.as_mut();
        write_all(writer, self.buffer, self.length)
    }
}

struct DownloadCompressed {
    file: LockedBoxedWrite,
    bucket: String,
    key: String,
    read_state: ReadState,
    read_size: usize,
}

impl DownloadCompressed {
    fn new(
        file: LockedBoxedWrite,
        bucket: impl Into<String>,
        key: impl Into<String>,
        total_size: u64,
    ) -> Self {
        let download_buffer_size = Config::global().download_buffer_size();
        let read_state = ReadState::new(download_buffer_size, total_size);
        Self {
            file: file.clone(),
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
            let file = self.file.clone();
            let fut = async move {
                let client = client.clone();
                let stream = download_streaming_range(&client, &bucket, &key, Some(range)).await?;
                let reader = create_reader(stream);
                let (read_size, buffer) = read_exact(&client, bucket, key, reader, &range).await?;
                Ok(DownloadCompressedChunk::new(file, buffer, read_size))
            };
            self.read_size = self.read_state.update(self.read_size);
            Some((Box::pin(fut), (self, client_in)))
        }
    }

    fn concurrent_downloader_tasks(&self) -> usize {
        Config::global().concurrent_compressed_downloader_tasks()
    }

    fn concurrent_writer_tasks(&self) -> usize {
        1 /* Data written in compressed form must be processed serially, this
          can be greater than 1, but the lock around the decompressor (which
          implements a the `Write` triat) will prevent parallelism. */
    }
}

/// Transforms a [Downloader] into a stream of futures.
fn create_download_readers_stream<'a, ClientT, DownloaderT, ChunkT>(
    s3: ClientT,
    downloader: DownloaderT,
) -> impl Stream<Item = BoxFuture<'a, Result<ChunkT>>> + 'a
where
    ClientT: S3 + Sync + Send + Clone + 'a,
    ChunkT: DownloaderChunk + Send + Unpin,
    DownloaderT: Downloader<Chunk = ChunkT> + 'a,
{
    let state = (downloader, s3);

    Box::pin(stream::unfold(state, |(downloader, s3)| async move {
        downloader.create_future(s3.clone())
    }))
}

fn write_all(writer: &mut dyn bio::Write, buffer: Vec<u8>, length: usize) -> Result<()> {
    writer.write_all(&buffer[..length]).map_err(Error::from)
}

fn write_all_at(writer: bio::File, file_offset: u64, buffer: Vec<u8>, length: usize) -> Result<()> {
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

/// Maps read chunks from a [Downloader] implementor to a task that writes the chunk.
fn map_download_readers_to_writer<'a, StreamT, ChunkT>(
    chunk_stream: StreamT,
) -> impl Stream<Item = impl Future<Output = Result<()>> + 'a> + 'a
where
    StreamT: Stream<Item = Result<ChunkT>> + 'a,
    ChunkT: DownloaderChunk + Send + 'static,
{
    chunk_stream
        .map(move |chunk| tokio::task::spawn_blocking(move || chunk?.bio_write_chunk()))
        .map(move |join_handle| async move { join_handle.await? })
}

/// Primary download entrypoint, which constructs either a [DownloadMultipleChunk] or a
/// [DownloadCompressedChunk] instance and configures a set of concurrent tasks appropriately, then
/// runs those tasks to completion.
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

    let stat = head_object_request(s3, bucket, key).await?;
    let total_size = stat
        .ok_or_else(|| Error::GetObjectInvalidKey(key.into()))?
        .size as u64;

    fn run_downloader<'a, T: Downloader + Send + 'a, ClientT>(
        s3: ClientT,
        downloader: T,
        decompress: bool,
    ) -> BoxFuture<'a, Result<()>>
    where
        ClientT: S3 + Send + Sync + Clone + 'a,
    {
        let concurrent_downloader_tasks = downloader.concurrent_downloader_tasks();
        let concurrent_writer_tasks = downloader.concurrent_writer_tasks();

        if decompress {
            let reader_chunk_stream = create_download_readers_stream(s3, downloader)
                .buffered(concurrent_downloader_tasks);
            let reader_result_stream = map_download_readers_to_writer(reader_chunk_stream);
            let fut = reader_result_stream
                .buffered(concurrent_writer_tasks)
                .try_collect();
            Box::pin(fut)
        } else {
            let reader_chunk_stream = create_download_readers_stream(s3, downloader)
                .buffer_unordered(concurrent_downloader_tasks);
            let reader_result_stream = map_download_readers_to_writer(reader_chunk_stream);
            let fut = reader_result_stream
                .buffer_unordered(concurrent_writer_tasks)
                .try_collect();
            Box::pin(fut)
        }
    }

    if decompress {
        let file_output: Box<dyn bio::Write + Send + Sync + Unpin> =
            Box::new(GzDecoder::new(bio::File::create(file)?));
        let file_output = Arc::new(Mutex::new(file_output));
        let downloader = DownloadCompressed::new(file_output, bucket, key, total_size);
        run_downloader(s3.clone(), downloader, decompress).await
    } else {
        let file_output = bio::File::create(file)?;
        let downloader = DownloadMultiple::new(file_output, bucket, key, total_size);
        run_downloader(s3.clone(), downloader, decompress).await
    }
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

pub(in crate) async fn download_with_dir<T>(
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
    std::fs::create_dir_all(parent_dir)?;

    let key = format!("{}", Path::new(s3_prefix).join(s3_suffix).display());
    let dest_path = format!("{}", dest_path.display());

    download(s3, bucket, &key, &dest_path).await?;

    Ok(())
}

pub(crate) async fn download_streaming<T>(
    s3: &T,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
) -> Result<ByteStream>
where
    T: S3 + Send + Clone,
{
    let range = None;
    download_streaming_range(s3, bucket, key, range).await
}

#[logfn(err = "ERROR")]
pub async fn download<T>(
    s3: &T,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    file: impl AsRef<Path>,
) -> Result<()>
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
