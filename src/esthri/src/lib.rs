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

use core::{
    pin::Pin,
    task::{Context, Poll},
};
use std::path::Path;
use std::{env, marker::Unpin};

use crypto::digest::Digest;
use crypto::md5::Md5;

use futures::{stream, Future, TryStream, TryStreamExt};

use redis::Commands;

use hyper::client::connect::HttpConnector;
use log::{info, warn};
use log_derive::logfn;
use tokio::{
    fs,
    io::{self, AsyncWrite},
    task,
};

#[cfg(feature = "blocking")]
pub mod blocking;
pub mod errors;
#[cfg(feature = "http_server")]
pub mod http_server;
pub mod rusoto;

pub(crate) mod compression;
pub(crate) mod types;

mod config;
mod ops;
mod retry;

/// Internal module used to call out operations that may block.
mod bio {
    pub(super) use std::fs;
    pub(super) use std::fs::File;
    pub(super) use std::io::prelude::*;
    pub(super) use std::io::BufReader;
}

pub use crate::errors::{Error, Result};

use crate::retry::handle_dispatch_error;
use crate::rusoto::*;

use crate::config::Config;
use crate::types::S3Listing;

pub use crate::types::{ObjectInfo, S3ListingItem, S3Object, S3PathParam};

pub use ops::download::{download, stream_download_helper};

pub use ops::download::download_with_transparent_decompression;
pub use ops::upload::upload_compressed;

#[cfg(feature = "http_server")]
pub(crate) use ops::download::download_streaming;

#[cfg(feature = "cli")]
pub use ops::upload::setup_upload_termination_handler;
pub use ops::upload::{abort_upload, upload, upload_from_reader};

pub use ops::sync::{sync, GlobFilter};

pub use ops::copy::copy;

pub const FILTER_EMPTY: Option<&[GlobFilter]> = None;

const EXPECT_SPAWN_BLOCKING: &str = "spawned task failed";

#[logfn(err = "ERROR")]
pub async fn head_object<T>(
    s3: &T,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
) -> Result<Option<ObjectInfo>>
where
    T: S3 + Send,
{
    let (bucket, key) = (bucket.as_ref(), key.as_ref());
    info!("head-object: bucket={}, key={}", bucket, key);
    head_object_request(s3, bucket, key).await
}

#[logfn(err = "ERROR")]
pub async fn list_objects<T>(
    s3: &T,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
) -> Result<Vec<String>>
where
    T: S3 + Send,
{
    let none: Option<&str> = None;
    list_objects_with_delim(s3, bucket, key, none).await
}

#[logfn(err = "ERROR")]
pub async fn list_directory<T>(
    s3: &T,
    bucket: impl AsRef<str>,
    dir_path: impl AsRef<str>,
) -> Result<Vec<String>>
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

pub async fn compute_etag(path: impl AsRef<Path>) -> Result<String> {
    let path = path.as_ref();
    if !path.exists() {
        return Err(Error::ETagNotPresent);
    }
    let f = bio::File::open(path)?;
    let stat = bio::fs::metadata(&path)?;
    let file_size = stat.len();
    compute_etag_from_reader(f, file_size).await
}

pub fn compute_etag_from_reader<T>(reader: T, length: u64) -> impl Future<Output = Result<String>>
where
    T: bio::Read + Send + 'static,
{
    use bio::*;
    async move {
        task::spawn_blocking(move || {
            let mut reader = BufReader::new(reader);
            let mut hash = Md5::new();
            let mut digests: Vec<[u8; 16]> = vec![];
            let mut remaining = length;
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
            } else if digests.len() == 1 && length < upload_part_size {
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
        })
        .await
        .expect(EXPECT_SPAWN_BLOCKING)
    }
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

    let metadata = if let Some(metadata) = hoo.metadata {
        metadata
    } else {
        return Err(Error::HeadObjectUnexpected("no metadata found".into()));
    };

    Ok(Some(ObjectInfo {
        e_tag,
        size,
        last_modified,
        metadata,
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

#[cfg(feature = "rustls")]
pub fn new_https_connector() -> HttpsConnector<HttpConnector> {
    HttpsConnector::with_webpki_roots()
}

#[cfg(feature = "nativetls")]
pub fn new_https_connector() -> HttpsConnector<HttpConnector> {
    HttpsConnector::new()
}

pub struct StreamWriter {
    pub path: String,
    file: fs::File,
    stream_key: String,
    stream_part_size: usize,
    client: Option<redis::Client>,
}
impl StreamWriter {
    pub async fn create(path: String, stream_key: String) -> Result<Self> {
        let file = fs::File::create(path.clone()).await?;
        let stream_part_size = Config::global().stream_part_size();

        let redis_url = if let Ok(mut url) = env::var("REDIS_SERVER_URL") {
            if !url.starts_with("redis://") {
                url = format!("redis://{}", url);
            }
            url
        } else {
            let default_url = Config::global().default_redis_server_url();
            info!(
                "REDIS_SERVER_URL env var not set. Falling back to {}",
                default_url
            );
            default_url
        };
        let client = redis::Client::open(redis_url).map_or_else(
            |err| {
                warn!("Redis client failure, {}. No streaming available.", err);
                None
            },
            Some,
        );
        Ok(Self {
            path: path.to_string(),
            file,
            stream_key,
            stream_part_size,
            client,
        })
    }
}
impl AsyncWrite for StreamWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut buf = buf;
        let mut written = 0;
        if let Some(client) = &self.client {
            let mut con = client.get_connection().map_err(|err| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("Issue establishing connection to redis server, {}.", err),
                )
            })?;
            let buf_len_max = self.stream_part_size;
            while !buf.len() > 0 {
                let to_write = std::cmp::min(buf.len(), buf_len_max);
                let f = Pin::new(&mut self.file);
                written = match f.poll_write(cx, &buf[..to_write])? {
                    Poll::Ready(t) => t,
                    Poll::Pending => return Poll::Pending,
                };

                let items = vec![(0_u8, &buf[..to_write])];
                con.xadd(self.stream_key.clone(), "*", &items)
                    .map_err(|err| {
                        io::Error::new(
                            io::ErrorKind::Other,
                            format!("Issue pushing to redis server, {}.", err),
                        )
                    })?;
                buf = &buf[written..];
            }
        } else {
            let f = Pin::new(&mut self.file);
            written = match f.poll_write(cx, buf)? {
                Poll::Ready(t) => t,
                Poll::Pending => return Poll::Pending,
            };
        }
        Poll::Ready(Ok(written))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let f = Pin::new(&mut self.file);
        f.poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let f = Pin::new(&mut self.file);
        f.poll_flush(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Stdio;
    use tempfile::tempdir;
    use tokio::process::Command;

    #[tokio::test]
    async fn stream_writer_test() {
        env_logger::init();
        let command: &str = "for i in {0..5};do echo ${i};sleep 1;done";
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("test.txt").to_string_lossy().to_string();
        {
            let mut cmd = Command::new("bash");
            cmd.arg("-c").arg(command);
            let mut writer = StreamWriter::create(log_path.clone(), log_path.clone())
                .await
                .unwrap();
            cmd.stdout(Stdio::piped());
            cmd.stderr(Stdio::piped());
            let mut child = cmd.spawn().unwrap();

            let mut stdout = child.stdout.take().unwrap();
            tokio::io::copy(&mut stdout, &mut writer).await.unwrap();
            child.wait_with_output().await.unwrap();
        }

        println!(
            "{}",
            std::fs::read_to_string(log_path)
                .unwrap()
                .parse::<String>()
                .unwrap()
        );
    }
}
