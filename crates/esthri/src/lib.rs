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

#[cfg(feature = "blocking")]
pub mod blocking;
pub mod errors;
pub mod rusoto;

pub(crate) mod compression;
pub(crate) mod tempfile;
pub(crate) mod types;

mod config;
mod ops;
mod retry;

use std::marker::Unpin;
use std::path::Path;

pub use crate::config::Config;
use crate::retry::handle_dispatch_error;
use crate::rusoto::*;
use crate::types::S3Listing;
use futures::{stream, TryStream, TryStreamExt};
use hyper::client::connect::HttpConnector;
use log::{info, warn};
use log_derive::logfn;
use md5::{Digest, Md5};
use tokio::{
    fs,
    io::{AsyncRead, AsyncReadExt, BufReader},
};

pub use errors::{Error, Result};
pub use ops::{
    copy::copy,
    download::{download, download_streaming, download_with_transparent_decompression},
    sync::{sync, GlobFilter},
    upload::{
        upload, upload_compressed, upload_compressed_with_storage_class, upload_from_reader,
        upload_with_storage_class, PendingUpload,
    },
};
pub use rusoto::HeadObjectInfo;
pub use types::{S3ListingItem, S3Object, S3PathParam};

pub const FILTER_EMPTY: Option<&[GlobFilter]> = None;

pub async fn compute_etag(path: impl AsRef<Path>) -> Result<String> {
    async fn inner(path: &Path) -> Result<String> {
        let f = match fs::File::open(path).await {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(Error::ETagNotPresent);
            }
            Err(e) => {
                return Err(e.into());
            }
        };
        let file_size = f.metadata().await?.len();
        compute_etag_from_reader(f, file_size).await
    }
    inner(path.as_ref()).await
}

pub async fn compute_etag_from_reader<T>(reader: T, length: u64) -> Result<String>
where
    T: AsyncRead + Unpin + Send + 'static,
{
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
        let mut blob = vec![0u8; upload_part_size];
        reader.read_exact(&mut blob).await?;
        hash.update(&blob);
        let hash_bytes: [u8; 16] = hash.finalize_reset().into();
        digests.push(hash_bytes);
        remaining -= upload_part_size as u64;
    }
    if digests.is_empty() {
        let hash_bytes = hash.finalize();
        let hex_digest = hex::encode(hash_bytes);
        Ok(format!("\"{}\"", hex_digest))
    } else if digests.len() == 1 && length < upload_part_size {
        let hex_digest = hex::encode(digests[0]);
        Ok(format!("\"{}\"", hex_digest))
    } else {
        let count = digests.len();
        let mut etag_hash = Md5::new();
        for digest_bytes in digests {
            etag_hash.update(&digest_bytes);
        }
        let final_hash = etag_hash.finalize();
        let hex_digest = hex::encode(final_hash);
        Ok(format!("\"{}-{}\"", hex_digest, count))
    }
}

#[logfn(err = "ERROR")]
pub async fn head_object<T>(
    s3: &T,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
) -> Result<Option<HeadObjectInfo>>
where
    T: S3 + Send,
{
    let (bucket, key) = (bucket.as_ref(), key.as_ref());
    info!("head-object: bucket={}, key={}", bucket, key);
    head_object_request(s3, bucket, key, None).await
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
pub use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};

#[cfg(feature = "rustls")]
pub fn new_https_connector() -> HttpsConnector<HttpConnector> {
    HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_only()
        .enable_http1()
        .build()
}

#[cfg(feature = "nativetls")]
pub use hyper_tls::HttpsConnector;

#[cfg(feature = "nativetls")]
pub fn new_https_connector() -> hyper_tls::HttpsConnector<HttpConnector> {
    HttpsConnector::new()
}
