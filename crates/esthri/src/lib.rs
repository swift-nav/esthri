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

pub mod aws_sdk;
#[cfg(feature = "blocking")]
pub mod blocking;
pub mod errors;

pub(crate) mod compression;
pub(crate) mod tempfile;
pub(crate) mod types;

mod config;
mod ops;
mod presign;

use std::{marker::Unpin, path::Path, time::Duration};

pub use crate::config::Config;
use crate::{aws_sdk::*, types::S3Listing};
use aws_smithy_runtime::client::http::hyper_014::HyperClientBuilder;
use futures::{stream, TryStream, TryStreamExt};
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
    delete::{delete, delete_streaming},
    download::{download, download_streaming},
    sync::{streaming::sync as sync_streaming, sync, GlobFilter},
    upload::{upload, upload_from_reader, PendingUpload},
};
pub use presign::{
    delete::{delete_file_presigned, presign_delete},
    download::{download_file_presigned, presign_get},
    multipart_upload::{
        abort_presigned_multipart_upload, complete_presigned_multipart_upload,
        setup_presigned_multipart_upload, upload_file_presigned_multipart_upload,
        PresignedMultipartUpload,
    },
    upload::{presign_put, upload_file_presigned},
};

pub use aws_sdk::HeadObjectInfo;
pub use types::{S3ListingItem, S3Object, S3PathParam};

pub use esthri_internals::new_https_connector;

pub const FILTER_EMPTY: Option<&[GlobFilter]> = None;

pub enum AwsCredProvider {
    DefaultProvider,
    Environment,
    Profile,
    Ecs,
    Imds,
    WebIdentityToken,
}

/// This function builds a AWS client using the default AWS region
/// and default credentials_provider
pub async fn init_default_s3client() -> Client {
    init_default_s3client_with_region(None::<&str>).await
}

/// This function builds a AWS client using default credentials_provider,
/// allowing you to optionally override the default aws_region
pub async fn init_default_s3client_with_region(region: Option<impl AsRef<str>>) -> Client {
    init_s3client_with_region(AwsCredProvider::DefaultProvider, region).await
}

/// This function builds a AWS client using the default AWS region.
pub async fn init_s3client(provider: AwsCredProvider) -> Client {
    init_s3client_with_region(provider, None::<&str>).await
}

/// This function builds a AWS client, while allowing you to optionally
/// override the default aws region.
pub async fn init_s3client_with_region(
    provider: AwsCredProvider,
    region: Option<impl AsRef<str>>,
) -> Client {
    let retry_config = aws_config::retry::RetryConfig::standard()
        .with_initial_backoff(Duration::from_millis(500))
        .with_max_attempts(5);
    let https_connector = new_https_connector();
    let hyper_client = HyperClientBuilder::new().build(https_connector);

    let sdk_config = if let Some(region) = region {
        aws_config::from_env()
            .region(Region::new(region.as_ref().to_owned()))
            .load()
            .await
    } else {
        aws_config::load_from_env().await
    };
    let config = match provider {
        AwsCredProvider::DefaultProvider => aws_sdk_s3::config::Builder::from(&sdk_config)
            .retry_config(retry_config)
            .http_client(hyper_client)
            .build(),
        AwsCredProvider::Environment => {
            let cred = aws_config::environment::EnvironmentVariableCredentialsProvider::new();
            aws_sdk_s3::config::Builder::from(&sdk_config)
                .retry_config(retry_config)
                .http_client(hyper_client)
                .credentials_provider(cred)
                .build()
        }
        AwsCredProvider::Profile => {
            let cred = aws_config::profile::ProfileFileCredentialsProvider::builder().build();
            aws_sdk_s3::config::Builder::from(&sdk_config)
                .retry_config(retry_config)
                .http_client(hyper_client)
                .credentials_provider(cred)
                .build()
        }
        AwsCredProvider::Ecs => {
            let cred = aws_config::ecs::EcsCredentialsProvider::builder().build();
            aws_sdk_s3::config::Builder::from(&sdk_config)
                .retry_config(retry_config)
                .http_client(hyper_client)
                .credentials_provider(cred)
                .build()
        }
        AwsCredProvider::Imds => {
            let cred = aws_config::imds::credentials::ImdsCredentialsProvider::builder().build();
            aws_sdk_s3::config::Builder::from(&sdk_config)
                .retry_config(retry_config)
                .http_client(hyper_client)
                .credentials_provider(cred)
                .build()
        }
        AwsCredProvider::WebIdentityToken => {
            let cred =
                aws_config::web_identity_token::WebIdentityTokenCredentialsProvider::builder()
                    .build();
            aws_sdk_s3::config::Builder::from(&sdk_config)
                .retry_config(retry_config)
                .http_client(hyper_client)
                .credentials_provider(cred)
                .build()
        }
    };

    aws_sdk_s3::Client::from_conf(config)
}

pub async fn compute_etag(path: impl AsRef<Path>) -> Result<String> {
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
            etag_hash.update(digest_bytes);
        }
        let final_hash = etag_hash.finalize();
        let hex_digest = hex::encode(final_hash);
        Ok(format!("\"{}-{}\"", hex_digest, count))
    }
}

#[logfn(err = "ERROR")]
pub async fn head_object(
    s3: &Client,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
) -> Result<Option<HeadObjectInfo>> {
    let (bucket, key) = (bucket.as_ref(), key.as_ref());
    info!("head-object: bucket={}, key={}", bucket, key);
    head_object_request(s3, bucket, key, None).await
}

#[logfn(err = "ERROR")]
pub async fn list_objects(
    s3: &Client,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
) -> Result<Vec<String>> {
    let none: Option<&str> = None;
    list_objects_with_delim(s3, bucket, key, none).await
}

#[logfn(err = "ERROR")]
pub async fn list_directory(
    s3: &Client,
    bucket: impl AsRef<str>,
    dir_path: impl AsRef<str>,
) -> Result<Vec<String>> {
    list_objects_with_delim(s3, bucket, dir_path, Some("/")).await
}

async fn list_objects_with_delim<S0, S1, S2>(
    s3: &Client,
    bucket: S0,
    key: S1,
    delim: Option<S2>,
) -> Result<Vec<String>>
where
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

pub fn list_objects_stream<'a>(
    s3: &'a Client,
    bucket: impl AsRef<str> + 'a,
    key: impl AsRef<str> + 'a,
) -> impl TryStream<Ok = Vec<S3ListingItem>, Error = Error> + Unpin + 'a {
    let no_delim: Option<&str> = None;
    list_objects_stream_with_delim(s3, bucket, key, no_delim)
}

pub fn list_directory_stream<'a>(
    s3: &'a Client,
    bucket: &'a str,
    key: &'a str,
) -> impl TryStream<Ok = Vec<S3ListingItem>, Error = Error> + Unpin + 'a {
    let slash_delim = Some("/");
    list_objects_stream_with_delim(s3, bucket, key, slash_delim)
}

fn list_objects_stream_with_delim(
    s3: &'_ Client,
    bucket: impl AsRef<str>,
    key: impl AsRef<str>,
    delimiter: Option<impl AsRef<str>>,
) -> impl TryStream<Ok = Vec<S3ListingItem>, Error = Error> + Unpin + '_ {
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

async fn list_objects_request(
    s3: &Client,
    bucket: &str,
    key: &str,
    continuation: Option<String>,
    delimiter: Option<String>,
) -> Result<S3Listing> {
    let lov2o = s3
        .list_objects_v2()
        .bucket(bucket)
        .prefix(key)
        .set_continuation_token(continuation)
        .set_delimiter(delimiter)
        .send()
        .await
        .map_err(|e| Error::ListObjectsFailed {
            prefix: key.to_string(),
            source: Box::new(e.into_service_error()),
        })?;

    let mut listing = S3Listing {
        continuation: lov2o.next_continuation_token,
        ..Default::default()
    };

    if let Some(contents) = lov2o.contents {
        for object in contents {
            match (object.key, object.e_tag) {
                (Some(key), Some(e_tag)) => {
                    listing.contents.push(S3Object {
                        key,
                        e_tag,
                        storage_class: object.storage_class,
                        size: object.size,
                        last_modified: object.last_modified,
                    });
                }
                (key, etag) => {
                    if key.is_none() {
                        warn!("unexpected: object key was null");
                    }
                    if etag.is_none() {
                        warn!("unexpected: object ETag was null");
                    }
                    continue;
                }
            }
        }
    }

    if let Some(common_prefixes) = lov2o.common_prefixes {
        for common_prefix in common_prefixes {
            match common_prefix.prefix {
                Some(prefix) => listing.common_prefixes.push(prefix),
                None => {
                    warn!("unexpected: prefix was null");
                    continue;
                }
            }
        }
    }

    Ok(listing)
}

pub mod opts {
    use aws_sdk_s3::types::StorageClass;
    use derive_builder::Builder;
    use glob::Pattern;

    #[derive(Debug, Clone, Builder)]
    pub struct AwsCopyOptParams {
        #[builder(default = "Some(StorageClass::Standard)")]
        pub storage_class: Option<StorageClass>,
        #[builder(default)]
        pub transparent_compression: bool,
    }

    #[derive(Debug, Clone, Builder)]
    pub struct EsthriPutOptParams {
        #[builder(default = "Some(StorageClass::Standard)")]
        pub storage_class: Option<StorageClass>,
        #[builder(default)]
        pub transparent_compression: bool,
    }

    impl From<SharedSyncOptParams> for EsthriPutOptParams {
        fn from(opt: SharedSyncOptParams) -> Self {
            Self {
                storage_class: Some(StorageClass::Standard),
                transparent_compression: opt.transparent_compression,
            }
        }
    }

    impl From<AwsCopyOptParams> for EsthriPutOptParams {
        fn from(opt: AwsCopyOptParams) -> Self {
            Self {
                storage_class: opt.storage_class,
                transparent_compression: opt.transparent_compression,
            }
        }
    }

    #[derive(Debug, Copy, Clone, Builder)]
    pub struct EsthriGetOptParams {
        #[builder(default)]
        pub transparent_compression: bool,
    }

    impl From<AwsCopyOptParams> for EsthriGetOptParams {
        fn from(opt: AwsCopyOptParams) -> Self {
            Self {
                transparent_compression: opt.transparent_compression,
            }
        }
    }

    impl From<SharedSyncOptParams> for EsthriGetOptParams {
        fn from(opt: SharedSyncOptParams) -> Self {
            Self {
                transparent_compression: opt.transparent_compression,
            }
        }
    }

    #[derive(Debug, Clone, Builder)]
    pub struct SharedSyncOptParams {
        #[builder(default)]
        pub include: Option<Vec<Pattern>>,
        #[builder(default)]
        pub exclude: Option<Vec<Pattern>>,
        #[builder(default)]
        pub transparent_compression: bool,
        #[builder(default)]
        pub delete: bool,
    }
}
