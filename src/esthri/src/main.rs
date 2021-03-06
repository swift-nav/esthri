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

use std::path::{Path, PathBuf};
use std::time::Duration;

use log::*;
use log_derive::logfn;

use esthri::rusoto::*;
use esthri::*;

use structopt::StructOpt;

use hyper::Client;
use tokio::runtime::Builder;

#[logfn(err = "ERROR")]
async fn log_etag(path: &Path) -> Result<String> {
    info!("s3etag: path={}", path.display());
    let etag = compute_etag(path).await?;
    debug!("s3etag: file={}, etag={}", path.display(), etag);
    Ok(etag)
}

#[derive(Debug, StructOpt)]
#[structopt(name = "esthri", about = "Simple S3 file transfer utility.")]
struct Cli {
    #[structopt(subcommand)]
    cmd: Command,
}

#[derive(Debug, StructOpt)]
enum Command {
    /// Upload an object to S3
    Put {
        /// Should the file be compressed during upload
        #[cfg(feature = "compression")]
        #[structopt(long)]
        compress: bool,
        /// The target bucket (example: my-bucket)
        #[structopt(long)]
        bucket: String,
        /// The key name of the object (example: a/key/name.bin)
        #[structopt(long)]
        key: String,
        /// The path of the local file to read
        file: PathBuf,
    },
    /// Download an object from S3
    Get {
        /// Should the file be decompressed during download
        #[cfg(feature = "compression")]
        #[structopt(long)]
        decompress: bool,
        /// The target bucket (example: my-bucket)
        #[structopt(long)]
        bucket: String,
        /// The key name of the object (example: a/key/name.bin)
        #[structopt(long)]
        key: String,
        /// The path of the local file to write
        file: PathBuf,
    },
    /// Manually abort a multipart upload
    Abort {
        /// The bucket of the multipart upload (example: my-bucket)
        #[structopt(long)]
        bucket: String,
        /// The key of the multipart upload (example: a/key/name.bin)
        #[structopt(long)]
        key: String,
        /// The upload_id for the multipart upload
        upload_id: String,
    },
    /// Compute and print the S3 ETag of the file
    Etag { file: PathBuf },
    /// Sync a directory with S3
    Sync {
        /// Source of the sync (example: s3://my-bucket/a/prefix/src)
        #[structopt(long)]
        source: SyncParam,
        /// Destination of the sync (example: s3://my-bucket/a/prefix/dst)
        #[structopt(long)]
        destination: SyncParam,
        /// Optional include glob pattern (see `man 3 glob`)
        #[structopt(long)]
        include: Option<Vec<String>>,
        /// Optional exclude glob pattern (see `man 3 glob`)
        #[structopt(long)]
        exclude: Option<Vec<String>>,
        #[cfg(feature = "compression")]
        #[structopt(long)]
        /// Enable compression, only valid on upload
        compress: bool,
    },
    /// Retreive the ETag for a remote object
    HeadObject {
        /// The bucket to target (example: my-bucket)
        #[structopt(long)]
        bucket: String,
        /// The key to target (example: a/key/name.bin)
        #[structopt(long)]
        key: String,
    },
    /// List remote objects in S3
    ListObjects {
        /// The bucket to target (example: my-bucket)
        #[structopt(long)]
        bucket: String,
        /// The key to target (example: a/key/name.bin)
        #[structopt(long)]
        key: String,
    },
    #[cfg(feature = "http_server")]
    /// Launch an HTTP server attached to the specified bucket
    ///
    /// This also supports serving dynamic archives of bucket contents
    Serve {
        /// The bucket to serve over HTTP (example: my-bucket)
        #[structopt(long)]
        bucket: String,
        /// The listening address for the server
        #[structopt(long, default_value = "127.0.0.1:3030")]
        address: std::net::SocketAddr,
    },
}

async fn async_main() -> Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "esthri=debug,esthri_lib=debug");
    }

    env_logger::init();

    let cli = Cli::from_args();
    let region = Region::default();

    info!("Starting, using region: {:?}...", region);

    let mut hyper_builder = Client::builder();
    hyper_builder.pool_idle_timeout(Duration::from_secs(20));

    let https_connector = new_https_connector();
    let http_client = HttpClient::from_builder(hyper_builder, https_connector);

    let credentials_provider = DefaultCredentialsProvider::new().unwrap();
    let s3 = S3Client::new_with(http_client, credentials_provider, Region::default());

    use Command::*;

    match cli.cmd {
        Put {
            ref bucket,
            ref key,
            ref file,
            ..
        } => {
            setup_upload_termination_handler();
            #[cfg(feature = "compression")]
            {
                if matches!(cli.cmd, Put { compress: true, .. }) {
                    upload_compressed(&s3, bucket, key, file).await?;
                } else {
                    upload(&s3, bucket, key, file).await?;
                }
            }
            #[cfg(not(feature = "compression"))]
            {
                upload(&s3, bucket, key, file).await?;
            }
        }

        Get {
            ref bucket,
            ref key,
            ref file,
            ..
        } => {
            #[cfg(feature = "compression")]
            {
                if matches!(
                    cli.cmd,
                    Get {
                        decompress: true,
                        ..
                    }
                ) {
                    download_decompressed(&s3, bucket, key, file).await?;
                } else {
                    download(&s3, bucket, key, file).await?;
                }
            }
            #[cfg(not(feature = "compression"))]
            {
                download(&s3, bucket, key, file).await?;
            }
        }

        Abort {
            bucket,
            key,
            upload_id,
        } => {
            abort_upload(&s3, &bucket, &key, &upload_id).await?;
        }

        Etag { file } => {
            log_etag(&file).await?;
        }

        Sync {
            ref source,
            ref destination,
            ref include,
            ref exclude,
            ..
        } => {
            #[cfg(feature = "compression")]
            let compress;
            #[cfg(feature = "compression")]
            {
                compress = matches!(cli.cmd, Sync { compress: true, .. });

                if compress && !(source.is_local() && destination.is_bucket()) {
                    return Err(errors::Error::InvalidSyncCompress);
                }
            }

            setup_upload_termination_handler();

            sync(
                &s3,
                source.clone(),
                destination.clone(),
                include.as_deref(),
                exclude.as_deref(),
                #[cfg(feature = "compression")]
                compress,
            )
            .await?;
        }

        HeadObject { bucket, key } => {
            head_object(&s3, &bucket, &key).await?;
        }

        ListObjects { bucket, key } => {
            list_objects(&s3, &bucket, &key).await?;
        }

        #[cfg(feature = "http_server")]
        Serve { bucket, address } => {
            http_server::run(s3.clone(), &bucket, &address).await?;
        }
    }

    Ok(())
}

fn main() -> Result<()> {
    let rt = Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to create tokio runtime");
    rt.block_on(async { async_main().await })
}
