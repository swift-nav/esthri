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

use std::time::Duration;

use log::*;

use esthri_lib::types::*;
use esthri_lib::*;

use rusoto_core::{HttpClient, Region};
use rusoto_credential::DefaultCredentialsProvider;
use rusoto_s3::S3Client;

use structopt::StructOpt;

use hyper::Client;
use hyper_tls::HttpsConnector;

use stable_eyre::eyre::Result;

#[cfg(feature)]
use esthri_lib::s3serve;

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
    /// Tail an object from S3
    Tail {
        #[structopt(long)]
        bucket: String,
        #[structopt(long)]
        key: String,
        #[structopt(long, default_value = "10")]
        interval: u64,
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
    S3Etag { file: String },
    /// Sync a directory with S3
    #[structopt(name = "sync")]
    SyncCmd {
        /// The direction of the sync: 'up' for local to remote, 'down' for remote to local
        #[structopt(long, default_value = "up")]
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
    },
    #[cfg(feature = "http_server")]
    /// Launch and HTTP server attached to the specified bucket
    Serve {
        #[structopt(long)]
        bucket: String,
        #[structopt(long, default_value = "127.0.0.1:3030")]
        address: std::net::SocketAddr,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    stable_eyre::install()?;

    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "esthri=debug,esthri_lib=debug");
    }

    env_logger::init();

    let cli = Cli::from_args();
    let region = Region::default();

    info!("Starting, using region: {:?}...", region);

    let mut hyper_builder = Client::builder();
    hyper_builder.pool_idle_timeout(Duration::from_secs(20));

    let https_connector = HttpsConnector::new();
    let http_client = HttpClient::from_builder(hyper_builder, https_connector);

    let credentials_provider = DefaultCredentialsProvider::new().unwrap();
    let s3 = S3Client::new_with(http_client, credentials_provider, Region::default());

    setup_cancel_handler();

    use Command::*;

    match cli.cmd {
        Put { bucket, key, file } => {
            upload(&s3, &bucket, &key, &file).await?;
        }

        Get { bucket, key, file } => {
            download(&s3, &bucket, &key, &file).await?;
        }

        Tail {
            bucket,
            key,
            interval,
        } => {
            let mut writer = tokio::io::stdout();
            tail(&s3, &mut writer, interval, &bucket, &key).await?;
        }

        Abort {
            bucket,
            key,
            upload_id,
        } => {
            abort_upload(&s3, &bucket, &key, &upload_id).await?;
        }

        S3Etag { file } => {
            log_etag(&file)?;
        }

        SyncCmd {
            direction,
            bucket,
            key,
            directory,
            include,
            exclude,
        } => {
            sync(
                &s3, direction, &bucket, &key, &directory, &include, &exclude,
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
