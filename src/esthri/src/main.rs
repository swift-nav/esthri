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

use esthri::rusoto::*;
use esthri::*;

use structopt::StructOpt;

use hyper::{client::connect::HttpConnector, Client};

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
        #[structopt(long)]
        source: SyncParam,
        #[structopt(long)]
        destination: SyncParam,
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
    /// Launch an HTTP server attached to the specified bucket
    ///
    /// This also supports serving dynamic archives of bucket contents
    Serve {
        /// The bucket to serve over HTTP
        #[structopt(long)]
        bucket: String,
        /// The listening address for the server
        #[structopt(long, default_value = "127.0.0.1:3030")]
        address: std::net::SocketAddr,
    },
}

#[cfg(feature = "rustls")]
fn new_https_connector() -> HttpsConnector<HttpConnector> {
    HttpsConnector::with_webpki_roots()
}

#[cfg(feature = "nativetls")]
fn new_https_connector() -> HttpsConnector<HttpConnector> {
    HttpsConnector::new()
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

    let https_connector = new_https_connector();
    let http_client = HttpClient::from_builder(hyper_builder, https_connector);

    let credentials_provider = DefaultCredentialsProvider::new().unwrap();
    let s3 = S3Client::new_with(http_client, credentials_provider, Region::default());

    use Command::*;

    match cli.cmd {
        Put { bucket, key, file } => {
            setup_upload_termination_handler();
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
            source,
            destination,
            include,
            exclude,
        } => {
            setup_upload_termination_handler();
            sync(&s3, source, destination, &include, &exclude).await?;
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
