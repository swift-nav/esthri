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

mod http_server;

use std::env;
use std::ffi::OsStr;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::str::FromStr;
use std::time::Duration;

use anyhow::Result;
use glob::Pattern;
use hyper::Client;
use log::*;
use log_derive::logfn;
use structopt::StructOpt;
use tokio::runtime::Builder;

use esthri::{rusoto::*, GlobFilter, PendingUpload, S3PathParam};

// Environment variable that can be set to set the path to the aws tool that esthri falls back to
const REAL_AWS_EXECUTABLE_ENV_NAME: &str = "ESTHRI_AWS_PATH";

// Default path to aws tool if the env var isn't set
const REAL_AWS_EXECUTABLE_DEFAULT: &str = "aws.real";

#[logfn(err = "ERROR")]
async fn log_etag(path: &Path) -> Result<String> {
    info!("s3etag: path={}", path.display());
    let etag = esthri::compute_etag(path).await?;
    debug!("s3etag: file={}, etag={}", path.display(), etag);
    Ok(etag)
}

enum Cli {
    Esthri(EsthriCli),
    AwsCompat(AwsCompatCli),
}

#[derive(Debug, StructOpt)]
#[structopt(name = "esthri", about = "Simple S3 file transfer utility.")]
struct EsthriCli {
    #[structopt(subcommand)]
    cmd: EsthriCommand,
}

#[derive(Debug, StructOpt)]
#[structopt(
    name = "esthri aws wrapper",
    about = "Simple S3 file transfer utility."
)]
struct AwsCompatCli {
    #[structopt(subcommand)]
    cmd: AwsCommand,
}

#[derive(Debug, StructOpt)]
enum AwsCommand {
    /// S3 compatibility layer, providing a compatibility CLI layer for the official AWS S3 CLI tool
    S3 {
        #[structopt(subcommand)]
        cmd: S3Command,
    },
}

// Esthri does this by default (and can currently only do this), exposing this
// as an option just ensures that Esthri will still be able to handle the case
// when the acl option is specified and set to this value
const S3_ACL_OPTIONS: &[&str] = &["bucket-owner-full-control"];

#[derive(Debug, StructOpt)]
enum S3Command {
    #[structopt(name = "cp")]
    Copy {
        source: S3PathParam,
        destination: S3PathParam,
        #[structopt(long)]
        #[allow(dead_code)]
        quiet: bool,
        #[structopt(long, possible_values(S3_ACL_OPTIONS))]
        #[allow(dead_code)]
        acl: Option<String>,
        #[structopt(long)]
        transparent_compression: bool,
    },
    Sync {
        source: S3PathParam,
        destination: S3PathParam,
        #[structopt(long)]
        include: Option<Vec<Pattern>>,
        #[structopt(long)]
        exclude: Option<Vec<Pattern>>,
        #[structopt(long)]
        #[allow(dead_code)]
        quiet: bool,
        #[structopt(long, possible_values(S3_ACL_OPTIONS))]
        #[allow(dead_code)]
        acl: Option<String>,
        #[structopt(long)]
        transparent_compression: bool,
    },
}

#[derive(Debug, StructOpt)]
enum EsthriCommand {
    /// Upload an object to S3
    Put {
        /// Should the file be compressed during upload
        #[structopt(long)]
        compress: bool,
        /// The target bucket (example: my-bucket)
        #[structopt(long)]
        bucket: String,
        /// The key name of the object (example: a/key/name.bin)
        #[structopt(long)]
        key: String,
        /// The storage class of the object (example: RRS)
        #[structopt(long = "storage", default_value = "STANDARDIA")]
        storage_class: String,
        /// The path of the local file to read
        file: PathBuf,
    },
    /// Download an object from S3
    Get {
        /// Should the file be decompressed during download
        #[structopt(long)]
        transparent_compression: bool,
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
        source: S3PathParam,
        /// Destination of the sync (example: s3://my-bucket/a/prefix/dst)
        #[structopt(long)]
        destination: S3PathParam,
        /// Optional include glob pattern (see `man 3 glob`)
        #[structopt(long)]
        include: Option<Vec<Pattern>>,
        /// Optional exclude glob pattern (see `man 3 glob`)
        #[structopt(long)]
        exclude: Option<Vec<Pattern>>,
        /// Enable compression, only valid on upload
        #[structopt(long)]
        transparent_compression: bool,
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

fn call_real_aws() {
    warn!("Falling back from esthri to the real AWS executable");
    let args = env::args().skip(1);

    let aws_tool_path = env::var(REAL_AWS_EXECUTABLE_ENV_NAME)
        .unwrap_or_else(|_| REAL_AWS_EXECUTABLE_DEFAULT.to_string());

    let err = Command::new(&aws_tool_path).args(args).exec();

    panic!(
        "Executing aws didn't work. Is it installed and available as {:?}? {:?}",
        aws_tool_path, err
    );
}

/// Checks if the Esthri CLI tool should run in "aws compatibility mode", where
/// it will intercept aws s3 commands that it can handle and then pass off aws
/// commands it cannot handle to the real aws command line tool.
fn is_aws_compatibility_mode() -> bool {
    let program_name = env::current_exe()
        .ok()
        .as_ref()
        .map(Path::new)
        .and_then(Path::file_name)
        .and_then(OsStr::to_str)
        .map(String::from);

    // Returns true if the binary is named 'aws' or if it was invoked from a hard link named 'aws'
    let is_run_as_aws = program_name.unwrap_or_else(|| "".to_string()) == *"aws";
    let has_aws_env_var = env::var("ESTHRI_AWS_COMPAT_MODE").is_ok();

    is_run_as_aws || has_aws_env_var
}

/// Allows for an escape hatch to be set that triggers always falling back from
/// esthri into the real aws tool.
fn should_always_fallback_to_aws() -> bool {
    env::var("ESTHRI_AWS_ALWAYS_FALLBACK").is_ok()
}

async fn dispatch_aws_cli(cmd: AwsCommand, s3: &S3Client) -> Result<()> {
    match cmd {
        AwsCommand::S3 { cmd } => {
            match cmd {
                S3Command::Copy {
                    ref source,
                    ref destination,
                    transparent_compression: compress,
                    ..
                } => {
                    // Works around structopt/clap not supporting flag values from environment variables
                    let compress =
                        compress || env::var("ESTHRI_AWS_COMPAT_MODE_COMPRESSION").is_ok();

                    if let Err(e) =
                        esthri::copy(s3, source.clone(), destination.clone(), compress).await
                    {
                        match e {
                            esthri::Error::BucketToBucketCpNotImplementedError => {
                                call_real_aws();
                            }
                            _ => {
                                return Err(e.into());
                            }
                        }
                    }
                }

                S3Command::Sync {
                    ref source,
                    ref destination,
                    ref include,
                    ref exclude,
                    transparent_compression: compress,
                    ..
                } => {
                    let clap = AwsCompatCli::clap();
                    let matches = clap.get_matches();
                    let matches = matches
                        .subcommand_matches("s3")
                        .expect("Expected s3 command")
                        .subcommand_matches("sync")
                        .expect("Expected sync command");

                    let user_filters = globs_to_filter_list(include, exclude, matches);

                    let mut filters = vec![];

                    if let Some(user_filters) = user_filters {
                        filters.extend(user_filters);
                    }

                    // Following the interface of `aws sync`, we should
                    // always fall back to including every file. See:
                    // https://docs.aws.amazon.com/cli/latest/reference/s3/index.html#use-of-exclude-and-include-filters
                    filters.push(GlobFilter::Include(Pattern::new("*")?));

                    // Works around structopt/clap not supporting flag values from environment variables
                    let compress =
                        compress || env::var("ESTHRI_AWS_COMPAT_MODE_COMPRESSION").is_ok();

                    esthri::sync(
                        s3,
                        source.clone(),
                        destination.clone(),
                        Some(&filters),
                        compress,
                    )
                    .await?;
                }
            };
        }
    }

    Ok(())
}

fn args_with_indices<'a, I: IntoIterator + 'a>(
    collection: I,
    name: &str,
    matches: &'a structopt::clap::ArgMatches,
) -> impl Iterator<Item = (usize, I::Item)> + 'a {
    matches
        .indices_of(name)
        .into_iter()
        .flatten()
        .zip(collection)
}

/// Gets a sorted list of include and exclude patterns, sorted in order
/// from last specified to first specified
fn globs_to_filter_list(
    include: &Option<Vec<Pattern>>,
    exclude: &Option<Vec<Pattern>>,
    matches: &structopt::clap::ArgMatches,
) -> Option<Vec<GlobFilter>> {
    if include.as_deref().is_some() || exclude.as_deref().is_some() {
        let includes: Vec<GlobFilter> = include
            .as_ref()
            .unwrap_or(&vec![])
            .iter()
            .cloned()
            .map(GlobFilter::Include)
            .collect();
        let excludes: Vec<GlobFilter> = exclude
            .as_ref()
            .unwrap_or(&vec![])
            .iter()
            .cloned()
            .map(GlobFilter::Exclude)
            .collect();

        let mut filters: Vec<(usize, GlobFilter)> = args_with_indices(includes, "include", matches)
            .chain(args_with_indices(excludes, "exclude", matches))
            .collect();

        filters.sort_by(|a, b| a.0.cmp(&b.0).reverse());

        let filters: Vec<GlobFilter> = filters.iter().cloned().map(|x| x.1).collect();

        Some(filters)
    } else {
        None
    }
}

async fn dispatch_esthri_cli(cmd: EsthriCommand, s3: &S3Client) -> Result<()> {
    use EsthriCommand::*;

    match cmd {
        Put {
            ref bucket,
            ref key,
            ref file,
            ref storage_class,
            compress,
        } => {
            let storage_class = S3StorageClass::from_str(storage_class.as_ref())
                .unwrap_or(S3StorageClass::StandardIA);

            if compress {
                esthri::upload_compressed_with_storage_class(s3, bucket, key, file, storage_class)
                    .await?;
            } else {
                esthri::upload_with_storage_class(s3, bucket, key, file, storage_class).await?;
            }
        }

        Get {
            ref bucket,
            ref key,
            ref file,
            transparent_compression,
        } => {
            if transparent_compression {
                esthri::download_with_transparent_decompression(s3, bucket, key, file).await?;
            } else {
                esthri::download(s3, bucket, key, file).await?;
            }
        }

        Abort {
            bucket,
            key,
            upload_id,
        } => {
            PendingUpload::new(&bucket, &key, &upload_id)
                .abort(s3)
                .await?;
        }

        Etag { file } => {
            log_etag(&file).await?;
        }

        Sync {
            ref source,
            ref destination,
            ref include,
            ref exclude,
            transparent_compression,
        } => {
            if transparent_compression && (source.is_bucket() && destination.is_bucket()) {
                return Err(esthri::errors::Error::InvalidSyncCompress.into());
            }
            let clap = EsthriCommand::clap();
            let matches = clap.get_matches();
            let matches = matches
                .subcommand_matches("sync")
                .expect("Expected sync command");
            let filters = globs_to_filter_list(include, exclude, matches);
            esthri::sync(
                s3,
                source.clone(),
                destination.clone(),
                filters.as_deref(),
                transparent_compression,
            )
            .await?;
        }

        HeadObject { bucket, key } => {
            esthri::head_object(s3, &bucket, &key).await?;
        }

        ListObjects { bucket, key } => {
            esthri::list_objects(s3, &bucket, &key).await?;
        }

        Serve { bucket, address } => {
            http_server::run(s3.clone(), &bucket, &address).await?;
        }
    }

    Ok(())
}

fn setup_upload_termination_handler(s3: S3Client) {
    tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for ctrl-c");
        eprintln!("ctrl-c");
        for p in esthri::PendingUpload::all() {
            if let Err(e) = p.abort(&s3).await {
                log::error!("failed to cancel multipart upload: {}", e);
            }
        }
        info!("\ncancelled");
        std::process::exit(0);
    });
}

async fn async_main() -> Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "esthri=debug,esthri_lib=debug");
    }

    eprintln!("{:?}", esthri::Config::global());

    env_logger::init();

    let aws_compat_mode = is_aws_compatibility_mode();
    let always_fallback = should_always_fallback_to_aws();

    if always_fallback && aws_compat_mode {
        info!("Set to always fallback to AWS executable");
        call_real_aws();
    }

    let cli = match aws_compat_mode {
        false => Cli::Esthri(EsthriCli::from_args()),
        true => {
            let args = AwsCompatCli::from_args_safe().map_err(|e| {
                call_real_aws();
                anyhow::anyhow!(e)
            })?;
            Cli::AwsCompat(args)
        }
    };

    let region = Region::default();

    info!("Starting, using region: {:?}...", region);

    let mut hyper_builder = Client::builder();
    hyper_builder.pool_idle_timeout(Duration::from_secs(20));

    let https_connector = esthri::new_https_connector();
    let http_client = HttpClient::from_builder(hyper_builder, https_connector);

    let credentials_provider = DefaultCredentialsProvider::new().unwrap();
    let s3 = S3Client::new_with(http_client, credentials_provider, Region::default());

    setup_upload_termination_handler(s3.clone());

    match cli {
        Cli::Esthri(esthri_cli) => dispatch_esthri_cli(esthri_cli.cmd, &s3).await?,
        Cli::AwsCompat(aws_cli) => dispatch_aws_cli(aws_cli.cmd, &s3).await?,
    }

    Ok(())
}

fn main() -> Result<()> {
    let rt = Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to create tokio runtime");
    rt.block_on(async { async_main().await })?;
    Ok(())
}
