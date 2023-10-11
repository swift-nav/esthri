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

#[cfg(not(windows))]
mod http_server;

mod cli_opts;
mod cli_utils;

use anyhow::Result;
use clap::{CommandFactory, Parser, Subcommand};
use cli_opts::*;
use cli_utils::*;
use esthri::aws_sdk::Client as S3Client;
use esthri::{self, GlobFilter, PendingUpload};
use glob::Pattern;
use log::*;
use tokio::runtime::Builder;

#[derive(Debug, Parser)]
#[clap(name = "esthri", about = "Simple S3 file transfer utility.", version)]
struct EsthriCli {
    #[clap(subcommand)]
    cmd: EsthriCommand,
}

#[derive(Debug, Parser)]
#[clap(
    name = "esthri aws wrapper",
    about = "Simple S3 file transfer utility.",
    version
)]
struct AwsCompatCli {
    #[clap(subcommand)]
    cmd: AwsCommand,
}

#[derive(Debug, Subcommand)]
enum AwsCommand {
    /// S3 compatibility layer, providing a compatibility CLI layer for the official AWS S3 CLI tool
    S3 {
        #[clap(subcommand)]
        cmd: S3Command,
    },
}

#[non_exhaustive]
#[derive(Debug, Subcommand)]
enum S3Command {
    #[clap(name = "cp")]
    Copy(AwsCopyParams),
    Sync(AwsSyncParams),
}

#[non_exhaustive]
#[derive(Debug, Subcommand)]
enum EsthriCommand {
    /// Upload an object to S3
    Put(EsthriPutParams),
    /// Download an object from S3
    Get(EsthriGetParams),
    /// Manually abort a multipart upload
    Abort(EsthriAbortParams),
    /// Compute and print the S3 ETag of the file
    Etag(EsthriEtagParams),
    /// Sync a directory with S3
    Sync(EsthriSyncParams),
    /// Retreive the ETag for a remote object
    HeadObject(EsthriHeadObjectParams),
    /// List remote objects in S3
    ListObjects(EsthriListObjectsParams),
    /// Delete objects from the specified bucket.
    DeleteObjects(EsthriDeleteObjectsParams),
    #[cfg(not(windows))]
    /// Launch an HTTP server attached to the specified bucket
    ///
    /// This also supports serving dynamic archives of bucket contents
    Serve(EsthriServeParams),
    /// Presign URL
    Presign(EsthriPresignParams),
}

async fn dispatch_aws_cli(cmd: AwsCommand, s3: &S3Client) -> Result<()> {
    setup_upload_termination_handler(s3.clone());
    match cmd {
        AwsCommand::S3 { cmd } => {
            match cmd {
                S3Command::Copy(params) => {
                    let opts = AwsCopyParams::build_opts(&params);

                    if let Err(e) =
                        esthri::copy(s3, params.source.clone(), params.destination.clone(), opts)
                            .await
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

                // ribbit
                S3Command::Sync(params) => {
                    let opts = AwsSyncParams::build_opts(&params);
                    let command = AwsCompatCli::command();
                    let matches = command.get_matches();
                    let matches = matches
                        .subcommand_matches("s3")
                        .expect("Expected s3 command")
                        .subcommand_matches("sync")
                        .expect("Expected sync command");

                    let user_filters = globs_to_filter_list(&opts.include, &opts.exclude, matches);

                    let mut filters = vec![];

                    if let Some(user_filters) = user_filters {
                        filters.extend(user_filters);
                    }

                    // Following the interface of `aws sync`, we should
                    // always fall back to including every file. See:
                    // https://docs.aws.amazon.com/cli/latest/reference/s3/index.html#use-of-exclude-and-include-filters
                    filters.push(GlobFilter::Include(Pattern::new("*")?));

                    esthri::sync(
                        s3,
                        params.source.clone(),
                        params.destination.clone(),
                        Some(&filters),
                        opts,
                    )
                    .await?;
                }
            };
        }
    }

    Ok(())
}

async fn dispatch_esthri_cli(cmd: EsthriCommand, s3: &S3Client) -> Result<()> {
    use EsthriCommand::*;

    match cmd {
        Put(params) => {
            setup_upload_termination_handler(s3.clone());
            let opts = EsthriPutParams::build_opts(&params);
            esthri::upload(s3, params.bucket, params.key, params.file, opts).await?;
        }

        Get(params) => {
            let opts = EsthriGetParams::build_opts(&params);
            esthri::download(s3, params.bucket, params.key, params.file, opts).await?;
        }

        Abort(params) => {
            PendingUpload::new(&params.bucket, &params.key, &params.upload_id)
                .abort(s3)
                .await?;
        }

        Etag(params) => {
            log_etag(&params.file).await?;
        }

        Sync(params) => {
            setup_upload_termination_handler(s3.clone());
            let opts = EsthriSyncParams::build_opts(&params);
            if params.transparent_compression
                && params.source.is_bucket()
                && params.destination.is_bucket()
            {
                return Err(esthri::errors::Error::InvalidSyncCompress.into());
            }
            let matches = EsthriCli::command().get_matches();
            let matches = matches
                .subcommand_matches("sync")
                .expect("Expected sync command");
            let filters = globs_to_filter_list(&opts.include, &opts.exclude, matches);
            let filters = filters.as_deref();
            esthri::sync(s3, params.source, params.destination, filters, opts).await?;
        }

        HeadObject(params) => {
            esthri::head_object(s3, &params.bucket, &params.key).await?;
        }

        ListObjects(params) => {
            esthri::list_objects(s3, &params.bucket, &params.key).await?;
        }

        DeleteObjects(params) => {
            esthri::delete(s3, params.bucket, &params.keys[..]).await?;
        }

        #[cfg(not(windows))]
        Serve(params) => {
            http_server::run(
                s3.clone(),
                &params.bucket,
                &params.address,
                params.index_html,
                &params.allowed_prefixes[..],
            )
            .await?;
        }

        Presign(params) => {
            println!(
                "{}",
                esthri::presign_get(s3, params.bucket, params.key, None,).await?
            );
        }
    }

    Ok(())
}

async fn async_main() -> Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "esthri=debug,esthri_lib=debug");
    }

    env_logger::init();

    let aws_compat_mode = is_aws_compatibility_mode();
    let always_fallback = should_always_fallback_to_aws();

    if always_fallback && aws_compat_mode {
        info!("Set to always fallback to AWS executable");
        call_real_aws();
    }

    let s3 = setup_s3client_with_cred_provider().await.unwrap();

    if aws_compat_mode {
        let args = AwsCompatCli::try_parse().map_err(|e| {
            call_real_aws();
            anyhow::anyhow!(e)
        })?;
        dispatch_aws_cli(args.cmd, &s3).await?
    } else {
        let args = EsthriCli::parse();
        dispatch_esthri_cli(args.cmd, &s3).await?
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
