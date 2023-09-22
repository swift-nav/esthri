use anyhow::{bail, Result};
use clap::ArgMatches;
use esthri::aws_sdk::Client as S3Client;
use esthri::{AwsCredProvider, GlobFilter};
use glob::Pattern;
use log::*;
use log_derive::logfn;
use std::{
    env,
    ffi::OsStr,
    path::Path,
    process::{Command, Stdio},
};

// Environment variable that can be set to set the path to the aws tool that esthri falls back to
const REAL_AWS_EXECUTABLE_ENV_NAME: &str = "ESTHRI_AWS_PATH";

// Default path to aws tool if the env var isn't set
const REAL_AWS_EXECUTABLE_DEFAULT: &str = "aws.real";

/// Gets a sorted list of include and exclude patterns, sorted in order
/// from last specified to first specified
pub fn globs_to_filter_list(
    include: &Option<Vec<Pattern>>,
    exclude: &Option<Vec<Pattern>>,
    matches: &ArgMatches,
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

fn args_with_indices<'a, I: IntoIterator + 'a>(
    collection: I,
    name: &str,
    matches: &'a ArgMatches,
) -> impl Iterator<Item = (usize, I::Item)> + 'a {
    matches
        .indices_of(name)
        .into_iter()
        .flatten()
        .zip(collection)
}

#[logfn(err = "ERROR")]
pub async fn log_etag(path: &Path) -> Result<String> {
    info!("s3etag: path={}", path.display());
    let etag = esthri::compute_etag(path).await?;
    debug!("s3etag: file={}, etag={}", path.display(), etag);
    Ok(etag)
}

pub fn call_real_aws() {
    warn!("Falling back from esthri to the real AWS executable");
    let args = env::args().skip(1);

    let aws_tool_path = env::var(REAL_AWS_EXECUTABLE_ENV_NAME)
        .unwrap_or_else(|_| REAL_AWS_EXECUTABLE_DEFAULT.to_string());

    let status = Command::new(&aws_tool_path)
        .args(args)
        .stdout(Stdio::inherit())
        .status()
        .unwrap_or_else(|_| {
            panic!(
                "Executing aws didn't work. Is it installed and available as {:?} ",
                aws_tool_path
            )
        });
    std::process::exit(status.code().unwrap_or(-1));
}

/// Checks if the Esthri CLI tool should run in "aws compatibility mode", where
/// it will intercept aws s3 commands that it can handle and then pass off aws
/// commands it cannot handle to the real aws command line tool.
pub fn is_aws_compatibility_mode() -> bool {
    let program_name = env::current_exe()
        .ok()
        .as_ref()
        .map(Path::new)
        .and_then(Path::file_name)
        .and_then(OsStr::to_str)
        .map(String::from);

    // Returns true if the binary is named 'aws' or if it was invoked from a hard link named 'aws'
    let is_run_as_aws = program_name.map_or(false, |s| s == "aws");
    let has_aws_env_var = env::var("ESTHRI_AWS_COMPAT_MODE").is_ok();

    is_run_as_aws || has_aws_env_var
}

/// Allows for an escape hatch to be set that triggers always falling back from
/// esthri into the real aws tool.
pub fn should_always_fallback_to_aws() -> bool {
    env::var("ESTHRI_AWS_ALWAYS_FALLBACK").is_ok()
}

/// Listening for SIGINT signal and exit
pub fn setup_upload_termination_handler(s3: S3Client) {
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
pub async fn setup_s3client_with_cred_provider() -> Result<S3Client> {
    match std::env::var("ESTHRI_CREDENTIAL_PROVIDER") {
        Ok(val) => match val.as_str() {
            "env" => Ok(esthri::init_s3client(AwsCredProvider::Environment).await),
            "profile" => Ok(esthri::init_s3client(AwsCredProvider::Profile).await),
            "container" => Ok(esthri::init_s3client(AwsCredProvider::Ecs).await),
            "instance_metadata" => Ok(esthri::init_s3client(AwsCredProvider::Imds).await),
            "k8s" => Ok(esthri::init_s3client(AwsCredProvider::WebIdentityToken).await),
            "" => Ok(esthri::init_s3client(AwsCredProvider::DefaultProvider).await),
            _ => {
                bail!("unsupported credential provider environment variable, program aborting");
            }
        },
        Err(_) => Ok(esthri::init_default_s3client().await),
    }
}
