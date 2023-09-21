use clap::Args;
use esthri::aws_sdk::StorageClass as S3StorageClass;
use esthri::{opts::*, S3PathParam};
use glob::Pattern;
use std::path::PathBuf;

// Esthri does this by default (and can currently only do this), exposing this
// as an option just ensures that Esthri will still be able to handle the case
// when the acl option is specified and set to this value
const S3_ACL_OPTIONS: &[&str] = &["bucket-owner-full-control"];

const STORAGE_CLASSES: &[&str] = S3StorageClass::values();

#[derive(Args, Debug, Clone)]
pub struct AwsCopyParams {
    pub source: S3PathParam,
    pub destination: S3PathParam,
    #[clap(long)]
    #[allow(dead_code)]
    pub quiet: bool,
    #[clap(long, value_parser = clap::builder::PossibleValuesParser::new(S3_ACL_OPTIONS))]
    #[allow(dead_code)]
    pub acl: Option<String>,
    #[clap(long, env = "ESTHRI_AWS_COMPAT_MODE_COMPRESSION")]
    pub transparent_compression: bool,
}

impl AwsCopyParams {
    pub fn build_opts(&self) -> AwsCopyOptParams {
        let opts = AwsCopyOptParamsBuilder::default()
            .transparent_compression(self.transparent_compression)
            .build()
            .unwrap();
        opts
    }
}

#[derive(Args, Debug, Clone)]
pub struct AwsSyncParams {
    pub source: S3PathParam,
    pub destination: S3PathParam,
    #[clap(long)]
    pub include: Option<Vec<Pattern>>,
    #[clap(long)]
    pub exclude: Option<Vec<Pattern>>,
    #[clap(long)]
    #[allow(dead_code)]
    pub quiet: bool,
    #[clap(long, value_parser = clap::builder::PossibleValuesParser::new(S3_ACL_OPTIONS))]
    #[allow(dead_code)]
    pub acl: Option<String>,
    #[clap(long)]
    pub transparent_compression: bool,
    #[clap(long)]
    pub delete: bool,
}

impl AwsSyncParams {
    pub fn build_opts(&self) -> SharedSyncOptParams {
        let opts = SharedSyncOptParamsBuilder::default()
            .include(self.include.clone())
            .exclude(self.exclude.clone())
            .transparent_compression(self.transparent_compression)
            .delete(self.delete)
            .build()
            .unwrap();
        opts
    }
}

#[derive(Args, Debug, Clone)]
pub struct EsthriPutParams {
    /// The target bucket (example: my-bucket)
    #[clap(long)]
    pub bucket: String,
    /// The key name of the object (example: a/key/name.bin)
    #[clap(long)]
    pub key: String,
    /// The path of the local file to read
    pub file: PathBuf,
    /// The storage class of the object (example: RRS)
    #[clap(long = "storage", value_parser = clap::builder::PossibleValuesParser::new(STORAGE_CLASSES))]
    pub storage_class: Option<S3StorageClass>,
    /// Should the file be compressed during upload
    #[clap(long)]
    pub compress: bool,
}

impl EsthriPutParams {
    pub fn build_opts(&self) -> EsthriPutOptParams {
        let opts = EsthriPutOptParamsBuilder::default()
            .storage_class(self.storage_class.clone())
            .transparent_compression(self.compress)
            .build()
            .unwrap();
        opts
    }
}

#[derive(Args, Debug, Clone)]
pub struct EsthriGetParams {
    /// The target bucket (example: my-bucket)
    #[clap(long)]
    pub bucket: String,
    /// The key name of the object (example: a/key/name.bin)
    #[clap(long)]
    pub key: String,
    /// The path of the local file to write
    pub file: PathBuf,
    /// Should the file be decompressed during download
    #[clap(long)]
    pub transparent_compression: bool,
}

impl EsthriGetParams {
    pub fn build_opts(&self) -> EsthriGetOptParams {
        let opts = EsthriGetOptParamsBuilder::default()
            .transparent_compression(self.transparent_compression)
            .build()
            .unwrap();
        opts
    }
}

#[derive(Args, Debug, Clone)]
pub struct EsthriAbortParams {
    /// The bucket of the multipart upload (example: my-bucket)
    #[clap(long)]
    pub bucket: String,
    /// The key of the multipart upload (example: a/key/name.bin)
    #[clap(long)]
    pub key: String,
    /// The upload_id for the multipart upload
    pub upload_id: String,
}

#[derive(Args, Debug, Clone)]
pub struct EsthriEtagParams {
    pub file: PathBuf,
}

#[derive(Args, Debug, Clone)]
pub struct EsthriSyncParams {
    #[clap(long)]
    pub source: S3PathParam,
    /// Destination of the sync (example: s3://my-bucket/a/prefix/dst)
    #[clap(long)]
    pub destination: S3PathParam,
    /// Optional include glob pattern (see `man 3 glob`)
    #[clap(long)]
    pub include: Option<Vec<Pattern>>,
    /// Optional exclude glob pattern (see `man 3 glob`)
    #[clap(long)]
    pub exclude: Option<Vec<Pattern>>,
    /// Enable compression, only valid on upload
    #[clap(long)]
    pub transparent_compression: bool,
    /// Enable delete
    #[clap(long)]
    pub delete: bool,
}

impl EsthriSyncParams {
    pub fn build_opts(&self) -> SharedSyncOptParams {
        let opts = SharedSyncOptParamsBuilder::default()
            .include(self.include.clone())
            .exclude(self.exclude.clone())
            .transparent_compression(self.transparent_compression)
            .delete(self.delete)
            .build()
            .unwrap();
        opts
    }
}

#[derive(Args, Debug, Clone)]
pub struct EsthriHeadObjectParams {
    /// The bucket to target (example: my-bucket)
    #[clap(long)]
    pub bucket: String,
    /// The key to target (example: a/key/name.bin)
    #[clap(long)]
    pub key: String,
}

#[derive(Args, Debug, Clone)]
pub struct EsthriListObjectsParams {
    /// The bucket to target (example: my-bucket)
    #[clap(long)]
    pub bucket: String,
    /// The key to target (example: a/key/name.bin)
    #[clap(long)]
    pub key: String,
}

#[derive(Args, Debug, Clone)]
pub struct EsthriDeleteObjectsParams {
    /// The bucket to delete from (example: my-bucket)
    #[clap(long)]
    pub bucket: String,
    /// The keys to delete (example: a/key/name.bin b/key/name.bin)
    #[clap(long = "key", required = true)]
    pub keys: Vec<String>,
}

#[derive(Args, Debug, Clone)]
pub struct EsthriServeParams {
    /// The bucket to serve over HTTP (example: my-bucket)
    #[clap(long)]
    pub bucket: String,
    /// The listening address for the server
    #[clap(long, default_value = "127.0.0.1:3030")]
    pub address: std::net::SocketAddr,
    /// Whether to serve "index.html" in place of "/" for directories
    #[clap(long)]
    pub index_html: bool,
    /// A prefix that is allowed for access, all other prefixes are rejected
    /// (specify this option more than once to allow multiple prefixes).
    #[clap(long = "allowed-prefix", name = "PREFIX", alias = "allowed-prefixes")]
    pub allowed_prefixes: Vec<String>,
}

#[derive(Args, Debug, Clone)]
pub struct EsthriPresignParams {
    /// The bucket to target (example: my-bucket)
    #[clap(long)]
    pub bucket: String,
    /// The key to target (example: a/key/name.bin)
    #[clap(long)]
    pub key: String,
}
