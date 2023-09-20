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

pub use std::error::Error as StdError;
use std::path::StripPrefixError;

use aws_sdk_s3::operation::abort_multipart_upload::AbortMultipartUploadError;
use aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadError;
use aws_sdk_s3::operation::copy_object::CopyObjectError;
use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadError;
use aws_sdk_s3::operation::delete_object::DeleteObjectError;
use aws_sdk_s3::operation::delete_objects::DeleteObjectsError;
use aws_sdk_s3::operation::get_bucket_location::GetBucketLocationError;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error;
use aws_sdk_s3::operation::put_object::PutObjectError;
use aws_sdk_s3::operation::upload_part::UploadPartError;
use aws_sdk_s3::presigning::PresigningConfigError;
use chrono::ParseError;
use glob::PatternError;
use tokio::task::JoinError;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("did not exist locally")]
    ETagNotPresent,
    #[error("invalid s3 etag")]
    InvalidS3ETag,

    #[error("no local etag")]
    NoLocalETag,

    #[error("s3 sync prefixes must end in a slash")]
    DirlikePrefixRequired,

    #[error("source file does not exist: {0:?}")]
    InvalidSourceFile(std::path::PathBuf),

    #[error("create_multipart_upload upload_id was none")]
    UploadIdNone,

    #[error("get_bucket_location location_constraint was none")]
    LocationConstraintNone,

    #[error("a read of zero occured")]
    ReadZero,

    #[error(transparent)]
    UploadPartFailed(#[from] Box<UploadPartError>),

    #[error(transparent)]
    CompletedMultipartUploadFailed(#[from] Box<CompleteMultipartUploadError>),

    #[error(transparent)]
    PutObjectFailed(#[from] Box<PutObjectError>),

    #[error(transparent)]
    CreateMultipartUploadFailed(#[from] Box<CreateMultipartUploadError>),

    #[error("did not expect body field of GetObjectOutput to be none")]
    GetObjectOutputBodyNone,

    #[error("exclude glob pattern error")]
    GlobPatternError(#[from] PatternError),

    #[error("head_object unexpected result: {0}")]
    HeadObjectUnexpected(String),

    #[error("unexpected: parent dir was null")]
    ParentDirNone,

    #[error("head object failed on prefix {prefix}: {source}")]
    HeadObjectFailure {
        prefix: String,
        #[source]
        source: Box<HeadObjectError>,
    },

    #[error(transparent)]
    HeadObjectFailedParseError(#[from] ParseError),

    #[error(transparent)]
    WalkDirFailed(#[from] walkdir::Error),

    #[error(transparent)]
    CopyObjectFailed(#[from] Box<CopyObjectError>),

    #[error("list objects failed on prefix {prefix}: {source}")]
    ListObjectsFailed {
        prefix: String,
        #[source]
        source: Box<ListObjectsV2Error>,
    },

    #[error(transparent)]
    AbortMultipartUploadFailed(#[from] Box<AbortMultipartUploadError>),

    #[error(transparent)]
    StripPrefixFailed(#[from] StripPrefixError),

    #[error(transparent)]
    GetObjectFailed(#[from] Box<GetObjectError>),

    #[error(transparent)]
    DeleteObjectFailed(#[from] Box<DeleteObjectError>),

    #[error(transparent)]
    DeleteObjectsFailed(#[from] Box<DeleteObjectsError>),

    #[error("invalid key, did not exist remotely: {0}")]
    GetObjectInvalidKey(String),

    #[error("invalid read, sizes did not match {0} and {1}")]
    GetObjectInvalidRead(usize, usize),

    #[error("remote object sized changed while reading")]
    GetObjectSizeChanged,

    #[error(transparent)]
    GetBucketLocationFailed(#[from] Box<GetBucketLocationError>),

    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error(transparent)]
    JoinError(#[from] JoinError),

    #[error("sync: compression is not valid for bucket to bucket transfers")]
    InvalidSyncCompress,

    #[error(transparent)]
    PersistError(#[from] tempfile::PathPersistError),

    #[error("cp: Local to local copy not implemented")]
    LocalToLocalCpNotImplementedError,

    #[error("cp: Bucket to bucket copy not implemented")]
    BucketToBucketCpNotImplementedError,

    #[error("Could not parse S3 filename")]
    CouldNotParseS3Filename,

    #[error("File is not gzip compressed")]
    FileNotCompressed,

    #[error("Metadata was none during sync")]
    MetadataNone,

    #[error("unknown storage class")]
    UnknownStorageClass(String),

    #[error("sync streaming only implemented for s3 to local")]
    SyncStreamingNotImplemented,

    #[error(transparent)]
    HTTPError(#[from] reqwest::Error),

    #[error("byte stream error")]
    ByteStreamError(String),

    #[error(transparent)]
    PresigningConfigError(#[from] PresigningConfigError),

    #[error("aws sdk error")]
    SdkError(String),
}

impl From<std::convert::Infallible> for Error {
    fn from(_: std::convert::Infallible) -> Self {
        unreachable!()
    }
}

impl From<Error> for std::io::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::IoError(e) => e,
            other => std::io::Error::new(std::io::ErrorKind::Other, other),
        }
    }
}
