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

pub use std::error::Error as StdError;
use std::path::StripPrefixError;

use chrono::ParseError;
use glob::PatternError;
use rusoto_core::RusotoError;
use rusoto_s3::{
    AbortMultipartUploadError, CompleteMultipartUploadError, CopyObjectError,
    CreateMultipartUploadError, GetObjectError, HeadObjectError, ListObjectsV2Error,
    PutObjectError, UploadPartError,
};
use tokio::task::JoinError;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("did not exist locally")]
    ETagNotPresent,

    #[error("s3 sync prefixes must end in a slash")]
    DirlikePrefixRequired,

    #[error("source file does not exist: {0:?}")]
    InvalidSourceFile(std::path::PathBuf),

    #[error("create_multipart_upload upload_id was none")]
    UploadIdNone,

    #[error("a read of zero occured")]
    ReadZero,

    #[error("upload_part failed")]
    UploadPartFailed(#[from] RusotoError<UploadPartError>),

    #[error("complete_multipart_upload failed")]
    CompletedMultipartUploadFailed(#[from] RusotoError<CompleteMultipartUploadError>),

    #[error("put_object failed")]
    PutObjectFailed(#[from] RusotoError<PutObjectError>),

    #[error("create_multipart_upload failed")]
    CreateMultipartUploadFailed(#[from] RusotoError<CreateMultipartUploadError>),

    #[error("did not expect body field of GetObjectOutput to be none")]
    GetObjectOutputBodyNone,

    #[error("exclude glob pattern error")]
    GlobPatternError(#[from] PatternError),

    #[error("head_object unexpected result: {0}")]
    HeadObjectUnexpected(String),

    #[error("unexpected: parent dir was null")]
    ParentDirNone,

    #[error(transparent)]
    HeadObjectFailure(#[from] RusotoError<HeadObjectError>),

    #[error(transparent)]
    HeadObjectFailedParseError(#[from] ParseError),

    #[error(transparent)]
    WalkDirFailed(#[from] walkdir::Error),

    #[error(transparent)]
    CopyObjectFailed(#[from] RusotoError<CopyObjectError>),

    #[error(transparent)]
    ListObjectsFailed(#[from] RusotoError<ListObjectsV2Error>),

    #[error(transparent)]
    AbortMultipartUploadFailed(#[from] RusotoError<AbortMultipartUploadError>),

    #[error(transparent)]
    StripPrefixFailed(#[from] StripPrefixError),

    #[error(transparent)]
    GetObjectFailed(#[from] RusotoError<GetObjectError>),

    #[error("invalid key, did not exist remotely: {0}")]
    GetObjectInvalidKey(String),

    #[error("invalid read, sizes did not match {0} and {1}")]
    GetObjectInvalidRead(usize, usize),

    #[error("remote object sized changed while reading")]
    GetObjectSizeChanged,

    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error(transparent)]
    JoinError(#[from] JoinError),

    #[error("sync: compression is only valid for upload")]
    InvalidSyncCompress,

    #[cfg(feature = "compression")]
    #[error(transparent)]
    PersistError(#[from] tempfile::PersistError),
}

impl From<std::convert::Infallible> for Error {
    fn from(_: std::convert::Infallible) -> Self {
        unreachable!()
    }
}
