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

#[cfg(feature = "rustls")]
mod rustls;

#[cfg(feature = "rustls")]
pub use rustls::rusoto_core::{ByteStream, HttpClient, Region, RusotoError, RusotoResult};

#[cfg(feature = "rustls")]
pub use rustls::rusoto_s3::{
    AbortMultipartUploadRequest, CompleteMultipartUploadRequest, CompletedMultipartUpload,
    CompletedPart, CopyObjectOutput, CopyObjectRequest, CreateMultipartUploadRequest,
    GetObjectError, GetObjectOutput, GetObjectRequest, HeadObjectOutput, HeadObjectRequest,
    ListObjectsV2Request, PutObjectRequest, S3Client, StreamingBody, UploadPartRequest, S3,
};

#[cfg(feature = "rustls")]
pub use rustls::rusoto_credential::DefaultCredentialsProvider;

#[cfg(feature = "rustls")]
pub use rustls::hyper_rustls::HttpsConnector;

#[cfg(feature = "nativetls")]
mod nativetls;

#[cfg(feature = "nativetls")]
pub use nativetls::rusoto_core::{ByteStream, HttpClient, Region, RusotoError, RusotoResult};

#[cfg(feature = "nativetls")]
pub use nativetls::rusoto_s3::{
    AbortMultipartUploadRequest, CompleteMultipartUploadRequest, CompletedMultipartUpload,
    CompletedPart, CopyObjectOutput, CopyObjectRequest, CreateMultipartUploadRequest,
    GetObjectError, GetObjectOutput, GetObjectRequest, HeadObjectOutput, HeadObjectRequest,
    ListObjectsV2Request, PutObjectRequest, S3Client, StreamingBody, UploadPartRequest, S3,
};

#[cfg(feature = "nativetls")]
pub use nativetls::rusoto_credential::DefaultCredentialsProvider;

#[cfg(feature = "nativetls")]
pub use nativetls::hyper_tls::HttpsConnector;
