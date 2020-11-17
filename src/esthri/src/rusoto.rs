#[cfg(feature = "rustls")]
pub use rusoto_rustls::rusoto_core::{ByteStream, HttpClient, Region, RusotoError, RusotoResult};

#[cfg(feature = "rustls")]
pub use rusoto_rustls::rusoto_s3::{
    AbortMultipartUploadRequest, CompleteMultipartUploadRequest, CompletedMultipartUpload,
    CompletedPart, CopyObjectOutput, CopyObjectRequest, CreateMultipartUploadRequest,
    GetObjectError, GetObjectOutput, GetObjectRequest, HeadObjectOutput, HeadObjectRequest,
    ListObjectsV2Request, PutObjectRequest, S3Client, StreamingBody, UploadPartRequest, S3,
};

#[cfg(feature = "rustls")]
pub use rusoto_rustls::rusoto_credential::DefaultCredentialsProvider;

#[cfg(feature = "rustls")]
pub use rusoto_rustls::hyper_rustls::HttpsConnector;

#[cfg(feature = "nativetls")]
pub use rusoto_nativetls::rusoto_core::{
    ByteStream, HttpClient, Region, RusotoError, RusotoResult,
};

#[cfg(feature = "nativetls")]
pub use rusoto_nativetls::rusoto_s3::{
    AbortMultipartUploadRequest, CompleteMultipartUploadRequest, CompletedMultipartUpload,
    CompletedPart, CopyObjectOutput, CopyObjectRequest, CreateMultipartUploadRequest,
    GetObjectError, GetObjectOutput, GetObjectRequest, HeadObjectOutput, HeadObjectRequest,
    ListObjectsV2Request, PutObjectRequest, S3Client, StreamingBody, UploadPartRequest, S3,
};

#[cfg(feature = "nativetls")]
pub use rusoto_nativetls::rusoto_credential::DefaultCredentialsProvider;

#[cfg(feature = "nativetls")]
pub use rusoto_nativetls::hyper_tls::HttpsConnector;
