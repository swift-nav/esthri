pub use std::error::Error;

#[derive(thiserror::Error, Debug)]
pub enum EsthriError {
    #[error("did not exist locally")]
    ETagNotPresent,

    #[error("s3 sync prefixes must end in a slash")]
    DirlikePrefixRequired,
}
