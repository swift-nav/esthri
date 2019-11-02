use thiserror::Error;

#[derive(Error, Debug)]
pub enum ETagErr {
    #[error("did not exist locally")]
    NotPresent,
}
