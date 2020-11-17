#[cfg(feature = "rustls")]
pub use hyper_rustls::{self, *};
#[cfg(feature = "rustls")]
pub use rusoto_core::{self, *};
#[cfg(feature = "rustls")]
pub use rusoto_credential::{self, *};
#[cfg(feature = "rustls")]
pub use rusoto_s3::{self, *};
#[cfg(feature = "rustls")]
pub use rusoto_signature::{self, *};
