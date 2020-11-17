#[cfg(feature = "nativetls")]
pub use hyper_tls::{self, *};
#[cfg(feature = "nativetls")]
pub use rusoto_core::{self, *};
#[cfg(feature = "nativetls")]
pub use rusoto_credential::{self, *};
#[cfg(feature = "nativetls")]
pub use rusoto_s3::{self, *};
#[cfg(feature = "nativetls")]
pub use rusoto_signature::{self, *};
