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
pub use hyper_rustls::{self, *};
#[cfg(feature = "rustls")]
pub use rusoto_core::{self, *};
#[cfg(feature = "rustls")]
pub use rusoto_credential::{self, *};
#[cfg(feature = "rustls")]
pub use rusoto_s3::{self, *};
#[cfg(feature = "rustls")]
pub use rusoto_signature::{self, *};
