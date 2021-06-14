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
