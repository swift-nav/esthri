/*
 * Copyright (C) 2022 Swift Navigation Inc.
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

pub use hyper;

use hyper::client::connect::HttpConnector;

#[cfg(feature = "rustls")]
pub use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};

#[cfg(feature = "rustls")]
pub fn new_https_connector() -> HttpsConnector<HttpConnector> {
    HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_only()
        .enable_http1()
        .build()
}

#[cfg(feature = "nativetls")]
pub use hyper_tls::HttpsConnector;

#[cfg(feature = "nativetls")]
pub fn new_https_connector() -> hyper_tls::HttpsConnector<HttpConnector> {
    HttpsConnector::new()
}
