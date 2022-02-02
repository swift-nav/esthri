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

use futures::Future;
use log::warn;
use std::time::Duration;

use tokio_retry::strategy::{jitter, ExponentialBackoff};
use tokio_retry::RetryIf;

use crate::rusoto::{RusotoError, RusotoResult};
use crate::Config;

pub async fn handle_dispatch_error<'a, F, R, T, E>(func: F) -> RusotoResult<T, E>
where
    F: Fn() -> R + 'a,
    R: Future<Output = RusotoResult<T, E>>,
    E: std::error::Error,
{
    let retry_strategy = ExponentialBackoff::from_millis(500)
        .max_delay(Duration::from_secs(10))
        .map(jitter)
        .take(Config::global().request_retries());

    RetryIf::spawn(retry_strategy, func, is_transient_err).await
}

fn is_transient_err<E>(err: &RusotoError<E>) -> bool {
    match err {
        RusotoError::HttpDispatch(ref res) => {
            warn!("Transient S3 dispatch error {}", res);
            true
        }
        RusotoError::Unknown(ref res) if res.status.is_server_error() => {
            warn!("Transient S3 server error: {}", res.body_as_str());
            true
        }
        _ => {
            false
        }
    }
}
