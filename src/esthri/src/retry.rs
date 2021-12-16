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

use std::time::Duration;

use backoff::ExponentialBackoff;
use futures::Future;
use log::debug;

use crate::rusoto::*;

pub async fn handle_dispatch_error<'a, T, E, F>(func: impl Fn() -> F + 'a) -> RusotoResult<T, E>
where
    F: Future<Output = RusotoResult<T, E>>,
    E: std::error::Error + Send + Sync + 'static,
{
    backoff::future::retry(default_backoff(), || async {
        func().await.map_err(from_rusoto_err::<E>)
    })
    .await
}

fn from_rusoto_err<E>(err: RusotoError<E>) -> backoff::Error<RusotoError<E>> {
    use backoff::Error;

    match err {
        RusotoError::HttpDispatch(_) => {
            debug!("Retrying S3 dispatch error");
            Error::transient(err)
        }
        RusotoError::Unknown(ref res) if res.status.is_server_error() => {
            debug!("Retrying S3 server error: {}", res.body_as_str());
            Error::transient(err)
        }
        _ => Error::Permanent(err),
    }
}

fn default_backoff() -> ExponentialBackoff {
    ExponentialBackoff {
        max_interval: Duration::from_secs(10),
        max_elapsed_time: Some(Duration::from_secs(45)),
        ..Default::default()
    }
}
