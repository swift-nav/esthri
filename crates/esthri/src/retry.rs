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

use std::time::Duration;

use backoff::ExponentialBackoff;
use futures::Future;
use log::debug;

use crate::rusoto::{RusotoError, RusotoResult};

pub async fn handle_dispatch_error<'a, F, R, T, E>(func: F) -> RusotoResult<T, E>
where
    F: Fn() -> R + 'a,
    R: Future<Output = RusotoResult<T, E>>,
    E: std::error::Error,
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
