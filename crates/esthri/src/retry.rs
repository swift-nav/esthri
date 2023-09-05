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

use futures::Future;
use log::warn;

use tokio_retry::{
    strategy::{jitter, ExponentialBackoff},
    RetryIf,
};

use crate::Config;
use aws_smithy_http::result::SdkError;

pub async fn handle_dispatch_error<'a, F, R, T>(func: F) -> Result<T, SdkError>
where
    F: Fn() -> R + 'a,
    R: Future<Output = Result<T, SdkError>>,
{
    let retry_strategy = ExponentialBackoff::from_millis(500)
        .max_delay(Duration::from_secs(10))
        .map(jitter)
        .take(Config::global().request_retries());

    RetryIf::spawn(retry_strategy, func, is_transient_err).await
}

fn is_transient_err(err: &SdkError) -> bool {
    match err {
        SdkError::ResponseError(error) => {
            warn!("Transient server error: {:?}", error);
            true
        }
        SdkError::TimeoutError(_) => {
            warn!("Transient timeout error");
            true
        }
        SdkError::ServiceError(error) => {
            if error.err().is_retryable() {
                warn!("Service server error: {:?}", error);
                true
            } else {
                false
            }
        }
        _ => false,
    }
}
