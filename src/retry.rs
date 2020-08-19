use std::time::Duration;

use backoff::{future::FutureOperation as _, ExponentialBackoff};
use futures::Future;
use rusoto_core::{RusotoError, RusotoResult};

pub async fn handle_dispatch_error<'a, T, E, F>(func: impl Fn() -> F + 'a) -> RusotoResult<T, E>
where
    F: Future<Output = RusotoResult<T, E>>,
    E: std::error::Error + Send + Sync + 'static,
{
    (|| async { func().await.map_err(from_rusoto_err::<E>) })
        .retry(default_backoff())
        .await
}

fn from_rusoto_err<E>(err: RusotoError<E>) -> backoff::Error<RusotoError<E>> {
    match err {
        RusotoError::HttpDispatch(_) => backoff::Error::Transient(err),
        _ => backoff::Error::Permanent(err),
    }
}

fn default_backoff() -> ExponentialBackoff {
    ExponentialBackoff {
        max_interval: Duration::from_secs(10),
        max_elapsed_time: Some(Duration::from_secs(45)),
        ..Default::default()
    }
}
