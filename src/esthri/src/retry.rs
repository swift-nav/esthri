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
            Error::Transient(err)
        }
        RusotoError::Unknown(ref res) if res.status.is_server_error() => {
            debug!("Retrying S3 server error: {}", res.body_as_str());
            Error::Transient(err)
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
