use std::convert::Infallible;
use std::str::FromStr;
use std::time::Duration;

use warp::http;
use warp::hyper::Body;
use warp::reject::Reject;
use warp::{http::Error, http::Response};
use warp::{http::StatusCode, Filter};

use rusoto_s3::{S3Client, S3};

use crate::streaming_download;

fn with_s3_client(
    s3_client: S3Client,
) -> impl Filter<Extract = (S3Client,), Error = Infallible> + Clone {
    warp::any().map(move || s3_client.clone())
}

pub async fn s3serve(s3_client: S3Client) -> Result<(), Infallible> {
    let routes = warp::path!("s3" / String)
        .and(with_s3_client(s3_client.clone()))
        .and_then(download);

    warp::serve(routes).run(([127, 0, 0, 1], 3030)).await;

    Ok(())
}

#[derive(Debug, PartialEq)]
struct EsthriInternalError;

impl Reject for EsthriInternalError {}

async fn download(path: String, s3: S3Client) -> Result<http::Response<Body>, warp::Rejection> {
    let stream = streaming_download(&s3, "esthri-test", &path).await;

    match stream {
        Ok(stream) => {
            let body = Body::wrap_stream(stream);

            Response::builder()
                .body(body)
                .map_err(|_| warp::reject::custom(EsthriInternalError {}))
        }
        Err(_) => Err(warp::reject::reject()),
    }
}
