use std::convert::Infallible;
use std::net::SocketAddr;

use log::*;

use warp::http;
use warp::hyper::Body;
use warp::reject::Reject;
use warp::http::Response;
use warp::Filter;
//use warp::http::Error;
//use warp::http::StatusCode;

use rusoto_s3::S3Client;

// Import all extensions that allow converting between futures-rs and tokio types
use tokio_util::compat::*;

use tokio_util::codec::BytesCodec;
use tokio_util::codec::FramedRead;

use async_tar::{Builder, Header};

use async_compression::futures::bufread::GzipEncoder;

use crate::download_streaming;
use crate::head_object_info;

fn with_bucket(bucket: String) -> impl Filter<Extract = (String,), Error = Infallible> + Clone {
    warp::any().map(move || bucket.clone())
}

fn with_s3_client(
    s3_client: S3Client,
) -> impl Filter<Extract = (S3Client,), Error = Infallible> + Clone {
    warp::any().map(move || s3_client.clone())
}

pub async fn s3serve(s3_client: S3Client, bucket: &str, address: &SocketAddr) -> Result<(), Infallible> {
    let routes = warp::path::full()
        .and(with_s3_client(s3_client.clone()))
        .and(with_bucket(bucket.to_string()))
        .and_then(download);

    warp::serve(routes).run(*address).await;

    Ok(())
}
#[derive(Debug, PartialEq)]
struct EsthriInternalError;

impl Reject for EsthriInternalError {}

async fn download(
    path: warp::path::FullPath,
    s3: S3Client,
    bucket: String,
) -> Result<http::Response<Body>, warp::Rejection> {
    let path = path.as_str().to_owned();
    let path = path.get(1..).map(Into::<String>::into).unwrap_or_default();
    let obj_info = {
        match head_object_info(&s3, &bucket, &path).await {
            Ok(obj_info) => {
                if let Some(obj_info) = obj_info {
                    obj_info
                } else {
                    warn!("object not found: {}", path);
                    return Err(warp::reject::reject());
                }
            }
            Err(err) => {
                warn!("head_object failed: {}", err);
                return Err(warp::reject::reject());
            }
        }
    };

    let stream = {
        match download_streaming(&s3, &bucket, &path).await {
            Ok(obj_info) => obj_info,
            Err(err) => {
                warn!("head_object failed: {}", err);
                return Err(warp::reject::reject());
            }
        }
    };

    let (tar_pipe_reader, tar_pipe_writer) = sluice::pipe::pipe();
    let mut archive = Builder::new(tar_pipe_writer);

    let gzip = GzipEncoder::new(tar_pipe_reader);

    tokio::spawn(async move {
        let mut header = Header::new_gnu();
        header.set_mode(0o0644);
        header.set_size(obj_info.size as u64);

        let mut stream_reader = stream.into_async_read().compat();

        if let Err(err) = archive
            .append_data(&mut header, path, &mut stream_reader)
            .await
        {
            warn!("tar append_data failed: {}", err);
        }
    });

    let framed_reader = FramedRead::new(gzip.compat(), BytesCodec::new());

    let body = Body::wrap_stream(framed_reader);

    Response::builder()
        .header("Content-Type", "application/binary")
        .body(body)
        .map_err(|_| warp::reject::custom(EsthriInternalError {}))
}
