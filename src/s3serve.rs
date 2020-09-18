use std::convert::Infallible;
use std::str::FromStr;
use std::time::Duration;

use log::{debug, warn};

use warp::http;
use warp::hyper::Body;
use warp::reject::Reject;
use warp::{http::Error, http::Response};
use warp::{http::StatusCode, Filter};

use rusoto_s3::{S3Client, S3};

use tokio_util::compat::*;

use futures::io::AsyncReadExt;
use futures::StreamExt;

use tokio_util::codec::BytesCodec;
use tokio_util::codec::FramedRead;

use tokio::sync::mpsc;

use async_tar::{Builder, Header};

use async_compression::futures::bufread::GzipEncoder;

use crate::head_object2;
use crate::streaming_download;

use bytes::{Bytes, BytesMut};

fn with_s3_client(
    s3_client: S3Client,
) -> impl Filter<Extract = (S3Client,), Error = Infallible> + Clone {
    warp::any().map(move || s3_client.clone())
}

pub async fn s3serve(s3_client: S3Client) -> Result<(), Infallible> {
    let routes = warp::path::full()
        .and(with_s3_client(s3_client.clone()))
        .and_then(download);

    warp::serve(routes).run(([127, 0, 0, 1], 3030)).await;

    Ok(())
}
#[derive(Debug, PartialEq)]
struct EsthriInternalError;

impl Reject for EsthriInternalError {}

async fn download(
    path: warp::path::FullPath,
    s3: S3Client,
) -> Result<http::Response<Body>, warp::Rejection> {
    let path = path.as_str().to_owned();
    let path = path.get(1..).map(Into::<String>::into).unwrap_or_default();
    let obj_info = {
        match head_object2(&s3, "esthri-test", &path).await {
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
        match streaming_download(&s3, "esthri-test", &path).await {
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

        let mut buffer: [u8; 4096] = [0; 4096];

        if let Err(err) = archive
            .async_append_data(&mut header, path, &mut stream_reader, &mut buffer[..])
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
