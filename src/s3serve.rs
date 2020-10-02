use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::{io, io::ErrorKind};

use log::*;

use warp::http;
use warp::http::Response;
use warp::hyper::Body;
use warp::reject::Reject;
use warp::Filter;

use rusoto_s3::{S3Client, S3};

// Import all extensions that allow converting between futures-rs and tokio types
use tokio_util::compat::*;

use tokio_util::codec::BytesCodec;
use tokio_util::codec::FramedRead;

use async_tar::{Builder, Header};

use async_compression::futures::bufread::GzipEncoder;

use futures::stream::{Stream, TryStreamExt};

use sluice::{pipe, pipe::PipeWriter};

use bytes::{Bytes, BytesMut};

use async_stream::stream;

use futures::stream::StreamExt;

use eyre::{eyre, Report};

use crate::download_streaming;
use crate::head_object_info;
use crate::list_objects_stream;

use serde_derive::{Deserialize, Serialize};

fn with_bucket(bucket: String) -> impl Filter<Extract = (String,), Error = Infallible> + Clone {
    warp::any().map(move || bucket.clone())
}

fn with_s3_client(
    s3_client: S3Client,
) -> impl Filter<Extract = (S3Client,), Error = Infallible> + Clone {
    warp::any().map(move || s3_client.clone())
}

#[derive(Deserialize, Serialize)]
struct Params {
    archive: Option<bool>,
}

pub async fn s3serve(
    s3_client: S3Client,
    bucket: &str,
    address: &SocketAddr,
) -> Result<(), Infallible> {
    let routes = warp::path::full()
        .and(with_s3_client(s3_client.clone()))
        .and(with_bucket(bucket.to_owned()))
        .and(warp::query::<Params>())
        .and_then(download);

    warp::serve(routes).run(*address).await;

    Ok(())
}

async fn abort_with_error(
    archive: Option<&mut Builder<PipeWriter>>,
    error_tracker: ErrorTrackerArc,
    err: Report,
) {
    let err = {
        if let Some(archive) = archive {
            if let Err(err) = archive.finish().await {
                Report::new(err).wrap_err("closing the archive")
            } else {
                err
            }
        } else {
            err
        }
    };
    ErrorTracker::record_error(error_tracker, err);
}

async fn stream_object_to_archive<T: S3 + Send>(
    s3: &T,
    bucket: &str,
    path: &str,
    archive: &mut Builder<PipeWriter>,
    error_tracker: ErrorTrackerArc,
) {
    let obj_info = {
        match head_object_info(s3, bucket, path).await {
            Ok(obj_info) => {
                if let Some(obj_info) = obj_info {
                    obj_info
                } else {
                    abort_with_error(Some(archive), error_tracker, eyre!("object not found: {}", path))
                        .await;
                    return;
                }
            }
            Err(err) => {
                abort_with_error(
                    Some(archive),
                    error_tracker,
                    err.wrap_err("s3 head operation failed"),
                )
                .await;
                return;
            }
        }
    };
    let stream = {
        match download_streaming(s3, bucket, path).await {
            Ok(byte_stream) => byte_stream,
            Err(err) => {
                abort_with_error(Some(archive), error_tracker, err.wrap_err("s3 download failed")).await;
                return;
            }
        }
    };
    let mut header = Header::new_gnu();
    header.set_mode(0o0644);
    header.set_size(obj_info.size as u64);
    let mut stream_reader = stream.into_async_read().compat();
    if let Err(err) = archive
        .append_data(&mut header, path, &mut stream_reader)
        .await
    {
        abort_with_error(
            Some(archive),
            error_tracker,
            Report::new(err).wrap_err("tar append failed"),
        )
        .await;
    }
}

struct ErrorTracker {
    has_error: AtomicBool,
    the_error: Mutex<Option<Box<eyre::Report>>>,
}

impl ErrorTracker {
    fn new() -> Self {
        ErrorTracker {
            has_error: AtomicBool::new(false),
            the_error: Mutex::new(None),
        }
    }
    fn record_error(error_tracker: ErrorTrackerArc, err: eyre::Report) {
        error_tracker.0.has_error.store(true, Ordering::Release);
        let mut the_error = error_tracker
            .0
            .the_error
            .lock()
            .expect("locking error field");
        *the_error = Some(Box::new(err));
    }
}

#[derive(Clone)]
struct ErrorTrackerArc(Arc<ErrorTracker>);

impl ErrorTrackerArc {
    fn new() -> ErrorTrackerArc {
        ErrorTrackerArc(Arc::new(ErrorTracker::new()))
    }
    fn has_error(&self) -> bool {
        self.0.has_error.load(Ordering::Acquire)
    }
}

impl Into<Option<io::Error>> for ErrorTrackerArc {
    fn into(self) -> Option<io::Error> {
        let the_error = self.0.the_error.lock().unwrap();
        let the_error = {
            if let Some(the_error) = &*the_error {
                the_error
            } else {
                return None;
            }
        };
        if let Some(the_error) = the_error.downcast_ref::<io::Error>() {
            Some(io::Error::new(the_error.kind(), format!("{}", the_error)))
        } else {
            Some(io::Error::new(ErrorKind::Other, format!("{}", the_error)))
        }
    }
}

async fn create_error_monitor_stream<T: Stream<Item = io::Result<BytesMut>> + Unpin>(
    error_tracker: ErrorTrackerArc,
    mut source_stream: T,
) -> impl Stream<Item = io::Result<BytesMut>> {
    stream! {
        let error_tracker = error_tracker.clone();
        loop {
            let item: Option<io::Result<BytesMut>> = source_stream.next().await;
            if let Some(item) = item {
                yield item;
            } else {
                if error_tracker.has_error() {
                    let the_error: Option<io::Error> = error_tracker.into();
                    if let Some(the_error) = the_error {
                        error!("stream error: {}", the_error);
                        yield Err(the_error);
                    } else {
                        error!("no error even though one was signaled");
                    }
                } else {
                    debug!("wrapped stream done, no error signaled");
                }
                break;
            }
        }
    }
}

async fn create_archive_stream(
    s3: S3Client,
    bucket: String,
    path: String,
    error_tracker: ErrorTrackerArc,
) -> impl Stream<Item = io::Result<BytesMut>> {
    let (tar_pipe_reader, tar_pipe_writer) = pipe::pipe();
    let mut archive = Builder::new(tar_pipe_writer);
    let error_tracker_reader = error_tracker.clone();
    tokio::spawn(async move {
        let mut object_list_stream = list_objects_stream(&s3, &bucket, &path);
        let error_tracker = error_tracker.clone();
        loop {
            match object_list_stream.try_next().await {
                Ok(None) => break,
                Ok(Some(items)) => {
                    for s3obj in items {
                        stream_object_to_archive(
                            &s3,
                            &bucket,
                            &s3obj.key,
                            &mut archive,
                            error_tracker.clone(),
                        )
                        .await;
                    }
                }
                Err(err) => {
                    let err = err.wrap_err("listing objects");
                    abort_with_error(Some(&mut archive), error_tracker.clone(), err).await;
                    break;
                }
            }
        }
    });
    let gzip = GzipEncoder::new(tar_pipe_reader);
    let framed_reader = FramedRead::new(gzip.compat(), BytesCodec::new());
    create_error_monitor_stream(error_tracker_reader, framed_reader).await
}

fn into_io_error(err: &eyre::Report) -> io::Error {
    io::Error::new(ErrorKind::Other, format!("{}", err))
}

async fn create_item_stream(
    s3: S3Client,
    bucket: String,
    path: String,
) -> impl Stream<Item = io::Result<Bytes>> {
    stream! {
        let mut stream = {
            match download_streaming(&s3, &bucket, &path).await {
                Ok(byte_stream) => byte_stream,
                Err(err) => {
                    yield Err(into_io_error(&err));
                    return;
                }
            }
        };
        loop {
            if let Some(data) = stream.next().await {
                yield data;
            } else {
                break;
            }
        }
    }
}

#[derive(Debug, PartialEq)]
struct EsthriInternalError;

impl Reject for EsthriInternalError {}

async fn download(
    path: warp::path::FullPath,
    s3: S3Client,
    bucket: String,
    params: Params,
) -> Result<http::Response<Body>, warp::Rejection> {
    let path = path.as_str().to_owned();
    let path = path.get(1..).map(Into::<String>::into).unwrap_or_default();

    let error_tracker = ErrorTrackerArc::new();

    debug!("params: archive: {:?}", params.archive);

    let body = if params.archive.unwrap_or(false) {
        let stream = create_archive_stream(s3.clone(), bucket, path, error_tracker).await;
        Body::wrap_stream(stream)
    } else {
        let stream = create_item_stream(s3.clone(), bucket, path).await;
        Body::wrap_stream(stream)
    };

    Response::builder()
        .header("Content-Type", "application/binary")
        .body(body)
        .map_err(|err| {
            error!("esthri internal error: {}", err);
            warp::reject::custom(EsthriInternalError {})
        })
}
