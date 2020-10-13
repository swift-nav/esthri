#![cfg_attr(feature = "aggressive_lint", deny(warnings))]

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::{io, io::ErrorKind};

use log::*;

use warp::http;
use warp::http::header::{
    CACHE_CONTROL, CONTENT_DISPOSITION, CONTENT_LENGTH, CONTENT_TYPE, ETAG, LAST_MODIFIED,
};
use warp::http::response;
use warp::http::Response;
use warp::hyper::Body;
use warp::Filter;

use mime_guess::mime::{APPLICATION_OCTET_STREAM, TEXT_HTML_UTF_8};

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
use crate::head_object;
use crate::list_directory_stream;
use crate::list_objects_stream;
use crate::S3ListingItem;

use serde_derive::{Deserialize, Serialize};

const ARCHIVE_ENTRY_MODE: u32 = 0o0644;
const LAST_MODIFIED_TIME_FMT: &str = "%a, %d %b %Y %H:%M:%S GMT";

#[derive(Deserialize)]
struct Params {
    archive: Option<bool>,
    archive_name: Option<String>,
    prefixes: Option<S3PrefixList>,
}

#[derive(Debug)]
struct S3PrefixList {
    prefixes: Vec<String>,
}

impl<'de> serde::Deserialize<'de> for S3PrefixList {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;

        let s = s.split('|').map(|x| {
            if !x.is_empty() {
                Ok(String::from(x))
            } else {
                Err("empty prefix")
            }
        });

        let s: Result<Vec<String>, _> = s.collect();
        let prefixes = s.map_err(serde::de::Error::custom)?;

        if prefixes.is_empty() {
            return Err(serde::de::Error::custom("empty prefix list"));
        }

        Ok(S3PrefixList { prefixes })
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

#[derive(Debug, PartialEq)]
struct EsthriRejection {
    message: String,
}

impl warp::reject::Reject for EsthriRejection {}

impl EsthriRejection {
    fn warp_rejection<MsgT: AsRef<str>>(message: MsgT) -> warp::Rejection {
        warp::reject::custom(EsthriRejection {
            message: message.as_ref().to_owned(),
        })
    }
    fn warp_result<T, MsgT: AsRef<str>>(message: MsgT) -> Result<T, warp::Rejection> {
        Err(EsthriRejection::warp_rejection(message))
    }
}

#[derive(Serialize)]
struct ErrorMessage {
    code: u16,
    message: String,
}

fn with_bucket(bucket: String) -> impl Filter<Extract = (String,), Error = Infallible> + Clone {
    warp::any().map(move || bucket.clone())
}

fn with_s3_client(
    s3_client: S3Client,
) -> impl Filter<Extract = (S3Client,), Error = Infallible> + Clone {
    warp::any().map(move || s3_client.clone())
}

pub fn esthri_filter(
    s3_client: S3Client,
    bucket: &str,
) -> impl Filter<Extract = (http::Response<Body>,), Error = warp::Rejection> + Clone {
    warp::path::full()
        .and(with_s3_client(s3_client))
        .and(with_bucket(bucket.to_owned()))
        .and(warp::query::<Params>())
        .and(warp::header::optional::<String>("if-none-match"))
        .and_then(download)
}

pub async fn run(
    s3_client: S3Client,
    bucket: &str,
    address: &SocketAddr,
) -> Result<(), Infallible> {
    let routes = esthri_filter(s3_client, bucket).recover(handle_rejection);
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
) -> bool {
    let obj_info = match head_object(s3, bucket, path).await {
        Ok(obj_info) => {
            if let Some(obj_info) = obj_info {
                obj_info
            } else {
                abort_with_error(
                    Some(archive),
                    error_tracker.clone(),
                    eyre!("object not found: {}", path),
                )
                .await;
                return !error_tracker.has_error();
            }
        }
        Err(err) => {
            abort_with_error(
                Some(archive),
                error_tracker.clone(),
                err.wrap_err("s3 head operation failed"),
            )
            .await;
            return !error_tracker.has_error();
        }
    };
    let stream = match download_streaming(s3, bucket, path).await {
        Ok(byte_stream) => byte_stream,
        Err(err) => {
            abort_with_error(
                Some(archive),
                error_tracker.clone(),
                err.wrap_err("s3 download failed"),
            )
            .await;
            return !error_tracker.has_error();
        }
    };
    let mut header = Header::new_gnu();
    header.set_mode(ARCHIVE_ENTRY_MODE);
    header.set_mtime(obj_info.last_modified.timestamp() as u64);
    header.set_size(obj_info.size as u64);
    let mut stream_reader = stream.into_async_read().compat();
    if let Err(err) = archive
        .append_data(&mut header, path, &mut stream_reader)
        .await
    {
        abort_with_error(
            Some(archive),
            error_tracker.clone(),
            Report::new(err).wrap_err("tar append failed"),
        )
        .await;
    }
    !error_tracker.has_error()
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
    prefixes: Vec<String>,
    error_tracker: ErrorTrackerArc,
) -> impl Stream<Item = io::Result<BytesMut>> {
    let (tar_pipe_reader, tar_pipe_writer) = pipe::pipe();
    let mut archive = Builder::new(tar_pipe_writer);
    let error_tracker_reader = error_tracker.clone();
    tokio::spawn(async move {
        for prefix in prefixes {
            let mut object_list_stream = list_objects_stream(&s3, &bucket, &prefix);
            let error_tracker = error_tracker.clone();
            loop {
                match object_list_stream.try_next().await {
                    Ok(None) => break,
                    Ok(Some(items)) => {
                        for s3obj in items {
                            let s3obj = s3obj.unwrap_object();
                            if !stream_object_to_archive(
                                &s3,
                                &bucket,
                                &s3obj.key,
                                &mut archive,
                                error_tracker.clone(),
                            )
                            .await
                            {
                                break;
                            }
                        }
                        if error_tracker.has_error() {
                            break;
                        }
                    }
                    Err(err) => {
                        let err = err.wrap_err("listing objects");
                        abort_with_error(Some(&mut archive), error_tracker.clone(), err).await;
                        break;
                    }
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

fn format_link(base: &str, path: &str, archive: bool) -> String {
    let path = path.strip_prefix(base).unwrap();
    if archive {
        format!(
            "<li><a href=\"{}\">{}</a> [<a href=\"{}?archive=true\">tgz</a>]</li>\n",
            path, path, path
        )
    } else {
        format!("<li><a href=\"{}\">{}</a></li>\n", path, path)
    }
}

async fn create_index_stream(
    s3: S3Client,
    bucket: String,
    path: String,
) -> impl Stream<Item = io::Result<Bytes>> {
    stream! {
        yield Ok(Bytes::from(format!("<html><h3>{}</h3>", path)));
        yield Ok(Bytes::from("<ul><li><a href=\".\">.</a> [<a href=\"?archive=true\">tgz</a>]</li>"));
        yield Ok(Bytes::from("<li><a href=\"..\">..</a> [<a href=\"..?archive=true\">tgz</a>]</li>\n"));
        let mut directory_list_stream = list_directory_stream(&s3, &bucket, &path);
        loop {
            match directory_list_stream.try_next().await {
                Ok(None) => break,
                Ok(Some(items)) => {
                    for s3obj in items {
                        match s3obj {
                            S3ListingItem::S3Object(o) => yield Ok(Bytes::from(format_link(&path, &o.key, false))),
                            S3ListingItem::S3CommonPrefix(cp) => yield Ok(Bytes::from(format_link(&path, &cp, true))),
                        }
                    }
                }
                Err(err) => {
                    yield Err(into_io_error(&err));
                }
            }
        }
        yield Ok(Bytes::from("</ul></html>\n"));
    }
}

async fn create_item_stream(
    s3: S3Client,
    bucket: String,
    path: String,
) -> impl Stream<Item = io::Result<Bytes>> {
    stream! {
    let mut stream = match download_streaming(&s3, &bucket, &path).await {
        Ok(byte_stream) => byte_stream,
        Err(err) => {
            yield Err(into_io_error(&err));
            return;
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

async fn item_pre_response<'a, T: S3 + Send>(
    s3: &T,
    bucket: String,
    path: String,
    if_none_match: Option<String>,
    mut resp_builder: response::Builder,
) -> Result<(response::Builder, Option<(String, String)>), warp::Rejection> {
    let obj_info = match head_object(s3, &bucket, &path).await {
        Ok(obj_info) => {
            if let Some(obj_info) = obj_info {
                obj_info
            } else {
                return Err(warp::reject::not_found());
            }
        }
        Err(err) => {
            let message = format!("error listing item: {}", err);
            return EsthriRejection::warp_result(message);
        }
    };
    let not_modified = if_none_match
        .map(|etag| etag == obj_info.e_tag)
        .unwrap_or(false);
    if not_modified {
        use warp::http::status::StatusCode;
        resp_builder = resp_builder.status(StatusCode::NOT_MODIFIED);
        Ok((resp_builder, None))
    } else {
        resp_builder = resp_builder.header(CONTENT_LENGTH, obj_info.size);
        resp_builder = resp_builder.header(ETAG, obj_info.e_tag);
        resp_builder = resp_builder.header(CACHE_CONTROL, "private,max-age=0");
        resp_builder = resp_builder.header(
            LAST_MODIFIED,
            obj_info
                .last_modified
                .format(LAST_MODIFIED_TIME_FMT)
                .to_string(),
        );
        let mime = mime_guess::from_path(&path);
        let mime = mime.first();
        if let Some(mime) = mime {
            resp_builder = resp_builder.header(CONTENT_TYPE, mime.essence_str());
        } else {
            resp_builder =
                resp_builder.header(CONTENT_TYPE, APPLICATION_OCTET_STREAM.essence_str());
        }
        Ok((resp_builder, Some((bucket, path))))
    }
}

/// An API error serializable to JSON.
async fn handle_rejection(err: warp::Rejection) -> Result<impl warp::Reply, Infallible> {
    use warp::http::StatusCode;
    let code;
    let message;
    if err.is_not_found() {
        code = StatusCode::NOT_FOUND;
        message = "not found".to_owned();
    } else if let Some(EsthriRejection {
        message: message_inner,
    }) = err.find()
    {
        code = StatusCode::BAD_REQUEST;
        message = message_inner.to_owned();
    } else if err
        .find::<warp::filters::body::BodyDeserializeError>()
        .is_some()
    {
        message = "deserialization error".to_owned();
        code = StatusCode::BAD_REQUEST;
    } else if err.find::<warp::reject::MethodNotAllowed>().is_some() {
        code = StatusCode::METHOD_NOT_ALLOWED;
        message = "not allowed".to_owned();
    } else if err.find::<warp::reject::InvalidQuery>().is_some() {
        code = StatusCode::METHOD_NOT_ALLOWED;
        message = "invalid query string".to_owned();
    } else {
        code = StatusCode::INTERNAL_SERVER_ERROR;
        message = format!("internal error: {:?}", err);
    }
    let json = warp::reply::json(&ErrorMessage {
        code: code.as_u16(),
        message,
    });
    Ok(warp::reply::with_status(json, code))
}

fn sanitize_filename(filename: String) -> String {
    let options = sanitize_filename::Options {
        windows: true,
        replacement: "_",
        ..Default::default()
    };
    sanitize_filename::sanitize_with_options(
        filename.strip_suffix("/").unwrap_or(&filename),
        options,
    )
}

async fn download(
    path: warp::path::FullPath,
    s3: S3Client,
    bucket: String,
    params: Params,
    if_none_match: Option<String>,
) -> Result<http::Response<Body>, warp::Rejection> {
    let path = path
        .as_str()
        .to_owned()
        .get(1..)
        .map(Into::<String>::into)
        .unwrap_or_default();
    let error_tracker = ErrorTrackerArc::new();
    let resp_builder = Response::builder();
    debug!(
        "path: {}, params: archive: {:?}, prefixes: {:?}",
        path, params.archive, params.prefixes
    );
    let (body, resp_builder) = if params.archive.unwrap_or(false) {
        let (archive_filename, prefixes) = if !path.is_empty() && params.prefixes.is_none() {
            let archive_filename = sanitize_filename(path.clone());
            (format!("{}.tgz", archive_filename), Some(vec![path]))
        } else if let Some(prefixes) = params.prefixes {
            if path.is_empty() {
                let archive_filename = params.archive_name.unwrap_or_else(|| "archive.tgz".into());
                (sanitize_filename(archive_filename), Some(prefixes.prefixes))
            } else {
                return Err(EsthriRejection::warp_rejection(
                    "path must be empty with prefixes",
                ));
            }
        } else {
            return Err(EsthriRejection::warp_rejection(
                "path and prefixes were empty",
            ));
        };
        if let Some(prefixes) = prefixes {
            let stream = create_archive_stream(s3.clone(), bucket, prefixes, error_tracker).await;
            (
                Some(Body::wrap_stream(stream)),
                resp_builder
                    .header(CONTENT_TYPE, APPLICATION_OCTET_STREAM.essence_str())
                    .header(
                        CONTENT_DISPOSITION,
                        format!("attachment; filename=\"{}\"", archive_filename),
                    ),
            )
        } else {
            (None, resp_builder)
        }
    } else if path.ends_with('/') || path.is_empty() {
        let stream = create_index_stream(s3.clone(), bucket, path).await;
        (
            Some(Body::wrap_stream(stream)),
            resp_builder.header(CONTENT_TYPE, TEXT_HTML_UTF_8.essence_str()),
        )
    } else {
        let (resp_builder, create_stream) =
            item_pre_response(&s3, bucket, path, if_none_match, resp_builder).await?;
        if let Some((bucket, path)) = create_stream {
            let stream = create_item_stream(s3.clone(), bucket, path).await;
            (Some(Body::wrap_stream(stream)), resp_builder)
        } else {
            (None, resp_builder)
        }
    };

    resp_builder
        .body(body.unwrap_or_else(Body::empty))
        .map_err(|err| EsthriRejection::warp_rejection(format!("{}", err)))
}
