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

use std::{
    cell::Cell,
    convert::Infallible,
    io::{self, ErrorKind},
    net::SocketAddr,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

use anyhow::anyhow;

use async_stream::stream;

use async_zip::{
    write::{EntryOptions, ZipFileWriter},
    Compression,
};
use bytes::{Bytes, BytesMut};
use futures::stream::{Stream, StreamExt, TryStreamExt};
use hyper::header::CONTENT_ENCODING;
use log::*;
use maud::{html, Markup, DOCTYPE};
use mime_guess::mime::{APPLICATION_OCTET_STREAM, TEXT_HTML_UTF_8};
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};

use tokio::{
    io::{AsyncWrite, DuplexStream},
    sync::oneshot,
};
use tokio_util::codec::{BytesCodec, FramedRead};
use warp::{
    http::{
        self,
        header::{
            CACHE_CONTROL, CONTENT_DISPOSITION, CONTENT_LENGTH, CONTENT_TYPE, ETAG, LAST_MODIFIED,
        },
        response, Response,
    },
    hyper::Body,
    Filter,
};

use esthri::download_streaming;
use esthri::head_object;
use esthri::list_directory_stream;
use esthri::list_objects_stream;
use esthri::rusoto::*;
use esthri::HeadObjectInfo;
use esthri::S3ListingItem;

const LAST_MODIFIED_TIME_FMT: &str = "%a, %d %b %Y %H:%M:%S GMT";
const MAX_BUF_SIZE: usize = 1024 * 1024;

#[derive(Deserialize)]
struct DownloadParams {
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
    the_error: Mutex<Option<Box<anyhow::Error>>>,
}

impl ErrorTracker {
    fn new() -> Self {
        ErrorTracker {
            has_error: AtomicBool::new(false),
            the_error: Mutex::new(None),
        }
    }
    fn record_error(error_tracker: ErrorTrackerArc, err: anyhow::Error) {
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

impl From<ErrorTrackerArc> for Option<io::Error> {
    fn from(s: ErrorTrackerArc) -> Option<io::Error> {
        let the_error = s.0.the_error.lock().unwrap();
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

type SenderT = oneshot::Sender<bool>;
type MaybeSenderT = Option<SenderT>;
type SharedSenderT = Mutex<Cell<MaybeSenderT>>;

static SHUTDOWN_TX: OnceCell<SharedSenderT> = OnceCell::new();

fn setup_termination_handler() {
    tokio::spawn(async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for ctrl-c");
        let _: Result<_, _> = (|| {
            SHUTDOWN_TX
                .get()
                .ok_or_else(|| {
                    error!("shutdown signaler not initialized");
                })?
                .lock()
                .map_err(|_| {
                    error!("failed to lock shutdown signaler");
                })?
                .take()
                .ok_or_else(|| {
                    error!("termination handler already triggered");
                })
                .map(|tx| tx.send(true))
                .map_err(|_| {
                    error!("error triggering termination handler");
                })
        })();
    });
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
        .and(warp::query::<DownloadParams>())
        .and(warp::header::optional::<String>("if-none-match"))
        .and_then(download)
}

pub async fn run(
    s3_client: S3Client,
    bucket: &str,
    address: &SocketAddr,
) -> Result<(), Infallible> {
    setup_termination_handler();

    let (tx, rx) = oneshot::channel();

    SHUTDOWN_TX
        .set(Mutex::new(Cell::new(Some(tx))))
        .ok()
        .expect("failed to set termination signaler");

    let still_alive = || "still alive";
    let health_check = warp::path(".esthri_health_check").map(still_alive);

    let routes = health_check
        .or(esthri_filter(s3_client, bucket))
        .recover(handle_rejection);

    let (addr, server) = warp::serve(routes).bind_with_graceful_shutdown(*address, async {
        rx.await.ok();
        debug!("got shutdown signal, waiting for all open connections to complete...");
    });

    info!("listening on: http://{}...", addr);
    let _ = tokio::task::spawn(server).await;

    info!("shutting down...");

    Ok(())
}

async fn abort_with_error(error_tracker: ErrorTrackerArc, err: anyhow::Error) {
    ErrorTracker::record_error(error_tracker, err);
}

async fn stream_object_to_archive<T: S3 + Send + Clone + Sync>(
    s3: &T,
    bucket: &str,
    path: &str,
    archive: &mut ZipFileWriter<DuplexStreamWrapper>,
    error_tracker: ErrorTrackerArc,
) -> bool {
    let stream = match download_streaming(s3, bucket, path, true).await {
        Ok(byte_stream) => (byte_stream).map_err(into_io_error),
        Err(err) => {
            abort_with_error(
                error_tracker.clone(),
                anyhow!(err).context("s3 download failed"),
            )
            .await;
            return !error_tracker.has_error();
        }
    };

    let stream_reader = stream.into_async_read();
    let mut stream_reader = tokio_util::compat::FuturesAsyncReadCompatExt::compat(stream_reader);

    let options = EntryOptions::new(path.to_string(), Compression::Deflate);

    match archive.write_entry_stream(options).await {
        Ok(mut writer) => {
            match tokio::io::copy(&mut stream_reader, &mut writer).await {
                Ok(_) => {}
                Err(err) => {
                    abort_with_error(
                        error_tracker.clone(),
                        anyhow!(err).context("failed to write to archive"),
                    )
                    .await;
                }
            }
            match writer.close().await {
                Ok(_) => {}
                Err(err) => {
                    abort_with_error(
                        error_tracker.clone(),
                        anyhow!(err).context("failed to close archive"),
                    )
                    .await;
                }
            }
        }
        Err(err) => {
            abort_with_error(
                error_tracker.clone(),
                anyhow!(err).context("tar append failed"),
            )
            .await;
        }
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
                return;
            }
        }
    }
}

// Work around for https://github.com/Majored/rs-async-zip/issues/11 By default,
// the writer calls shutdown on the Writer to finalize the compressed file. For
// the duplex pipe, this causes the writer to no longer work so we work around
// it by calling flush on shutdown instead.
struct DuplexStreamWrapper(DuplexStream);
impl AsyncWrite for DuplexStreamWrapper {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, io::Error>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), io::Error>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), io::Error>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }
}

async fn create_archive_stream(
    s3: S3Client,
    bucket: String,
    prefixes: Vec<String>,
    error_tracker: ErrorTrackerArc,
) -> impl Stream<Item = io::Result<BytesMut>> {
    let (zip_reader, zip_writer) = tokio::io::duplex(MAX_BUF_SIZE);
    let mut writer = ZipFileWriter::new(DuplexStreamWrapper(zip_writer));

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
                                &mut writer,
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
                        let err = anyhow!(err).context("listing objects");
                        abort_with_error(error_tracker.clone(), err).await;
                        break;
                    }
                }
            }
        }
        match writer.close().await {
            Ok(_) => {}
            Err(err) => {
                let err = anyhow!(err).context("closing zip writer");
                abort_with_error(error_tracker.clone(), err).await;
            }
        }
    });
    let framed_reader = FramedRead::new(zip_reader, BytesCodec::new());
    create_error_monitor_stream(error_tracker_reader, framed_reader).await
}

fn into_io_error<E: std::error::Error>(err: E) -> io::Error {
    io::Error::new(ErrorKind::Other, format!("{}", err))
}

fn get_stripped_path<'a>(base: &str, path: &'a str) -> &'a str {
    path.strip_prefix(base).unwrap()
}

fn format_archive_download_button(path: &str, tooltip: &str) -> Markup {
    html! {
        a class="btn btn-primary btn-sm" role="button" href=(path)
            title=(tooltip) {
            span class="fa fa-file-archive fa-lg" { }
        }
    }
}

fn format_prefix_link(prefix: &str) -> Markup {
    html! {
        div {
            i class="far fa-folder fa-fw pe-4" { }
            a href=(prefix) {
                (prefix)
            }
        }
        (format_archive_download_button(&format!("{}?archive=true", prefix), &format!("Download tgz archive of {}", prefix)))
    }
}

#[allow(clippy::branches_sharing_code)]
fn format_object_link(path: &str) -> Markup {
    html! {
        div {
            i class="far fa-file fa-fw pe-4" { }
                a href=(path) {
                    (path)
                }
        }
    }
}

fn format_bucket_item(path: &str, archive: bool) -> Markup {
    html! {
        @if archive {
            (format_prefix_link(path))
        } @else {
            (format_object_link(path))
        }
    }
}

fn format_title(bucket: &str, path: &str) -> Markup {
    let path_components: Vec<&str> = path
        .strip_suffix('/')
        .unwrap_or(path)
        .split_terminator('/')
        .collect();

    let path_length = path_components.len();

    html! {
        h5 {
            a href=("../".repeat(path_length)) {
                (bucket)
            }

            @for (i, component) in path_components.iter().enumerate() {
                " > "
                @if i == path_length - 1 {
                    a href="." class="pe-1" {
                        (component)
                    }

                    (format_archive_download_button(".?archive=true",&format!("Download tgz archive of {}", component)))
                } @else {
                    a href=("../".repeat(path_length - 1 - i)) {
                        (component)
                    }
                }
            }
        }
    }
}

async fn create_listing_page(s3: S3Client, bucket: String, path: String) -> io::Result<Bytes> {
    let mut directory_list_stream = list_directory_stream(&s3, &bucket, &path);
    let mut elements = vec![];

    loop {
        match directory_list_stream.try_next().await {
            Ok(None) => break,
            Ok(Some(items)) => {
                for s3obj in items {
                    match s3obj {
                        S3ListingItem::S3Object(o) => elements
                            .push(format_bucket_item(get_stripped_path(&path, &o.key), false)),
                        S3ListingItem::S3CommonPrefix(cp) => {
                            elements.push(format_bucket_item(get_stripped_path(&path, &cp), true))
                        }
                    }
                }
            }
            Err(err) => {
                return Err(into_io_error(err));
            }
        }
    }

    let document = html! {
        (DOCTYPE)
        meta charset="utf-8";
        title {
            "Esthri " (bucket) " - " (path)
        }
        link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/bootstrap/5.1.1/css/bootstrap.min.css";
        link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.4/css/all.min.css";

        div."container p-2" {
            (format_title(&bucket, &path))

            ul class="list-group" {
                @for element in elements {
                    li class="list-group-item d-flex justify-content-between align-items-center" {
                        (element)
                    }
                }
            }
        }
    };

    Ok(Bytes::from(document.into_string()))
}

async fn create_item_stream(
    s3: S3Client,
    bucket: String,
    path: String,
) -> impl Stream<Item = esthri::Result<Bytes>> {
    stream! {
        // We don't decompress the file here as it will be served with the
        // correct content-encoding and be decompressed by the browser.
        let mut stream = match download_streaming(&s3, &bucket, &path, false).await {
            Ok(byte_stream) => byte_stream,
            Err(err) => {
                yield Err(err);
                return;
            }
        };
        loop {
            if let Some(data) = stream.next().await {
                yield data;
            } else {
                return;
            }
        }
    }
}

async fn get_obj_info<'a, T: S3 + Send>(
    s3: &T,
    bucket: &str,
    path: &str,
) -> Result<HeadObjectInfo, warp::Rejection> {
    match head_object(s3, &bucket, &path).await {
        Ok(obj_info) => {
            if let Some(obj_info) = obj_info {
                Ok(obj_info)
            } else {
                Err(warp::reject::not_found())
            }
        }
        Err(err) => {
            let message = format!("error listing item: {}", err);
            EsthriRejection::warp_result(message)
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
    let obj_info = get_obj_info(s3, &bucket, &path).await?;

    let not_modified = if_none_match
        .map(|etag| etag.strip_prefix("W/").map(String::from).unwrap_or(etag))
        .map(|etag| etag == obj_info.e_tag)
        .unwrap_or(false);
    if not_modified {
        use warp::http::status::StatusCode;
        resp_builder = resp_builder.status(StatusCode::NOT_MODIFIED);
        Ok((resp_builder, None))
    } else {
        resp_builder = resp_builder.header(CONTENT_LENGTH, obj_info.size);
        resp_builder = resp_builder.header(ETAG, &obj_info.e_tag);
        resp_builder = resp_builder.header(CACHE_CONTROL, "private,max-age=0");
        resp_builder = resp_builder.header(
            LAST_MODIFIED,
            obj_info
                .last_modified
                .format(LAST_MODIFIED_TIME_FMT)
                .to_string(),
        );

        if obj_info.is_esthri_compressed() {
            resp_builder = resp_builder.header(CONTENT_ENCODING, "gzip");
        }

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
        filename.strip_suffix('/').unwrap_or(&filename),
        options,
    )
}

async fn download(
    path: warp::path::FullPath,
    s3: S3Client,
    bucket: String,
    params: DownloadParams,
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
            (format!("{}.zip", archive_filename), Some(vec![path]))
        } else if let Some(prefixes) = params.prefixes {
            if path.is_empty() {
                let archive_filename = params.archive_name.unwrap_or_else(|| "archive.zip".into());
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
        let listing_page = create_listing_page(s3.clone(), bucket, path).await;
        let header = resp_builder.header(CONTENT_TYPE, TEXT_HTML_UTF_8.essence_str());

        match listing_page {
            Ok(page) => (Some(Body::from(page)), header),
            Err(e) => (Some(Body::from(e.to_string())), header),
        }
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
