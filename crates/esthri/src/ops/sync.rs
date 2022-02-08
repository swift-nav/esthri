/*
 * Copyright (C) 2021 Swift Navigation Inc.
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
    borrow::Cow,
    path::{Path, PathBuf},
};

use futures::{Future, Stream, StreamExt, TryStreamExt};
use glob::Pattern;
use log::{debug, info, warn};
use log_derive::logfn;
use tokio::{
    fs::{self, File},
    io::BufReader,
};
use walkdir::WalkDir;

use crate::{
    compression::compress_to_tempfile,
    compute_etag,
    config::Config,
    errors::{Error, Result},
    handle_dispatch_error, list_objects_stream,
    rusoto::*,
    tempfile::TEMP_FILE_PREFIX,
    types::ListingMetadata,
    types::{S3ListingItem, S3PathParam},
};

struct MappedPathResult {
    file_path: Box<dyn AsRef<Path>>,
    source_path: PathBuf,
    local_etag: Result<Option<String>>,
    metadata: Option<ListingMetadata>,
}

type MapPathResult = Result<MappedPathResult>;

#[derive(Debug, Clone)]
pub enum GlobFilter {
    Include(Pattern),
    Exclude(Pattern),
}

/// Syncs between S3 prefixes and local directories
///
/// # Arguments
///
/// * `s3` - S3 client
/// * `source` - S3 prefix or local directory to sync from
/// * `destination` - S3 prefix or local directory to sync to
/// * `glob_filter` - An (optional) slice of filters that specify whether files
///                   should be included or not. These are processed in order,
///                   with the first matching filter determining whether the
///                   file will be included or excluded. If not supplied, then
///                   all files will be synced.
#[logfn(err = "ERROR")]
pub async fn sync<T>(
    s3: &T,
    source: S3PathParam,
    destination: S3PathParam,
    glob_filters: Option<&[GlobFilter]>,
    compressed: bool,
) -> Result<()>
where
    T: S3 + Sync + Send + Clone,
{
    let filters: Vec<GlobFilter> = match glob_filters {
        Some(filters) => {
            let mut filters = filters.to_vec();
            if !filters.iter().any(|x| matches!(x, GlobFilter::Include(_))) {
                filters.push(GlobFilter::Include(Pattern::new("*")?));
            }
            filters
        }
        None => vec![GlobFilter::Include(Pattern::new("*")?)],
    };

    match (source, destination) {
        (S3PathParam::Local { path }, S3PathParam::Bucket { bucket, key }) => {
            info!(
                "sync-up, local directory: {}, bucket: {}, key: {}",
                path.display(),
                bucket,
                key
            );

            sync_local_to_remote(s3, &bucket, &key, &path, &filters, compressed).await?;
        }
        (S3PathParam::Bucket { bucket, key }, S3PathParam::Local { path }) => {
            info!(
                "sync-down, local directory: {}, bucket: {}, key: {}",
                path.display(),
                bucket,
                key
            );

            sync_remote_to_local(s3, &bucket, &key, &path, &filters, compressed).await?;
        }
        (
            S3PathParam::Bucket {
                bucket: source_bucket,
                key: source_key,
            },
            S3PathParam::Bucket {
                bucket: destination_bucket,
                key: destination_key,
            },
        ) => {
            info!(
                "sync-across, bucket: {}, source_key: {}, bucket: {}, destination_key: {}",
                source_bucket, source_key, destination_bucket, destination_key
            );

            sync_across(
                s3,
                &source_bucket,
                &source_key,
                &destination_bucket,
                &destination_key,
                &filters,
            )
            .await?;
        }
        _ => {
            warn!("Local to Local copy not implemented");
        }
    }

    Ok(())
}

fn process_globs<'a, P: AsRef<Path> + 'a>(path: P, filters: &[GlobFilter]) -> Option<P> {
    let mut excluded = false;
    let mut included = false;
    {
        let path = path.as_ref();
        for pattern in filters {
            match pattern {
                GlobFilter::Include(filter) => {
                    if filter.matches(path.to_string_lossy().as_ref()) {
                        included = true;
                        break;
                    }
                }
                GlobFilter::Exclude(filter) => {
                    if filter.matches(path.to_string_lossy().as_ref()) {
                        excluded = true;
                        break;
                    }
                }
            }
        }
    }
    if included && !excluded {
        Some(path)
    } else {
        None
    }
}

fn create_dirent_stream<'a>(
    directory: &'a Path,
    filters: &'a [GlobFilter],
) -> impl Stream<Item = Result<(String, Option<ListingMetadata>)>> + 'a {
    async_stream::stream! {
        for entry in WalkDir::new(directory) {
            let entry = if let Ok(entry) = entry {
                entry
            } else {
                yield Err(entry.err().unwrap().into());
                return;
            };
            if entry.file_name().to_string_lossy().contains(TEMP_FILE_PREFIX) {
                continue;
            }
            let metadata = entry.metadata();
            let stat = if let Ok(stat) = metadata {
                stat
            } else {
                yield Err(metadata.err().unwrap().into());
                return;
            };
            if stat.is_dir() {
                continue;
            }
            if entry.path_is_symlink() {
                warn!("symlinks are ignored");
                continue;
            }
            let path = entry.path();
            debug!("local path={}", path.display());
            if process_globs(&path, filters).is_some() {
                yield Ok((path.to_string_lossy().into(), ListingMetadata::none()));
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum SyncCmd {
    Up,
    UpCompressed,
    Down,

    DownCompressed,
}

fn translate_paths<StreamT>(
    input_stream: StreamT,
    sync_cmd: SyncCmd,
) -> impl Stream<Item = impl Future<Output = MapPathResult>>
where
    StreamT: Stream<Item = Result<(String, Option<ListingMetadata>)>>,
{
    use std::str::FromStr;
    input_stream.map(move |params| async move {
        let (path, metadata) = params?;
        let source_path = PathBuf::from_str(&path)?;
        let file_path: Box<dyn AsRef<Path>> = Box::new(path);

        let (file_path, source_path, local_etag) = match sync_cmd {
            SyncCmd::Up => {
                let local_etag = if metadata.is_some() {
                    compute_etag(&source_path).await.map(Option::Some)
                } else {
                    Ok(None)
                };
                (file_path, source_path, local_etag)
            }
            SyncCmd::UpCompressed => {
                // If we're syncing up with compression, then everything
                // should be compressed
                let (tmp, _) = compress_to_tempfile(&source_path).await?;
                let local_etag = if metadata.is_some() {
                    compute_etag(&source_path).await.map(Option::Some)
                } else {
                    Ok(None)
                };
                (tmp.into_path(), source_path, local_etag)
            }
            SyncCmd::Down => {
                let local_etag = compute_etag(&source_path).await.map(Option::Some);
                (file_path, source_path, local_etag)
            }
            SyncCmd::DownCompressed => {
                // if we're syncing down with compression, then there
                // could be both esthri compressed files and non
                // compressed files within the prefix. We should only
                // try to decompress the esthri compressed files
                if metadata
                    .as_ref()
                    .expect("Should have metadata")
                    .esthri_compressed
                {
                    if !source_path.exists() {
                        (file_path, source_path, Err(Error::ETagNotPresent))
                    } else {
                        // Check if we already have a copy locally
                        // of the uncompressed file by recompressing
                        // the local file to see if it matches with
                        // the compressed version
                        let (tmp, _) = compress_to_tempfile(&source_path).await?;
                        let local_etag = compute_etag(tmp.path()).await.map(Option::Some);
                        (file_path, source_path, local_etag)
                    }
                } else {
                    let local_etag = compute_etag(&source_path).await.map(Option::Some);
                    (file_path, source_path, local_etag)
                }
            }
        };

        Ok(MappedPathResult {
            file_path,
            source_path,
            local_etag,
            metadata,
        })
    })
}

fn local_to_remote_sync_tasks<ClientT, StreamT>(
    s3: ClientT,
    bucket: String,
    key: String,
    directory: PathBuf,
    dirent_stream: StreamT,
    transparent_compression: bool,
) -> impl Stream<Item = impl Future<Output = Result<()>>>
where
    ClientT: S3 + Send + Clone,
    StreamT: Stream<Item = MapPathResult>,
{
    use super::upload::upload_from_reader;
    dirent_stream
        .map(move |entry| {
            (
                s3.clone(),
                bucket.clone(),
                key.clone(),
                directory.clone(),
                entry.unwrap(),
            )
        })
        .map(move |clones| async move {
            let (s3, bucket, key, directory, entry) = clones;
            let MappedPathResult {
                file_path: filepath,
                source_path,
                local_etag,
                metadata: object_info,
            } = entry;
            let path = Path::new(&source_path);
            let remote_path = Path::new(&key);
            let stripped_path = path.strip_prefix(&directory);
            let stripped_path = match stripped_path {
                Err(e) => {
                    warn!(
                        "unexpected: failed to strip prefix: {}, {:?}, {:?}",
                        e, &path, &directory
                    );
                    return Ok(());
                }
                Ok(result) => result,
            };
            let remote_path = remote_path.join(&stripped_path);
            let remote_path = remote_path.to_string_lossy();
            debug!("checking remote: {}", remote_path);
            let local_etag = local_etag?;
            let metadata = if transparent_compression {
                Some(crate::compression::compressed_file_metadata())
            } else {
                None
            };

            if let Some(object_info) = object_info {
                let remote_etag = object_info.e_tag;
                let local_etag = local_etag.unwrap();
                if remote_etag != local_etag {
                    info!(
                        "etag mis-match: {}, remote_etag={}, local_etag={}",
                        remote_path, remote_etag, local_etag
                    );
                    let f = File::open(&*filepath).await?;
                    let reader = BufReader::new(f);
                    let size = fs::metadata(&*filepath).await?.len();
                    upload_from_reader(&s3, bucket, remote_path, reader, size, metadata).await?;
                } else {
                    debug!(
                        "etags matched: {}, remote_etag={}, local_etag={}",
                        remote_path, remote_etag, local_etag
                    );
                }
            } else {
                info!("file did not exist remotely: {}", remote_path);
                let f = File::open(&*filepath).await?;
                let reader = BufReader::new(f);
                let size = fs::metadata(&*filepath).await?.len();
                upload_from_reader(&s3, bucket, remote_path, reader, size, metadata).await?;
            }
            Ok(())
        })
}

async fn sync_local_to_remote<T>(
    s3: &T,
    bucket: &str,
    key: &str,
    directory: impl AsRef<Path>,
    filters: &[GlobFilter],
    compressed: bool,
) -> Result<()>
where
    T: S3 + Send + Clone,
{
    let directory = directory.as_ref();
    let task_count = Config::global().concurrent_sync_tasks();

    let dirent_stream = create_dirent_stream(directory, filters).and_then(|(filename, _)| async {
        let path = Path::new(&filename);
        let remote_path = Path::new(&key);
        let stripped_path = match path.strip_prefix(&directory) {
            Ok(result) => result,
            Err(e) => {
                unreachable!(
                    "unexpected: failed to strip prefix: {}, {:?}, {:?}",
                    e, &path, &directory
                );
            }
        };
        let remote_path = remote_path.join(&stripped_path);
        let remote_path = remote_path.to_string_lossy();
        match head_object_request(s3, bucket, &remote_path, None).await? {
            Some(metadata) => {
                let esthri_compressed = metadata.is_esthri_compressed();
                Ok((
                    filename,
                    ListingMetadata::some(key.to_owned(), metadata.e_tag, esthri_compressed),
                ))
            }
            None => Ok((filename, ListingMetadata::none())),
        }
    });

    let cmd = if compressed {
        SyncCmd::UpCompressed
    } else {
        SyncCmd::Up
    };

    let etag_stream = translate_paths(dirent_stream, cmd).buffer_unordered(task_count);
    let sync_tasks = local_to_remote_sync_tasks(
        s3.clone(),
        bucket.into(),
        key.into(),
        directory.into(),
        etag_stream,
        compressed,
    );

    sync_tasks
        .buffer_unordered(task_count)
        .try_collect()
        .await?;

    Ok(())
}

#[logfn(err = "ERROR")]
async fn copy_object_request<T>(
    s3: &T,
    source_bucket: &str,
    source_key: &str,
    file_name: &str,
    dest_bucket: &str,
    dest_key: &str,
) -> Result<CopyObjectOutput>
where
    T: S3 + Send,
{
    let res = handle_dispatch_error(|| async {
        let cor = CopyObjectRequest {
            bucket: dest_bucket.to_string(),
            copy_source: format!("{}/{}", source_bucket, file_name),
            key: file_name.replace(source_key, dest_key),
            ..Default::default()
        };

        s3.copy_object(cor).await
    })
    .await;

    Ok(res?)
}

async fn sync_across<T>(
    s3: &T,
    source_bucket: &str,
    source_prefix: &str,
    dest_bucket: &str,
    destination_key: &str,
    filters: &[GlobFilter],
) -> Result<()>
where
    T: S3 + Send,
{
    let mut stream = list_objects_stream(s3, source_bucket, source_prefix);

    while let Some(from_entries) = stream.try_next().await? {
        for entry in from_entries {
            if let S3ListingItem::S3Object(src_object) = entry {
                let path = process_globs(&src_object.key, filters);

                if let Some(_accept) = path {
                    let mut should_copy_file: bool = true;
                    let new_file = src_object.key.replace(source_prefix, destination_key);
                    let dest_object_info =
                        head_object_request(s3, dest_bucket, &new_file, None).await?;

                    if let Some(dest_object) = dest_object_info {
                        if dest_object.e_tag == src_object.e_tag {
                            should_copy_file = false;
                        }
                    }

                    if should_copy_file {
                        copy_object_request(
                            s3,
                            source_bucket,
                            source_prefix,
                            &src_object.key,
                            dest_bucket,
                            destination_key,
                        )
                        .await?;
                    }
                }
            }
        }
    }

    Ok(())
}

fn flattened_object_listing<'a, ClientT>(
    s3: &'a ClientT,
    bucket: &'a str,
    key: &'a str,
    directory: &'a Path,
    filters: &'a [GlobFilter],
    transparent_decompress: bool,
) -> impl Stream<Item = Result<(String, Option<ListingMetadata>)>> + 'a
where
    ClientT: S3 + Send + Clone,
{
    let prefix = if key.ends_with('/') {
        Cow::Borrowed(key)
    } else {
        Cow::Owned(format!("{}/", key))
    };

    async_stream::stream! {
        let mut stream = list_objects_stream(s3, bucket, prefix);
        loop {
            let entries = match stream.try_next().await {
                Ok(Some(entries)) => entries,
                Ok(None) => break,
                Err(err) => {
                    yield Err(err);
                    return;
                }
            };
            let mut entries = futures::stream::iter(entries)
                .map(|entry| async move {
                    let entry = entry.unwrap_object();
                    debug!("key={}", entry.key);
                    let compressed = if transparent_decompress {
                        head_object_request(s3, bucket, &entry.key, None)
                            .await?
                            .expect("No head info?")
                            .is_esthri_compressed()
                    } else {
                        false
                    };
                    Ok((entry, compressed))
                })
                .buffered(Config::global().concurrent_sync_tasks());
            loop {
                let (entry, compressed) = match entries.try_next().await {
                    Ok(Some(entry)) => entry,
                    Ok(None) => break,
                    Err(err) => {
                        yield Err(err);
                        return;
                    }
                };
                let path_result = Path::new(&entry.key).strip_prefix(key);
                if let Ok(s3_suffix) = path_result {
                    if process_globs(&s3_suffix, filters).is_some() {
                        let local_path: String = directory.join(&s3_suffix).to_string_lossy().into();
                        let s3_suffix = s3_suffix.to_string_lossy().into();
                        yield Ok((
                            local_path,
                            ListingMetadata::some(s3_suffix, entry.e_tag, compressed),
                        ));
                    }
                } else {
                    yield Err(path_result.err().unwrap().into());
                    return;
                }
            }
        }
    }
}

fn remote_to_local_sync_tasks<ClientT, StreamT>(
    s3: ClientT,
    bucket: String,
    key: String,
    directory: PathBuf,
    input_stream: StreamT,
    transparent_decompress: bool,
) -> impl Stream<Item = impl Future<Output = Result<()>>>
where
    ClientT: S3 + Sync + Send + Clone,
    StreamT: Stream<Item = MapPathResult>,
{
    input_stream
        .map(move |entry| {
            (
                s3.clone(),
                bucket.clone(),
                key.clone(),
                directory.clone(),
                transparent_decompress,
                entry.unwrap(),
            )
        })
        .map(
            |(s3, bucket, key, directory, decompress, entry)| async move {
                let MappedPathResult {
                    source_path,
                    local_etag,
                    metadata,
                    ..
                } = entry;
                let metadata = metadata.unwrap();
                match local_etag {
                    Ok(Some(local_etag)) => {
                        if local_etag != metadata.e_tag {
                            debug!(
                                "etag mismatch: {}, local etag={}, remote etag={}",
                                source_path.display(),
                                local_etag,
                                metadata.e_tag
                            );
                            download_with_dir(
                                &s3,
                                &bucket,
                                &key,
                                &metadata.s3_suffix,
                                &directory,
                                decompress,
                            )
                            .await?;
                        } else {
                            debug!("etag match: {}", source_path.display());
                        }
                    }
                    Err(err) => match err {
                        Error::ETagNotPresent => {
                            debug!("file did not exist locally: {}", source_path.display());
                            download_with_dir(
                                &s3,
                                &bucket,
                                &key,
                                &metadata.s3_suffix,
                                &directory,
                                decompress,
                            )
                            .await?;
                        }
                        _ => {
                            warn!("s3 etag error: {}", err);
                        }
                    },
                    Ok(None) => warn!("no local etag"),
                }
                Ok(())
            },
        )
}

async fn sync_remote_to_local<T>(
    s3: &T,
    bucket: &str,
    key: &str,
    directory: impl AsRef<Path>,
    filters: &[GlobFilter],
    transparent_decompress: bool,
) -> Result<()>
where
    T: S3 + Sync + Send + Clone,
{
    let directory = directory.as_ref();
    let task_count = Config::global().concurrent_sync_tasks();
    let object_listing =
        flattened_object_listing(s3, bucket, key, directory, filters, transparent_decompress);

    let cmd = if transparent_decompress {
        SyncCmd::DownCompressed
    } else {
        SyncCmd::Down
    };

    let etag_stream = translate_paths(object_listing, cmd).buffer_unordered(task_count);
    let sync_tasks = remote_to_local_sync_tasks(
        s3.clone(),
        bucket.into(),
        key.into(),
        directory.into(),
        etag_stream,
        transparent_decompress,
    );

    sync_tasks
        .buffer_unordered(task_count)
        .try_collect()
        .await?;

    Ok(())
}

async fn download_with_dir<T>(
    s3: &T,
    bucket: &str,
    s3_prefix: &str,
    s3_suffix: &str,
    local_dir: impl AsRef<Path>,
    transparent_decompress: bool,
) -> Result<()>
where
    T: S3 + Sync + Send + Clone,
{
    let local_dir = local_dir.as_ref();
    let dest_path = local_dir.join(s3_suffix);

    let parent_dir = dest_path.parent().ok_or(Error::ParentDirNone)?;
    fs::create_dir_all(parent_dir).await?;

    let key = format!("{}", Path::new(s3_prefix).join(s3_suffix).display());
    let dest_path = format!("{}", dest_path.display());

    if transparent_decompress {
        crate::download_with_transparent_decompression(s3, bucket, &key, &dest_path).await?;
    } else {
        crate::download(s3, bucket, &key, &dest_path).await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_process_globs() {
        let filters = vec![
            GlobFilter::Exclude(Pattern::new("*-blah.csv").unwrap()),
            GlobFilter::Include(Pattern::new("*.csv").unwrap()),
        ];

        assert!(process_globs("data.sbp", &filters[..]).is_none());
        assert!(process_globs("yes.csv", &filters[..]).is_some());
        assert!(process_globs("no-blah.csv", &filters[..]).is_none());
    }

    #[test]
    fn test_process_globs_exclude_all() {
        let filters = vec![GlobFilter::Include(Pattern::new("*.png").unwrap())];

        assert!(process_globs("a-fancy-thing.png", &filters[..]).is_some());
        assert!(process_globs("horse.gif", &filters[..]).is_none());
    }

    #[test]
    fn test_process_globs_explicit_exclude() {
        let filters = vec![
            GlobFilter::Include(Pattern::new("*.png").unwrap()),
            GlobFilter::Exclude(Pattern::new("*.txt").unwrap()),
            GlobFilter::Include(Pattern::new("*").unwrap()),
        ];

        assert!(process_globs("a-fancy-thing.png", &filters[..]).is_some());
        assert!(process_globs("horse.gif", &filters[..]).is_some());
        assert!(process_globs("myfile.txt", &filters[..]).is_none());
    }

    #[test]
    fn test_process_globs_precedence() {
        let filters = vec![
            GlobFilter::Exclude(Pattern::new("*").unwrap()),
            GlobFilter::Include(Pattern::new("*").unwrap()),
        ];

        assert!(process_globs("a-fancy-thing.png", &filters[..]).is_none());
        assert!(process_globs("horse.gif", &filters[..]).is_none());
        assert!(process_globs("myfile.txt", &filters[..]).is_none());
    }
}
