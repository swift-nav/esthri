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

#![cfg_attr(feature = "aggressive_lint", deny(warnings))]

use std::path::{Path, PathBuf};

use futures::{Future, Stream, StreamExt, TryStreamExt};
use glob::Pattern;
use log::{debug, info, warn};
use log_derive::logfn;
use walkdir::WalkDir;

use crate::{
    compute_etag,
    config::Config,
    errors::{Error, Result},
    handle_dispatch_error, head_object_request, list_objects_stream,
    rusoto::*,
    types::ListingMetadata,
    types::{S3ListingItem, S3PathParam},
};

type MapEtagResult = Result<(PathBuf, Result<String>, Option<ListingMetadata>)>;

#[logfn(err = "ERROR")]
pub async fn sync<T>(
    s3: &T,
    source: S3PathParam,
    destination: S3PathParam,
    includes: Option<&[impl AsRef<str>]>,
    excludes: Option<&[impl AsRef<str>]>,
    #[cfg(feature = "compression")] compressed: bool,
) -> Result<()>
where
    T: S3 + Sync + Send + Clone,
{
    #[cfg(not(feature = "compression"))]
    let compressed = false;

    let mut glob_excludes: Vec<Pattern> = vec![];
    let mut glob_includes: Vec<Pattern> = vec![];

    if let Some(excludes) = excludes {
        for exclude in excludes {
            let exclude = exclude.as_ref();
            match Pattern::new(exclude) {
                Err(e) => {
                    return Err(Error::GlobPatternError(e));
                }
                Ok(p) => {
                    glob_excludes.push(p);
                }
            }
        }
    }

    if let Some(includes) = includes {
        for include in includes {
            let include = include.as_ref();
            match Pattern::new(include) {
                Err(e) => {
                    return Err(Error::GlobPatternError(e));
                }
                Ok(p) => {
                    glob_includes.push(p);
                }
            }
        }
    } else {
        glob_includes.push(Pattern::new("*")?);
    }

    match (source, destination) {
        (S3PathParam::Local { path }, S3PathParam::Bucket { bucket, key }) => {
            info!(
                "sync-up, local directory: {}, bucket: {}, key: {}",
                path.display(),
                bucket,
                key
            );

            sync_local_to_remote(
                s3,
                &bucket,
                &key,
                &path,
                &glob_includes,
                &glob_excludes,
                compressed,
            )
            .await?;
        }
        (S3PathParam::Bucket { bucket, key }, S3PathParam::Local { path }) => {
            info!(
                "sync-down, local directory: {}, bucket: {}, key: {}",
                path.display(),
                bucket,
                key
            );

            sync_remote_to_local(
                s3,
                &bucket,
                &key,
                &path,
                &glob_includes,
                &glob_excludes,
                compressed,
            )
            .await?;
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
                &glob_includes,
                &glob_excludes,
            )
            .await?;
        }
        _ => {
            warn!("Local to Local copy not implemented");
        }
    }

    Ok(())
}

fn process_globs<'a, P: AsRef<Path> + 'a>(
    path: P,
    glob_includes: &[Pattern],
    glob_excludes: &[Pattern],
) -> Option<P> {
    let mut excluded = false;
    let mut included = false;
    {
        let path = path.as_ref();
        for pattern in glob_excludes {
            if pattern.matches(path.to_string_lossy().as_ref()) {
                excluded = true;
                break;
            }
        }
        for pattern in glob_includes {
            if pattern.matches(path.to_string_lossy().as_ref()) {
                included = true;
                break;
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
    glob_includes: &'a [Pattern],
    glob_excludes: &'a [Pattern],
) -> impl Stream<Item = Result<(String, Option<ListingMetadata>)>> + 'a {
    async_stream::stream! {
        for entry in WalkDir::new(directory) {
            let entry = if let Ok(entry) = entry {
                entry
            } else {
                yield Err(entry.err().unwrap().into());
                return;
            };
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
            if process_globs(&path, glob_includes, glob_excludes).is_some() {
                yield Ok((path.to_string_lossy().into(), ListingMetadata::none()));
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum SyncCompressionDirection {
    None,
    Up,
    Down,
}

fn map_paths_to_etags<StreamT>(
    input_stream: StreamT,
    compressed: SyncCompressionDirection,
) -> impl Stream<Item = impl Future<Output = MapEtagResult>>
where
    StreamT: Stream<Item = Result<(String, Option<ListingMetadata>)>>,
{
    use std::str::FromStr;
    input_stream.map(move |params| async move {
        let (path, metadata) = params?;
        let path = PathBuf::from_str(&path)?;
        #[cfg(feature = "compression")]
        {
            use crate::compression::{
                compress_and_replace, compress_to_tempfile, compressed_path_to_path,
            };

            let (path, local_etag) = match compressed {
                SyncCompressionDirection::None => {
                    let local_etag = compute_etag(&path).await;
                    (path, local_etag)
                }
                SyncCompressionDirection::Up => {
                    if path.extension().map(|e| e == "gz").unwrap_or(false) {
                        let local_etag = compute_etag(&path).await;
                        (path, local_etag)
                    } else {
                        info!("compressing and replacing: {}", path.display());
                        let path = compress_and_replace(path).await?;
                        let local_etag = compute_etag(&path).await;
                        (path, local_etag)
                    }
                }
                SyncCompressionDirection::Down => {
                    if path.extension().map(|e| e == "gz").unwrap_or(false) {
                        let uncompressed_path = compressed_path_to_path(&path);

                        if !uncompressed_path.exists() {
                            (path, Err(Error::ETagNotPresent))
                        } else {
                            // Check if we already have a copy locally
                            // of the uncompressed file by recompressing
                            // the local file to see if it matches with
                            // the compressed version
                            let (temp_compressed, _) =
                                compress_to_tempfile(&uncompressed_path).await?;
                            let local_etag = compute_etag(&temp_compressed).await;
                            (path, local_etag)
                        }
                    } else {
                        let local_etag = compute_etag(&path).await;
                        (path, local_etag)
                    }
                }
            };

            Ok((path, local_etag, metadata))
        }
        #[cfg(not(feature = "compression"))]
        {
            if matches!(compressed, SyncCompressionDirection::None) {
                let local_etag = compute_etag(&path).await;
                Ok((path, local_etag, metadata))
            } else {
                panic!("compression feature not enabled");
            }
        }
    })
}

fn local_to_remote_sync_tasks<ClientT, StreamT>(
    s3: ClientT,
    bucket: String,
    key: String,
    directory: PathBuf,
    dirent_stream: StreamT,
) -> impl Stream<Item = impl Future<Output = Result<()>>>
where
    ClientT: S3 + Send + Clone,
    StreamT: Stream<Item = MapEtagResult>,
{
    use super::upload::upload;
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
            let (path, local_etag, _metadata) = entry;
            let path = Path::new(&path);
            let remote_path = Path::new(&key);
            let stripped_path = path.strip_prefix(&directory);
            let stripped_path = match stripped_path {
                Err(e) => {
                    warn!("unexpected: failed to strip prefix: {}", e);
                    return Ok(());
                }
                Ok(result) => result,
            };
            let remote_path = remote_path.join(&stripped_path);
            let remote_path = remote_path.to_string_lossy();
            debug!("checking remote: {}", remote_path);
            let local_etag = local_etag?;
            let object_info = head_object_request(&s3, &bucket, &remote_path).await?;
            if let Some(object_info) = object_info {
                let remote_etag = object_info.e_tag;
                if remote_etag != local_etag {
                    info!(
                        "etag mis-match: {}, remote_etag={}, local_etag={}",
                        remote_path, remote_etag, local_etag
                    );
                    upload(&s3, bucket, &remote_path, &path).await?;
                } else {
                    debug!(
                        "etags matched: {}, remote_etag={}, local_etag={}",
                        remote_path, remote_etag, local_etag
                    );
                }
            } else {
                info!("file did not exist remotely: {}", remote_path);
                upload(&s3, bucket, &remote_path, &path).await?;
            }
            Ok(())
        })
}

async fn sync_local_to_remote<T>(
    s3: &T,
    bucket: &str,
    key: &str,
    directory: impl AsRef<Path>,
    glob_includes: &[Pattern],
    glob_excludes: &[Pattern],
    compressed: bool,
) -> Result<()>
where
    T: S3 + Send + Clone,
{
    let directory = directory.as_ref();
    let task_count = Config::global().concurrent_sync_tasks();
    let dirent_stream = create_dirent_stream(directory, glob_includes, glob_excludes);
    let compression = if compressed {
        SyncCompressionDirection::Up
    } else {
        SyncCompressionDirection::None
    };

    let etag_stream = map_paths_to_etags(dirent_stream, compression).buffer_unordered(task_count);
    let sync_tasks = local_to_remote_sync_tasks(
        s3.clone(),
        bucket.into(),
        key.into(),
        directory.into(),
        etag_stream,
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
            copy_source: format!("{}/{}", source_bucket.to_string(), &file_name),
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
    glob_includes: &[Pattern],
    glob_excludes: &[Pattern],
) -> Result<()>
where
    T: S3 + Send,
{
    let mut stream = list_objects_stream(s3, source_bucket, source_prefix);

    while let Some(from_entries) = stream.try_next().await? {
        for entry in from_entries {
            if let S3ListingItem::S3Object(src_object) = entry {
                let path = process_globs(&src_object.key, glob_includes, glob_excludes);

                if let Some(_accept) = path {
                    let mut should_copy_file: bool = true;
                    let new_file = src_object.key.replace(source_prefix, destination_key);
                    let dest_object_info = head_object_request(s3, dest_bucket, &new_file).await?;

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
    glob_includes: &'a [Pattern],
    glob_excludes: &'a [Pattern],
) -> impl Stream<Item = Result<(String, Option<ListingMetadata>)>> + 'a
where
    ClientT: S3 + Send + Clone,
{
    async_stream::stream! {
        let mut stream = list_objects_stream(s3, bucket, key);
        loop {
            let entries_result = stream.try_next().await;
            if let Ok(entries_option) = entries_result {
                if let Some(entries) = entries_option {
                    for entry in entries {
                        let entry = entry.unwrap_object();
                        debug!("key={}", entry.key);
                        let path_result = Path::new(&entry.key).strip_prefix(key);
                        if let Ok(s3_suffix) = path_result {
                            if process_globs(&s3_suffix, glob_includes, glob_excludes).is_some() {
                                let local_path: String = directory.join(&s3_suffix).to_string_lossy().into();
                                let s3_suffix = s3_suffix.to_string_lossy().into();
                                yield Ok((local_path, ListingMetadata::some(s3_suffix, entry.e_tag)));
                            }
                        } else {
                            yield Err(path_result.err().unwrap().into());
                            return;
                        }
                    }
                } else {
                    break;
                }
            } else {
                yield Err(entries_result.err().unwrap());
                return;
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
    decompress: bool,
) -> impl Stream<Item = impl Future<Output = Result<()>>>
where
    ClientT: S3 + Sync + Send + Clone,
    StreamT: Stream<Item = MapEtagResult>,
{
    use super::download::download_with_dir;
    input_stream
        .map(move |entry| {
            (
                s3.clone(),
                bucket.clone(),
                key.clone(),
                directory.clone(),
                decompress,
                entry.unwrap(),
            )
        })
        .map(
            |(s3, bucket, key, directory, decompress, entry)| async move {
                let (path, local_etag, metadata) = entry;
                let metadata = metadata.unwrap();
                match local_etag {
                    Ok(local_etag) => {
                        if local_etag != metadata.e_tag {
                            debug!(
                                "etag mismatch: {}, local etag={}, remote etag={}",
                                path.display(),
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
                            debug!("etag match: {}", path.display());
                        }
                    }
                    Err(err) => match err {
                        Error::ETagNotPresent => {
                            debug!("file did not exist locally: {}", path.display());
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
    glob_includes: &[Pattern],
    glob_excludes: &[Pattern],
    decompress: bool,
) -> Result<()>
where
    T: S3 + Sync + Send + Clone,
{
    let directory = directory.as_ref();
    let task_count = Config::global().concurrent_sync_tasks();
    let object_listing =
        flattened_object_listing(s3, bucket, key, directory, glob_includes, glob_excludes);

    let compression = if decompress {
        SyncCompressionDirection::Down
    } else {
        SyncCompressionDirection::None
    };

    let etag_stream = map_paths_to_etags(object_listing, compression).buffer_unordered(task_count);
    let sync_tasks = remote_to_local_sync_tasks(
        s3.clone(),
        bucket.into(),
        key.into(),
        directory.into(),
        etag_stream,
        decompress,
    );

    sync_tasks
        .buffer_unordered(task_count)
        .try_collect()
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_process_globs() {
        let includes = vec![Pattern::new("*.csv").unwrap()];
        let excludes = vec![Pattern::new("*-blah.csv").unwrap()];

        assert!(process_globs("data.sbp", &includes[..], &excludes[..]).is_none());
        assert!(process_globs("yes.csv", &includes[..], &excludes[..]).is_some());
        assert!(process_globs("no-blah.csv", &includes[..], &excludes[..]).is_none());
    }

    #[test]
    fn test_process_globs_exclude_all() {
        let includes = vec![Pattern::new("*.png").unwrap()];
        let excludes = vec![];

        assert!(process_globs("a-fancy-thing.png", &includes[..], &excludes[..]).is_some());
        assert!(process_globs("horse.gif", &includes[..], &excludes[..]).is_none());
    }
}
