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

#![cfg_attr(feature = "aggressive_lint", deny(warnings))]

use std::{
    ffi::OsString,
    path::{Path, PathBuf},
};

use flate2::{read::GzEncoder, Compression};
use futures::Future;
use log::debug;
use tokio::task;

use super::{Error, Result, EXPECT_SPAWN_BLOCKING};

use crate::Error::FileNotCompressed;
/// Internal module used to call out operations that may block.
mod bio {
    pub(super) use std::fs;
    pub(super) use std::fs::File;
    pub(super) use std::io::BufReader;
    pub(super) use std::io::Seek;
    pub(super) use std::io::SeekFrom;
    pub(super) use tempfile::NamedTempFile;
}

fn rewind_file<T: bio::Seek>(file: &mut T) -> Result<()> {
    file.seek(bio::SeekFrom::Start(0))
        .map(|_| ())
        .map_err(Error::from)
}

fn named_tempfile<P: AsRef<Path>>(dir: P) -> Result<bio::NamedTempFile> {
    tempfile::Builder::new()
        .prefix(".esthri_temp.")
        .suffix(".gz")
        .tempfile_in(dir.as_ref())
        .map_err(Error::from)
}

pub(crate) fn compress_to_tempfile(
    path: impl AsRef<Path>,
) -> impl Future<Output = Result<(bio::NamedTempFile, u64)>> {
    use bio::*;
    let path = path.as_ref().to_path_buf();
    async move {
        task::spawn_blocking(move || {
            let size = path.metadata()?.len();
            debug!("old file size: {}", size);
            debug!("compressing: {}", path.display());
            let buf_reader = BufReader::new(File::open(&path)?);
            let mut reader = GzEncoder::new(buf_reader, Compression::default());
            let pwd = std::env::current_dir()?;
            let mut compressed = named_tempfile(path.parent().unwrap_or(&pwd))?;
            let size = std::io::copy(&mut reader, &mut compressed)?;
            rewind_file(&mut compressed)?;
            debug!("new file size: {}", size);
            Ok((compressed, size))
        })
        .await
        .expect(EXPECT_SPAWN_BLOCKING)
    }
}

pub(crate) async fn compress_and_replace(path: impl AsRef<Path>) -> Result<PathBuf> {
    use bio::*;
    let path = path.as_ref();
    debug!("compressing (and renaming): {}", path.display());
    let temp_path = {
        let (temp_file, _size) = compress_to_tempfile(path).await?;
        let (_temp_file, temp_path) = temp_file.keep()?;
        temp_path
    };
    let file_gz = path_to_compressed_path(path);
    debug!("renaming {} to {}", path.display(), file_gz.display());
    fs::rename(temp_path, &file_gz)?;
    let permissions = path.metadata()?.permissions();
    fs::set_permissions(&file_gz, permissions)?;
    fs::remove_file(path)?;
    Ok(file_gz)
}

pub(crate) fn path_to_compressed_path(path: &Path) -> PathBuf {
    let file_gz = path
        .extension()
        .map(OsString::from)
        .map(|mut ext| {
            ext.push(".gz");
            path.to_path_buf().with_extension(ext)
        })
        .unwrap_or_else(|| path.to_path_buf().with_extension("gz"));
    file_gz
}

pub(crate) fn compressed_path_to_path(path: &Path) -> Result<PathBuf> {
    let mut pathbuf = path.to_path_buf();
    let ext = path.extension().ok_or(FileNotCompressed)?;
    let filename = path.file_name().ok_or(FileNotCompressed)?;
    if ext == "gz" {
        pathbuf.set_file_name(filename.to_string_lossy().strip_suffix(".gz").unwrap());
        Ok(pathbuf)
    } else {
        Err(FileNotCompressed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compressed_path_to_path() {
        assert_eq!(
            Path::new("myfile.tar"),
            compressed_path_to_path(Path::new("myfile.tar.gz")).unwrap()
        );
        assert!(matches!(
            compressed_path_to_path(Path::new("myfile.tar")),
            Err(FileNotCompressed),
        ));
        assert_eq!(
            Path::new("/tmp/path/to/mycode.rs"),
            compressed_path_to_path(Path::new("/tmp/path/to/mycode.rs.gz")).unwrap()
        );
    }
}
