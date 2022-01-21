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

use std::{collections::HashMap, path::Path};

use flate2::{read::GzEncoder, Compression};
use futures::Future;
use log::debug;
use tokio::task;

use super::{Error, Result, EXPECT_SPAWN_BLOCKING};

/// Internal module used to call out operations that may block.
mod bio {
    pub(super) use std::fs::File;
    pub(super) use std::io::BufReader;
    pub(super) use std::io::Seek;
    pub(super) use std::io::SeekFrom;
    pub(super) use tempfile::NamedTempFile;
}

pub const ESTHRI_METADATA_COMPRESS_KEY: &str = "esthri_compress_version";

pub(crate) fn compressed_file_metadata() -> HashMap<String, String> {
    let mut m = HashMap::new();
    m.insert(
        ESTHRI_METADATA_COMPRESS_KEY.to_string(),
        env!("CARGO_PKG_VERSION").to_string(),
    );
    m
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
