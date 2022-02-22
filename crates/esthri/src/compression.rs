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

use async_compression::tokio::bufread::GzipEncoder;
use log::debug;
use tokio::{
    fs::File,
    io::{self, BufReader},
};

use crate::{tempfile::TempFile, Result};

pub const ESTHRI_METADATA_COMPRESS_KEY: &str = "esthri_compress_version";

pub async fn compress_to_tempfile(path: &Path) -> Result<(TempFile, u64)> {
    debug!("compressing: {}", path.display());
    let mut src = {
        let f = File::open(path).await?;
        let size = f.metadata().await?.len();
        debug!("old file size: {}", size);
        GzipEncoder::new(BufReader::new(f))
    };
    let mut dest = TempFile::new(Some(".gz")).await?;
    let new_size = io::copy(&mut src, dest.file_mut()).await?;
    dest.rewind().await?;
    debug!("new file size: {}", new_size);
    Ok((dest, new_size))
}

pub fn compressed_file_metadata() -> HashMap<String, String> {
    let mut m = HashMap::new();
    m.insert(
        ESTHRI_METADATA_COMPRESS_KEY.to_string(),
        env!("CARGO_PKG_VERSION").to_string(),
    );
    m
}
