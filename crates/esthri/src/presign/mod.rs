/*
 * Copyright (C) 2022 Swift Navigation Inc.
 * Contact: Swift Navigation <dev@swiftnav.com>
 *
 * This source is subject to the license found in the file 'LICENSE' which must
 * be be distributed together with this source. All other rights reserved.
 *
 * THIS CODE AND INFORMATION IS PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND,
 * EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A PARTICULAR PURPOSE.
 */

//! Functions for presigning S3 requests.
//! These can be used to allow an S3 object owner to share an object with others
//! who do not have access for a short period of time.
//!
//! The submodules here have functions both for generating the URLs as well as
//! helpers for using the URLs and are split by S3 method (GET, PUT, DELETE) as
//! well as multipart PUT.

use std::{path::Path, time::Duration};

use tokio::fs::File;

use crate::{compression::compress_to_tempfile, opts::EsthriPutOptParams, Result};

pub(super) mod delete;
pub(super) mod download;
pub(super) mod multipart_upload;
pub(super) mod upload;

pub(crate) const DEAFULT_EXPIRATION: Duration = Duration::from_secs(60 * 60);

pub fn n_parts(file_size: usize, chunk_size: usize) -> usize {
    let mut n = file_size / chunk_size;
    if file_size % chunk_size != 0 {
        n += 1;
    }
    n
}

pub(crate) async fn file_maybe_compressed(
    filepath: &Path,
    opts: &EsthriPutOptParams,
) -> Result<File> {
    if opts.transparent_compression {
        Ok(compress_to_tempfile(filepath).await?.0.take_file())
    } else {
        Ok(File::open(filepath).await?)
    }
}
