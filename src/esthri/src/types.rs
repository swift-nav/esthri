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

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::result::Result as StdResult;

use chrono::{DateTime, Utc};
use regex::Regex;

pub(super) struct GlobalData {
    pub(super) bucket: Option<String>,
    pub(super) key: Option<String>,
    pub(super) upload_id: Option<String>,
}

#[derive(Debug)]
pub struct ObjectInfo {
    pub e_tag: String,
    pub size: i64,
    pub last_modified: DateTime<Utc>,
    pub metadata: HashMap<String, String>,
    pub(crate) parts: u64,
}

impl ObjectInfo {
    pub fn is_esthri_compressed(&self) -> bool {
        self.metadata
            .contains_key(crate::compression::ESTHRI_METADATA_COMPRESS_KEY)
    }
}

#[derive(Debug, Clone)]
pub enum S3PathParam {
    Local { path: PathBuf },
    Bucket { bucket: String, key: String },
}

impl S3PathParam {
    pub fn new_local<P: AsRef<Path>>(path: P) -> S3PathParam {
        S3PathParam::Local {
            path: path.as_ref().into(),
        }
    }
    pub fn new_bucket<S1: AsRef<str>, S2: AsRef<str>>(bucket: S1, key: S2) -> S3PathParam {
        S3PathParam::Bucket {
            bucket: bucket.as_ref().into(),
            key: key.as_ref().into(),
        }
    }
    pub fn is_local(&self) -> bool {
        matches!(self, S3PathParam::Local { .. })
    }
    pub fn is_bucket(&self) -> bool {
        matches!(self, S3PathParam::Bucket { .. })
    }
}

impl std::str::FromStr for S3PathParam {
    type Err = String;

    fn from_str(s: &str) -> StdResult<Self, Self::Err> {
        let s3_format = Regex::new(r"^s3://(?P<bucket>[^/]+)/(?P<key>.*)$").unwrap();

        if let Some(captures) = s3_format.captures(s) {
            let bucket = captures.name("bucket").unwrap().as_str();
            let key = captures.name("key").unwrap().as_str();
            Ok(S3PathParam::new_bucket(bucket, key))
        } else {
            Ok(S3PathParam::new_local(s))
        }
    }
}

#[derive(Debug, Clone)]
pub enum S3ListingItem {
    S3Object(S3Object),
    S3CommonPrefix(String),
}

impl S3ListingItem {
    pub(crate) fn object(o: S3Object) -> S3ListingItem {
        S3ListingItem::S3Object(o)
    }
    pub(crate) fn common_prefix<S: AsRef<str>>(cp: S) -> S3ListingItem {
        S3ListingItem::S3CommonPrefix(cp.as_ref().into())
    }
    pub(crate) fn prefix(&self) -> String {
        match self {
            S3ListingItem::S3Object(o) => o.key.clone(),
            S3ListingItem::S3CommonPrefix(cp) => cp.clone(),
        }
    }
    pub(crate) fn unwrap_object(self) -> S3Object {
        match self {
            S3ListingItem::S3Object(o) => o,
            S3ListingItem::S3CommonPrefix(_cp) => panic!("invalid type"),
        }
    }
}

#[derive(Default)]
pub(crate) struct S3Listing {
    pub(crate) continuation: Option<String>,
    pub(crate) contents: Vec<S3Object>,
    pub(crate) common_prefixes: Vec<String>,
}

impl S3Listing {
    pub(crate) fn combined(self) -> Vec<S3ListingItem> {
        let common_prefixes = self
            .common_prefixes
            .into_iter()
            .map(S3ListingItem::common_prefix);
        let contents = self.contents.into_iter().map(S3ListingItem::object);
        common_prefixes.chain(contents).collect()
    }
    pub(crate) fn count(&self) -> usize {
        self.contents.len() + self.common_prefixes.len()
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.count() == 0
    }
}

#[derive(Debug, Clone)]
pub struct S3Object {
    pub key: String,
    pub e_tag: String,
}

/// Used to track parallel reads when downloading data from S3.
#[derive(Debug, Clone, Copy)]
pub(super) struct ReadState {
    read_size: usize,
    remaining: u64,
    offset: u64,
    total: u64,
}

impl ToString for ReadState {
    fn to_string(&self) -> String {
        format!(
            "bytes={}-{}",
            self.offset,
            self.offset + self.read_size() as u64 - 1,
        )
    }
}

impl ReadState {
    /// Creates an object for tracking read sizes and offsets when requesting data from S3.  Goals
    /// are as follows:
    /// * Uses [ToString@ReadSize] to build a Range header value to request data from S3, see the [MDN docs](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Range) and [S3 docs](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html#API_GetObject_RequestSyntax) for reference
    /// * Tracks when the end of the object is reached and adjusts the read size accordingly
    /// * Allows the read process to know if the object size has changed, and abort the read
    ///
    /// # Arguments
    /// * `read_size` - the size of each read to request from S3
    /// * `total` - the total size of the remote object on S3
    pub(super) fn new(read_size: usize, total: u64) -> Self {
        ReadState {
            offset: 0,
            read_size: usize::min(total as usize, read_size),
            remaining: total,
            total,
        }
    }
    /// Advance the read to the next chunk.
    pub(super) fn update(&mut self, amount: usize) -> usize {
        self.remaining -= amount as u64;
        self.offset += amount as u64;
        self.read_size()
    }
    /// Fetch the next read size
    pub(super) fn read_size(&self) -> usize {
        usize::min(self.remaining as usize, self.read_size)
    }
    /// Indicates if the read process is completed
    pub(super) fn complete(&self) -> bool {
        self.remaining == 0
    }
    /// Tracks the total object size on S3
    pub(super) fn total(&self) -> u64 {
        self.total
    }
    /// The current read offset
    pub(super) fn offset(&self) -> u64 {
        self.offset
    }
}

/// For syncing from remote to local, or local to remote, "metadata" is attached to the listing in
/// the remote case (the S3 "suffix" and the ETag) so that we can make the comparison to decide if
/// we need to download or upload.
pub(super) struct ListingMetadata {
    pub(super) s3_suffix: String,
    pub(super) e_tag: String,
    pub(super) esthri_compressed: bool,
}

impl ListingMetadata {
    pub(super) fn some(s3_suffix: String, e_tag: String, esthri_compressed: bool) -> Option<Self> {
        Some(Self {
            s3_suffix,
            e_tag,
            esthri_compressed,
        })
    }
    pub(super) fn none() -> Option<Self> {
        None
    }
}
