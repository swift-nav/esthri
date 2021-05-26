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

use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use regex::Regex;

pub(crate) struct GlobalData {
    pub(crate) bucket: Option<String>,
    pub(crate) key: Option<String>,
    pub(crate) upload_id: Option<String>,
}

#[derive(Debug)]
pub struct ObjectInfo {
    pub e_tag: String,
    pub size: i64,
    pub last_modified: DateTime<Utc>,
}

#[derive(Debug)]
pub enum SyncParam {
    Local { path: PathBuf },
    Bucket { bucket: String, key: String },
}

impl SyncParam {
    pub fn new_local<P: AsRef<Path>>(path: P) -> SyncParam {
        SyncParam::Local {
            path: path.as_ref().into(),
        }
    }
    pub fn new_bucket<S1: AsRef<str>, S2: AsRef<str>>(bucket: S1, key: S2) -> SyncParam {
        SyncParam::Bucket {
            bucket: bucket.as_ref().into(),
            key: key.as_ref().into(),
        }
    }
}

impl std::str::FromStr for SyncParam {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s3_format = Regex::new(r"^s3://(?P<bucket>[^/]+)/(?P<key>.*)$").unwrap();

        if let Some(captures) = s3_format.captures(s) {
            let bucket = captures.name("bucket").unwrap().as_str();
            let key = captures.name("key").unwrap().as_str();
            Ok(SyncParam::new_bucket(bucket, key))
        } else {
            Ok(SyncParam::new_local(s))
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
pub (in super) struct ReadSize {
    read_size: usize,
    remaining: u64,
    offset: u64,
    total: u64,
}

impl ToString for ReadSize {
    fn to_string(&self) -> String {
        format!(
            "bytes={}-{}",
            self.offset,
            self.offset + self.read_size() as u64 - 1,
        )
    }
}

impl ReadSize {
    /// Creats an object for tracking read sizes and offsets when requesting data from S3.  Goals
    /// are as follows:
    /// * Uses [ToString@ReadSize] to build a Range header value to request data from S3, see the [MDN docs](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Range) and [S3 docs](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html#API_GetObject_RequestSyntax) for reference
    /// * Tracks when the end of the object is reached and adjusts the read size accordingly
    /// * Allows the read process to know if the object size has changed, and abort the read
    ///
    /// # Arguments
    /// * `read_size` - the size of each read to request from S3
    /// * `total` - the total size of the remote object on S3
    pub (in super) fn new(read_size: usize, total: u64) -> Self {
        ReadSize {
            offset: 0,
            read_size: usize::min(total as usize, read_size),
            remaining: total,
            total,
        }
    }
    /// Advance the read to the next chunk.
    pub (in super) fn update(&mut self, amount: usize) -> usize {
        self.remaining -= amount as u64;
        self.offset += amount as u64;
        self.read_size()
    }
    /// Fetch the next read size
    pub (in super) fn read_size(&self) -> usize {
        usize::min(self.remaining as usize, self.read_size)
    }
    /// Indicates if the read process is completed
    pub (in super) fn complete(&self) -> bool {
        self.remaining == 0
    }
    /// Tracks the total object size on S3
    pub (in super) fn total(&self) -> u64 {
        self.total
    }
    /// The current read offset
    pub (in super) fn offset(&self) -> u64 {
        self.offset
    }
}
