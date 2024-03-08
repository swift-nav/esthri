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

use std::fmt::{Display, Formatter};
use std::str::FromStr;
use std::{
    path::{Path, PathBuf},
    result::Result as StdResult,
};

use aws_sdk_s3::primitives::DateTime;
use aws_sdk_s3::types::ObjectStorageClass;
use regex::Regex;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

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

impl FromStr for S3PathParam {
    type Err = String;

    fn from_str(s: &str) -> StdResult<Self, Self::Err> {
        let s3_format = Regex::new(r"^s3://(?P<bucket>[^/]+)/(?P<key>.*)$").unwrap();

        if let Some(captures) = s3_format.captures(s) {
            let bucket = captures
                .name("bucket")
                .ok_or("s3 bucket not found".to_string())?
                .as_str();
            let key = captures
                .name("key")
                .ok_or("s3 key not found".to_string())?
                .as_str();
            Ok(S3PathParam::new_bucket(bucket, key))
        } else {
            Ok(S3PathParam::new_local(s))
        }
    }
}

impl Display for S3PathParam {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            S3PathParam::Local { path } => write!(f, "{}", path.display()),
            S3PathParam::Bucket { bucket, key } => write!(f, "s3://{bucket}/{key}"),
        }
    }
}

impl Serialize for S3PathParam {
    fn serialize<S: Serializer>(&self, serializer: S) -> StdResult<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for S3PathParam {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> StdResult<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        S3PathParam::from_str(&s).map_err(serde::de::Error::custom)
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
    pub fn unwrap_object(self) -> S3Object {
        match self {
            S3ListingItem::S3Object(o) => o,
            S3ListingItem::S3CommonPrefix(_cp) => panic!("invalid type"),
        }
    }

    pub fn as_object(self) -> Option<S3Object> {
        match self {
            S3ListingItem::S3Object(o) => Some(o),
            S3ListingItem::S3CommonPrefix(_cp) => None,
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
    pub storage_class: Option<ObjectStorageClass>,
    pub size: Option<i64>,
    pub last_modified: Option<DateTime>,
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
