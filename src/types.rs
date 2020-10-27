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

use chrono::{DateTime, Utc};

#[derive(Debug)]
pub struct ObjectInfo {
    pub e_tag: String,
    pub size: i64,
    pub last_modified: DateTime<Utc>,
}
