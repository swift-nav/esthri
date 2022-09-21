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

use log_derive::logfn;

use crate::errors::Result;
use crate::rusoto::{DeleteObjectRequest, S3};
// use crate::{
//     download, download_with_transparent_decompression, upload, upload_compressed, S3PathParam,
// };

#[logfn(err = "ERROR")]
pub async fn delete<T>(s3: &T, bucket: impl AsRef<str>, key: impl AsRef<str>) -> Result<()>
where
    T: S3 + Sync + Send + Clone,
{
    let bucket: &str = bucket.as_ref();
    let key: &str = key.as_ref();
    let dor: DeleteObjectRequest = DeleteObjectRequest {
        bucket: bucket.to_owned(),
        key: key.to_owned(),
        ..Default::default()
    };
    s3.delete_object(dor).await?;
    eprintln!("delete invoked");
    Ok(())
}
