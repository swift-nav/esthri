use std::io::Cursor;

use tokio::time::{delay_for, timeout, Duration};

use esthri_lib::{tail, upload_from_reader};

mod common;

#[tokio::test]
async fn test_tail() {
    let s3client = common::get_s3client();

    let key = "test_tail/test.txt";

    let mut writer = vec![];

    let upload = async {
        upload_str("line 1", key).await;
        delay_for(Duration::from_secs(3)).await;

        upload_str("line 1\nline 2", key).await;
        delay_for(Duration::from_secs(3)).await;

        upload_str("line 1\nline 2\nline 3", key).await;
        delay_for(Duration::from_secs(3)).await;
    };

    upload_str("", key).await;

    tokio::select! {
        _ = upload => (),
        _ = tail(s3client.as_ref(), &mut writer, 2, common::TEST_BUCKET, key) => (),
    };

    assert_eq!("line 1\nline 2\nline 3", String::from_utf8(writer).unwrap());
}

#[tokio::test]
async fn test_tail_static() {
    let key = "test_tail/static.txt";
    let contents = "important file contents";

    upload_str(contents, key).await;

    let s3client = common::get_s3client();
    let mut writer = vec![];

    timeout(
        Duration::from_secs(3),
        tail(s3client.as_ref(), &mut writer, 1, common::TEST_BUCKET, key),
    )
    .await
    .ok();

    assert_eq!(contents, String::from_utf8(writer).unwrap());
}

async fn upload_str(contents: &str, key: &str) {
    let mut reader = Cursor::new(contents);
    let s3client = common::get_s3client();

    upload_from_reader(
        s3client.as_ref(),
        common::TEST_BUCKET,
        key,
        &mut reader,
        contents.len() as u64,
    )
    .await
    .unwrap();
}
