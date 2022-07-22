use redis::{streams::StreamMaxlen, AsyncCommands};
use rusoto_s3::S3;
use tokio::fs::{self, write};
use std::env;
use tempdir::TempDir;

use crate::{config::Config, errors::Result, ops::upload::upload};

const REDIS_PREFIX: &str = "redis://";

pub struct RedisStreamer {
    pub stream_key: String,
    stream_part_size: usize,
    client: redis::Client,
}
impl RedisStreamer {
    pub async fn create(stream_key: String) -> Result<Self> {
        let stream_part_size = Config::global().stream_part_size();
        let redis_url = Self::get_redis_url();
        let client = redis::Client::open(redis_url)?;
        Ok(Self {
            stream_key,
            stream_part_size,
            client,
        })
    }
    pub async fn add(&self, stream_field: String, mut buf: &[u8]) -> Result<()> {
        let mut con = self.client.get_async_connection().await?;

        while !buf.is_empty() {
            let idx = usize::min(buf.len(), self.stream_part_size);
            let items: Vec<(&String, &[u8])> = vec![(&stream_field, &buf[..idx])];
            con.xadd_maxlen(
                self.stream_key.clone(),
                StreamMaxlen::Equals(self.stream_part_size),
                "*",
                &items,
            )
            .await?;
            buf = &buf[idx..];
        }

        Ok(())
    }

    pub async fn download(stream_key: String, stream_field: String) -> Result<Vec<u8>> {
        let redis_url = Self::get_redis_url();
        let client = redis::Client::open(redis_url)?;
        let mut con = client.get_async_connection().await?;
        let mut start = "-".to_string();
        let end = "+".to_string();
        let mut output: Vec<u8> = vec![];
        loop {
            let result: redis::streams::StreamRangeReply = con
                .xrange_count(stream_key.clone(), &start, &end, 2_u8)
                .await?;
            if result.ids.is_empty() {
                break;
            }
            for stream_id in result.ids {
                let buffer: Option<Vec<u8>> = stream_id.get(&stream_field);
                if let Some(buffer_) = buffer {
                    output.extend_from_slice(buffer_.as_ref());
                    let next_start_end = stream_id.id.splitn(2, '-').collect::<Vec<_>>();
                    start = format!(
                        "{}-{}",
                        next_start_end[0],
                        1 + next_start_end[1].parse::<u32>()?
                    );
                }
            }
        }
        Ok(output)
    }

    pub async fn upload_stream_to_s3<'a, T>(s3: &T, bucket: String, stream_key: String, stream_field: String) -> Result<()>
        where T: S3 + Send
    {
        let data = Self::download(stream_key.clone(), stream_field).await?;
        let path = {
            crate::tempfile::TempFile::new(std::path::PathBuf::from("temp"), None).await?.path().to_string_lossy().to_string()
        };
        write(path.clone(), data).await?;
        upload(s3, bucket, stream_key, path).await?;
        Ok(())
    }

    pub async fn delete_stream(&self) -> Result<()> {
        let mut con = self.client.get_async_connection().await?;
        con.del(self.stream_key.clone()).await?;
        Ok(())
    }

    fn get_redis_url() -> String {
        let mut redis_url = env::var("REDIS_SERVER_URL").unwrap_or_else(|_| {
            env::var("SITL_REDIS_URL")
                .unwrap_or_else(|_| Config::global().default_redis_server_url())
        });
        if !redis_url.starts_with(REDIS_PREFIX) {
            redis_url = format!("{REDIS_PREFIX}{}", redis_url);
        }
        redis_url
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::{
        fs,
        io::{AsyncReadExt, AsyncSeekExt, SeekFrom},
        process::Command,
    };

    #[ignore]
    #[tokio::test]
    async fn redis_streamer_test() {
        let log_path = tempfile()?;

        let (sender, receiver) = std::sync::mpsc::channel();
        let log_path_clone = log_path.clone();
        let stream_key = log_path.clone().display().to_string();
        let stream_key_clone = stream_key.clone();
        let stream_field = "sitl".to_string();
        let stream_field_clone = stream_field.clone();
        tokio::spawn(async move {
            let rs = RedisStreamer::create(stream_key_clone).await.unwrap();
            let mut bytes_read = 0;
            while receiver.try_recv().is_err() {
                if let Ok(mut f) = fs::File::open(&log_path_clone).await {
                    f.seek(SeekFrom::Start(bytes_read as u64)).await.unwrap();
                    let mut buf = Vec::new();

                    if f.read_to_end(&mut buf).await.unwrap() > 0 {
                        rs.add(stream_field_clone.clone(), &buf).await.unwrap();
                        bytes_read += buf.len();
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });
        let handle = tokio::spawn(async move {
            let mut old = vec![];
            let now = tokio::time::Instant::now();
            while now.elapsed() < Duration::from_secs(6) {
                let new = RedisStreamer::download(stream_key.clone(), stream_field.clone())
                    .await
                    .unwrap();
                if new.len() > old.len() {
                    old = new;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            old
        });
        {
            let stdout = std::fs::File::create(log_path).unwrap();
            let cmd = "for i in {0..5};do echo ${i};sleep 1;done".to_string();
            Command::new("bash")
                .arg("-c")
                .arg(cmd)
                .stdout(stdout.try_clone().unwrap())
                .stderr(stdout)
                .spawn()
                .unwrap()
                .wait_with_output()
                .await
                .unwrap();
        }

        sender.send(()).unwrap();
        let res = handle.await.unwrap();
        assert_eq!(String::from_utf8_lossy(&res), "0\n1\n2\n3\n4\n5\n");
    }

    async fn setup_client() -> redis::aio::Connection {
        let redis_url = RedisStreamer::get_redis_url();
        let client = redis::Client::open(redis_url).unwrap();
        client.get_async_connection().await.unwrap()
    }

    #[ignore]
    #[tokio::test]
    async fn redis_streamer_maxlen_test() {
        let stream_key = format!("{:?}", tokio::time::Instant::now());
        let stream_field = "sitl".to_string();
        let max_stream_entry_size = Config::global().stream_part_size() as u64;
        let buf: Vec<u8> = (0..(max_stream_entry_size * 3))
            .map(|x| (x % u8::MAX as u64) as u8)
            .collect::<Vec<_>>();
        let rs = RedisStreamer::create(stream_key.clone()).await.unwrap();
        rs.add(stream_field.clone(), &buf).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let buf_new = RedisStreamer::download(stream_key.clone(), stream_field.clone())
            .await
            .unwrap();
        assert_eq!(buf_new, buf);

        let mut start = "-".to_string();
        let end = "+".to_string();
        let mut con = setup_client().await;
        let mut output: Vec<Vec<u8>> = vec![];
        loop {
            let result: redis::streams::StreamRangeReply = con
                .xrange_count(stream_key.clone(), &start, &end, 2_u8)
                .await
                .unwrap();
            if result.ids.is_empty() {
                break;
            }
            for stream_id in result.ids {
                let buffer: Option<Vec<u8>> = stream_id.get(&stream_field);
                if let Some(buffer_) = buffer {
                    let next_start_end = stream_id.id.splitn(2, '-').collect::<Vec<_>>();
                    start = format!(
                        "{}-{}",
                        next_start_end[0],
                        1 + next_start_end[1].parse::<u32>().unwrap()
                    );
                    output.push(buffer_.clone());
                }
            }
        }
        assert_eq!(output.len(), 3);
        output.iter().for_each(|buf| {
            assert!(buf.len() <= max_stream_entry_size as usize);
        });
    }
}
