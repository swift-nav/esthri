use std::{iter, sync::Arc};

use bytes::{Bytes, BytesMut};
use once_cell::sync::OnceCell;
use tokio::sync::{Mutex, MutexGuard, Notify};

use crate::Config;

static POOL: OnceCell<Pool> = OnceCell::new();

pub struct Pool {
    shared: Arc<Shared>,
}

impl Pool {
    pub fn global() -> &'static Pool {
        POOL.get_or_init(|| {
            let conf = Config::global();
            Pool::new(
                conf.concurrent_upload_tasks(),
                conf.upload_part_size() as usize,
            )
        })
    }

    pub fn new(num_buffers: usize, buf_len: usize) -> Self {
        log::debug!(
            "new pool num_buffers={} buf_len={} total_bytes={}",
            num_buffers,
            buf_len,
            num_buffers * buf_len
        );
        let buffers = iter::repeat(BytesMut::new())
            .map(Mutex::new)
            .take(num_buffers)
            .collect();
        Pool {
            shared: Arc::new(Shared {
                buffers,
                notify: Notify::new(),
                buf_len,
            }),
        }
    }

    pub async fn get(&self) -> Buffer<'_> {
        loop {
            for lock in self.shared.buffers.iter() {
                if let Ok(mut guard) = lock.try_lock() {
                    guard.clear();
                    guard.resize(self.shared.buf_len, 0);
                    return Buffer {
                        buffer: guard,
                        shared: Arc::clone(&self.shared),
                    };
                }
            }
            self.shared.notify.notified().await;
        }
    }
}

pub struct Buffer<'pool> {
    buffer: MutexGuard<'pool, BytesMut>,
    shared: Arc<Shared>,
}

impl Buffer<'_> {
    pub fn freeze(mut self) -> Bytes {
        self.buffer.split().freeze()
    }
}

impl Drop for Buffer<'_> {
    fn drop(&mut self) {
        self.shared.notify.notify_one();
    }
}

impl AsRef<[u8]> for Buffer<'_> {
    fn as_ref(&self) -> &[u8] {
        &self.buffer
    }
}

impl AsMut<[u8]> for Buffer<'_> {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.buffer
    }
}

struct Shared {
    buffers: Vec<Mutex<BytesMut>>,
    notify: Notify,
    buf_len: usize,
}
