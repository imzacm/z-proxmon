use std::sync::atomic::{AtomicU64, Ordering};

use compio::BufResult;
use compio::io::{AsyncReadAt, AsyncWriteAt};
use tempfile::TempDir;
use triomphe::Arc;

use crate::monitor::State;
use crate::structures::SystemStats;

#[derive(Clone)]
pub struct MonitorLog {
    _log_dir: Arc<TempDir>,
    log_file: compio::fs::File,
    current_size: Arc<AtomicU64>,
}

impl MonitorLog {
    pub async fn new(state: Arc<State>) -> Result<Self, std::io::Error> {
        let log_dir = compio::runtime::spawn_blocking(TempDir::new).await.unwrap()?;
        let log_file = log_dir.path().join("log");

        let log_file = compio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(log_file)
            .await?;

        let current_size = Arc::new(AtomicU64::new(0));

        compio::runtime::spawn(log_updates(state, log_file.clone(), current_size.clone())).detach();

        Ok(Self { _log_dir: Arc::new(log_dir), log_file, current_size })
    }

    pub async fn cursor(&self) -> Result<LogCursor<'_>, std::io::Error> {
        let len = self.current_size.load(Ordering::Acquire);
        Ok(LogCursor {
            log_file: &self.log_file,
            len,
            pos: 0,
            buffer: Vec::new(),
            read_buffer: Vec::new(),
        })
    }
}

/// Snapshot of the log file.
pub struct LogCursor<'a> {
    log_file: &'a compio::fs::File,
    len: u64,
    pos: u64,
    buffer: Vec<u8>,
    read_buffer: Vec<u8>,
}

impl LogCursor<'_> {
    pub async fn next(&mut self) -> Result<Option<SystemStats>, bincode::error::DecodeError> {
        loop {
            if self.pos >= self.len {
                return Ok(None);
            }
            // Default to reading 4096 bytes at a time.
            let mut read_len = 4096;
            if !self.buffer.is_empty() {
                let result =
                    bincode::serde::decode_from_slice(&self.buffer, bincode::config::standard());
                match result {
                    Ok((stats, n)) => {
                        self.buffer.drain(..n);
                        return Ok(Some(stats));
                    }
                    Err(bincode::error::DecodeError::UnexpectedEnd { additional }) => {
                        read_len = read_len.max(additional);
                    }
                    Err(error) => return Err(error),
                }
            }

            let mut buffer = std::mem::take(&mut self.read_buffer);
            if buffer.len() < read_len {
                buffer.resize(read_len, 0);
            }

            let BufResult(result, buffer) = self.log_file.read_at(buffer, self.pos).await;
            let n = result.expect("Failed to read from log file");
            if n == 0 {
                return Ok(None);
            }
            self.pos += n as u64;
            self.buffer.extend_from_slice(&buffer[..n]);
            self.read_buffer = buffer;
        }
    }
}

async fn log_updates(state: Arc<State>, mut log_file: compio::fs::File, current_size: Arc<AtomicU64>) {
    let mut buffer = Vec::new();
    let mut offset = 0;

    current_size.store(0, Ordering::Release);

    loop {
        if Arc::strong_count(&current_size) == 1 {
            return;
        }

        let stats_listener = state.stats.observe();

        {
            let stats = state.stats.latest_value_async().await;
            bincode::serde::encode_into_std_write(
                &*stats,
                &mut buffer,
                bincode::config::standard(),
            )
            .unwrap();

            let mut remaining = buffer.len();
            while remaining > 0 {
                let BufResult(result, buf) = log_file.write_at(buffer, offset).await;
                let n = result.expect("Failed to write to log file");
                if n == 0 {
                    panic!("Failed to write to log file");
                }
                buffer = buf;
                if n < remaining {
                    buffer.drain(..n);
                } else {
                    buffer.clear();
                }
                offset += n as u64;
                remaining -= n;
            }

            current_size.store(offset, Ordering::Release);
        }

        stats_listener.await;
    }
}
