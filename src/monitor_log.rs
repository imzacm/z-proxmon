use std::path::{Path, PathBuf};
use std::time::Instant;

use compio::BufResult;
use compio::io::{AsyncReadAt, AsyncWriteAt};
use tempfile::TempDir;
use triomphe::Arc;
use z_sync::Lock16;

use crate::monitor::State;
use crate::structures::SystemStats;

#[derive(Clone)]
struct LogFile {
    file: compio::fs::File,
    size: u64,
    start_time: time::OffsetDateTime,
}

#[derive(Clone)]
pub struct MonitorLog {
    _log_dir: Arc<TempDir>,
    log_files: Arc<Lock16<Vec<Lock16<LogFile>>>>,
}

impl MonitorLog {
    pub async fn new(state: Arc<State>) -> Result<Self, std::io::Error> {
        let log_dir = compio::runtime::spawn_blocking(TempDir::new).await.unwrap()?;
        let log_files = Arc::new(Lock16::new(Vec::new()));

        compio::runtime::spawn(log_updates(state, log_dir.path().to_path_buf(), log_files.clone()))
            .detach();

        Ok(Self { _log_dir: Arc::new(log_dir), log_files })
    }

    pub async fn cursor(&self) -> Result<LogCursor<'_>, std::io::Error> {
        Ok(LogCursor {
            log_files: &self.log_files,
            file_index: 0,
            pos: 0,
            buffer: Vec::new(),
            read_buffer: Vec::new(),
        })
    }
}

/// Snapshot of the log file.
pub struct LogCursor<'a> {
    log_files: &'a Lock16<Vec<Lock16<LogFile>>>,
    file_index: usize,
    pos: u64,
    buffer: Vec<u8>,
    read_buffer: Vec<u8>,
}

impl LogCursor<'_> {
    /// Skips to the file containing this timestamp.
    pub async fn skip_to_hour(&mut self, timestamp: time::OffsetDateTime) {
        self.pos = 0;
        self.buffer.clear();
        self.read_buffer.clear();

        let files_lock = self.log_files.read_async().await;

        let mut target_index = 0;
        for (index, file_lock) in files_lock.iter().enumerate() {
            let file = file_lock.read_async().await;

            if file.start_time <= timestamp {
                target_index = index;
            } else {
                break;
            }
        }

        self.file_index = target_index;
    }

    pub async fn next(&mut self) -> Result<Option<SystemStats>, bincode::error::DecodeError> {
        loop {
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

            let log_file = {
                let files_lock = self.log_files.read_async().await;
                if self.file_index >= files_lock.len() {
                    return Ok(None);
                }
                files_lock[self.file_index].read_async().await.clone()
            };

            let BufResult(result, buffer) = log_file.file.read_at(buffer, self.pos).await;
            let n = result.expect("Failed to read from log file");
            if n == 0 {
                let files_lock = self.log_files.read_async().await;
                if self.file_index == files_lock.len() - 1 {
                    return Ok(None);
                }
                self.file_index += 1;
                self.pos = 0;
                continue;
            }
            self.pos += n as u64;
            self.buffer.extend_from_slice(&buffer[..n]);
            self.read_buffer = buffer;
        }
    }
}

async fn create_log_file(
    dir: &Path,
    files: &mut Vec<Lock16<LogFile>>,
) -> Result<usize, std::io::Error> {
    let file_name = files.len().to_string();

    let file = compio::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .open(dir.join(file_name))
        .await?;

    let index = files.len();
    files.push(Lock16::new(LogFile { file, size: 0, start_time: time::OffsetDateTime::now_utc() }));
    Ok(index)
}

async fn log_updates(
    state: Arc<State>,
    log_dir: PathBuf,
    log_files: Arc<Lock16<Vec<Lock16<LogFile>>>>,
) {
    let mut log_file_created_at: Instant;

    {
        let mut lock = log_files.write_async().await;
        debug_assert!(lock.is_empty());
        create_log_file(&log_dir, &mut lock).await.expect("Failed to create log file");

        log_file_created_at = Instant::now();
    }

    let mut buffer = Vec::new();

    loop {
        if Arc::strong_count(&log_files) == 1 {
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

            if log_file_created_at.elapsed() >= std::time::Duration::from_hours(1) {
                let mut lock = log_files.write_async().await;

                create_log_file(&log_dir, &mut lock).await.expect("Failed to create log file");

                log_file_created_at = Instant::now();
            }

            let files_lock = log_files.read_async().await;
            let mut log_file = files_lock.last().unwrap().write_async().await;
            let LogFile { file, size: offset, .. } = &mut *log_file;

            let mut remaining = buffer.len();
            while remaining > 0 {
                let BufResult(result, buf) = file.write_at(buffer, *offset).await;
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
                *offset += n as u64;
                remaining -= n;
            }
        }

        stats_listener.await;
    }
}
