#![deny(unused_imports, clippy::all)]
#![feature(thread_local)]

mod cgroup_info;
mod get_stats;
mod http_server;
mod io_wait;
mod monitor;
mod nvidia;
mod proxmox;
mod structures;

use triomphe::Arc;

#[derive(Debug)]
pub enum Error {
    Io { source: std::io::Error },
    InvalidString { source: std::string::FromUtf8Error },
    Status { stdout: String, stderr: String, status: std::process::ExitStatus },
    Json { source: serde_json::Error },
    Nvidia(nvml_wrapper::error::NvmlError),
}

impl From<std::io::Error> for Error {
    fn from(source: std::io::Error) -> Self {
        Error::Io { source }
    }
}

impl From<serde_json::Error> for Error {
    fn from(source: serde_json::Error) -> Self {
        Error::Json { source }
    }
}

impl From<nvml_wrapper::error::NvmlError> for Error {
    fn from(source: nvml_wrapper::error::NvmlError) -> Self {
        Error::Nvidia(source)
    }
}

fn main() {
    let port = std::env::args()
        .nth(1)
        .or_else(|| std::env::var("PORT").ok())
        .map(|s| s.parse::<u16>().unwrap())
        .unwrap_or(8080);

    compio::runtime::Runtime::new()
        .expect("Failed to create compio runtime")
        .block_on(async move {
            let state: Arc<monitor::State> = Arc::default();
            let monitor_future = monitor::monitor(&state);
            let server_future = http_server::start_server(port, state.clone());

            futures_util::join!(monitor_future, server_future);
        })
}
