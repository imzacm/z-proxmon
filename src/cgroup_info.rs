use std::ffi::OsStr;
use std::io::{Cursor, Write};
use std::num::NonZeroUsize;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::time::Instant;

use compio::io::AsyncBufRead;
use z_sync::{Lazy, Lock16};

use crate::structures::ProcessCGroupInfo;

impl ProcessCGroupInfo {
    pub async fn from_pid(pid: u32) -> Result<Self, crate::Error> {
        let mut info = get_cgroup_info_from_pid(pid).await?;

        if let Some(docker_container_id) = &info.docker_container_id {
            // A name we cannot get is worth carrying on without: the container id, the LXC it sits
            // in and the process itself are all still worth showing.
            info.docker_container_name =
                get_docker_container_name(info.lxc_vm_id, docker_container_id).await;
        }

        Ok(info)
    }
}

async fn get_cgroup_info_from_pid(pid: u32) -> Result<ProcessCGroupInfo, std::io::Error> {
    // "/proc/" (8) + pid (max 10) + "/cgroup" (7)
    let mut path_bytes = [0u8; 25];
    write!(&mut path_bytes[..], "/proc/{pid}/cgroup").unwrap();
    let null_index = path_bytes.iter().position(|&b| b == 0).unwrap();
    let path = Path::new(OsStr::from_bytes(&path_bytes[..null_index]));

    let file = compio::fs::File::open(path).await?;
    let mut reader = compio::io::BufReader::new(Cursor::new(file));

    // Scan lines for Proxmox LXC patterns (e.g., "0::/lxc/104/ns" or similar)
    loop {
        let buffer = reader.fill_buf().await?;
        if buffer.is_empty() {
            break;
        }

        // We need complete lines.
        let last_newline = buffer.iter().rposition(|&b| b == b'\n').unwrap();
        let buffer = &buffer[..=last_newline];

        let os_str = OsStr::from_bytes(buffer);
        let str = os_str.to_string_lossy();

        for line in str.lines() {
            let Some((_, rest)) = line.split_once("/lxc/") else { continue };
            let id_str = rest.split('/').next().unwrap_or("");

            let Ok(vm_id) = id_str.parse::<u32>() else { continue };

            let mut info = ProcessCGroupInfo {
                lxc_vm_id: Some(vm_id),
                lxc_name: None,
                docker_container_id: None,
                docker_container_name: None,
            };

            // Look for docker container ID
            if let Some(index) = line.find("docker-") {
                let start = index + 7;
                // Docker IDs are 64 characters.
                let end = start + 64;
                if line.len() >= end {
                    let container_id = &line[start..end];
                    info.docker_container_id = Some(container_id.into());
                }
            }
            // Fallback for older cgroupfs drivers
            else if let Some(index) = line.find("/docker/") {
                let start = index + 8;
                let end = start + 64;
                if line.len() >= end {
                    let container_id = &line[start..end];
                    info.docker_container_id = Some(container_id.to_string());
                }
            }

            return Ok(info);
        }

        let len = buffer.len();
        reader.consume(len);
    }

    Ok(ProcessCGroupInfo::default())
}

/// How long a container whose name could not be read is left alone before asking again.
///
/// `pct exec` into a container that is unwell can take a long time or never answer at all, and
/// before this a failure was not remembered: the same lookup ran again on the very next pass over
/// the same process, several times a second.
const NAME_RETRY_AFTER: std::time::Duration = std::time::Duration::from_secs(60);

/// What is known about one container id's name.
enum CachedName {
    /// Docker answered. A container id's name does not change, so this stands for the run.
    Known(String),
    /// The lookup failed, and is not worth trying again until this passes.
    Failed { until: Instant },
}

/// The container's name, or `None` if docker will not say - which is not worth failing over, so
/// the reason is logged here rather than handed back.
async fn get_docker_container_name(
    lxc_vm_id: Option<u32>,
    docker_container_id: &str,
) -> Option<String> {
    type Cache = lru::LruCache<String, CachedName>;

    const CAP: NonZeroUsize = NonZeroUsize::new(200).unwrap();

    // A process-wide cache shared across all runtime threads. `Lazy` defers the (non-const)
    // `LruCache` allocation to first use, and `Lock16` provides async-aware interior mutability so
    // waiters yield instead of parking a runtime thread.
    static CACHE: Lazy<Lock16<Cache>> = Lazy::new(|| Lock16::new(lru::LruCache::new(CAP)));

    {
        let mut cache = CACHE.write_async().await;
        match cache.get(docker_container_id) {
            Some(CachedName::Known(name)) => return Some(name.clone()),
            Some(CachedName::Failed { until }) if *until > Instant::now() => return None,
            _ => {}
        }
    }

    match run_docker_inspect(lxc_vm_id, docker_container_id).await {
        Ok(name) => {
            let mut cache = CACHE.write_async().await;
            cache.put(docker_container_id.into(), CachedName::Known(name.clone()));
            Some(name)
        }
        Err(error) => {
            eprintln!("Failed to name docker container {docker_container_id}: {error:?}");
            let mut cache = CACHE.write_async().await;
            cache.put(
                docker_container_id.into(),
                CachedName::Failed { until: Instant::now() + NAME_RETRY_AFTER },
            );
            None
        }
    }
}

async fn run_docker_inspect(
    lxc_vm_id: Option<u32>,
    docker_container_id: &str,
) -> Result<String, crate::Error> {
    let mut command = compio::process::Command::new("docker");
    if let Some(lxc_vm_id) = lxc_vm_id {
        command = compio::process::Command::new("pct");
        command.arg("exec").arg(lxc_vm_id.to_string()).args(["--", "docker"]);
    }

    let child = command
        .arg("inspect")
        .arg("--format")
        .arg("{{.Name}}")
        .arg(docker_container_id)
        .stdout(std::process::Stdio::piped())
        .unwrap()
        .stderr(std::process::Stdio::piped())
        .unwrap()
        .spawn()?;

    let output = child.wait_with_output().await?;

    if output.status.success() {
        let mut stdout = output.stdout;

        if stdout.first().copied() == Some(b'/') {
            stdout.remove(0);
        }

        if stdout.last().copied() == Some(b'\n') {
            stdout.pop();
        }

        return String::from_utf8(stdout).map_err(|source| crate::Error::InvalidString { source });
    }

    let stdout = String::from_utf8(output.stdout).unwrap_or_else(|error| {
        let output = error.into_bytes();
        String::from_utf8_lossy(&output).into_owned()
    });
    let stderr = String::from_utf8(output.stderr).unwrap_or_else(|error| {
        let output = error.into_bytes();
        String::from_utf8_lossy(&output).into_owned()
    });

    Err(crate::Error::Status { stdout, stderr, status: output.status })
}
