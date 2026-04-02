use std::cell::{LazyCell, RefCell};
use std::ffi::OsStr;
use std::io::{Cursor, Write};
use std::num::NonZeroUsize;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use compio::io::AsyncBufRead;

use crate::structures::ProcessCGroupInfo;

impl ProcessCGroupInfo {
    pub async fn from_pid(pid: u32) -> Result<Self, crate::Error> {
        let mut info = get_cgroup_info_from_pid(pid).await?;

        if let Some(docker_container_id) = &info.docker_container_id {
            let name = get_docker_container_name(info.lxc_vm_id, docker_container_id).await?;
            info.docker_container_name = Some(name);
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

async fn get_docker_container_name(
    lxc_vm_id: Option<u32>,
    docker_container_id: &str,
) -> Result<String, crate::Error> {
    type Cache = lru::LruCache<String, String>;

    const CAP: NonZeroUsize = NonZeroUsize::new(200).unwrap();

    #[thread_local]
    static CACHE: LazyCell<RefCell<Cache>> =
        LazyCell::new(|| RefCell::new(lru::LruCache::new(CAP)));

    {
        let mut cache = CACHE.borrow_mut();
        if let Some(name) = cache.get(docker_container_id) {
            return Ok(name.clone());
        }
    }

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

    {
        let mut cache = CACHE.borrow_mut();
        if let Some(name) = cache.get(docker_container_id) {
            return Ok(name.clone());
        }
    }

    if output.status.success() {
        let mut stdout = output.stdout;

        if stdout.first().copied() == Some(b'/') {
            stdout.remove(0);
        }

        if stdout.last().copied() == Some(b'\n') {
            stdout.pop();
        }

        let name =
            String::from_utf8(stdout).map_err(|source| crate::Error::InvalidString { source })?;

        {
            let mut cache = CACHE.borrow_mut();
            cache.put(docker_container_id.into(), name.clone());
        }

        return Ok(name);
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
