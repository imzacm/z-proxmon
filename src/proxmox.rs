use serde::Deserialize;

use crate::structures::{
    BaseSystemStats, DiskStats, ProxmoxLxcStats, ProxmoxNodeStats, StorageStats,
};

impl ProxmoxNodeStats {
    pub async fn get() -> Result<Self, crate::Error> {
        let mut lxc = get_node_stats::<LxcJson>("localhost", "lxc").await?;

        lxc.data.sort_unstable_by_key(|item| item.vmid);

        Ok(Self {
            io_delay: 0.0,
            lxc: lxc.data.into_iter().map(LxcItem::into_stats).collect(),
        })
    }
}

async fn get_node_stats<T>(node: &str, ty: &str) -> Result<T, crate::Error>
where
    T: serde::de::DeserializeOwned,
{
    let child = compio::process::Command::new("pvesh")
        .arg("get")
        .arg(format!("/nodes/{}/{}", node, ty))
        .arg("--output-format")
        .arg("json")
        .stdout(std::process::Stdio::piped())
        .unwrap()
        .stderr(std::process::Stdio::piped())
        .unwrap()
        .spawn()?;

    let output = child.wait_with_output().await?;

    if output.status.success() {
        let stdout = String::from_utf8(output.stdout)
            .map_err(|source| crate::Error::InvalidString { source })?;
        return Ok(serde_json::from_str(&stdout)?);
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

#[derive(Debug, Deserialize)]
struct LxcItem {
    cpu: u8,
    // cpus: u8,
    disk: u64,
    // diskread: u64,
    // diskwrite: u64,
    maxdisk: u64,
    maxmem: u64,
    maxswap: u64,
    mem: u64,
    name: String,
    // netin: u64,
    // netout: u64,
    pid: u32,
    // pressurecpufull: String,
    // pressurecpusome: String,
    // pressureiofull: String,
    // pressureiosome: String,
    // pressurememoryfull: String,
    // pressurememorysome: String,
    // status: String,
    swap: u64,
    // tags: String,
    uptime: u64,
    vmid: u32,
}

impl LxcItem {
    fn into_stats(self) -> ProxmoxLxcStats {
        let make_storage_stats = |used: u64, total: u64| -> StorageStats {
            let usage = if total > 0 { (used as f32) / (total as f32) } else { 0.0 };

            StorageStats { usage, total, used, available: total - used }
        };

        ProxmoxLxcStats {
            vm_id: self.vmid,
            pid: self.pid,
            name: self.name,
            base: BaseSystemStats {
                cpu_usage: self.cpu as f32,
                cpus: Vec::new(),
                ram: make_storage_stats(self.mem, self.maxmem),
                swap: make_storage_stats(self.swap, self.maxswap),
                uptime: self.uptime,
            },
            disk: DiskStats {
                kind: sysinfo::DiskKind::Unknown(0),
                name: "LXC Root Disk".to_string(),
                file_system: "unknown".to_string(),
                mount_point: "/".to_string(),
                is_removable: false,
                is_read_only: false,
                stats: make_storage_stats(self.disk, self.maxdisk),
            },
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(transparent)]
struct LxcJson {
    data: Vec<LxcItem>,
}
