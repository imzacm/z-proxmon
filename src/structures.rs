use serde::{Deserialize, Serialize};

#[derive(Default, Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
pub struct StorageStats {
    pub usage: f32,
    pub total: u64,
    pub used: u64,
    pub available: u64,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CpuStats {
    pub name: String,
    pub usage: f32,
    pub vendor_id: String,
    pub brand: String,
    pub frequency: u64,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BaseSystemStats {
    pub cpu_usage: f32,
    pub cpus: Vec<CpuStats>,
    pub ram: StorageStats,
    pub swap: StorageStats,
    pub uptime: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DiskStats {
    pub kind: sysinfo::DiskKind,
    pub name: String,
    pub file_system: String,
    pub mount_point: String,
    pub is_removable: bool,
    pub is_read_only: bool,
    pub stats: StorageStats,
}

impl Default for DiskStats {
    fn default() -> Self {
        Self {
            kind: sysinfo::DiskKind::Unknown(0),
            name: String::new(),
            file_system: String::new(),
            mount_point: String::new(),
            is_removable: false,
            is_read_only: false,
            stats: StorageStats::default(),
        }
    }
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TemperatureStats {
    pub label: String,
    pub current: Option<f32>,
    pub max: Option<f32>,
    pub critical: Option<f32>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProxmoxLxcStats {
    pub vm_id: u32,
    pub pid: u32,
    pub name: String,
    pub base: BaseSystemStats,
    pub disk: DiskStats,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProxmoxNodeStats {
    pub io_delay: f64,
    pub lxc: Vec<ProxmoxLxcStats>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProcessCGroupInfo {
    pub lxc_vm_id: Option<u32>,
    pub lxc_name: Option<String>,
    pub docker_container_id: Option<String>,
    pub docker_container_name: Option<String>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GpuProcessStats {
    pub pid: u32,
    pub process_name: String,
    pub memory_used: Option<u64>,
    pub cgroup_info: ProcessCGroupInfo,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GpuStats {
    pub name: String,
    pub temperature: u32,
    pub utilization: u32,
    pub power_usage_watts: f32,
    pub memory: StorageStats,
    pub processes: Vec<GpuProcessStats>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProcessStats {
    pub pid: u32,
    pub name: String,
    pub cpu_usage: f32,
    pub memory_used: u64,
    pub cgroup_info: ProcessCGroupInfo,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SystemStats {
    #[serde(with = "time::serde::timestamp::option")]
    pub updated_at: Option<time::OffsetDateTime>,
    pub base: BaseSystemStats,
    pub disks: Vec<DiskStats>,
    pub temperatures: Vec<TemperatureStats>,
    pub proxmox: ProxmoxNodeStats,
    pub gpus: Vec<GpuStats>,
    pub processes: Vec<ProcessStats>,
}
