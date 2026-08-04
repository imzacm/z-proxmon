use serde::{Deserialize, Serialize};
use tapo::responses::EnergyUsageResult;
use triomphe::Arc;

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
    pub pid: Option<u32>,
    pub name: String,
    pub base: BaseSystemStats,
    pub disk: DiskStats,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProxmoxNodeStats {
    pub io_delay: f64,
    pub lxc: Arc<Vec<ProxmoxLxcStats>>,
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

/// When one independently-gathered section was last refreshed, and whether its source has fallen
/// far enough behind its own cadence to be worth saying so on the page.
#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceStatus {
    #[serde(with = "time::serde::timestamp::option")]
    pub updated_at: Option<time::OffsetDateTime>,
    pub stale: bool,
}

/// One entry per source that fills part of [`SystemStats`].
///
/// Each is gathered by its own task at its own rate, so any one of them can fall behind - or stop
/// entirely, if whatever it asks never answers - while the rest carry on. This is what the page
/// reads to say which of its tables it should no longer be believed.
#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceStatuses {
    pub base: SourceStatus,
    pub processes: SourceStatus,
    pub disks: SourceStatus,
    pub temperatures: SourceStatus,
    pub io_delay: SourceStatus,
    pub lxc: SourceStatus,
    pub gpus: SourceStatus,
    pub cgroups: SourceStatus,
    pub energy: SourceStatus,
}

impl SourceStatuses {
    /// Every status against the name the log and the page know it by.
    pub fn named(&self) -> [(&'static str, &SourceStatus); 9] {
        [
            ("base", &self.base),
            ("processes", &self.processes),
            ("disks", &self.disks),
            ("temperatures", &self.temperatures),
            ("io_delay", &self.io_delay),
            ("lxc", &self.lxc),
            ("gpus", &self.gpus),
            ("cgroups", &self.cgroups),
            ("energy", &self.energy),
        ]
    }
}

/// The assembled picture the page and the log are handed.
///
/// The sections are shared pointers because they are gathered separately and most of them do not
/// change from one publish to the next: a section nothing has touched costs a refcount bump here
/// and another when [`ObservableLock`](z_sync::observable_lock::ObservableLock) clones this on its
/// way out, where it used to cost two deep copies of every string and vector in it.
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct SystemStats {
    #[serde(with = "time::serde::timestamp::option")]
    pub updated_at: Option<time::OffsetDateTime>,
    pub sources: SourceStatuses,
    pub base: Arc<BaseSystemStats>,
    pub disks: Arc<Vec<DiskStats>>,
    pub temperatures: Arc<Vec<TemperatureStats>>,
    pub proxmox: ProxmoxNodeStats,
    pub gpus: Arc<Vec<GpuStats>>,
    pub processes: Arc<Vec<ProcessStats>>,
    pub energy_usage: Option<EnergyUsageResult>,
}
