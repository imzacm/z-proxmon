use std::collections::HashMap;

use nvml_wrapper::Nvml;
use nvml_wrapper::enum_wrappers::device::TemperatureSensor;
use nvml_wrapper::enums::device::UsedGpuMemory;

use crate::structures::{
    GpuProcessStats, GpuStats, ProcessCGroupInfo, ProxmoxLxcStats, StorageStats,
};

pub async fn fetch_gpu_stats(
    nvml: &Nvml,
    processes: &HashMap<sysinfo::Pid, sysinfo::Process>,
    lxc_list: &[ProxmoxLxcStats],
) -> Result<Vec<GpuStats>, crate::Error> {
    let mut gpus = Vec::new();
    let device_count = nvml.device_count()?;

    for index in 0..device_count {
        let device = nvml.device_by_index(index)?;

        // 1. Gather Base GPU Metrics
        let name = device.name()?;
        let temp = device.temperature(TemperatureSensor::Gpu)?;
        let memory_info = device.memory_info()?;
        let utilization = device.utilization_rates()?.gpu;
        // NVML returns power in milliwatts
        let power_usage_watts = device.power_usage()? as f32 / 1000.0;

        // 2. Gather Running Processes
        let mut mapped_processes = Vec::new();

        let mut running_processes = Vec::new();

        match device.running_compute_processes() {
            Ok(procs) => running_processes = procs,
            Err(err) => eprintln!("Failed to get compute processes: {err:?}"),
        }

        match device.running_graphics_processes() {
            Ok(procs) => running_processes.extend(procs),
            Err(err) => eprintln!("Failed to get graphics processes: {err:?}"),
        }

        for proc in running_processes {
            let pid = proc.pid;
            // NVML gives us the process name if available
            let process_name = processes
                .get(&sysinfo::Pid::from_u32(pid))
                .map_or("Unknown".into(), |p| p.name().to_string_lossy());

            let memory_used = match proc.used_gpu_memory {
                UsedGpuMemory::Unavailable => None,
                UsedGpuMemory::Used(used) => Some(used),
            };

            // TODO: Parallelise this.
            let mut cgroup_info = ProcessCGroupInfo::from_pid(pid).await?;

            // If we found an ID, look up the name in our existing LXC stats list
            if let Some(lxc_vm_id) = cgroup_info.lxc_vm_id
                && let Some(lxc) = lxc_list.iter().find(|l| l.vm_id == lxc_vm_id)
            {
                cgroup_info.lxc_name = Some(lxc.name.clone());
            }

            mapped_processes.push(GpuProcessStats {
                pid,
                process_name: process_name.into_owned(),
                memory_used,
                cgroup_info,
            });
        }

        gpus.push(GpuStats {
            name,
            temperature: temp,
            utilization,
            power_usage_watts,
            memory: StorageStats {
                usage: (memory_info.used as f32 / memory_info.total as f32) * 100.0,
                total: memory_info.total,
                used: memory_info.used,
                available: memory_info.free,
            },
            processes: mapped_processes,
        });
    }

    Ok(gpus)
}
