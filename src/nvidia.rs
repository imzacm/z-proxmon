use nvml_wrapper::Nvml;
use nvml_wrapper::enum_wrappers::device::TemperatureSensor;
use nvml_wrapper::enums::device::UsedGpuMemory;

use crate::structures::{GpuProcessStats, GpuStats, StorageStats};

/// Every GPU, and what is running on it.
///
/// `names` is this source's own `System`, kept only to put a name to the handful of pids the
/// driver reports - it is refreshed for those pids alone, so this asks the kernel for a few
/// processes rather than all of them. Which container each of those pids belongs to is left to
/// whoever assembles the page's picture, since that is a lookup against sections other sources
/// publish and nothing this one should be made to wait for.
pub fn fetch_gpu_stats(
    nvml: &Nvml,
    names: &mut sysinfo::System,
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
            let memory_used = match proc.used_gpu_memory {
                UsedGpuMemory::Unavailable => None,
                UsedGpuMemory::Used(used) => Some(used),
            };

            mapped_processes.push(GpuProcessStats {
                pid: proc.pid,
                // Filled in below, once every pid on every device is known and they can be looked
                // up in one refresh.
                process_name: String::new(),
                memory_used,
                cgroup_info: Default::default(),
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

    fill_process_names(&mut gpus, names);

    Ok(gpus)
}

/// Put a name to every pid the driver reported, in one refresh for all of them.
fn fill_process_names(gpus: &mut [GpuStats], names: &mut sysinfo::System) {
    let pids: Vec<sysinfo::Pid> = gpus
        .iter()
        .flat_map(|gpu| gpu.processes.iter())
        .map(|process| sysinfo::Pid::from_u32(process.pid))
        .collect();

    if pids.is_empty() {
        return;
    }

    names.refresh_processes_specifics(
        sysinfo::ProcessesToUpdate::Some(&pids),
        true,
        sysinfo::ProcessRefreshKind::nothing(),
    );

    for process in gpus.iter_mut().flat_map(|gpu| gpu.processes.iter_mut()) {
        let name = names
            .process(sysinfo::Pid::from_u32(process.pid))
            .map_or("Unknown".into(), |p| p.name().to_string_lossy());

        process.process_name.clear();
        process.process_name.push_str(&name);
    }
}
