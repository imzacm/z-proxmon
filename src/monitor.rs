use std::sync::atomic::{AtomicUsize, Ordering};

use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use z_sync::Notify16;
use z_sync::observable_lock::ObservableLock16;

use crate::io_wait::IoWait;
use crate::nvidia::fetch_gpu_stats;
use crate::structures::{ProcessCGroupInfo, ProxmoxNodeStats, SystemStats};

#[derive(Default)]
pub struct State {
    pub stats: ObservableLock16<SystemStats>,
    listener_count: AtomicUsize,
    listener_notify: Notify16,
}

impl State {
    pub fn add_listener(&self) -> impl Drop {
        struct Guard<'a> {
            count: &'a AtomicUsize,
        }

        impl Drop for Guard<'_> {
            fn drop(&mut self) {
                self.count.fetch_sub(1, Ordering::Relaxed);
            }
        }

        self.listener_count.fetch_add(1, Ordering::Relaxed);
        self.listener_notify.notify(usize::MAX);
        Guard { count: &self.listener_count }
    }
}

pub async fn monitor(state: &State) {
    let mut system = sysinfo::System::new_all();
    let mut disks = sysinfo::Disks::new();
    let mut components = sysinfo::Components::new();
    let mut io_wait = IoWait::default();

    let nvml = match nvml_wrapper::Nvml::init() {
        Ok(nvml) => Some(nvml),
        Err(error) => {
            eprintln!("Failed to initialize NVML: {error:?}");
            None
        }
    };

    loop {
        let mut listener = state.listener_notify.listener();
        while state.listener_count.load(Ordering::Relaxed) == 0 {
            listener.await;
            listener = state.listener_notify.listener();
        }

        let mut system_take = std::mem::take(&mut system);
        let system_future = compio::runtime::spawn_blocking(move || {
            system_take.refresh_all();
            system_take
        });

        let mut disks_take = std::mem::take(&mut disks);
        let disks_future = compio::runtime::spawn_blocking(move || {
            disks_take.refresh(true);
            disks_take
        });

        let mut components_take = std::mem::take(&mut components);
        let components_future = compio::runtime::spawn_blocking(move || {
            components_take.refresh(true);
            components_take
        });

        let io_wait_future = io_wait.update();

        let proxmox_future = ProxmoxNodeStats::get();

        let (system_result, disks_result, components_results, io_wait_result, proxmox_result) = futures_util::join!(
            system_future,
            disks_future,
            components_future,
            io_wait_future,
            proxmox_future
        );
        system = system_result.unwrap();
        disks = disks_result.unwrap();
        components = components_results.unwrap();

        let mut proxmox = ProxmoxNodeStats::default();

        match proxmox_result {
            Ok(p) => proxmox = p,
            Err(error) => eprintln!("Proxmox error: {error:?}"),
        }

        match io_wait_result {
            Ok(io_delay) => proxmox.io_delay = io_delay,
            Err(error) => eprintln!("IO wait error: {error:?}"),
        }

        let mut gpus = Vec::new();
        if let Some(nvml) = &nvml {
            match fetch_gpu_stats(nvml, system.processes(), &proxmox.lxc).await {
                Ok(stats) => gpus = stats,
                Err(error) => eprintln!("GPU stats error: {error:?}"),
            }
        }

        {
            let mut stats = state.stats.write_async().await;
            stats.updated_at = Some(time::OffsetDateTime::now_utc());
            stats.update_system(&system);
            stats.update_disks(&disks);
            stats.update_components(&components);

            stats.proxmox = proxmox;
            stats.gpus = gpus;

            let mut futures = FuturesUnordered::new();
            for process in &stats.processes {
                let pid = process.pid;
                futures.push(async move {
                    let result = ProcessCGroupInfo::from_pid(pid).await;
                    (pid, result)
                });
            }

            while let Some((pid, result)) = futures.next().await {
                let mut cgroup_info = match result {
                    Ok(info) => info,
                    Err(error) => {
                        eprintln!("Failed to get LXC ID for process {pid}: {error:?}");
                        continue;
                    }
                };

                if let Some(lxc_vm_id) = cgroup_info.lxc_vm_id {
                    let lxc_name = stats
                        .proxmox
                        .lxc
                        .iter()
                        .find_map(
                            |l| if l.vm_id == lxc_vm_id { Some(l.name.clone()) } else { None },
                        )
                        .unwrap();

                    cgroup_info.lxc_name = Some(lxc_name);
                }

                let process = stats.processes.iter_mut().find(|p| p.pid == pid).unwrap();
                process.cgroup_info = cgroup_info;
            }
        }

        compio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
}
