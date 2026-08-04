use std::sync::atomic::{AtomicUsize, Ordering};

use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use z_sync::Notify16;
use z_sync::observable_lock::ObservableLock32;

use crate::io_wait::IoWait;
use crate::nvidia::fetch_gpu_stats;
use crate::structures::{ProcessCGroupInfo, ProxmoxNodeStats, SystemStats};
use crate::tapo::TapoClient;

/// How far behind the stats may fall, while something is watching them, before it is said out
/// loud.
const STALL_WARNING: std::time::Duration = std::time::Duration::from_secs(30);

#[derive(Default)]
pub struct State {
    /// `…32` rather than `…16` for the width of the notification epoch. A `NotifyStateU16` counts
    /// notifications in eight bits, so a listener armed across a long wait misses its wakeup
    /// outright if the count wraps back onto the value it snapshotted - and an SSE stream is armed
    /// while it writes to its client, which is as long as that client takes to read. At four
    /// writes a second eight bits wrap every minute, where sixteen take four and a half hours.
    pub stats: ObservableLock32<SystemStats>,
    listener_count: AtomicUsize,
    listener_notify: Notify16,
}

impl State {
    /// Whether anything is currently watching, and so whether the monitor is running at all.
    pub fn has_listeners(&self) -> bool {
        self.listener_count.load(Ordering::Relaxed) > 0
    }

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

/// Says so in the log if the stats stop being refreshed while the page is watching.
///
/// A wedged cycle is otherwise silent: the streams stay open on their keep-alives and simply never
/// carry another update, so the page looks connected and reads as a dead server with nothing in
/// the journal to say which of the things the cycle waits on stopped answering.
pub async fn watch_for_stalls(state: &State) {
    let mut warned = false;

    loop {
        compio::time::sleep(STALL_WARNING).await;

        // Nothing watching means the cycle is parked on purpose, and stale stats are correct.
        let Some(updated_at) =
            state.stats.latest_value().updated_at.filter(|_| state.has_listeners())
        else {
            warned = false;
            continue;
        };

        let age = time::OffsetDateTime::now_utc() - updated_at;
        if age < STALL_WARNING {
            warned = false;
            continue;
        }

        // One line per stall, not one per check.
        if !warned {
            warned = true;
            eprintln!(
                "Stats have not been refreshed for {} seconds - the monitor cycle is waiting on \
                 something",
                age.whole_seconds()
            );
        }
    }
}

pub async fn monitor(state: &State, tapo_client: Option<TapoClient>) {
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

        // Read off the worker rather than awaited, so a plug on the end of a wifi link cannot hold
        // up the cycle. `None` simply means it has not answered since the last look.
        let energy_usage_result = tapo_client.as_ref().and_then(TapoClient::poll_energy);

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

            match energy_usage_result {
                Some(Ok(energy_usage)) => stats.energy_usage = Some(energy_usage),
                Some(Err(error)) => eprintln!("Energy usage error: {error:?}"),
                // No reading this time round: the plug answers on its own schedule now, so the
                // last one stands rather than the figure blinking out between readings.
                None => {}
            }

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
                    cgroup_info.lxc_name = stats.proxmox.lxc.iter().find_map(|l| {
                        if l.vm_id == lxc_vm_id { Some(l.name.clone()) } else { None }
                    });
                }

                let process = stats.processes.iter_mut().find(|p| p.pid == pid).unwrap();
                process.cgroup_info = cgroup_info;
            }
        }

        compio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
}
