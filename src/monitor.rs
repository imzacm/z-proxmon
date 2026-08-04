//! Gathering the statistics the page shows.
//!
//! Every statistic has its own source task, refreshing at its own rate and publishing into its own
//! slot; [`monitor`] waits for one of them to say something has changed, puts the sections
//! together, and publishes the result. Nothing here waits on anything else, which is the point: a
//! source that is slow, or that asks something which never answers, can only leave its own section
//! standing still. It used to be one loop through every source in turn, where the same thing
//! stopped the page dead.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::time::Duration;

use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use tapo::responses::EnergyUsageResult;
use triomphe::Arc;
use z_sync::observable_lock::ObservableLock32;
use z_sync::{AtomicArc, Notify16, Notify32};

use crate::get_stats::{fill_disks, fill_processes, fill_temperatures};
use crate::io_wait::IoWait;
use crate::nvidia::fetch_gpu_stats;
use crate::structures::{
    BaseSystemStats, DiskStats, GpuStats, ProcessCGroupInfo, ProcessStats, ProxmoxLxcStats,
    ProxmoxNodeStats, SourceStatus, SourceStatuses, SystemStats, TemperatureStats,
};
use crate::tapo::TapoClient;

// How often each source goes back to whatever it reads. They differ by an order of magnitude
// because the things behind them do: a CPU figure is worth four a second, and the container list
// costs a `pvesh` process to fetch and barely moves between one second and the next.
const SYSTEM_INTERVAL: Duration = Duration::from_millis(250);
const IO_DELAY_INTERVAL: Duration = Duration::from_millis(250);
const GPU_INTERVAL: Duration = Duration::from_millis(500);
const CGROUP_INTERVAL: Duration = Duration::from_millis(500);
const ENERGY_INTERVAL: Duration = Duration::from_secs(1);
const LXC_INTERVAL: Duration = Duration::from_secs(2);
const TEMPERATURE_INTERVAL: Duration = Duration::from_secs(2);
const DISK_INTERVAL: Duration = Duration::from_secs(5);

/// Floor on how often the picture is rebuilt.
///
/// The fast sources land within a few milliseconds of each other, and without this each of them
/// would cost a separate pass over every section and a separate event to every open page.
const MIN_PUBLISH_INTERVAL: Duration = Duration::from_millis(100);

/// How often the assembler looks up even though no source has said anything.
///
/// This is what lets a section go stale on the page: if the source behind it has stopped answering
/// altogether then nothing is going to wake anybody, and the staleness has to be noticed rather
/// than announced.
const HEARTBEAT: Duration = Duration::from_secs(1);

/// A source that has answered within three of its own intervals is doing fine; this is the floor
/// under that, so the fastest sources are not called stale over a momentary hiccup.
const MIN_STALE_AFTER_SECS: i64 = 10;

/// `last_ok` before a source has ever succeeded.
const NEVER: i64 = i64::MIN;

fn now_unix() -> i64 {
    time::OffsetDateTime::now_utc().unix_timestamp()
}

/// One section of the page, as last gathered by the source that owns it.
struct Slot<T> {
    /// Read without a lock, so the assembler never waits behind a source that is writing.
    value: AtomicArc<Arc<T>>,
    /// Unix seconds of the last successful gather, or [`NEVER`].
    last_ok: AtomicI64,
    interval: Duration,
}

impl<T: Default> Slot<T> {
    fn new(interval: Duration) -> Self {
        Self {
            value: AtomicArc::new(Arc::new(T::default())),
            last_ok: AtomicI64::new(NEVER),
            interval,
        }
    }
}

impl<T> Slot<T> {
    fn load(&self) -> Arc<T> {
        self.value.load()
    }

    /// Hand on a gather the source keeps its own copy of.
    ///
    /// A value equal to the one already published is not handed on: the source is recorded as
    /// having answered, but a section that has not moved wakes nobody and costs no copy.
    fn publish(&self, value: &T, changed: &Notify32)
    where
        T: Clone + PartialEq,
    {
        self.mark_ok();
        if *self.load() == *value {
            return;
        }
        self.value.store(Arc::new(value.clone()));
        changed.notify(usize::MAX);
    }

    /// [`Self::publish`] for a value built fresh each round, which can be handed on as it is.
    fn publish_owned(&self, value: T, changed: &Notify32)
    where
        T: PartialEq,
    {
        self.mark_ok();
        if *self.load() == value {
            return;
        }
        self.value.store(Arc::new(value));
        changed.notify(usize::MAX);
    }

    /// [`Self::publish_owned`] for a value that cannot be compared, so always handed on.
    fn publish_always(&self, value: T, changed: &Notify32) {
        self.mark_ok();
        self.value.store(Arc::new(value));
        changed.notify(usize::MAX);
    }

    fn mark_ok(&self) {
        self.last_ok.store(now_unix(), Ordering::Relaxed);
    }

    /// How long this source may go without answering before its section is not to be believed.
    fn stale_after_secs(&self) -> i64 {
        ((self.interval.as_secs_f64() * 3.0).ceil() as i64).max(MIN_STALE_AFTER_SECS)
    }

    /// When this section was last gathered, and whether it has fallen behind.
    ///
    /// `watching_for_secs` is how long something has been watching. Staleness is not judged before
    /// a source has had that long to answer, because the sources park when nobody is watching and
    /// every section would otherwise read as stale for the first moments after a page is opened.
    fn status(&self, now: time::OffsetDateTime, watching_for_secs: i64) -> SourceStatus {
        let threshold = self.stale_after_secs();

        let last_ok = self.last_ok.load(Ordering::Relaxed);
        let updated_at = (last_ok != NEVER)
            .then(|| time::OffsetDateTime::from_unix_timestamp(last_ok).unwrap_or(now));

        let stale = watching_for_secs > threshold
            && updated_at.is_none_or(|at| (now - at).whole_seconds() > threshold);

        SourceStatus { updated_at, stale }
    }
}

/// Every section, and the one notification that says any of them has moved.
struct Sources {
    /// Bumped by a source that has published something new. One notification for all of them
    /// keeps the assembler's wait to a single armed listener.
    changed: Notify32,
    base: Slot<BaseSystemStats>,
    processes: Slot<Vec<ProcessStats>>,
    disks: Slot<Vec<DiskStats>>,
    temperatures: Slot<Vec<TemperatureStats>>,
    io_delay: Slot<f64>,
    lxc: Slot<Vec<ProxmoxLxcStats>>,
    gpus: Slot<Vec<GpuStats>>,
    cgroups: Slot<HashMap<u32, ProcessCGroupInfo>>,
    energy: Slot<Option<EnergyUsageResult>>,
}

impl Default for Sources {
    fn default() -> Self {
        Self {
            changed: Notify32::new(),
            base: Slot::new(SYSTEM_INTERVAL),
            processes: Slot::new(SYSTEM_INTERVAL),
            disks: Slot::new(DISK_INTERVAL),
            temperatures: Slot::new(TEMPERATURE_INTERVAL),
            io_delay: Slot::new(IO_DELAY_INTERVAL),
            lxc: Slot::new(LXC_INTERVAL),
            gpus: Slot::new(GPU_INTERVAL),
            cgroups: Slot::new(CGROUP_INTERVAL),
            energy: Slot::new(ENERGY_INTERVAL),
        }
    }
}

impl Sources {
    fn snapshot(&self) -> Snapshot {
        Snapshot {
            base: self.base.load(),
            processes: self.processes.load(),
            disks: self.disks.load(),
            temperatures: self.temperatures.load(),
            io_delay: self.io_delay.load(),
            lxc: self.lxc.load(),
            gpus: self.gpus.load(),
            cgroups: self.cgroups.load(),
            energy: self.energy.load(),
        }
    }

    fn statuses(&self, now: time::OffsetDateTime, watching_for_secs: i64) -> SourceStatuses {
        SourceStatuses {
            base: self.base.status(now, watching_for_secs),
            processes: self.processes.status(now, watching_for_secs),
            disks: self.disks.status(now, watching_for_secs),
            temperatures: self.temperatures.status(now, watching_for_secs),
            io_delay: self.io_delay.status(now, watching_for_secs),
            lxc: self.lxc.status(now, watching_for_secs),
            gpus: self.gpus.status(now, watching_for_secs),
            cgroups: self.cgroups.status(now, watching_for_secs),
            energy: self.energy.status(now, watching_for_secs),
        }
    }
}

/// Every section as it stood at one moment, held by pointer.
struct Snapshot {
    base: Arc<BaseSystemStats>,
    processes: Arc<Vec<ProcessStats>>,
    disks: Arc<Vec<DiskStats>>,
    temperatures: Arc<Vec<TemperatureStats>>,
    io_delay: Arc<f64>,
    lxc: Arc<Vec<ProxmoxLxcStats>>,
    gpus: Arc<Vec<GpuStats>>,
    cgroups: Arc<HashMap<u32, ProcessCGroupInfo>>,
    energy: Arc<Option<EnergyUsageResult>>,
}

impl Snapshot {
    /// Whether nothing at all has moved since `other`, by pointer rather than by value: a source
    /// that republishes an equal section never gets this far, so identical pointers mean there is
    /// genuinely nothing new to send.
    fn is_same(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.base, &other.base)
            && Arc::ptr_eq(&self.processes, &other.processes)
            && Arc::ptr_eq(&self.disks, &other.disks)
            && Arc::ptr_eq(&self.temperatures, &other.temperatures)
            && Arc::ptr_eq(&self.io_delay, &other.io_delay)
            && Arc::ptr_eq(&self.lxc, &other.lxc)
            && Arc::ptr_eq(&self.gpus, &other.gpus)
            && Arc::ptr_eq(&self.cgroups, &other.cgroups)
            && Arc::ptr_eq(&self.energy, &other.energy)
    }
}

pub struct State {
    pub stats: ObservableLock32<SystemStats>,
    sources: Sources,
    listener_count: AtomicUsize,
    listener_notify: Notify16,
    /// Unix seconds of when the first of the current listeners arrived, or [`NEVER`] if nothing is
    /// watching.
    listeners_since: AtomicI64,
}

impl Default for State {
    fn default() -> Self {
        Self {
            stats: ObservableLock32::default(),
            sources: Sources::default(),
            listener_count: AtomicUsize::new(0),
            listener_notify: Notify16::new(),
            listeners_since: AtomicI64::new(NEVER),
        }
    }
}

impl State {
    pub fn add_listener(&self) -> impl Drop {
        struct Guard<'a> {
            state: &'a State,
        }

        impl Drop for Guard<'_> {
            fn drop(&mut self) {
                if self.state.listener_count.fetch_sub(1, Ordering::Relaxed) == 1 {
                    self.state.listeners_since.store(NEVER, Ordering::Relaxed);
                }
            }
        }

        if self.listener_count.fetch_add(1, Ordering::Relaxed) == 0 {
            self.listeners_since.store(now_unix(), Ordering::Relaxed);
        }
        self.listener_notify.notify(usize::MAX);
        Guard { state: self }
    }

    /// Park until something is watching. Every source starts its round with this, so a machine
    /// nobody has the page open on does no work at all.
    async fn wait_for_listeners(&self) {
        let mut listener = self.listener_notify.listener();
        while self.listener_count.load(Ordering::Relaxed) == 0 {
            listener.await;
            listener = self.listener_notify.listener();
        }
    }

    /// How long something has been watching, or `None` if nothing is.
    fn watching_for_secs(&self, now: time::OffsetDateTime) -> Option<i64> {
        match self.listeners_since.load(Ordering::Relaxed) {
            NEVER => None,
            since => Some(now.unix_timestamp() - since),
        }
    }
}

/// Says a source's failure once rather than on every round.
///
/// A source that cannot reach what it reads usually cannot reach it for a while, and at four
/// rounds a second that fills the journal with the same line. The section's staleness carries the
/// ongoing signal; this is only for saying what went wrong.
struct ErrorLog {
    source: &'static str,
    last: Option<String>,
}

impl ErrorLog {
    fn new(source: &'static str) -> Self {
        Self { source, last: None }
    }

    fn failed(&mut self, error: impl std::fmt::Debug) {
        let text = format!("{error:?}");
        if self.last.as_deref() != Some(text.as_str()) {
            eprintln!("{} error: {text}", self.source);
            self.last = Some(text);
        }
    }

    fn ok(&mut self) {
        self.last = None;
    }
}

/// Start every source, then keep the assembled picture up to date.
pub async fn monitor(state: Arc<State>, tapo_client: Option<TapoClient>) {
    compio::runtime::spawn(system_source(state.clone())).detach();
    compio::runtime::spawn(disk_source(state.clone())).detach();
    compio::runtime::spawn(temperature_source(state.clone())).detach();
    compio::runtime::spawn(io_delay_source(state.clone())).detach();
    compio::runtime::spawn(lxc_source(state.clone())).detach();
    compio::runtime::spawn(gpu_source(state.clone())).detach();
    compio::runtime::spawn(cgroup_source(state.clone())).detach();
    compio::runtime::spawn(energy_source(state.clone(), tapo_client)).detach();

    assemble(&state).await
}

/// CPU, memory and the processes using them.
async fn system_source(state: Arc<State>) {
    let mut system = sysinfo::System::new_all();
    let mut base = BaseSystemStats::default();
    let mut processes = Vec::new();

    loop {
        state.wait_for_listeners().await;

        // sysinfo's refresh blocks, so it goes to the pool rather than holding up this thread.
        let mut taken = std::mem::take(&mut system);
        system = compio::runtime::spawn_blocking(move || {
            taken.refresh_all();
            taken
        })
        .await
        .expect("Failed to refresh system stats");

        base.init_with_system(&system);
        state.sources.base.publish(&base, &state.sources.changed);

        fill_processes(&mut processes, &system);
        state.sources.processes.publish(&processes, &state.sources.changed);

        compio::time::sleep(SYSTEM_INTERVAL).await;
    }
}

/// Physical disks. Slow to read - a `statvfs` per mount - and slow to change.
async fn disk_source(state: Arc<State>) {
    let mut disks = sysinfo::Disks::new();
    let mut stats = Vec::new();

    loop {
        state.wait_for_listeners().await;

        let mut taken = std::mem::take(&mut disks);
        disks = compio::runtime::spawn_blocking(move || {
            taken.refresh(true);
            taken
        })
        .await
        .expect("Failed to refresh disks");

        fill_disks(&mut stats, &disks);
        state.sources.disks.publish(&stats, &state.sources.changed);

        compio::time::sleep(DISK_INTERVAL).await;
    }
}

async fn temperature_source(state: Arc<State>) {
    let mut components = sysinfo::Components::new();
    let mut temperatures = Vec::new();

    loop {
        state.wait_for_listeners().await;

        let mut taken = std::mem::take(&mut components);
        components = compio::runtime::spawn_blocking(move || {
            taken.refresh(true);
            taken
        })
        .await
        .expect("Failed to refresh components");

        fill_temperatures(&mut temperatures, &components);
        state.sources.temperatures.publish(&temperatures, &state.sources.changed);

        compio::time::sleep(TEMPERATURE_INTERVAL).await;
    }
}

async fn io_delay_source(state: Arc<State>) {
    let mut io_wait = IoWait::default();
    let mut log = ErrorLog::new("IO wait");

    loop {
        state.wait_for_listeners().await;

        match io_wait.update().await {
            Ok(io_delay) => {
                log.ok();
                state.sources.io_delay.publish_owned(io_delay, &state.sources.changed);
            }
            Err(error) => log.failed(error),
        }

        compio::time::sleep(IO_DELAY_INTERVAL).await;
    }
}

/// The node's containers.
///
/// This is the one that spawns a `pvesh` process, and the one most able to sit there forever if
/// the node's own services are unwell. Now that it is on its own, that costs the container table
/// and nothing else.
async fn lxc_source(state: Arc<State>) {
    let mut log = ErrorLog::new("Proxmox");

    loop {
        state.wait_for_listeners().await;

        match crate::proxmox::get_lxc().await {
            Ok(lxc) => {
                log.ok();
                state.sources.lxc.publish_owned(lxc, &state.sources.changed);
            }
            Err(error) => log.failed(error),
        }

        compio::time::sleep(LXC_INTERVAL).await;
    }
}

async fn gpu_source(state: Arc<State>) {
    let nvml = match nvml_wrapper::Nvml::init() {
        Ok(nvml) => Some(nvml),
        Err(error) => {
            eprintln!("Failed to initialize NVML: {error:?}");
            None
        }
    };

    let mut names = sysinfo::System::new();
    let mut log = ErrorLog::new("GPU stats");

    loop {
        state.wait_for_listeners().await;

        match &nvml {
            Some(nvml) => match fetch_gpu_stats(nvml, &mut names) {
                Ok(gpus) => {
                    log.ok();
                    state.sources.gpus.publish_owned(gpus, &state.sources.changed);
                }
                Err(error) => log.failed(error),
            },
            // No NVML on this machine. An empty list, kept current, so the page shows no GPUs
            // rather than a section that looks stuck.
            None => state.sources.gpus.publish_owned(Vec::new(), &state.sources.changed),
        }

        compio::time::sleep(GPU_INTERVAL).await;
    }
}

/// Which container each interesting process belongs to.
///
/// This is where `pct exec … docker inspect` happens, which used to run while the assembled stats
/// were held under their write lock. It is now nowhere near that lock, and a lookup that hangs
/// costs the container names on a few rows.
async fn cgroup_source(state: Arc<State>) {
    let mut log = ErrorLog::new("Process cgroup");

    loop {
        state.wait_for_listeners().await;

        // The pids worth resolving: the processes the page lists, and whatever is on the GPUs.
        // Both are lock-free reads of what those sources last published, never a wait on them.
        let processes = state.sources.processes.load();
        let gpus = state.sources.gpus.load();

        let mut pids: Vec<u32> = processes.iter().map(|process| process.pid).collect();
        pids.extend(gpus.iter().flat_map(|gpu| gpu.processes.iter()).map(|process| process.pid));
        pids.sort_unstable();
        pids.dedup();

        let mut futures = FuturesUnordered::new();
        for pid in pids {
            futures.push(async move { (pid, ProcessCGroupInfo::from_pid(pid).await) });
        }

        let mut cgroups = HashMap::new();
        while let Some((pid, result)) = futures.next().await {
            match result {
                Ok(info) => {
                    cgroups.insert(pid, info);
                }
                // The process ended between being listed and being looked up, which is the usual
                // way this fails and not worth a word.
                Err(crate::Error::Io { source })
                    if source.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => log.failed(error),
            }
        }

        state.sources.cgroups.publish_owned(cgroups, &state.sources.changed);

        compio::time::sleep(CGROUP_INTERVAL).await;
    }
}

async fn energy_source(state: Arc<State>, tapo_client: Option<TapoClient>) {
    let mut log = ErrorLog::new("Energy usage");

    loop {
        state.wait_for_listeners().await;

        match tapo_client.as_ref().and_then(TapoClient::poll_energy) {
            Some(Ok(energy_usage)) => {
                log.ok();
                state.sources.energy.publish_always(Some(energy_usage), &state.sources.changed);
            }
            Some(Err(error)) => log.failed(error),
            // With a plug, nothing new means it has not answered yet, and the section goes stale
            // if that lasts. With no plug there is nothing to wait for, so it is kept current and
            // empty.
            None if tapo_client.is_none() => {
                state.sources.energy.publish_always(None, &state.sources.changed)
            }
            None => {}
        }

        compio::time::sleep(ENERGY_INTERVAL).await;
    }
}

/// Wait for something to change, put the sections together, publish.
async fn assemble(state: &State) {
    let mut published: Option<Snapshot> = None;
    let mut statuses = SourceStatuses::default();
    let mut last_publish: Option<std::time::Instant> = None;

    loop {
        // Armed before anything is read, so a source publishing while this round is being put
        // together is waited on rather than missed.
        let changed = state.sources.changed.listener();

        let now = time::OffsetDateTime::now_utc();
        let watching_for = state.watching_for_secs(now);
        let snapshot = state.sources.snapshot();
        let next_statuses = state.sources.statuses(now, watching_for.unwrap_or(0));

        let sections_moved = published.as_ref().is_none_or(|last| !last.is_same(&snapshot));
        if sections_moved || next_statuses != statuses {
            // Only while something is watching: with nobody there the sources park on purpose and
            // every section reads as fresh again, which is not a source recovering.
            if watching_for.is_some() {
                report_staleness(&statuses, &next_statuses);
            }
            statuses = next_statuses;

            publish(state, &snapshot, &statuses, now).await;

            published = Some(snapshot);
            last_publish = Some(std::time::Instant::now());
        }

        // Woken by a source, or by the heartbeat - which is what lets a section go stale on the
        // page once its source has stopped saying anything at all.
        futures_util::future::select(changed, std::pin::pin!(compio::time::sleep(HEARTBEAT))).await;

        if let Some(since) = last_publish.map(|at| at.elapsed())
            && since < MIN_PUBLISH_INTERVAL
        {
            compio::time::sleep(MIN_PUBLISH_INTERVAL - since).await;
        }
    }
}

/// Put the sections together and hand the result to the page and the log.
async fn publish(
    state: &State,
    snapshot: &Snapshot,
    statuses: &SourceStatuses,
    now: time::OffsetDateTime,
) {
    // The joins are lookups between sections that are already in hand - no files, no processes,
    // nothing that can block - and they are done before the lock is taken rather than under it.
    let mut processes = (*snapshot.processes).clone();
    for process in &mut processes {
        process.cgroup_info = cgroup_info_for(process.pid, snapshot);
    }

    let mut gpus = (*snapshot.gpus).clone();
    for process in gpus.iter_mut().flat_map(|gpu| gpu.processes.iter_mut()) {
        process.cgroup_info = cgroup_info_for(process.pid, snapshot);
    }

    let mut stats = state.stats.write_async().await;

    stats.updated_at = Some(now);
    stats.sources = statuses.clone();

    // Sections nothing has touched are a refcount bump each, here and again when the lock clones
    // this on its way out to the page.
    stats.base = snapshot.base.clone();
    stats.disks = snapshot.disks.clone();
    stats.temperatures = snapshot.temperatures.clone();
    stats.proxmox = ProxmoxNodeStats { io_delay: *snapshot.io_delay, lxc: snapshot.lxc.clone() };
    stats.energy_usage = (*snapshot.energy).clone();

    stats.gpus = Arc::new(gpus);
    stats.processes = Arc::new(processes);
}

/// What is known about the container a pid sits in, with the LXC's name filled in from the
/// container list.
fn cgroup_info_for(pid: u32, snapshot: &Snapshot) -> ProcessCGroupInfo {
    let Some(info) = snapshot.cgroups.get(&pid) else {
        return ProcessCGroupInfo::default();
    };

    let mut info = info.clone();
    if let Some(lxc_vm_id) = info.lxc_vm_id {
        info.lxc_name = snapshot
            .lxc
            .iter()
            .find_map(|lxc| if lxc.vm_id == lxc_vm_id { Some(lxc.name.clone()) } else { None });
    }
    info
}

/// One line when a source stops answering, one when it starts again.
///
/// Without this a wedged source is invisible from the journal: the page's streams stay open on
/// their keep-alives and one of its tables quietly stops moving.
fn report_staleness(before: &SourceStatuses, after: &SourceStatuses) {
    for ((name, before), (_, after)) in before.named().into_iter().zip(after.named()) {
        if before.stale == after.stale {
            continue;
        }

        if after.stale {
            eprintln!("{name} has stopped refreshing - that section of the page is out of date");
        } else {
            eprintln!("{name} is refreshing again");
        }
    }
}
