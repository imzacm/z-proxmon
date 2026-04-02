use std::borrow::Cow;

use crate::structures::{
    BaseSystemStats, CpuStats, DiskStats, ProcessCGroupInfo, ProcessStats, StorageStats,
    SystemStats, TemperatureStats,
};

impl StorageStats {
    pub fn new_ram(system: &sysinfo::System) -> Self {
        let mut this = Self {
            usage: 0.0,
            total: system.total_memory(),
            used: system.used_memory(),
            available: system.available_memory(),
        };
        this.usage = this.used as f32 / this.total as f32;
        this
    }

    pub fn new_swap(system: &sysinfo::System) -> Self {
        let mut this = Self {
            usage: 0.0,
            total: system.total_swap(),
            used: system.used_swap(),
            available: system.free_swap(),
        };
        this.usage = this.used as f32 / this.total as f32;
        this
    }

    pub fn from_disk(disk: &sysinfo::Disk) -> Self {
        let mut this = Self {
            usage: 0.0,
            total: disk.total_space(),
            used: 0,
            available: disk.available_space(),
        };
        this.used = disk.total_space() - disk.available_space();
        this.usage = this.used as f32 / this.total as f32;
        this
    }
}

impl CpuStats {
    pub fn init_from_cpu(&mut self, cpu: &sysinfo::Cpu) {
        self.usage = cpu.cpu_usage();

        if self.name != cpu.name() {
            self.name.clear();
            self.name.push_str(cpu.name());
        }

        if self.vendor_id != cpu.vendor_id() {
            self.vendor_id.clear();
            self.vendor_id.push_str(cpu.vendor_id());
        }

        if self.brand != cpu.brand() {
            self.brand.clear();
            self.brand.push_str(cpu.brand());
        }

        self.frequency = cpu.frequency();
    }
}

impl BaseSystemStats {
    pub fn init_with_system(&mut self, system: &sysinfo::System) {
        self.cpu_usage = system.global_cpu_usage();

        self.cpus.resize(system.cpus().len(), CpuStats::default());
        for (cpu, stats) in system.cpus().iter().zip(self.cpus.iter_mut()) {
            stats.init_from_cpu(cpu);
        }

        self.ram = StorageStats::new_ram(system);
        self.swap = StorageStats::new_swap(system);
        self.uptime = sysinfo::System::uptime();
    }
}

impl DiskStats {
    pub fn init_with_disk(&mut self, disk: &sysinfo::Disk) {
        self.kind = disk.kind();

        if disk.name() != self.name.as_str() {
            match disk.name().to_string_lossy() {
                Cow::Borrowed(name) => {
                    self.name.clear();
                    self.name.push_str(name);
                }
                Cow::Owned(name) => {
                    self.name = name;
                }
            }
        }

        if disk.file_system() != self.file_system.as_str() {
            match disk.file_system().to_string_lossy() {
                Cow::Borrowed(fs) => {
                    self.file_system.clear();
                    self.file_system.push_str(fs);
                }
                Cow::Owned(fs) => {
                    self.file_system = fs;
                }
            }
        }

        if disk.mount_point() != self.mount_point.as_str() {
            match disk.mount_point().to_string_lossy() {
                Cow::Borrowed(mp) => {
                    self.mount_point.clear();
                    self.mount_point.push_str(mp);
                }
                Cow::Owned(mp) => {
                    self.mount_point = mp;
                }
            }
        }

        self.is_removable = disk.is_removable();
        self.is_read_only = disk.is_read_only();
        self.stats = StorageStats::from_disk(disk);
    }
}

impl TemperatureStats {
    pub fn init_with_component(&mut self, component: &sysinfo::Component) {
        if component.label() != self.label.as_str() {
            self.label.clear();
            self.label.push_str(component.label());
        }
        self.current = component.temperature();
        self.max = component.max();
        self.critical = component.critical();
    }
}

impl ProcessStats {
    pub fn init_with_process(&mut self, process: &sysinfo::Process) {
        self.pid = process.pid().as_u32();

        if process.name() != self.name.as_str() {
            match process.name().to_string_lossy() {
                Cow::Borrowed(name) => {
                    self.name.clear();
                    self.name.push_str(name);
                }
                Cow::Owned(name) => {
                    self.name = name;
                }
            }
        }

        self.cpu_usage = process.cpu_usage();
        self.memory_used = process.memory();
        self.cgroup_info = ProcessCGroupInfo::default();
    }
}

impl SystemStats {
    pub fn update_system(&mut self, system: &sysinfo::System) {
        self.base.init_with_system(system);

        self.processes.resize(system.processes().len(), ProcessStats::default());
        for (process, stats) in system.processes().values().zip(self.processes.iter_mut()) {
            stats.init_with_process(process);
        }
        self.processes.sort_unstable_by(|a, b| {
            b.cpu_usage.partial_cmp(&a.cpu_usage).unwrap_or(std::cmp::Ordering::Equal)
        });
        self.processes.truncate(10);
    }

    pub fn update_disks(&mut self, disks: &sysinfo::Disks) {
        self.disks.resize(disks.len(), DiskStats::default());
        for (disk, stats) in disks.iter().zip(self.disks.iter_mut()) {
            stats.init_with_disk(disk);
        }
    }

    pub fn update_components(&mut self, components: &sysinfo::Components) {
        self.temperatures.resize(components.len(), TemperatureStats::default());
        for (component, stats) in components.iter().zip(self.temperatures.iter_mut()) {
            stats.init_with_component(component);
        }
    }
}
