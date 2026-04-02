use compio::BufResult;
use compio::fs::File;
use compio::io::AsyncReadAtExt;

#[derive(Default, Debug)]
pub struct IoWait {
    stat_file: Option<File>,
    total: f64,
    io_wait: f64,
}

impl IoWait {
    pub async fn update(&mut self) -> Result<f64, std::io::Error> {
        if self.stat_file.is_none() {
            let file = File::open("/proc/stat").await?;
            self.stat_file = Some(file);
        }
        let stat_file = self.stat_file.as_ref().unwrap();

        // Absolute maximum length of a cpuN line in /proc/stat is 224 bytes.
        let BufResult(result, buffer) = stat_file.read_exact_at([0u8; 224], 0).await;
        result?;

        let line = std::str::from_utf8(&buffer).expect("Invalid UTF-8 in /proc/stat");
        assert!(line.starts_with("cpu"));
        let parts = line.split_whitespace()
            // Skip "cpu".
            .skip(1)
            .filter_map(|s| s.parse().ok())
            .collect::<Vec<f64>>();

        // iowait is the 5th column (index 4)
        if parts.len() < 5 {
            return Ok(0.0);
        }

        let iowait = parts[4];
        let total: f64 = parts.iter().sum();

        let delta_iowait = iowait - self.io_wait;
        let delta_total = total - self.total;

        let mut real_io_delay = 0.0;

        // Avoid division by zero on the very first update
        if delta_total > 0.0 && self.total > 0.0 {
            real_io_delay = (delta_iowait / delta_total) * 100.0;
        }

        self.io_wait = iowait;
        self.total = total;

        Ok(real_io_delay)
    }
}

/*
// 1. Put these OUTSIDE your main polling loop to store the previous state
let mut prev_iowait: f64 = 0.0;
let mut prev_total: f64 = 0.0;

// ... inside your loop { ... } ...

// 2. Read the first line of /proc/stat
if let Ok(stat_data) = std::fs::read_to_string("/proc/stat") {
    if let Some(cpu_line) = stat_data.lines().next() {
        // iowait is the 5th column (index 4)
        if parts.len() >= 5 {
            let iowait = parts[4];
            let total: f64 = parts.iter().sum();

            let delta_iowait = iowait - prev_iowait;
            let delta_total = total - prev_total;

            let mut real_io_delay = 0.0;

            // Avoid division by zero on the very first loop iteration
            if delta_total > 0.0 && prev_total > 0.0 {
                real_io_delay = (delta_iowait / delta_total) * 100.0;
            }

            // Save current values for the next tick
            prev_iowait = iowait;
            prev_total = total;

            // 3. Write this to your state!
            if let Ok(mut w) = monitor_state.write() {
                w.proxmox.io_delay = real_io_delay;
            }
        }
    }
}
*/
