use std::cell::RefCell;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use std::time::{Instant,Duration};
use std::thread;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct MonitorThread {
    handle: thread::JoinHandle<Vec<(Duration, SystemUsage)>>,
    running: Arc<AtomicBool>,
}

impl MonitorThread {
    pub fn spawn(sampling_period: Duration) -> Option<MonitorThread> {
        if SystemSample::sample().is_none() {
            return None;
        }
        let running = Arc::new(AtomicBool::new(true));
        let r = Arc::clone(&running);
        let handle = thread::spawn(move || {
            let mut samples = Vec::new();
            let mut previous = SystemSample::sample().unwrap();
            let start = Instant::now();
            while r.load(Ordering::SeqCst) {
                std::thread::sleep(sampling_period);
                let current = SystemSample::sample().unwrap();
                let usage = SystemUsage::compute(&previous, &current, sampling_period);
                samples.push((start.elapsed(), usage));
                previous = current;
            }

            samples
        });
        Some(MonitorThread{handle, running})
    }

    pub fn join(self) -> Vec<(Duration, SystemUsage)> {
        self.running.store(false, Ordering::SeqCst);
        self.handle.join().expect("failed to join monitoring thread")
    }
}


#[derive(Abomonation, Clone, Copy, Debug)]
pub struct SystemUsage {
    cpu: CpuUsage,
    net: NetworkUsage,
    mem: MemorySample
}

impl SystemUsage {
    pub fn compute(prev: &SystemSample, current: &SystemSample, elapsed: Duration) -> SystemUsage {
        let secs = elapsed.as_secs_f64();
        SystemUsage {
            cpu: CpuUsage {
                user: (current.cpu.user - prev.cpu.user) as f64 / (current.cpu.total - prev.cpu.total) as f64,
                system: (current.cpu.system - prev.cpu.system) as f64 / (current.cpu.total - prev.cpu.total) as f64,
            },
            net: NetworkUsage {
                tx: (current.net.tx - prev.net.tx) as f64 / secs,
                rx: (current.net.rx - prev.net.rx) as f64 / secs,
            },
            mem: current.mem,
        }
    }
}

#[derive(Abomonation, Clone, Copy, Debug)]
pub struct CpuUsage {
    user: f64,
    system: f64,
}

#[derive(Abomonation, Clone, Copy, Debug)]
pub struct NetworkUsage {
    tx: f64,
    rx: f64,
}

#[derive(Abomonation, Clone, Copy, Debug)]
pub struct SystemSample {
    cpu: CpuSample,
    net: NetworkSample,
    mem: MemorySample,
}

impl SystemSample {
    pub fn sample() -> Option<Self> {
        CpuSample::sample().and_then(|cpu| {
            NetworkSample::sample().and_then(|net| {
                MemorySample::sample().and_then(|mem| Some(SystemSample { cpu, net, mem }))
            })
        })
    }
}

#[derive(Abomonation, Clone, Copy, Debug)]
pub struct CpuSample {
    user: u64,
    system: u64,
    total: u64,
}

thread_local! {
    static CPU_BUFFER: RefCell<Vec<u64>> = RefCell::new(Vec::new());
    static MEMORY_BUFFER: RefCell<String> = RefCell::new(String::new());
}

impl CpuSample {
    pub fn sample() -> Option<CpuSample> {
        CPU_BUFFER.with(|buf| {
            File::open("/proc/stat")
                .and_then(|f| {
                    let f = BufReader::new(f);
                    buf.borrow_mut().clear();
                    buf.borrow_mut().extend(
                        f.lines()
                            .map(|line| line.expect("problem reading line in `cat`"))
                            .next()
                            .unwrap()
                            .split_whitespace()
                            .skip(1)
                            .map(|s| {
                                s.parse::<u64>().expect("error parsing CPU stat")}),
                    );
                    let buf = buf.borrow();
                    let total: u64 = buf.iter().sum();
                    let user = buf[0];
                    let system = buf[2];
                    Ok(CpuSample {
                        user,
                        system,
                        total,
                    })
                })
                .ok()
        })
    }
}

#[derive(Abomonation, Clone, Copy, Debug)]
pub struct NetworkSample {
    tx: u64,
    rx: u64,
}

impl NetworkSample {
    pub fn sample() -> Option<NetworkSample> {
        File::open("/proc/net/dev")
            .and_then(|dev| {
                let dev = BufReader::new(dev);
                let mut rx: u64 = 0;
                let mut tx: u64 = 0;
                for line in dev.lines().skip(2) {
                    let line = line.expect("Error getting line");
                    let tokens: Vec<&str> = line.split_whitespace().collect();
                    rx += tokens[1].parse::<u64>().expect("Cannot parse as u64");
                    tx += tokens[9].parse::<u64>().expect("Cannot parse as u64");
                }
                Ok(NetworkSample { tx, rx })
            })
            .ok()
    }
}

#[derive(Abomonation, Clone, Copy, Debug)]
pub struct MemorySample {
    total: u64,
    used: u64,
}

impl MemorySample {
    fn get(s: &str, key: &str) -> u64 {
        s.lines()
            .filter(|l| l.starts_with(key))
            .next()
            .expect("missing key in memory string")
            .split_whitespace()
            .nth(1)
            .expect("missing data")
            .parse::<u64>()
            .expect("cannot parse")
    }

    pub fn sample() -> Option<Self> {
        MEMORY_BUFFER.with(|buf| {
            buf.borrow_mut().clear();
            File::open("/proc/meminfo")
                .and_then(|mut f| {
                    f.read_to_string(&mut buf.borrow_mut())
                        .expect("error reading memory file");
                    let buf = buf.borrow();
                    let total = Self::get(&buf, "MemTotal");
                    let free = Self::get(&buf, "MemFree");
                    let buffers = Self::get(&buf, "Buffers");
                    let cached = Self::get(&buf, "Cached");
                    let slab = Self::get(&buf, "Slab");
                    let used = total - free - buffers - cached - slab;
                    Ok(MemorySample { total, used })
                })
                .ok()
        })
    }
}
