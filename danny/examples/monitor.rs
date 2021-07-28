use danny::sysmonitor::*;
use std::time::{Duration, Instant};

fn main() {
    let rate = Duration::from_secs(1);
    let monitor = MonitorThread::spawn(rate).unwrap();
    std::thread::sleep(Duration::from_secs(10));
    let usage = monitor.join();
    println!("{:#?}", usage)
}
