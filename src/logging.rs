use crate::config::Config;
use env_logger::Builder;
use heapsize::HeapSizeOf;
use std::io::Write;
use std::ops::Deref;
use std::process::Command;

fn get_hostname() -> String {
    let output = Command::new("hostname")
        .output()
        .expect("Failed to run the hostname command");
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

// This is the place to hook into if we want to use syslog
pub fn init_logging(_conf: &Config) -> () {
    let hostname = get_hostname();
    Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .format(move |buf, record| {
            writeln!(buf, "[{}] {}: {}", hostname, record.level(), record.args())
        })
        .init();
    log_panics::init();
}

pub trait ToSpaceString {
    fn to_space_string(self) -> String;
}

impl ToSpaceString for usize {
    fn to_space_string(self) -> String {
        let bytes = self;
        if bytes >= 1024 * 1024 * 1024 {
            format!("{:.2} Gb", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
        } else if bytes >= 1024 * 1024 {
            format!("{:.2} Mb", bytes as f64 / (1024.0 * 1024.0))
        } else if bytes >= 1024 {
            format!("{:.2} Kb", bytes as f64 / 1024.0)
        } else {
            format!("{} bytes", bytes)
        }
    }
}
