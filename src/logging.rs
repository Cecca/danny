use crate::config::Config;
use env_logger::Builder;
use std::io::Write;
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
        .format(move |buf, record| {
            writeln!(buf, "[{}] {}: {}", hostname, record.level(), record.args())
        })
        .init();
}
