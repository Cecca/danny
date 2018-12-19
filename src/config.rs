use core::any::Any;
use timely::communication::allocator::generic::GenericBuilder;
use timely::communication::initialize::Configuration as TimelyConfig;

#[derive(Deserialize, Debug)]
pub struct Config {
    #[serde(default)]
    process_id: usize,
    #[serde(default = "Config::default_threads")]
    threads: usize,
    #[serde(default = "Config::default_processes")]
    processes: usize,
    #[serde(default = "Config::default_hosts")]
    hosts: Vec<String>,
    #[serde(default = "Config::default_report")]
    report: bool,
}

#[allow(dead_code)]
impl Config {
    pub fn get() -> Config {
        match envy::prefixed("DANNY_").from_env::<Config>() {
            Ok(config) => config,
            Err(error) => panic!("{:#?}", error),
        }
    }

    fn default_threads() -> usize {
        1
    }

    fn default_processes() -> usize {
        1
    }

    fn default_hosts() -> Vec<String> {
        Vec::new()
    }

    fn default_report() -> bool {
        false
    }

    pub fn get_timely_builder(&self) -> (Vec<GenericBuilder>, Box<dyn Any + 'static>) {
        let timely_config = if self.processes > 1 {
            let hosts: Vec<String> = if self.hosts.len() > 0 {
                self.hosts.clone()
            } else {
                (0..self.processes)
                    .map(|i| format!("localhost:{}", 2101 + i))
                    .collect()
            };
            assert!(hosts.len() == self.processes);
            println!(
                "Running on {:#?}, using {} threads in each process",
                hosts, self.threads
            );
            TimelyConfig::Cluster {
                threads: self.threads,
                process: self.process_id,
                addresses: hosts,
                report: self.report,
                log_fn: Box::new(|_| None),
            }
        } else if self.threads > 1 {
            println!("Running on {} threads", self.threads);
            TimelyConfig::Process(self.threads)
        } else {
            println!("Running on a single thread");
            TimelyConfig::Thread
        };
        match timely_config.try_build() {
            Ok(pair) => pair,
            Err(msg) => panic!("Error while configuring timely: {}", msg),
        }
    }
}
