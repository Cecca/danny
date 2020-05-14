use argh::FromArgs;
use core::any::Any;
use rand::rngs::StdRng;
use rand::RngCore;
use rand::SeedableRng;
use rand_xorshift::XorShiftRng;
use std::convert::TryFrom;
use std::fmt::Debug;
use std::path::Path;
use std::process::{Child, Command};
use timely::communication::allocator::generic::GenericBuilder;
use timely::communication::{Allocator, Configuration as TimelyConfig, WorkerGuards};
use timely::worker::Worker;

pub fn get_hostname() -> String {
    let output = Command::new("hostname")
        .output()
        .expect("Failed to run the hostname command");
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

/// command line configuration for DANNY
#[derive(FromArgs, Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    /// the similarity threshold
    #[argh(option)]
    pub threshold: f64,

    /// the algortihm to be used
    #[argh(option)]
    pub algorithm: String,

    /// the value of k for lsh algorithms
    #[argh(option)]
    pub k: Option<usize>,

    /// the value of k2 for lsh algorithms
    #[argh(option)]
    pub k2: Option<usize>,

    /// the number of sketch bits for lsh algorithms
    #[argh(option, default = "0")]
    pub sketch_bits: usize,

    /// don't set this manually
    #[argh(option)]
    pub process_id: Option<usize>,

    /// number of threads to use
    #[argh(option, default = "1")]
    pub threads: usize,

    /// the hosts to run on
    #[argh(option, from_str_fn(parse_hosts))]
    pub hosts: Option<Hosts>,

    /// the seed for the random number generator
    #[argh(option, default = "4258726345")]
    pub seed: u64,

    /// the number of bits to use for sketches
    #[argh(option, default = "0.01")]
    pub sketch_epsilon: f64,

    /// the required recall for lsh algorithms
    #[argh(option, default = "0.8")]
    pub recall: f64,

    /// don't remove duplicates from the output
    #[argh(switch)]
    pub no_dedup: bool,

    /// don't verify output pairs
    #[argh(switch)]
    pub no_verify: bool,

    /// number of repetitions to squash together
    #[argh(option, default = "1")]
    pub repetition_batch: usize,

    /// the left dataset to be joined
    #[argh(positional)]
    pub left_path: String,

    /// the right dataset to be joined
    #[argh(positional)]
    pub right_path: String,
}

impl Config {
    /// If the command line contains a single argument (other than the command name)
    /// tries to decode the first argument into a `Config` struct. If this fails,
    /// proceeds to reading the command line arguments.
    pub fn get() -> Config {
        match std::env::args().nth(1) {
            Some(arg1) => match Config::decode(&arg1) {
                Some(config) => config,
                None => argh::from_env(),
            },
            None => argh::from_env(),
        }
    }

    fn encode(&self) -> String {
        base64::encode(&bincode::serialize(&self).unwrap())
    }

    fn decode(string: &str) -> Option<Self> {
        let bytes = base64::decode(string).ok()?;
        bincode::deserialize(&bytes).ok()
    }

    pub fn master_hostname(&self) -> Option<String> {
        self.hosts.as_ref().map(|hosts| hosts.hosts[0].name.clone())
    }

    pub fn is_master(&self) -> bool {
        self.process_id.unwrap_or(0) == 0
    }

    // pub fn get_timely_builder(&self) -> (Vec<GenericBuilder>, Box<dyn Any + 'static>) {
    //     // let timely_config = if self.hosts.len() > 1 {
    //     //     let hosts: Vec<String> = self.hosts.clone();
    //     //     info!(
    //     //         "Running on {:?}, using {} threads in each process",
    //     //         hosts, self.threads
    //     //     );
    //     //     TimelyConfig::Cluster {
    //     //         threads: self.threads,
    //     //         process: self.process_id.expect("process id must be set"),
    //     //         addresses: hosts,
    //     //         report: false,
    //     //         log_fn: Box::new(|_| None),
    //     //     }
    //     // } else if self.threads > 1 {
    //     //     println!("Running on {} threads", self.threads);
    //     //     TimelyConfig::Process(self.threads)
    //     // } else {
    //     //     println!("Running on a single thread");
    //     //     TimelyConfig::Thread
    //     // };
    //     // match timely_config.try_build() {
    //     //     Ok(pair) => pair,
    //     //     Err(msg) => panic!("Error while configuring timely: {}", msg),
    //     // }
    //     unimplemented!("REMOVE")
    // }

    pub fn get_random_generator(&self, instance: usize) -> XorShiftRng {
        let mut seeder = StdRng::seed_from_u64(self.seed);
        let mut seed = seeder.next_u64();
        for _ in 0..instance {
            seed = seeder.next_u64();
        }
        XorShiftRng::seed_from_u64(seed)
    }

    pub fn get_total_workers(&self) -> usize {
        if let Some(hosts) = self.hosts.as_ref() {
            hosts.hosts.len() * self.threads
        } else {
            self.threads
        }
    }

    pub fn hosts_string(&self) -> String {
        self.hosts
            .as_ref()
            .map(|hosts| hosts.to_strings().join("__"))
            .unwrap_or(String::new())
    }

    pub fn get_num_hosts(&self) -> usize {
        if let Some(hosts) = self.hosts.as_ref() {
            hosts.hosts.len()
        } else {
            1
        }
    }

    fn with_process_id(&self, process_id: usize) -> Self {
        Self {
            process_id: Some(process_id),
            ..self.clone()
        }
    }

    pub fn execute<T, F>(&self, func: F) -> Option<Result<WorkerGuards<T>, String>>
    where
        T: Send + 'static,
        F: Fn(&mut Worker<Allocator>) -> T + Send + Sync + 'static,
    {
        if self.hosts.is_some() && self.process_id.is_none() {
            let exec = std::env::args().nth(0).unwrap();
            info!("spawning executable {:?}", exec);
            // This is the top level invocation, which should spawn the processes with ssh
            let handles: Vec<std::process::Child> = self
                .hosts
                .as_ref()
                .unwrap()
                .hosts
                .iter()
                .enumerate()
                .map(|(pid, host)| {
                    let encoded_config = self.with_process_id(pid).encode();
                    info!("Connecting to {}", host.name);
                    Command::new("ssh")
                        .arg(&host.name)
                        .arg(&exec)
                        .arg(encoded_config)
                        .spawn()
                        .expect("problem spawning the ssh process")
                })
                .collect();

            for mut h in handles {
                info!("Waiting for ssh process to finish");
                h.wait().expect("problem waiting for the ssh process");
            }

            None
        } else {
            info!("Worker invocation");
            let c = match &self.hosts {
                None => {
                    if self.threads == 1 {
                        TimelyConfig::Thread
                    } else {
                        TimelyConfig::Process(self.threads)
                    }
                }
                Some(hosts) => TimelyConfig::Cluster {
                    threads: self.threads,
                    process: self.process_id.expect("missing process id"),
                    addresses: hosts.to_strings(),
                    report: false,
                    log_fn: Box::new(|_| None),
                },
            };
            Some(timely::execute(c, func))
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Host {
    name: String,
    port: String,
}

impl Host {
    fn to_string(&self) -> String {
        format!("{}:{}", self.name, self.port)
    }
}

impl TryFrom<&str> for Host {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let mut tokens = value.split(":");
        let name = tokens.next().ok_or("missing host part")?.to_owned();
        let port = tokens.next().ok_or("missing port part")?.to_owned();
        Ok(Self { name, port })
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Hosts {
    hosts: Vec<Host>,
}

impl Hosts {
    fn to_strings(&self) -> Vec<String> {
        self.hosts.iter().map(|h| h.to_string()).collect()
    }
}

fn parse_hosts(arg: &str) -> Result<Hosts, String> {
    use std::fs::File;
    use std::io::{BufRead, BufReader};
    use std::path::PathBuf;

    let path = PathBuf::from(arg);
    if path.is_file() {
        let f = File::open(path).or(Err("error opening hosts file"))?;
        let reader = BufReader::new(f);
        let mut hosts = Vec::new();
        for line in reader.lines() {
            let line = line.or(Err("error reading line"))?;
            if line.len() > 0 {
                let host = Host::try_from(line.as_str())?;
                hosts.push(host);
            }
        }
        Ok(Hosts { hosts })
    } else {
        let tokens = arg.split(",");
        let mut hosts = Vec::new();
        for token in tokens {
            let host = Host::try_from(token)?;
            hosts.push(host);
        }
        Ok(Hosts { hosts })
    }
}

#[derive(Debug)]
pub enum ExecError {
    /// Not actually an error
    RemoteExecution,
    /// Actually an error, with message
    Error(String),
}
