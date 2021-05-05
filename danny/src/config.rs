use argh::FromArgs;
use rand::rngs::StdRng;
use rand::RngCore;
use rand::SeedableRng;
use rand_xorshift::XorShiftRng;
use std::{convert::TryFrom, thread::Thread, time::Duration};
use std::{fmt::Debug, time::Instant};

use std::process::Command;

use timely::communication::{Allocator, Configuration as TimelyConfig, WorkerGuards};
use timely::worker::Worker;

use crate::{experiment::Experiment, join::Balance};

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

    /// profile the code with the given frequency
    #[argh(option)]
    pub profile: Option<i32>,

    /// number of repetitions to squash together
    #[argh(option, default = "1")]
    pub repetition_batch: usize,

    /// what to load balance
    #[argh(option, default = "Balance::Load")]
    pub balance: Balance,

    /// wether to run again the given configuration,
    /// even if already present in the database
    #[argh(switch)]
    pub rerun: bool,

    /// kill the run if it takes more than the specified time, in seconds
    #[argh(option)]
    pub timeout: Option<u64>,

    /// the dataset to be self-joined
    #[argh(positional)]
    pub path: String,
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

    pub fn sha(&self) -> String {
        use sha2::Digest;
        let mut sha = sha2::Sha256::new();

        // IMPORTANT: Don't change the order of the following statements!
        sha.input(format!("{}", self.algorithm));
        sha.input(format!("{}", self.algorithm_version()));
        sha.input(format!("{}", self.threshold));
        sha.input(format!(
            "{}",
            self.k.map(|k| format!("{}", k)).unwrap_or("".to_owned())
        ));
        sha.input(format!(
            "{}",
            self.k2.map(|k2| format!("{}", k2)).unwrap_or("".to_owned())
        ));
        sha.input(format!("{}", self.sketch_bits));
        sha.input(format!("{}", self.sketch_epsilon));
        sha.input(format!("{}", self.threads));
        sha.input(format!("{}", self.hosts_string()));
        sha.input(format!("{}", self.seed));
        sha.input(format!("{}", self.recall));
        sha.input(format!("{:?}", self.balance));
        sha.input(format!("{}", self.repetition_batch));
        sha.input(format!("{}", self.path));

        if let Some(prof) = self.profile {
            sha.input(format!("{}", prof));
        }

        if let Some(timeout) = self.timeout {
            sha.input(format!("{}", timeout));
        }

        format!("{:x}", sha.result())
    }

    pub fn algorithm_version(&self) -> u8 {
        use crate::lsh::algorithms;
        match self.algorithm.as_ref() {
            "local-lsh" => algorithms::LOCAL_LSH_VERSION,
            "one-level-lsh" => algorithms::ONE_LEVEL_LSH_VERSION,
            "two-level-lsh" => algorithms::TWO_LEVEL_LSH_VERSION,
            "cartesian" => crate::baseline::ALL_2_ALL_VERSION,
            algorithm => panic!("don't know the version of {}", algorithm),
        }
    }

    pub fn master_hostname(&self) -> Option<String> {
        self.hosts.as_ref().map(|hosts| hosts.hosts[0].name.clone())
    }

    pub fn is_master(&self) -> bool {
        self.process_id.unwrap_or(0) == 0
    }

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
            let mut handles: Vec<std::process::Child> = self
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

            // now wait for the workers, with a timeout if configured
            if let Some(timeout) = self.timeout {
                let timeout = Duration::from_secs(timeout);
                let timer = Instant::now();
                loop {
                    if timer.elapsed() > timeout {
                        // report the experiment as timed out
                        Experiment::from_config(self.clone()).save_timed_out(timeout);

                        // kill all the ssh process
                        for mut h in handles {
                            h.kill().expect("problems killing the ssh process");
                        }
                        // most of the times the above killing is not sufficient, so we should
                        // go to each worker and manually kill each `danny` process
                        info!("spawning pkill calls");
                        let killers: Vec<std::process::Child> = self
                            .hosts
                            .as_ref()
                            .unwrap()
                            .hosts
                            .iter()
                            .enumerate()
                            .map(|(_, host)| {
                                Command::new("ssh")
                                    .arg(&host.name)
                                    .arg("pkill danny")
                                    .spawn()
                                    .expect("problem spawning the ssh process")
                            })
                            .collect();
                        for mut h in killers {
                            h.wait().expect("problem killing danny processes");
                        }

                        break;
                    }
                    if handles.iter_mut().all(|h| {
                        h.try_wait()
                            .expect("problem polling the ssh process")
                            .is_some()
                    }) {
                        info!("all workers done");
                        break;
                    }
                    std::thread::sleep(Duration::from_secs(1));
                }
            } else {
                for mut h in handles {
                    info!("Waiting for ssh process to finish");
                    h.wait().expect("problem waiting for the ssh process");
                }
                info!("All workers done");
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

pub fn parse_hosts(arg: &str) -> Result<Hosts, String> {
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
