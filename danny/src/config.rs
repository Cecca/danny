use argh::FromArgs;
use core::any::Any;
use rand::rngs::StdRng;
use rand::RngCore;
use rand::SeedableRng;
use rand_xorshift::XorShiftRng;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;
use timely::communication::allocator::generic::GenericBuilder;
use timely::communication::initialize::Configuration as TimelyConfig;

pub fn get_hostname() -> String {
    let output = Command::new("hostname")
        .output()
        .expect("Failed to run the hostname command");
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

/// command line configuration for DANNY
#[derive(FromArgs)]
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
    #[argh(option)]
    pub hosts: Vec<String>,

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
    pub fn get() -> Config {
        argh::from_env()
    }

    pub fn master_hostname(&self) -> Option<String> {
        if !self.hosts.is_empty() {
            let hn = self.hosts[0]
                .split(':')
                .next()
                .expect("Can't split the host string");
            Some(hn.to_owned())
        } else {
            None
        }
    }

    pub fn is_master(&self) -> bool {
        self.process_id.unwrap_or(0) == 0
    }

    pub fn get_timely_builder(&self) -> (Vec<GenericBuilder>, Box<dyn Any + 'static>) {
        let timely_config = if self.hosts.len() > 1 {
            let hosts: Vec<String> = self.hosts.clone();
            info!(
                "Running on {:?}, using {} threads in each process",
                hosts, self.threads
            );
            TimelyConfig::Cluster {
                threads: self.threads,
                process: self.process_id.expect("process id must be set"),
                addresses: hosts,
                report: false,
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

    pub fn get_random_generator(&self, instance: usize) -> XorShiftRng {
        let mut seeder = StdRng::seed_from_u64(self.seed);
        let mut seed = seeder.next_u64();
        for _ in 0..instance {
            seed = seeder.next_u64();
        }
        XorShiftRng::seed_from_u64(seed)
    }

    pub fn get_total_workers(&self) -> usize {
        if self.hosts.is_empty() {
            self.threads
        } else {
            self.hosts.len() * self.threads
        }
    }

    pub fn get_num_hosts(&self) -> usize {
        self.hosts.len()
    }
}
