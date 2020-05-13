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

#[derive(Deserialize, Debug)]
pub struct Config {
    #[serde(default)]
    process_id: usize,
    #[serde(default = "Config::default_threads")]
    threads: usize,
    #[serde(default = "Config::default_hosts")]
    hosts: Vec<String>,
    #[serde(default = "Config::default_report")]
    report: bool,
    #[serde(default = "Config::default_seed")]
    seed: u64,
    #[serde(default = "Config::default_baselines_path")]
    baselines_path: PathBuf,
    #[serde(default = "Config::default_sketch_epsilon")]
    sketch_epsilon: f64,
    #[serde(default = "Config::default_timeout")]
    timeout: Option<u64>,
    #[serde(default = "Config::default_recall")]
    recall: f64,
    #[serde(default = "Config::default_repetition_batch")]
    repetition_batch: usize,
    #[serde(default = "Config::default_no_dedup")]
    pub no_dedup: bool,
    #[serde(default = "Config::default_no_verify")]
    pub no_verify: bool,
}

#[allow(dead_code)]
impl Config {
    pub fn help_str() -> &'static str {
        "Environment configuration:
            
            DANNY_THREADS     number of threads to be used in each process (default=1)
            DANNY_HOSTS       comma separated list of hosts:port on which 
                              to run (default=no hosts)
            DANNY_PROCESS_ID  in the context of multiple processes, the unique identifier
                              of the process, ranging from 0 until $DANNY_PROCESSES
            DANNY_SEED        The seed for the random number generator
            DANNY_SKETCH_EPSILON  The value of epsilon for the sketcher (if used)
            DANNY_BASELINES_PATH  The path to the baselines file
            DANNY_TIMEOUT     Number of seconds before killing a run (default: unbounded)
            DANNY_RECALL    Guaranteed recall (default: 0.5)
            DANNY_REPETITION_BATCH  The number of repetitions to squash into a distributed round
            DANNY_NO_DEDUP  Don't perform duplicate elimination
            DANNY_NO_VERIFY  Don't perform verification of distances
        "
    }

    pub fn get() -> Config {
        match envy::prefixed("DANNY_").from_env::<Config>() {
            Ok(config) => config,
            Err(error) => panic!("{:#?}", error),
        }
    }

    fn default_no_dedup() -> bool {
        false
    }

    fn default_no_verify() -> bool {
        false
    }

    fn default_repetition_batch() -> usize {
        1
    }

    pub fn get_repetition_batch(&self) -> usize {
        self.repetition_batch
    }

    fn default_timeout() -> Option<u64> {
        None
    }

    pub fn get_timeout(&self) -> Option<Duration> {
        self.timeout.map(|t| Duration::from_secs(t))
    }

    fn default_baselines_path() -> PathBuf {
        PathBuf::from("baselines.csv")
    }

    fn default_seed() -> u64 {
        98_768_473_876_234
    }

    fn default_sketch_epsilon() -> f64 {
        0.01
    }

    fn default_cost_balance() -> f64 {
        0.5
    }

    fn default_repetition_cost() -> f64 {
        100.0
    }

    fn default_sampling_factor() -> f64 {
        10.0
    }

    fn default_threads() -> usize {
        1
    }

    fn default_hosts() -> Vec<String> {
        Vec::new()
    }

    fn default_report() -> bool {
        false
    }

    fn default_recall() -> f64 {
        0.5
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
        self.process_id == 0
    }

    pub fn get_baselines_path(&self) -> PathBuf {
        self.baselines_path.clone()
    }

    pub fn get_recall(&self) -> f64 {
        return self.recall;
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

    pub fn get_sketch_epsilon(&self) -> f64 {
        self.sketch_epsilon
    }

    pub fn get_random_generator(&self, instance: usize) -> XorShiftRng {
        let mut seeder = StdRng::seed_from_u64(self.seed);
        let mut seed = seeder.next_u64();
        for _ in 0..instance {
            seed = seeder.next_u64();
        }
        XorShiftRng::seed_from_u64(seed)
    }

    pub fn get_seed(&self) -> u64 {
        self.seed
    }

    pub fn get_threads(&self) -> usize {
        self.threads
    }

    pub fn get_total_workers(&self) -> usize {
        if self.hosts.is_empty() {
            self.threads
        } else {
            self.hosts.len() * self.threads
        }
    }

    pub fn get_hosts(&self) -> &Vec<String> {
        &self.hosts
    }

    pub fn get_num_hosts(&self) -> usize {
        self.hosts.len()
    }
}

/// command line configuration for DANNY
#[derive(FromArgs)]
pub struct CmdlineConfig {
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
    #[argh(option, default = "5")]
    pub sketch_bits: usize,

    /// don't set this manually
    #[argh(option)]
    pub process_id: Option<usize>,

    /// number of threads to use
    #[argh(option)]
    pub threads: usize,

    /// the hosts to run on
    #[argh(option)]
    pub hosts: Vec<String>,

    /// the seed for the random number generator
    #[argh(option)]
    pub seed: u64,

    /// the number of bits to use for sketches
    #[argh(option)]
    pub sketch_epsilon: f64,

    /// the required recall for lsh algorithms
    #[argh(option)]
    pub recall: f64,

    // #[serde(default = "Config::default_no_dedup")]
    // pub no_dedup: bool,
    // #[serde(default = "Config::default_no_verify")]
    // pub no_verify: bool,
    /// the left dataset to be joined
    #[argh(positional)]
    pub left_path: String,

    /// the right dataset to be joined
    #[argh(positional)]
    pub right_path: String,
}

impl CmdlineConfig {
    pub fn get() -> CmdlineConfig {
        argh::from_env()
    }
}
