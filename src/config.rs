use core::any::Any;
use rand::rngs::StdRng;
use rand::RngCore;
use rand::SeedableRng;
use rand_xorshift::XorShiftRng;
use std::path::PathBuf;
use std::process::Command;
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
    #[serde(default = "Config::default_estimator_samples")]
    estimator_samples: usize,
    #[serde(default = "Config::default_cost_balance")]
    cost_balance: f64,
    #[serde(default = "Config::default_bloom_elements")]
    bloom_elements: usize,
    #[serde(default = "Config::default_bloom_fpp")]
    bloom_fpp: f64,
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
            DANNY_ESTIMATOR_SAMPLES  The number of vectors to sample _in each worker_ to
                                     estimate the best k value
            DANNY_COST_BALANCE In the adaptive algorithm, a number less than 1 gives more weight
                               to the collisions, a number larger than 1 penalizes the repetitions
            DANNY_BLOOM_ELEMENTS   Number of elements expected in the bloom filters (power of two)
            DANNY_BLOOM_FPP    False positive rate of the bloom filter
        "
    }

    pub fn get() -> Config {
        match envy::prefixed("DANNY_").from_env::<Config>() {
            Ok(config) => config,
            Err(error) => panic!("{:#?}", error),
        }
    }

    fn default_estimator_samples() -> usize {
        100
    }

    fn default_bloom_elements() -> usize {
        30
    }

    pub fn get_bloom_elements(&self) -> usize {
        self.bloom_elements
    }

    fn default_bloom_fpp() -> f64 {
        0.05
    }

    pub fn get_bloom_fpp(&self) -> f64 {
        self.bloom_fpp
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
        1.0
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

    pub fn get_estimator_samples(&self) -> usize {
        self.estimator_samples
    }

    pub fn get_cost_balance(&self) -> f64 {
        self.cost_balance
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

#[derive(Debug, Clone, Copy)]
pub enum ParamK {
    Exact(usize),
    Adaptive(usize, usize),
}

impl ParamK {
    pub fn to_string(&self) -> String {
        match self {
            ParamK::Exact(k) => format!("Exact({})", k),
            ParamK::Adaptive(min_k, max_k) => format!("Adaptive({},{})", min_k, max_k),
        }
    }
}

pub struct CmdlineConfig {
    pub measure: String,
    pub threshold: f64,
    pub left_path: String,
    pub right_path: String,
    pub algorithm: String,
    pub k: Option<ParamK>,
    pub sketch_bits: Option<usize>,
}

impl CmdlineConfig {
    pub fn get() -> CmdlineConfig {
        let matches = clap_app!(danny =>
            (version: "0.1")
            (author: "Matteo Ceccarello <mcec@itu.dk>")
            (about: format!("Distributed Approximate Near Neighbours, Yo!\n\n{}", Config::help_str()).as_ref())
            (@arg ALGORITHM: -a --algorithm +takes_value "The algorithm to be used: (fixed-lsh, all-2-all)")
            (@arg MEASURE: -m --measure +required +takes_value "The similarity measure to be used")
            (@arg K: -k +takes_value "The number of concatenations of the hash function")
            (@arg ADAPTIVE_K: --("adaptive-k") +takes_value "The max number of concatenations of the hash function in the adaptive algorithm: auto sets it. Overridden by -k")
            (@arg THRESHOLD: -r --range +required +takes_value "The similarity threshold")
            (@arg BITS: --("sketch-bits") +takes_value "The number of bits to use for sketching")
            (@arg LEFT: +required "Path to the left hand side of the join")
            (@arg RIGHT: +required "Path to the right hand side of the join")
        )
        .get_matches();

        let measure = matches
            .value_of("MEASURE")
            .expect("measure is a required argument")
            .to_owned();
        let threshold: f64 = matches
            .value_of("THRESHOLD")
            .expect("range is a required argument")
            .parse()
            .expect("Cannot convert the threshold into a f64");
        let left_path = matches
            .value_of("LEFT")
            .expect("left is a required argument")
            .to_owned();
        let right_path = matches
            .value_of("RIGHT")
            .expect("right is a required argument")
            .to_owned();
        let algorithm = matches
            .value_of("ALGORITHM")
            .unwrap_or("all-2-all")
            .to_owned();
        let k = matches
            .value_of("K")
            .map(|k_str| {
                let _k = k_str
                    .parse::<usize>()
                    .expect("k should be an unsigned integer");
                ParamK::Exact(_k)
            })
            .or_else(|| {
                matches.value_of("ADAPTIVE_K").map(|adaptive_k_str| {
                    if adaptive_k_str.contains(',') {
                        let mut tokens = adaptive_k_str.split(',');
                        let min_k = tokens
                            .next()
                            .unwrap()
                            .parse::<usize>()
                            .expect("k should be an unsigned integer");
                        let max_k = tokens
                            .next()
                            .unwrap()
                            .parse::<usize>()
                            .expect("k should be an unsigned integer");
                        ParamK::Adaptive(min_k, max_k)
                    } else {
                        let _k = adaptive_k_str
                            .parse::<usize>()
                            .expect("k should be an unsigned integer");
                        ParamK::Adaptive(0, _k)
                    }
                })
            });
        let sketch_bits = matches.value_of("BITS").map(|bits_str| {
            bits_str
                .parse::<usize>()
                .expect("The number of bits should be an integer")
        });
        CmdlineConfig {
            measure,
            threshold,
            left_path,
            right_path,
            algorithm,
            k,
            sketch_bits,
        }
    }

    pub fn get_sketch_bits(&self) -> usize {
        self.sketch_bits.unwrap_or(1024)
    }
}
