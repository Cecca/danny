use core::any::Any;
use rand::rngs::StdRng;
use rand::RngCore;
use rand::SeedableRng;
use rand_xorshift::XorShiftRng;
use timely::communication::allocator::generic::GenericBuilder;
use timely::communication::initialize::Configuration as TimelyConfig;

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
            DANNY_REPORT      ???"
    }

    pub fn get() -> Config {
        match envy::prefixed("DANNY_").from_env::<Config>() {
            Ok(config) => config,
            Err(error) => panic!("{:#?}", error),
        }
    }

    fn default_seed() -> u64 {
        98768473876234
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
        if self.hosts.len() > 0 {
            let hn = self.hosts[0]
                .split(":")
                .next()
                .expect("Can't split the host string");
            Some(hn.to_owned())
        } else {
            None
        }
    }

    pub fn get_timely_builder(&self) -> (Vec<GenericBuilder>, Box<dyn Any + 'static>) {
        let timely_config = if self.hosts.len() > 1 {
            let hosts: Vec<String> = self.hosts.clone();
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

    pub fn get_random_generator(&self, instance: usize) -> XorShiftRng {
        let mut seeder = StdRng::seed_from_u64(self.seed);
        let mut seed = seeder.next_u64();
        for _ in 0..instance {
            seed = seeder.next_u64();
        }
        XorShiftRng::seed_from_u64(seed)
    }

    pub fn get_threads(&self) -> usize {
        self.threads
    }

    pub fn get_total_workers(&self) -> usize {
        if self.hosts.len() == 0 {
            self.threads
        } else {
            self.hosts.len() * self.threads
        }
    }
}
