use crate::config::*;
use crate::version;
use chrono::prelude::*;
use rusqlite::*;
use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Write;
use std::path::PathBuf;

pub struct Experiment {
    date: DateTime<Utc>,
    config: Config,
    // Table with Counter name, step, and count
    step_counters: Vec<(String, u32, u64)>,
    // Hostname, interface, transmitted, received
    network: Vec<(String, String, usize, usize)>,
    output_size: Option<usize>,
    total_time_ms: Option<u64>,
}

impl Experiment {
    pub fn from_config(config: Config) -> Experiment {
        Self {
            date: Utc::now(),
            config,
            step_counters: Vec::new(),
            network: Vec::new(),
            output_size: None,
            total_time_ms: None,
        }
        // let experiment = Experiment::new()
        //     .tag("threads_per_worker", config.threads)
        //     .tag("num_hosts", config.get_num_hosts())
        //     .tag("total_threads", config.get_total_workers())
        //     .tag("seed", config.seed)
        //     .tag("sketch_epsilon", config.sketch_epsilon)
        //     .tag("required_recall", config.recall)
        //     .tag("no_dedup", config.no_dedup)
        //     .tag("no_verify", config.no_verify)
        //     // .tag("measure", cmdline.measure.clone())
        //     .tag("threshold", config.threshold)
        //     .tag("left_path", config.left_path.clone())
        //     .tag("right_path", config.right_path.clone())
        //     .tag("algorithm", config.algorithm.clone())
        //     .tag("git_revision", version::short_sha())
        //     .tag("git_commit_date", version::commit_date());
        // let experiment = if config.k.is_some() {
        //     let k_str = config.k.unwrap().to_string().clone();
        //     let exp = experiment.tag("k", k_str);
        //     if config.k2.is_some() {
        //         let k2_str = config.k2.unwrap().to_string().clone();
        //         exp.tag("k2", k2_str)
        //     } else {
        //         exp
        //     }
        // } else {
        //     experiment
        // };
        // // if cmdline.sketch_bits.is_some() {
        // experiment.tag("sketch_bits", config.sketch_bits)
        // // } else {
        // //     experiment
        // // }
    }

    pub fn set_output_size(&mut self, output_size: usize) {
        self.output_size.replace(output_size);
    }

    pub fn set_total_time_ms(&mut self, total_time_ms: u64) {
        self.total_time_ms.replace(total_time_ms);
    }
    pub fn append_step_counter(&mut self, kind: String, step: u32, count: u64) {
        self.step_counters.push((kind, step, count));
    }

    pub fn append_network_info(
        &mut self,
        host: String,
        iface: String,
        transmitted: usize,
        received: usize,
    ) {
        self.network.push((host, iface, transmitted, received));
    }

    fn sha(&self) -> String {
        use sha2::Digest;
        let datestr = self.date.to_rfc2822();
        let mut sha = sha2::Sha256::new();
        sha.input(datestr);
        // I know that the following is implementation-dependent, but I just need
        // to have a identifier to join different tables created in this run.
        sha.input(format!("{:?}", self.config));
        sha.input(format!("{:?}", self.step_counters));

        format!("{:x}", sha.result())[..6].to_owned()
    }

    pub fn save(self) {
        unimplemented!()
        // let json_str =
        //     serde_json::to_string(&self).expect("Error converting the experiment to string");
        // info!("Writing result file");
        // let mut file = OpenOptions::new()
        //     .create(true)
        //     .append(true)
        //     .open("results.json")
        //     .expect("Error opening file");
        // file.write_all(json_str.as_bytes())
        //     .expect("Error writing data");
        // file.write_all(b"\n").expect("Error writing final newline");
        // info!("Results file written");
    }
}

fn create_tables_if_needed(conn: &Connection) {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS result (
            sha           TEXT PRIMARY KEY,
            date          TEXT NOT NULL,
            threshold     REAL NOT NULL,
            algorithm     TEXT NOT NULL,
            k             INTEGER,
            k2            INTEGER,
            sketch_bits   INTEGER,
            threads       INTEGER,
            hosts         TEXT NOT NULL,
            sketch_epsilon  REAL NOT NULL,
            required_recall  REAL NOT NULL,
            no_dedup      BOOL,
            no_verify     BOOL,
            repetition_batch    INTEGER,
            left_path      TEXT NOT NULL,
            right_path      TEXT NOT NULL,

            total_time_ms    INTEGER,
            output_size      INTEGER,
            recall           REAL,
            speedup          REAL
            )",
        params![],
    )
    .expect("Error creating main table");

    // conn.execute(
    //     "CREATE VIEW IF NOT EXISTS main_recent AS
    //     SELECT sha, max(date) AS date, seed, threads, hosts, dataset, algorithm, parameters, diameter, total_time_ms
    //     FROM main
    //     GROUP BY seed, threads, hosts, dataset, algorithm, parameters",
    //     params![]
    // )
    // .expect("Error creating the main_recent view");

    conn.execute(
        "CREATE TABLE IF NOT EXISTS counters (
            sha       TEXT NOT NULL,
            kind   TEXT NOT NULL,
            step      INTEGER NOT NULL,
            count     INTEGER NOT NULL,
            FOREIGN KEY (sha) REFERENCES main (sha)
            )",
        params![],
    )
    .expect("error creating counters table");
}
