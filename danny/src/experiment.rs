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
    network: Vec<(String, String, u32, u32)>,
    output_size: Option<u32>,
    total_time_ms: Option<u32>,
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

    pub fn set_output_size(&mut self, output_size: u32) {
        self.output_size.replace(output_size);
    }

    pub fn set_total_time_ms(&mut self, total_time_ms: u64) {
        self.total_time_ms.replace(total_time_ms as u32);
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
        self.network.push((host, iface, transmitted as u32, received as u32));
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
    fn get_db_path() -> std::path::PathBuf {
        let mut path = std::env::home_dir().expect("unable to get home directory");
        path.push("danny-results.sqlite");
        path
    }

    pub fn save(self) {
        let sha = self.sha();
        let dbpath = Self::get_db_path();
        let mut conn = Connection::open(dbpath).expect("error connecting to the database");
        create_tables_if_needed(&conn);

        let recall: Option<f64> = None;
        let speedup: Option<f64> = None;

        let tx = conn.transaction().expect("problem starting transaction");

        {
            // Insert into main table
            tx.execute(
                "INSERT INTO main (
                    sha,
                    date,
                    threshold,
                    algorithm,
                    k,
                    k2,
                    sketch_bits,
                    threads,
                    hosts,
                    sketch_epsilon,
                    required_recall,
                    no_dedup,
                    no_verify,
                    repetition_batch,
                    left_path,
                    right_path,

                    total_time_ms,
                    output_size,
                    recall,
                    speedup
                )
                 VALUES (
                     ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20
                 )",
                params![
                    sha, 
                    self.date.to_rfc3339(),
                    self.config.threshold,
                    self.config.algorithm,
                    self.config.k.map(|k| k as u32),
                    self.config.k2.map(|k2| k2 as u32),
                    self.config.sketch_bits as u32,
                    self.config.threads as u32,
                    self.config.hosts_string(),
                    self.config.sketch_epsilon,
                    self.config.recall,
                    self.config.no_dedup,
                    self.config.no_verify,
                    self.config.repetition_batch as u32,
                    self.config.left_path,
                    self.config.right_path,
                    self.total_time_ms,
                    self.output_size,
                    recall,
                    speedup
                    ],
            )
            .expect("error inserting into main table");

            // TODO Insert into counters table
            // let mut stmt = tx
            //     .prepare(
            //         "INSERT INTO counters ( sha, kind, step, count
            //         ) VALUES ( ?1, ?2, ?3, ?45 )",
            //     )
            //     .expect("failed to prepare statement");
            // for (kind, step, count) in self.step_counters.iter() {
            //     stmt.execute(params![sha, kind, step, *count as u32])
            //         .expect("Failure to insert into counters table");
            // }

            // TODO: insert into network table
        }

        tx.commit().expect("error committing insertions");
        conn.close().expect("error inserting into the database");
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
