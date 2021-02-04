use crate::config::*;
use chrono::prelude::*;

use rusqlite::*;
use std::path::{Path, PathBuf};

pub struct Experiment {
    db_path: PathBuf,
    date: DateTime<Utc>,
    config: Config,
    // Table with Counter name, step, and count
    step_counters: Vec<(String, u32, i64)>,
    // Hostname, interface, transmitted, received
    network: Vec<(String, String, i64, i64)>,
    output_size: Option<u32>,
    total_time_ms: Option<u32>,
    recall: Option<f64>,
    speedup: Option<f64>,
}

impl Experiment {
    pub fn from_config(config: Config) -> Experiment {
        Self {
            db_path: Self::default_db_path(),
            date: Utc::now(),
            config,
            step_counters: Vec::new(),
            network: Vec::new(),
            output_size: None,
            total_time_ms: None,
            recall: None,
            speedup: None,
        }
    }

    pub fn with_date(self, date: DateTime<Utc>) -> Self {
        Self { date, ..self }
    }

    pub fn with_database<P: AsRef<Path>>(self, path: P) -> Self {
        Self {
            db_path: path.as_ref().into(),
            ..self
        }
    }

    pub fn set_output_size(&mut self, output_size: u32) {
        self.output_size.replace(output_size);
    }

    pub fn set_total_time_ms(&mut self, total_time_ms: u64) {
        self.total_time_ms.replace(total_time_ms as u32);
    }

    pub fn set_recall_speedup(&mut self, recall: f64, speedup: f64) {
        self.recall.replace(recall);
        self.speedup.replace(speedup);
    }

    pub fn append_step_counter(&mut self, kind: String, step: u32, count: i64) {
        self.step_counters.push((kind, step, count));
    }

    pub fn append_network_info(
        &mut self,
        host: String,
        iface: String,
        transmitted: i64,
        received: i64,
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

        format!("{:x}", sha.result())
    }

    fn default_db_path() -> std::path::PathBuf {
        #[allow(deprecated)]
        let mut path = std::env::home_dir().expect("unable to get home directory");
        path.push("danny-results.sqlite");
        path
    }

    fn get_conn(&self) -> Connection {
        let dbpath = &self.db_path;
        let conn = Connection::open(dbpath).expect("error connecting to the database");
        db_migrate(&conn);
        conn
    }

    pub fn get_baseline(&self) -> Option<(u32, u32)> {
        let conn = self.get_conn();
        conn.query_row(
            "
            SELECT total_time_ms, output_size
            FROM result_recent
            WHERE path = ?1
              AND threshold = ?2
              AND algorithm = 'all-2-all'
              AND sketch_bits = 0",
            params![
                self.config.path.trim_end_matches("/"),
                self.config.threshold
            ],
            |row| {
                Ok((
                    row.get(0).expect("error getting time"),
                    row.get(1).expect("error getting count"),
                ))
            },
        )
        .optional()
        .expect("error running query")
    }

    pub fn already_run(&self) -> Option<String> {
        if self.config.rerun {
            return None;
        }
        let conn = self.get_conn();
        conn.query_row(
            "SELECT sha FROM result WHERE params_sha == ?1",
            params![self.config.sha()],
            |row| Ok(row.get(0).expect("error getting sha")),
        )
        .optional()
        .expect("error running query")
    }

    pub fn save(self) {
        let sha = self.sha();
        let mut conn = self.get_conn();

        let output_size = self.output_size.expect("missing output size");
        let total_time_ms = self.total_time_ms.expect("missing total time");

        let (recall, speedup) = if let (Some(recall), Some(speedup)) = (self.recall, self.speedup) {
            trace!("Speedup and recall set manually (valid during CSV import)");
            (Some(recall), Some(speedup))
        } else if let Some((base_time, base_count)) = self.get_baseline() {
            let recall = output_size as f64 / base_count as f64;
            let speedup = base_time as f64 / total_time_ms as f64;
            info!("Recall {} and speedup {}", recall, speedup);
            (Some(recall), Some(speedup))
        } else {
            warn!("Missing baseline from the database");
            (None, None)
        };

        let tx = conn.transaction().expect("problem starting transaction");

        {
            // Insert into main table
            tx.execute(
                "INSERT INTO result (
                    sha,
                    code_version,
                    date,
                    params_sha,
                    seed,
                    threshold,
                    algorithm,
                    algorithm_version,
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
                    path,

                    total_time_ms,
                    output_size,
                    recall,
                    speedup,

                    balance
                )
                 VALUES (
                     ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24
                 )",
                params![
                    sha,
                    env!("VERGEN_SHA_SHORT"),
                    self.date.to_rfc3339(),
                    self.config.sha(),
                    self.config.seed as u32,
                    self.config.threshold,
                    self.config.algorithm,
                    self.config.algorithm_version(),
                    // We insert 0 instad of null because the handling of NULL in SQL is complicated
                    self.config.k.unwrap_or(0) as u32,
                    self.config.k2.unwrap_or(0) as u32,
                    self.config.sketch_bits as u32,
                    self.config.threads as u32,
                    self.config.hosts_string(),
                    self.config.sketch_epsilon,
                    self.config.recall,
                    self.config.no_dedup,
                    self.config.no_verify,
                    self.config.repetition_batch as u32,
                    self.config.path.trim_end_matches("/"),
                    self.total_time_ms,
                    self.output_size,
                    recall,
                    speedup,
                    format!("{:?}", self.config.balance)
                ],
            )
            .expect("error inserting into main table");

            let mut stmt = tx
                .prepare(
                    "INSERT INTO counters ( sha, kind, step, count )
                 VALUES ( ?1, ?2, ?3, ?4 )",
                )
                .expect("failed to prepare statement");
            for (kind, step, count) in self.step_counters.iter() {
                stmt.execute(params![sha, kind, step, count])
                    .expect("failure in inserting network information");
            }

            let mut stmt = tx
                .prepare(
                    "INSERT INTO network ( sha, hostname, interface, transmitted, received )
                 VALUES ( ?1, ?2, ?3, ?4, ?5 )",
                )
                .expect("failed to prepare statement");
            for (hostname, interface, transmitted, received) in self.network.iter() {
                stmt.execute(params![sha, hostname, interface, transmitted, received])
                    .expect("failure in inserting network information");
            }
        }

        tx.commit().expect("error committing insertions");
        conn.close().expect("error inserting into the database");
    }
}

fn db_migrate(conn: &Connection) {
    let version: u32 = conn
        .query_row(
            "SELECT user_version FROM pragma_user_version",
            params![],
            |row| row.get(0),
        )
        .expect("cannot get version of the database");
    info!("Current database version is {}", version);

    if version < 1 {
        info!("Applying migration v1");
        conn.execute_batch(include_str!("migrations/v1.sql"))
            .expect("error applying version 1");
    }
    if version < 2 {
        info!("Applying migration v2");
        conn.execute_batch(include_str!("migrations/v2.sql"))
            .expect("error applying version 2");
    }
    if version < 3 {
        info!("Applying migration v3");
        conn.execute_batch(include_str!("migrations/v3.sql"))
            .expect("error applying version 3");
    }

    info!("Database migration completed!");
}
