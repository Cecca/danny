use crate::{config::*, logging::ProfileFunction};
use chrono::prelude::*;
use rusqlite::*;
use std::path::{Path, PathBuf};
use std::{collections::HashMap, time::Duration};
use crate::sysmonitor::{DATASTRUCTURES_BYTES, SystemUsage};

pub struct Experiment {
    db_path: PathBuf,
    date: DateTime<Utc>,
    config: Config,
    // Table with Counter name, worker, step, and count
    step_counters: Vec<(String, usize, u32, i64)>,
    // Hostname, interface, transmitted, received
    network: Vec<(String, String, i64, i64)>,
    profile: Vec<ProfileFunction>,
    system: Vec<(Duration, String, SystemUsage)>,
    datastructures_bytes: Vec<(String, usize)>,
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
            profile: Vec::new(),
            system: Vec::new(),
            datastructures_bytes: Vec::new(),
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

    pub fn append_step_counter(&mut self, kind: String, worker: usize, step: u32, count: i64) {
        self.step_counters.push((kind, worker, step, count));
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

    pub fn add_profile(&mut self, prof: Vec<ProfileFunction>) {
        self.profile.extend(prof.into_iter())
    }

    pub fn add_system_usage(&mut self, usage: Vec<(Duration, String, SystemUsage)>) {
        self.system.extend(usage.into_iter())
    }

    pub fn add_datastructures_bytes(&mut self, bytes: Vec<(String, usize)>) {
        self.datastructures_bytes.extend(bytes.into_iter())
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
              AND algorithm = 'cartesian'
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

    pub fn already_run(&self) -> Option<i64> {
        if self.config.rerun {
            return None;
        }
        let conn = self.get_conn();
        conn.query_row(
            "SELECT id FROM result WHERE params_sha == ?1",
            params![self.config.sha()],
            |row| Ok(row.get(0).expect("error getting id")),
        )
        .optional()
        .expect("error running query")
    }

    pub fn save(mut self) {
        let mut conn = self.get_conn();

        if self.config.dry_run {
            // In a dry run, don't report these numbers because they are meaningless
            self.total_time_ms.take();
            self.output_size.take();
        }

        let (recall, speedup) = if let (Some(recall), Some(speedup)) = (self.recall, self.speedup) {
            trace!("Speedup and recall set manually (valid during CSV import)");
            (Some(recall), Some(speedup))
        } else if let Some((base_time, base_count)) = self.get_baseline() {
            if let Some(output_size) = self.output_size {
                if let Some(total_time_ms) = self.total_time_ms {
                    let recall = output_size as f64 / base_count as f64;
                    let speedup = base_time as f64 / total_time_ms as f64;
                    info!("Recall {} and speedup {}", recall, speedup);
                    (Some(recall), Some(speedup))
                } else {
                    (None, None)
                }
            } else {
                (None, None)
            }
        } else {
            warn!("Missing baseline from the database");
            (None, None)
        };

        let tx = conn.transaction().expect("problem starting transaction");

        {
            // Insert into main table
            tx.execute(
                "INSERT INTO result (
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
                    balance,
                    path,

                    total_time_ms,
                    output_size,
                    recall,
                    speedup,

                    profile_frequency,
                    dry_run
                )
                 VALUES (
                     ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25
                 )",
                params![
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
                    false, // no verify
                    false, // no dedup
                    self.config.repetition_batch as u32,
                    format!("{:?}", self.config.balance),
                    self.config.path.trim_end_matches("/"),
                    self.total_time_ms,
                    self.output_size,
                    recall,
                    speedup,
                    self.config.profile.unwrap_or(0),
                    self.config.dry_run
                ],
            )
            .expect("error inserting into main table");

            let id = tx.last_insert_rowid();

            let kind_id: HashMap<String, i64> = tx
                .prepare("SELECT kind_id, kind FROM enum_kind")
                .expect("fail to prepare statement")
                .query_map(NO_PARAMS, |row| {
                    Ok((row.get(1).unwrap(), row.get(0).unwrap()))
                })
                .expect("fail to prepare query")
                .map(|p| p.unwrap())
                .collect();

            let mut stmt = tx
                .prepare(
                    "INSERT INTO system ( id, time, hostname, cpu_user, cpu_system, net_tx, net_rx, mem_total, mem_used )
                 VALUES ( ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9 )",
                )
                .expect("failed to prepare statement");
            for (time, hostname, usage) in self.system.iter() {
                stmt.execute(params![
                    id,
                    time.as_secs_f64(),
                    hostname,
                    usage.cpu.user,
                    usage.cpu.system,
                    usage.net.tx,
                    usage.net.rx,
                    usage.mem.total as i64,
                    usage.mem.used as i64
                ])
                .expect("failure in inserting counters information");
            }

            let mut stmt = tx
                .prepare(
                    "INSERT INTO counters_raw ( id, kind_id, worker, step, count )
                 VALUES ( ?1, ?2, ?3, ?4, ?5 )",
                )
                .expect("failed to prepare statement");
            for (kind, worker, step, count) in self.step_counters.iter() {
                stmt.execute(params![
                    id,
                    kind_id
                        .get(kind)
                        .unwrap_or_else(|| panic!("failed to retrieve value for key {}", kind)),
                    *worker as i64,
                    step,
                    count
                ])
                .expect("failure in inserting counters information");
            }

            let mut stmt = tx
                .prepare(
                    "INSERT INTO network ( id, hostname, interface, transmitted, received )
                 VALUES ( ?1, ?2, ?3, ?4, ?5 )",
                )
                .expect("failed to prepare statement");
            for (hostname, interface, transmitted, received) in self.network.iter() {
                stmt.execute(params![id, hostname, interface, transmitted, received])
                    .expect("failure in inserting network information");
            }

            let mut stmt = tx
                .prepare(
                    "INSERT INTO datastructures_bytes ( id, hostname, datastructures_bytes )
                 VALUES ( ?1, ?2, ?3 )",
                )
                .expect("failed to prepare statement");
            for (hostname, bytes) in self.datastructures_bytes.iter() {
                stmt.execute(params![id, hostname, *bytes as i64])
                    .expect("failure in inserting network information");
            }

            let mut stmt = tx
                .prepare(
                    "INSERT INTO profile (id, hostname, thread, name, frame_count)
                    VALUES ( ?1, ?2, ?3, ?4, ?5 )",
                )
                .expect("failed to prepare statement");
            for prof in self.profile.iter() {
                stmt.execute(params![
                    id,
                    prof.hostname,
                    prof.thread,
                    prof.name,
                    prof.count
                ])
                .expect("failure to run the prepared statement");
            }
        }

        tx.commit().expect("error committing insertions");
        conn.close().expect("error inserting into the database");
    }

    pub fn save_timed_out(self, timeout: Duration) {
        let mut conn = self.get_conn();

        let tx = conn.transaction().expect("problem starting transaction");
        {
            // Insert into main table
            tx.execute(
                "INSERT INTO result (
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
                    balance,
                    path,

                    total_time_ms,
                    output_size,
                    recall,
                    speedup,

                    profile_frequency,
                    dry_run
                )
                 VALUES (
                     ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25
                 )",
                params![
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
                    false, // no verify
                    false, // no dedup
                    self.config.repetition_batch as u32,
                    format!("{:?}", self.config.balance),
                    self.config.path.trim_end_matches("/"),
                    timeout.as_millis() as i64,
                    None::<i64>,
                    None::<f64>,
                    None::<f64>,
                    self.config.profile.unwrap_or(0),
                    self.config.dry_run
                ],
            )
            .expect("error inserting into main table");
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
    if version < 4 {
        info!("Applying migration v4");
        conn.execute_batch(include_str!("migrations/v4.sql"))
            .expect("error applying version 4");
    }
    if version < 5 {
        info!("Applying migration v5");
        conn.execute_batch(include_str!("migrations/v5.sql"))
            .expect("error applying version 5");
    }
    if version < 6 {
        info!("Applying migration v6");
        conn.execute_batch(include_str!("migrations/v6.sql"))
            .expect("error applying version 6");
    }
    if version < 7 {
        info!("Applying migration v7");
        conn.execute_batch(include_str!("migrations/v7.sql"))
            .expect("error applying version 7");
    }
    if version < 8 {
        info!("Applying migration v8");
        conn.execute_batch(include_str!("migrations/v8.sql"))
            .expect("error applying version 8");
    }
    if version < 9 {
        info!("Applying migration v9");
        conn.execute_batch(include_str!("migrations/v9.sql"))
            .expect("error applying version 9");
    }
    if version < 10 {
        info!("Applying migration v10");
        conn.execute_batch(include_str!("migrations/v10.sql"))
            .expect("error applying version 10");
    }
    if version < 11 {
        info!("Applying migration v11");
        conn.execute_batch(include_str!("migrations/v11.sql"))
            .expect("error applying version 11");
    }
    if version < 12 {
        info!("Applying migration v12");
        conn.execute_batch(include_str!("migrations/v12.sql"))
            .expect("error applying version 12");
    }

    info!("Database migration completed!");
}
