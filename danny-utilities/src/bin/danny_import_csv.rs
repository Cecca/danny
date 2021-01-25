#[macro_use]
extern crate serde_derive;

use argh::*;
use bzip2::read::BzDecoder;
use chrono::prelude::*;

use danny::config::*;
use danny::experiment::*;
use log::*;

use serde::*;
use std::collections::HashMap;
use std::fs::File;
use std::io::*;
use std::path::PathBuf;
use std::path::*;

#[derive(Debug, Deserialize)]
struct ResultRecord {
    // These are the fields that are part of all the files
    git_revision: String,
    required_recall: Option<f64>,
    threshold: f64,
    git_commit_date: String,
    seed: u64,
    sketch_epsilon: f64,
    algorithm: String,
    total_threads: usize,
    threads_per_worker: usize,
    date: String,
    right_path: String,
    k: Option<usize>,
    k2: Option<usize>,
    no_dedup: Option<bool>,
    num_hosts: u32,
    sketch_bits: u32,
    no_verify: Option<bool>,
    left_path: String,
    repetition_batch: Option<usize>,

    // fields of main result
    speedup: Option<f64>,
    recall: Option<f64>,
    output_size: Option<u64>,
    total_time_ms: Option<u32>,

    // additional fields for step counters
    count: Option<u64>,
    worker: Option<u32>,
    step: Option<u32>,
    kind: Option<String>,

    // additional fields for network
    hostname: Option<String>,
    interface: Option<String>,
    transmitted: Option<u64>,
    received: Option<u64>,
}

impl ResultRecord {
    fn get_config(&self) -> Config {
        Config {
            threshold: self.threshold,
            algorithm: self.algorithm.clone(),
            k: self.k,
            k2: self.k2,
            sketch_bits: self.sketch_bits as usize,
            process_id: None,
            threads: self.threads_per_worker as usize,
            hosts: Some(
                parse_hosts("sss00:2001,sss01:2001,sss02:2001,sss03:2001,sss04:2001").unwrap(),
            ),
            seed: self.seed,
            sketch_epsilon: self.sketch_epsilon,
            recall: self.required_recall.unwrap_or(0.8),
            no_dedup: self.no_dedup.unwrap_or(false),
            no_verify: self.no_verify.unwrap_or(false),
            repetition_batch: self.repetition_batch.unwrap_or(1),
            rerun: false,
            left_path: self.left_path.clone(),
            right_path: self.right_path.clone(),
        }
    }

    fn get_date(&self) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(&self.date)
            .unwrap()
            .with_timezone(&Utc)
    }

    fn get_main_result(&self) -> MainResult {
        MainResult {
            total_time_ms: self.total_time_ms.expect("total_time_ms"),
            output_size: self.output_size.expect("output_size"),
            recall: if self.recall.expect("recall") >= 0.0 {
                Some(self.recall.unwrap())
            } else {
                None
            },
            speedup: if self.speedup.expect("speedup") >= 0.0 {
                Some(self.speedup.unwrap())
            } else {
                None
            },
        }
    }

    fn get_counters_result(&self) -> CountersResult {
        CountersResult {
            count: self.count.unwrap(),
            // worker: self.worker.unwrap(),
            step: self.step.unwrap(),
            kind: self.kind.as_ref().unwrap().clone(),
        }
    }

    fn get_network_result(&self) -> NetworkResult {
        NetworkResult {
            hostname: self.hostname.as_ref().unwrap().clone(),
            interface: self.interface.as_ref().unwrap().clone(),
            transmitted: self.transmitted.unwrap(),
            received: self.received.unwrap(),
        }
    }
}

#[derive(Debug)]
struct MainResult {
    total_time_ms: u32,
    output_size: u64,
    recall: Option<f64>,
    speedup: Option<f64>,
}

// we perform the aggregation by worker
#[derive(Debug)]
struct CountersResult {
    count: u64,
    step: u32,
    kind: String,
}

#[derive(Debug)]
struct NetworkResult {
    hostname: String,
    interface: String,
    transmitted: u64,
    received: u64,
}

/// import csv files into the database
#[derive(FromArgs, Clone, Debug)]
struct Args {
    /// name of the table to import
    #[argh(option)]
    pub table: String,

    /// path to the database
    #[argh(option)]
    pub database: PathBuf,

    /// path to the file to import
    #[argh(option)]
    pub result: PathBuf,

    /// path to the file to import
    #[argh(option)]
    pub counters: PathBuf,

    /// path to the file to import
    #[argh(option)]
    pub network: PathBuf,
}

fn get_reader<P: AsRef<Path>>(path: P) -> Box<dyn BufRead> {
    let path = path.as_ref();
    if path.extension().unwrap() == ".bz2" {
        // println!("Reading compressed file");
        Box::new(BufReader::new(BzDecoder::new(File::open(path).unwrap())))
    } else {
        // println!("Reading uncompressed file");
        Box::new(BufReader::new(File::open(path).unwrap()))
    }
}

fn num_lines<P: AsRef<Path>>(path: P) -> u64 {
    get_reader(path).lines().count() as u64
}

fn main() {
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .init();

    let args: Args = argh::from_env();

    let mut rdr = csv::Reader::from_reader(get_reader(&args.result));

    let mut data = HashMap::new();

    info!("Reading results data");
    let mut pl = progress_logger::ProgressLogger::builder()
        .with_expected_updates(num_lines(&args.result))
        .with_items_name("records")
        .start();
    for result in rdr.deserialize() {
        let result: ResultRecord = result.unwrap();
        let config = result.get_config();
        data.insert(
            config.sha(),
            (
                config,
                result.get_date(),
                result.get_main_result(),
                HashMap::<(String, u32), i64>::new(),
                Vec::<NetworkResult>::new(),
            ),
        );
        pl.update(1u64);
    }
    pl.stop();

    info!("Reading network data");
    let mut rdr = csv::Reader::from_reader(get_reader(&args.network));
    let mut pl = progress_logger::ProgressLogger::builder()
        .with_expected_updates(num_lines(&args.network))
        .with_items_name("records")
        .start();
    for result in rdr.deserialize() {
        let result: ResultRecord = result.unwrap();
        let config = result.get_config();
        let net = result.get_network_result();
        let tuple = data.get_mut(&config.sha()).unwrap();
        tuple.4.push(net);
        pl.update(1u64);
    }
    pl.stop();

    info!("Reading counters data");
    for csv in std::fs::read_dir(&args.counters).expect("error reading directory contents") {
        let csv = csv.expect("error getting content");
        if csv.file_type().unwrap().is_file() {
            let csv = csv.path();
            info!("Reading {:?}", csv);
            let mut rdr = csv::Reader::from_reader(get_reader(&csv));
            let mut pl = progress_logger::ProgressLogger::builder()
                .with_expected_updates(num_lines(&csv))
                .with_items_name("records")
                .start();
            for result in rdr.deserialize() {
                let result: ResultRecord = result.unwrap();
                let config = result.get_config();
                let counts = result.get_counters_result();
                let tuple = data.get_mut(&config.sha()).unwrap();
                *tuple.3.entry((counts.kind, counts.step)).or_insert(0i64) += counts.count as i64;
                pl.update(1u64);
            }
            pl.stop();
        }
    }

    let mut pl = progress_logger::ProgressLogger::builder()
        .with_expected_updates(data.len() as u64)
        .with_items_name("experiments")
        .start();
    for (_sha, (config, date, main, counts, net)) in data.into_iter() {
        let mut experiment = Experiment::from_config(config)
            .with_date(date)
            .with_database(&args.database);
        experiment.set_output_size(main.output_size as u32);
        experiment.set_total_time_ms(main.total_time_ms as u64);
        if let (Some(recall), Some(speedup)) = (main.recall, main.speedup) {
            experiment.set_recall_speedup(recall, speedup);
        }
        for ((kind, step), count) in counts.into_iter() {
            experiment.append_step_counter(kind, step, count);
        }
        for net_res in net.into_iter() {
            experiment.append_network_info(
                net_res.hostname,
                net_res.interface,
                net_res.transmitted as i64,
                net_res.received as i64,
            );
        }
        experiment.save();
        pl.update(1u64);
    }
    pl.stop();
}
