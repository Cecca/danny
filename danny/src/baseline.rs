use crate::config::Config;
use crate::io::*;
use crate::logging::*;
use crate::operators::*;
use abomonation::Abomonation;
use serde::de::Deserialize;
use std::clone::Clone;
use std::fmt::Debug;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::time::Instant;
use timely::dataflow::operators::capture::Extract;
use timely::dataflow::operators::generic::operator::source;
use timely::dataflow::operators::*;
use timely::Data;

#[derive(Serialize, Deserialize)]
pub struct Baselines {
    path: PathBuf,
    baselines: Vec<(String, String, f64, usize, u64, String)>,
}

impl Baselines {
    pub fn new(config: &Config) -> Self {
        unimplemented!("use SQLite")
        // let path = config.get_baselines_path();
        // info!("Reading baseline from {:?}", path);
        // let mut baselines = Vec::new();
        // if let Ok(file) = File::open(path.clone()) {
        //     let file = BufReader::new(file);
        //     for line in file.lines() {
        //         let line = line.expect("Problem reading line");
        //         let mut tokens = line.split(',');
        //         let left = tokens
        //             .next()
        //             .expect("There should be the left path")
        //             .to_owned();
        //         let right = tokens
        //             .next()
        //             .expect("There should be the right path")
        //             .to_owned();
        //         let range: f64 = tokens
        //             .next()
        //             .expect("There should be the range")
        //             .parse()
        //             .expect("Problem parsing range");
        //         let count: usize = tokens
        //             .next()
        //             .expect("There should be the count")
        //             .parse()
        //             .expect("Problem parsing the count");
        //         let seconds: u64 = tokens
        //             .next()
        //             .expect("There should be the number of seconds")
        //             .parse()
        //             .expect("Problem parsing the seconds");
        //         let version: String = tokens
        //             .next()
        //             .expect("There should be the git version")
        //             .to_owned();
        //         baselines.push((left, right, range, count, seconds, version));
        //     }
        // };
        // Baselines { path, baselines }
    }

    #[allow(clippy::float_cmp)]
    pub fn get_count(&self, left: &str, right: &str, range: f64) -> Option<usize> {
        self.baselines
            .iter()
            .find(|(l, r, t, _, _, _)| l == left && r == right && t == &range)
            .map(|tup| tup.3)
    }

    #[allow(clippy::float_cmp)]
    pub fn get_times_secs(&self, left: &str, right: &str, range: f64) -> Option<Vec<u64>> {
        let times: Vec<u64> = self
            .baselines
            .iter()
            .filter(|(l, r, t, _, _, _)| l == left && r == right && t == &range)
            .map(|tup| tup.4)
            .collect();
        if times.is_empty() {
            None
        } else {
            Some(times)
        }
    }

    pub fn add(self, left: &str, right: &str, range: f64, count: usize, seconds: u64) {
        if self.get_count(left, right, range).is_none() {
            info!("Writing baseline to {:?}", self.path);
            let mut file = OpenOptions::new()
                .append(true)
                .create(true)
                .open(self.path)
                .expect("problem opening file for writing");
            writeln!(
                file,
                "{},{},{},{},{},{}",
                left,
                right,
                range,
                count,
                seconds,
                crate::version::short_sha()
            )
            .expect("Error appending line");
        } else {
            info!(
                "Baseline entry already present in {:?}, skipping",
                self.path
            );
        }
    }

    pub fn recall(&self, left: &str, right: &str, range: f64, count: usize) -> Option<f64> {
        self.get_count(left, right, range)
            .map(|base_count| count as f64 / base_count as f64)
    }

    pub fn speedup(&self, left: &str, right: &str, range: f64, seconds: f64) -> Option<f64> {
        self.get_times_secs(left, right, range).map(|times| {
            let avg_base_time = times.iter().sum::<u64>() as f64 / times.len() as f64;
            avg_base_time / seconds
        })
    }
}

#[cfg(feature = "seq-all-2-all")]
pub fn sequential<T, F>(thresh: f64, left_path: &str, right_path: &str, sim_fn: F) -> usize
where
    for<'de> T: ReadDataFile + Deserialize<'de>,
    F: Fn(&T, &T) -> f64,
{
    let mut left = Vec::new();
    let mut right = Vec::new();
    ReadBinaryFile::read_binary(left_path.into(), |_| true, |_, v| left.push(v));
    ReadBinaryFile::read_binary(right_path.into(), |_| true, |_, v| right.push(v));
    println!(
        "Loaded data:\n  left: {}\n  right: {}",
        left.len(),
        right.len(),
    );
    let mut pl = progress_logger::ProgressLogger::builder()
        .with_frequency(Duration::from_secs(10))
        .with_items_name("pairs")
        .with_expected_updates((left.len() * right.len()) as u64)
        .start();

    let mut sim_cnt = 0;
    for l in left.iter() {
        for r in right.iter() {
            let sim = sim_fn(l, r);
            if sim >= thresh {
                sim_cnt += 1;
            }
        }
        pl.update_light(right.len() as u64);
    }
    pl.stop();
    sim_cnt
}

#[cfg(feature = "all-2-all")]
pub fn all_pairs_parallel<T, F>(
    threshold: f64,
    left_path: &str,
    right_path: &str,
    sim_pred: F,
    config: &Config,
) -> usize
where
    for<'de> T: Deserialize<'de> + ReadDataFile + Data + Sync + Send + Clone + Abomonation + Debug,
    F: Fn(&T, &T) -> bool + Send + Clone + Sync + 'static,
{
    let start_time = Instant::now();
    let timely_builder = config.get_timely_builder();
    // This channel is used to get the results
    let (output_send_ch, recv) = ::std::sync::mpsc::channel();
    let output_send_ch = Arc::new(Mutex::new(output_send_ch));

    let (global_left, global_right) = load_vectors(left_path, right_path, &config);

    timely::execute::execute_from(timely_builder.0, timely_builder.1, move |worker| {
        let index = worker.index();
        let peers = worker.peers() as u64;
        info!("Started worker {}/{}", index, peers);
        let sim_pred = sim_pred.clone();

        worker.dataflow::<u32, _, _>(|scope| {
            let output_send_ch = output_send_ch.lock().unwrap().clone();
            let global_left = Arc::clone(&global_left);
            let global_right = Arc::clone(&global_right);

            let matrix = MatrixDescription::for_workers(peers as usize);
            let (row, col) = matrix.row_major_to_pair(index as u64);
            source(scope, "Source", move |capability| {
                let mut cap = Some(capability);
                let left = Arc::clone(&global_left);
                let right = Arc::clone(&global_right);
                move |output| {
                    if let Some(cap) = cap.take() {
                        info!("Starting to count pairs (memory {})", proc_mem!());
                        let mut count = 0usize;
                        let mut pl = progress_logger::ProgressLogger::builder()
                            .with_frequency(Duration::from_secs(60))
                            .with_items_name("pairs")
                            .with_expected_updates(
                                (left.chunk_len(row as usize) * right.chunk_len(col as usize))
                                    as u64,
                            )
                            .start();
                        for (_lk, lv) in left.iter_chunk(row as usize) {
                            let mut pairs_looked = 0_u64;
                            for (_rk, rv) in right.iter_chunk(col as usize) {
                                if sim_pred(lv, rv) {
                                    count += 1;
                                }
                                pairs_looked += 1;
                            }
                            pl.update_light(pairs_looked);
                        }
                        pl.stop();

                        info!(
                            "Worker {} outputting count {} (memory {})",
                            index,
                            count,
                            proc_mem!()
                        );
                        output.session(&cap).give(count);
                    }
                }
            })
            .exchange(|_| 0)
            .capture_into(output_send_ch);
        });
    })
    .expect("Something went wrong with the timely dataflow execution");

    if config.is_master() {
        let count: usize = recv
            .extract()
            .iter()
            .map(|pair| pair.1.clone().iter().sum::<usize>())
            .next() // The iterator has one item for each timestamp. We have just one timestamp, 0
            .expect("Failed to get the result out of the channel");
        let end_time = Instant::now();
        let elapsed = (end_time - start_time).as_secs();
        Baselines::new(config).add(left_path, right_path, threshold, count, elapsed);
        count
    } else {
        0
    }
}
