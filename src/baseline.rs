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
use timely::dataflow::*;
use timely::Data;

#[derive(Serialize, Deserialize)]
pub struct Baselines {
    path: PathBuf,
    baselines: Vec<(String, String, f64, usize)>,
}

impl Baselines {
    pub fn new(config: &Config) -> Self {
        let path = config.get_baselines_path();
        info!("Reading baseline from {:?}", path);
        let mut baselines = Vec::new();
        match File::open(path.clone()) {
            Ok(file) => {
                let file = BufReader::new(file);
                for line in file.lines() {
                    let line = line.expect("Problem reading line");
                    let mut tokens = line.split(",");
                    let left = tokens
                        .next()
                        .expect("There should be the left path")
                        .to_owned();
                    let right = tokens
                        .next()
                        .expect("There should be the right path")
                        .to_owned();
                    let range: f64 = tokens
                        .next()
                        .expect("There should be the range")
                        .parse()
                        .expect("Problem parsing range");
                    let count: usize = tokens
                        .next()
                        .expect("There should be the count")
                        .parse()
                        .expect("Problem parsing the count");
                    baselines.push((left, right, range, count));
                }
            }
            Err(_) => (),
        };
        Baselines { path, baselines }
    }

    pub fn get(&self, left: &String, right: &String, range: f64) -> Option<usize> {
        self.baselines
            .iter()
            .find(|(l, r, t, _)| l == left && r == right && t == &range)
            .map(|tup| tup.3)
    }

    pub fn add(self, left: &String, right: &String, range: f64, count: usize) -> () {
        if self.get(left, right, range).is_none() {
            info!("Writing baseline to {:?}", self.path);
            let mut file = OpenOptions::new()
                .append(true)
                .create(true)
                .open(self.path)
                .expect("problem opening file for writing");
            writeln!(file, "{},{},{},{}", left, right, range, count).expect("Error appending line");
        } else {
            info!(
                "Baseline entry already present in {:?}, skipping",
                self.path
            );
        }
    }

    pub fn recall(&self, left: &String, right: &String, range: f64, count: usize) -> Option<f64> {
        self.get(left, right, range)
            .map(|base_count| count as f64 / base_count as f64)
    }
}

pub fn sequential<T, F>(thresh: f64, left_path: &String, right_path: &String, sim_fn: F) -> usize
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
    let mut pl = ProgressLogger::new(
        Duration::from_secs(1),
        "pairs".to_owned(),
        Some((left.len() * right.len()) as u64),
    );

    let mut sim_cnt = 0;
    for l in left.iter() {
        for r in right.iter() {
            let sim = sim_fn(l, r);
            if sim >= thresh {
                sim_cnt += 1;
            }
        }
        pl.add(right.len() as u64);
    }
    pl.done();
    sim_cnt
}

pub fn all_pairs_parallel<T, F>(
    threshold: f64,
    left_path: &String,
    right_path: &String,
    sim_pred: F,
    config: &Config,
) -> usize
where
    for<'de> T: Deserialize<'de> + ReadDataFile + Data + Sync + Send + Clone + Abomonation + Debug,
    F: Fn(&T, &T) -> bool + Send + Clone + Sync + 'static,
{
    let timely_builder = config.get_timely_builder();
    // This channel is used to get the results
    let (output_send_ch, recv) = ::std::sync::mpsc::channel();
    let output_send_ch = Arc::new(Mutex::new(output_send_ch));

    let left_path = left_path.clone();
    let right_path = right_path.clone();
    let left_path_2 = left_path.clone();
    let right_path_2 = right_path.clone();

    let (global_left_read, global_right_read, send_coords, io_barrier, reader_handle) =
        load_global_vecs_new::<T>(left_path.clone(), right_path.clone(), config);

    timely::execute::execute_from(timely_builder.0, timely_builder.1, move |worker| {
        let index = worker.index();
        let peers = worker.peers() as u64;
        info!("Started worker {}/{}", index, peers);
        let sim_pred = sim_pred.clone();

        // let (mut left, mut right, probe) =
        worker.dataflow::<u32, _, _>(|scope| {
            let output_send_ch = output_send_ch.lock().unwrap().clone();

            let send_coords = send_coords.lock().unwrap().clone();
            let matrix = MatrixDescription::for_workers(peers as usize);
            let (row, col) = matrix.row_major_to_pair(index as u64);
            send_coords
                .send((row, col))
                .expect("Error while pushing into coordinates channel");
            io_barrier.wait();

            let global_left_read = global_left_read.clone();
            let global_right_read = global_right_read.clone();

            source(scope, "Source", move |capability| {
                let mut cap = Some(capability);
                move |output| {
                    if let Some(mut cap) = cap.take() {
                        info!("Starting to count pairs (memory {})", proc_mem!());
                        let left = Arc::clone(&global_left_read.read().unwrap());
                        let right = Arc::clone(&global_right_read.read().unwrap());
                        let mut count = 0usize;
                        let mut pl = ProgressLogger::new(
                            Duration::from_secs(60),
                            "pairs".to_owned(),
                            Some(
                                (left.chunk_len(row as usize) * right.chunk_len(col as usize))
                                    as u64,
                            ),
                        );
                        for (lk, lv) in left.iter_chunk(row as usize) {
                            let mut pairs_looked = 0;
                            for (rk, rv) in right.iter_chunk(col as usize) {
                                if sim_pred(lv, rv) {
                                    count += 1;
                                }
                                pairs_looked += 1;
                            }
                            pl.add(pairs_looked);
                        }
                        pl.done();

                        info!(
                            "Worker {} outputting count {} (memory {})",
                            index,
                            count,
                            proc_mem!()
                        );
                        output.session(&mut cap).give(count);
                    }
                }
            })
            .exchange(|_| 0)
            .capture_into(output_send_ch);
        });
    })
    .expect("Something went wrong with the timely dataflow execution");

    reader_handle
        .join()
        .expect("Problem joining the reader thread");

    if config.is_master() {
        let count: usize = recv
            .extract()
            .iter()
            .map(|pair| pair.1.clone().iter().sum::<usize>())
            .next() // The iterator has one item for each timestamp. We have just one timestamp, 0
            .expect("Failed to get the result out of the channel");
        Baselines::new(config).add(&left_path_2, &right_path_2, threshold, count);
        count
    } else {
        0
    }
}
