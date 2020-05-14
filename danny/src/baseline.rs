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
use timely::communication::Allocator;
use timely::dataflow::operators::capture::Extract;
use timely::dataflow::operators::generic::operator::source;
use timely::dataflow::operators::*;
use timely::worker::Worker;
use timely::Data;

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
    worker: &mut Worker<Allocator>,
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
    use std::cell::RefCell;
    use std::rc::Rc;
    let result = Rc::new(RefCell::new(0usize));
    let result_read = Rc::clone(&result);

    let start_time = Instant::now();

    let (global_left, global_right) = load_vectors(worker, left_path, right_path, &config);

    let index = worker.index();
    let peers = worker.peers() as u64;
    info!("Started worker {}/{}", index, peers);
    let sim_pred = sim_pred.clone();

    worker.dataflow::<u32, _, _>(|scope| {
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
                            (left.chunk_len(row as usize) * right.chunk_len(col as usize)) as u64,
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
        .unary(
            timely::dataflow::channels::pact::Pipeline,
            "count collection",
            move |_, _| {
                move |input, output| {
                    input.for_each(|t, data| {
                        let data = data.replace(Vec::new());
                        for c in data.into_iter() {
                            *result.borrow_mut() += c;
                        }
                        output.session(&t).give(());
                    });
                }
            },
        );
    });

    if worker.index() == 0 {
        result_read.replace(0)
    } else {
        0
    }
}
