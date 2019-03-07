use crate::config::Config;
use crate::experiment::Experiment;
use crate::io::*;
use crate::logging::init_event_logging;
use crate::logging::*;
use crate::lsh::functions::*;
use crate::lsh::operators::*;
use crate::operators::Route;
use crate::operators::*;
use crate::sketch::*;
use abomonation::Abomonation;
use serde::de::Deserialize;
use std::clone::Clone;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Barrier, Mutex, RwLock};
use std::thread;
use std::time::Instant;
use timely::dataflow::operators::capture::{Event as TimelyEvent, Extract};
use timely::dataflow::operators::*;
use timely::dataflow::*;
use timely::Data;

pub fn fixed_param_lsh<D, F, H, O, S, V>(
    left_path: &String,
    right_path: &String,
    hash_fn: LSHCollection<H, O>,
    sketcher_pair: Option<(S, SketchPredicate<V>)>,
    sim_pred: F,
    config: &Config,
    experiment: &mut Experiment,
) -> usize
where
    for<'de> D:
        ReadBinaryFile + Deserialize<'de> + Data + Sync + Send + Clone + Abomonation + Debug,
    F: Fn(&D, &D) -> bool + Send + Clone + Sync + 'static,
    H: LSHFunction<Input = D, Output = O> + Sync + Send + Clone + 'static,
    O: Data + Sync + Send + Clone + Abomonation + Debug + Route + Eq + Hash,
    S: Sketcher<Input = D, Output = V> + Send + Sync + Clone + 'static,
    V: Data + Debug + Sync + Send + Clone + Abomonation + SketchEstimate + BitBasedSketch,
{
    let timely_builder = config.get_timely_builder();
    // This channel is used to get the results
    let (output_send_ch, recv) = channel();
    let output_send_ch = Arc::new(Mutex::new(output_send_ch));

    let (send_exec_summary, recv_exec_summary) = channel();
    let send_exec_summary = Arc::new(Mutex::new(send_exec_summary));

    let left_path = left_path.clone();
    let right_path = right_path.clone();
    let left_path_final = left_path.clone();
    let right_path_final = right_path.clone();
    let repetitions = hash_fn.repetitions();

    let (global_left_read, global_right_read, send_coords, io_barrier, reader_handle) =
        load_global_vecs_new(left_path.clone(), right_path.clone(), config);

    timely::execute::execute_from(timely_builder.0, timely_builder.1, move |mut worker| {
        let execution_summary = init_event_logging(&worker);
        let output_send_ch = output_send_ch.lock().unwrap().clone();
        let sim_pred = sim_pred.clone();
        let index = worker.index();
        let peers = worker.peers() as u64;

        let send_coords = send_coords.lock().unwrap().clone();
        let matrix_coords =
            MatrixDescription::for_workers(peers as usize).row_major_to_pair(index as u64);
        debug!("Sending coordinates {:?}", matrix_coords);
        send_coords
            .send(matrix_coords)
            .expect("Error while pushing into coordinates channel");
        debug!("Waiting for input to be loaded");
        io_barrier.wait();
        debug!("After worker barrier!");

        let global_left_read = global_left_read.clone();
        let global_right_read = global_right_read.clone();

        let hash_fn = hash_fn.clone();
        let sketcher_pair = sketcher_pair.clone();

        let probe = worker.dataflow::<u32, _, _>(move |scope| {
            let mut probe = ProbeHandle::new();
            let sketcher_pair = sketcher_pair;

            let matrix = MatrixDescription::for_workers(peers as usize);

            let candidates = match sketcher_pair {
                Some((sketcher, sketch_predicate)) => {
                    let left_hashes = source_hashed_sketched(
                        scope,
                        Arc::clone(&global_left_read),
                        hash_fn.clone(),
                        sketcher.clone(),
                        matrix,
                        MatrixDirection::Rows,
                        probe.clone(),
                    );
                    let right_hashes = source_hashed_sketched(
                        scope,
                        Arc::clone(&global_right_read),
                        hash_fn.clone(),
                        sketcher.clone(),
                        matrix,
                        MatrixDirection::Rows,
                        probe.clone(),
                    );
                    left_hashes
                        .bucket_batched(&right_hashes)
                        .filter_sketches(sketch_predicate)
                }
                None => {
                    let left_hashes = source_hashed(
                        scope,
                        Arc::clone(&global_left_read),
                        hash_fn.clone(),
                        matrix,
                        MatrixDirection::Rows,
                        probe.clone(),
                    );
                    let right_hashes = source_hashed(
                        scope,
                        Arc::clone(&global_right_read),
                        hash_fn.clone(),
                        matrix,
                        MatrixDirection::Columns,
                        probe.clone(),
                    );
                    left_hashes.bucket_batched(&right_hashes)
                }
            };

            let global_left = Arc::clone(&global_left_read.read().unwrap());
            let global_right = Arc::clone(&global_right_read.read().unwrap());

            candidates
                .pair_route(matrix)
                .map(|pair| pair.1)
                .approximate_distinct(1 << 30, 0.05, 123123123)
                .filter(move |(lk, rk)| {
                    let lv = &global_left[lk];
                    let rv = &global_right[rk];
                    sim_pred(lv, rv)
                })
                .count()
                .inspect(|c| info!("Partial count {}", c))
                .exchange(|_| 0)
                .probe_with(&mut probe)
                .capture_into(output_send_ch);

            probe
        });

        // Do the stepping even though it's not strictly needed: we use it to wait for the dataflow
        // to finish
        worker.step_while(|| probe.less_than(&(repetitions as u32)));

        info!(
            "Execution summary for worker {}: {:?}",
            index, execution_summary
        );
        collect_execution_summaries(execution_summary, send_exec_summary.clone(), &mut worker);
    })
    .unwrap();

    reader_handle
        .join()
        .expect("Problem joining the reader thread");

    if config.is_master() {
        let mut exec_summaries = Vec::new();
        for summary in recv_exec_summary.iter() {
            match summary {
                TimelyEvent::Messages(_, msgs) => exec_summaries.extend(msgs),
                _ => (),
            }
        }
        let global_summary = exec_summaries
            .iter()
            .fold(FrozenExecutionSummary::zero(), |a, b| a.sum(b));
        // From `recv` we get an entry for each timestamp, containing a one-element vector with the
        // count of output pairs for a given timestamp. We sum across all the timestamps, so we need to
        // remove the duplicates
        let count: usize = recv
            .extract()
            .iter()
            .map(|pair| pair.1.clone().iter().sum::<usize>())
            .sum();

        let precision = count as f64 / global_summary.distinct_pairs as f64;
        let potential_pairs =
            D::num_elements(left_path_final.into()) * D::num_elements(right_path_final.into());
        let fraction_distinct = global_summary.distinct_pairs as f64 / potential_pairs as f64;
        global_summary.add_to_experiment("execution_summary", experiment);
        info!(
            "Evaluated fraction of the potential pairs: {} ({}/{})",
            fraction_distinct, global_summary.distinct_pairs, potential_pairs
        );
        info!("Precision: {}", precision);
        info!("Global summary {:?}", global_summary);

        count
    } else {
        0
    }
}
