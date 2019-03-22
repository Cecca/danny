use crate::bloom::*;
use crate::config::*;
use crate::dataset::ChunkedDataset;
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
use rand::{Rng, SeedableRng};
use serde::de::Deserialize;
use std::cell::RefCell;
use std::clone::Clone;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;
use std::ops::Deref;
use std::rc::Rc;
use std::sync::mpsc::channel;
use std::sync::{Arc, Barrier, Mutex, RwLock};
use std::thread;
use std::time::Duration;
use std::time::Instant;
use timely::communication::allocator::Allocate;
use timely::dataflow::channels::pact::Pipeline as PipelinePact;
use timely::dataflow::operators::capture::{Event as TimelyEvent, Extract};
use timely::dataflow::operators::*;
use timely::dataflow::*;
use timely::order::Product;
use timely::progress::timestamp::Timestamp;
use timely::worker::Worker;
use timely::Data;

#[allow(clippy::too_many_arguments)]
pub fn estimate_best_k_from_sample<A, K, D, F, H, B, R>(
    worker: &mut Worker<A>,
    global_left: Arc<RwLock<Arc<ChunkedDataset<K, D>>>>,
    global_right: Arc<RwLock<Arc<ChunkedDataset<K, D>>>>,
    n: usize,
    max_k: usize,
    builder: B,
    rng: R,
) -> usize
where
    A: Allocate,
    D: Clone + Data + Debug + Abomonation + Send + Sync,
    K: Data + Debug + Send + Sync + Abomonation + Clone + Eq + Hash + Route,
    H: Clone + Hash + Eq + Debug + Send + Sync + Data + Abomonation,
    F: LSHFunction<Input = D, Output = H> + Clone + Sync + Send + 'static,
    B: Fn(usize, &mut R) -> LSHCollection<F, H>,
    R: Rng + SeedableRng + Send + Clone + ?Sized + 'static,
{
    let peers = worker.peers();
    let global_left = Arc::clone(&global_left);
    let global_right = Arc::clone(&global_right);

    let left_local = LocalData::new();
    let right_local = LocalData::new();
    let left_local_2 = left_local.clone();
    let right_local_2 = right_local.clone();

    let mut rng_2 = rng.clone();
    let rng = Arc::new(Mutex::new(rng));
    let rng = Arc::clone(&rng);
    let rng = rng.lock().unwrap().clone();
    let probe = worker.dataflow::<u32, _, _>(move |scope| {
        let mut p1 = ProbeHandle::new();
        let mut p2 = p1.clone();
        let matrix = MatrixDescription::for_workers(peers as usize);
        info!("Collecting samples");
        collect_sample::<_, _, _, F, _, _>(
            scope,
            global_left,
            n,
            matrix,
            MatrixDirection::Rows,
            rng.clone(),
        )
        .exchange(|_| 0)
        .probe_with(&mut p1)
        .capture_into(left_local);
        collect_sample::<_, _, _, F, _, _>(
            scope,
            global_right,
            n,
            matrix,
            MatrixDirection::Columns,
            rng.clone(),
        )
        .exchange(|_| 0)
        .probe_with(&mut p2)
        .capture_into(right_local);
        p1
    });
    worker.step_while(|| probe.less_than(&1));

    let best_k = if worker.index() == 0 {
        info!("Finding best k locally on the master");
        Some(estimate_best_k(
            left_local_2.data().to_vec(),
            right_local_2.data().to_vec(),
            max_k,
            builder,
            &mut rng_2,
        ))
    } else {
        None
    };

    info!("Sending the best k value around");
    let k_local = LocalData::new();
    let k_local_2 = k_local.clone();

    let best_k: Vec<usize> = best_k.iter().cloned().collect();
    let probe = worker.dataflow::<u32, _, _>(move |scope| {
        let mut probe = ProbeHandle::new();
        let best_k = dbg!(best_k.clone());
        best_k
            .to_stream(scope)
            .broadcast()
            .probe_with(&mut probe)
            .capture_into(k_local);
        probe
    });
    worker.step_while(|| probe.less_than(&1));

    let best_k = *k_local_2.data().iter().next().expect("No value to extract");
    info!("Received best k value: {:?}", best_k);
    best_k
}

#[allow(clippy::too_many_arguments)]
pub fn fixed_param_lsh<D, F, H, O, S, V, B, R>(
    left_path: &String,
    right_path: &String,
    k: ParamK,
    hash_collection_builder: B,
    sketcher_pair: Option<(S, SketchPredicate<V>)>,
    sim_pred: F,
    rng: &mut R,
    config: &Config,
    experiment: &mut Experiment,
) -> usize
where
    for<'de> D:
        ReadBinaryFile + Deserialize<'de> + Data + Sync + Send + Clone + Abomonation + Debug,
    F: Fn(&D, &D) -> bool + Send + Clone + Sync + 'static,
    H: LSHFunction<Input = D, Output = O> + Sync + Send + Clone + 'static,
    O: Data + Sync + Send + Clone + Abomonation + Debug + Route + Eq + Hash + Ord,
    S: Sketcher<Input = D, Output = V> + Send + Sync + Clone + 'static,
    V: Data + Debug + Sync + Send + Clone + Abomonation + SketchEstimate + BitBasedSketch,
    R: Rng + SeedableRng + Send + Sync + Clone + 'static,
    B: Fn(usize, &mut R) -> LSHCollection<H, O> + Sized + Send + Sync + Clone + 'static,
{
    let timely_builder = config.get_timely_builder();
    // This channel is used to get the results
    let (output_send_ch, recv) = channel();
    let output_send_ch = Arc::new(Mutex::new(output_send_ch));

    let (send_exec_summary, recv_exec_summary) = channel();
    let send_exec_summary = Arc::new(Mutex::new(send_exec_summary));

    let (send_k, recv_k) = channel();
    let send_k = Arc::new(Mutex::new(send_k));

    let batch_size = config.get_batch_size();

    let left_path = left_path.clone();
    let right_path = right_path.clone();
    let left_path_final = left_path.clone();
    let right_path_final = right_path.clone();

    let hash_collection_builder = hash_collection_builder.clone();
    let rng = rng.clone();

    let (global_left_read, global_right_read, send_coords, io_barrier, reader_handle) =
        load_global_vecs_new(left_path.clone(), right_path.clone(), config);

    let estimator_samples = config.get_estimator_samples();
    let bloom_fpp = config.get_bloom_fpp();
    let bloom_elements = config.get_bloom_elements();

    let bloom_filter = Arc::new(AtomicBloomFilter::<(u32, u32)>::new(
        4usize.gb_to_bits(),
        5,
        rng.clone(),
    ));

    timely::execute::execute_from(timely_builder.0, timely_builder.1, move |mut worker| {
        let bloom_filter = Arc::clone(&bloom_filter);
        let hash_collection_builder = hash_collection_builder.clone();
        let mut rng = rng.clone();
        let execution_summary = init_event_logging(&worker);
        let output_send_ch = output_send_ch
            .lock()
            .expect("Cannot get lock on output channel")
            .clone();
        let sim_pred = sim_pred.clone();
        let index = worker.index();
        let peers = worker.peers() as u64;

        let send_coords = send_coords
            .lock()
            .expect("Cannot get lock on coordinate channel")
            .clone();
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

        let send_k = send_k.lock().unwrap().clone();

        let hash_fn = match k {
            ParamK::Exact(k) => {
                send_k.send(k);
                hash_collection_builder(k, &mut rng)
            }
            ParamK::Max(max_k) => {
                let best_k = estimate_best_k_from_sample(
                    worker,
                    global_left_read.clone(),
                    global_right_read.clone(),
                    estimator_samples,
                    max_k,
                    hash_collection_builder.clone(),
                    rng.clone(),
                );
                send_k.send(best_k);
                info!("Building collection with k={}", best_k);
                hash_collection_builder(best_k, &mut rng)
            }
        };

        let hash_fn = hash_fn.clone();
        let sketcher_pair = sketcher_pair.clone();

        let probe = worker.dataflow::<u32, _, _>(move |scope| {
            let bloom_filter = Arc::clone(&bloom_filter);
            let mut outer = scope.clone();
            outer.scoped::<Product<u32, u32>, _, _>("inner-dataflow", |inner| {
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
                        )
                        .enter(inner);
                        let right_hashes = source_hashed_sketched(
                            scope,
                            Arc::clone(&global_right_read),
                            hash_fn.clone(),
                            sketcher.clone(),
                            matrix,
                            MatrixDirection::Rows,
                            probe.clone(),
                        )
                        .enter(inner);
                        left_hashes
                            .bucket(&right_hashes, batch_size)
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
                        )
                        .enter(inner);
                        let right_hashes = source_hashed(
                            scope,
                            Arc::clone(&global_right_read),
                            hash_fn.clone(),
                            matrix,
                            MatrixDirection::Columns,
                            probe.clone(),
                        )
                        .enter(inner);
                        left_hashes.bucket(&right_hashes, batch_size)
                    }
                };

                let global_left = Arc::clone(
                    &global_left_read
                        .read()
                        .expect("Cannot get the lock on the global left dataset"),
                );
                let global_right = Arc::clone(
                    &global_right_read
                        .read()
                        .expect("Cannot get the lock on the global right dataset"),
                );

                candidates_filter_count(
                    candidates,
                    Arc::clone(&global_left_read),
                    Arc::clone(&global_right_read),
                    sim_pred,
                    Arc::clone(&bloom_filter),
                )
                .exchange(|_| 0)
                .leave()
                .probe_with(&mut probe)
                .capture_into(output_send_ch);

                probe
            })
        });

        // Do the stepping even though it's not strictly needed: we use it to wait for the dataflow
        // to finish
        // worker.step_while(|| probe.less_than(&(repetitions as u32)));
        worker.step_while(|| probe.with_frontier(|f| !f.is_empty()));

        info!(
            "Execution summary for worker {}: {:?}",
            index, execution_summary
        );
        collect_execution_summaries(execution_summary, send_exec_summary.clone(), &mut worker);
    })
    .expect("Problems with the dataflow");

    reader_handle
        .join()
        .expect("Problem joining the reader thread");

    if config.is_master() {
        let actual_k = recv_k.recv().expect("Unable to get actual k");
        info!("Actual k that was used is {}", actual_k);
        experiment.add_tag("actual_k", actual_k);

        let mut exec_summaries = Vec::new();
        for summary in recv_exec_summary.iter() {
            if let TimelyEvent::Messages(_, msgs) = summary {
                exec_summaries.extend(msgs);
            }
        }
        let global_summary = exec_summaries
            .iter()
            .fold(FrozenExecutionSummary::default(), |a, b| a.sum(b));
        // From `recv` we get an entry for each timestamp, containing a one-element vector with the
        // count of output pairs for a given timestamp. We sum across all the timestamps, so we need to
        // remove the duplicates
        let count: u64 = recv
            .extract()
            .iter()
            .map(|pair| pair.1.clone().iter().sum::<u64>())
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

        count as usize
    } else {
        0
    }
}

pub fn candidates_filter_count<G, T, K, D, F>(
    candidates: Stream<G, (K, K)>,
    global_left: Arc<RwLock<Arc<ChunkedDataset<K, D>>>>,
    global_right: Arc<RwLock<Arc<ChunkedDataset<K, D>>>>,
    sim_pred: F,
    bloom_filter: Arc<AtomicBloomFilter<(K, K)>>,
) -> Stream<G, u64>
where
    G: Scope<Timestamp = T>,
    T: Timestamp + Succ,
    K: Data + Route + Sync + Send + Clone + Abomonation + Debug + Hash,
    D: Data + Sync + Send + Clone + Abomonation + Debug,
    F: Fn(&D, &D) -> bool + Send + Clone + Sync + 'static,
{
    let peers = candidates.scope().peers();
    let matrix = MatrixDescription::for_workers(peers as usize);
    let global_left = global_left.read().unwrap().clone();
    let global_right = global_right.read().unwrap().clone();

    candidates
        .pair_route(matrix)
        .map(|pair| pair.1)
        .approximate_distinct_atomic(Arc::clone(&bloom_filter))
        .unary(PipelinePact, "count-matching", move |_, _| {
            let mut pl =
                ProgressLogger::new(Duration::from_secs(60), "comparisons".to_owned(), None);
            move |input, output| {
                input.for_each(|t, d| {
                    let mut data = d.replace(Vec::new());
                    let count = data
                        .drain(..)
                        .filter(|(lk, rk)| {
                            let lv = &global_left[lk];
                            let rv = &global_right[rk];
                            sim_pred(lv, rv)
                        })
                        .count() as u64;
                    pl.add(count);
                    let mut session = output.session(&t);
                    session.give(count);
                });
            }
        })
        .stream_sum()
}
