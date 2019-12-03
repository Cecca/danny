use crate::config::*;
use crate::dataset::ChunkedDataset;
use crate::experiment::Experiment;
use crate::io::*;
use crate::join::*;
use crate::logging::init_event_logging;
use crate::logging::*;
use crate::lsh::operators::RepetitionStopWatch;
use crate::operators::*;
use danny_base::lsh::*;
use danny_base::sketch::*;
use rand::{Rng, SeedableRng};
use serde::de::Deserialize;
use std::clone::Clone;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use timely::dataflow::operators::capture::{Event as TimelyEvent, Extract};
use timely::dataflow::operators::generic::source;
use timely::dataflow::operators::*;
use timely::dataflow::*;
use timely::progress::Timestamp;
use timely::ExchangeData;

#[allow(clippy::too_many_arguments)]
pub fn two_round_lsh<D, F, H, B, R, S, V>(
    left_path: &str,
    right_path: &str,
    range: f64,
    k: usize,
    k2: usize,
    hash_function_builder: B,
    hash_function_builder_2: B,
    sketcher: S,
    sketch_pred: SketchPredicate<V>,
    sim_pred: F,
    rng: &mut R,
    config: &Config,
    experiment: &mut Experiment,
) -> usize
where
    for<'de> D: ReadBinaryFile + Deserialize<'de> + ExchangeData + Debug + SketchEstimate,
    F: Fn(&D, &D) -> bool + Send + Clone + Sync + 'static,
    H: LSHFunction<Input = D, Output = u32> + Sync + Send + Clone + 'static,
    S: Sketcher<Input = D, Output = V> + Send + Sync + Clone + 'static,
    V: SketchData + Debug,
    R: Rng + SeedableRng + Send + Sync + Clone + 'static,
    B: Fn(usize, &mut R) -> H + Sized + Send + Sync + Clone + 'static,
{
    let network = NetworkGauge::start();

    let timely_builder = config.get_timely_builder();
    // This channel is used to get the results
    let (output_send_ch, recv) = channel();
    let output_send_ch = Arc::new(Mutex::new(output_send_ch));

    let (send_exec_summary, recv_exec_summary) = channel();
    let send_exec_summary = Arc::new(Mutex::new(send_exec_summary));

    let hasher = TensorCollection::new(k, range, hash_function_builder, rng);
    let hasher = Arc::new(hasher);

    let hasher_intern = TensorCollection::new(k2, range, hash_function_builder_2, rng);
    let hasher_intern = Arc::new(hasher_intern);

    debug!(
        "Left dataset has {} points, right has {}",
        D::num_elements(left_path.into()),
        D::num_elements(right_path.into())
    );
    let (global_left, global_right) = load_vectors(left_path, right_path, &config);

    timely::execute::execute_from(timely_builder.0, timely_builder.1, move |mut worker| {
        let global_left = Arc::clone(&global_left);
        let global_right = Arc::clone(&global_right);
        let hasher = Arc::clone(&hasher);
        let hasher_intern = Arc::clone(&hasher_intern);
        let execution_summary = init_event_logging(&worker);
        let output_send_ch = output_send_ch
            .lock()
            .expect("Cannot get lock on output channel")
            .clone();
        let sim_pred = sim_pred.clone();
        let sketch_pred = sketch_pred.clone();

        let sketcher = sketcher.clone();
        let sketcher = Arc::new(sketcher);

        let probe = worker.dataflow::<u32, _, _>(move |scope| {
            let mut probe = ProbeHandle::new();
            let matrix = MatrixDescription::for_workers(scope.peers() as usize);

            let left_hashes = source_hashed_two_round(
                scope,
                Arc::clone(&global_left),
                Arc::clone(&sketcher),
                Arc::clone(&hasher),
                Arc::clone(&hasher_intern),
                matrix,
                MatrixDirection::Rows,
            );
            let right_hashes = source_hashed_two_round(
                scope,
                Arc::clone(&global_right),
                Arc::clone(&sketcher),
                Arc::clone(&hasher),
                Arc::clone(&hasher_intern),
                matrix,
                MatrixDirection::Columns,
            );
            left_hashes
                .join_map_slice(
                    &right_hashes,
                    move |(outer_repetition, _hash), left_vals, right_vals| {
                        let mut cnt = 0;
                        let mut total = 0;
                        let mut sketch_cnt = 0;
                        let mut duplicate_cnt = 0;
                        let repetitions = hasher_intern.repetitions();
                        let mut joiner = Joiner::default();
                        for rep in 0..repetitions {
                            joiner.clear();
                            for (_, (outer_pool, inner_pool, s, v)) in left_vals.iter() {
                                joiner.push_left(
                                    hasher_intern.hash(inner_pool, rep),
                                    (s, v, outer_pool, inner_pool),
                                );
                            }
                            for (_, (outer_pool, inner_pool, s, v)) in right_vals.iter() {
                                joiner.push_right(
                                    hasher_intern.hash(inner_pool, rep),
                                    (s, v, outer_pool, inner_pool),
                                );
                            }

                            joiner.join_map(|_hash, l, r| {
                                total += 1;
                                if !hasher_intern.already_seen(&l.3, &r.3, rep)
                                    && !hasher.already_seen(&l.2, &r.2, *outer_repetition)
                                {
                                    if sketch_pred.eval(l.0, r.0) {
                                        if sim_pred(&(l.1).1, &(r.1).1) {
                                            cnt += 1;
                                        }
                                    } else {
                                        sketch_cnt += 1;
                                    }
                                } else {
                                    duplicate_cnt += 1;
                                }
                            });
                        }
                        vec![cnt]
                    },
                )
                // .bucket_pred_lsh(
                //     &right_hashes,
                //     Arc::clone(&hasher),
                //     Arc::clone(&hasher_intern),
                //     move |l, r| sim_pred(&l.1, &r.1),
                //     move |l, r| sketch_pred.eval(l, r),
                // )
                .exchange(|_| 0) // Bring all the counts to the first worker
                .probe_with(&mut probe)
                .capture_into(output_send_ch);
            probe
        });

        // Do the stepping even though it's not strictly needed: we use it to wait for the dataflow
        // to finish
        // worker.step_while(|| probe.less_than(&(repetitions as u32)));
        worker.step_while(|| probe.with_frontier(|f| !f.is_empty()));

        collect_execution_summaries(execution_summary, send_exec_summary.clone(), &mut worker);
    })
    .expect("Problems with the dataflow");

    let network_summaries = network.map(|n| n.measure().collect_from_workers(&config));

    if config.is_master() {
        let mut exec_summaries = Vec::new();
        for summary in recv_exec_summary.iter() {
            if let TimelyEvent::Messages(_, msgs) = summary {
                exec_summaries.extend(msgs);
            }
        }
        for summary in exec_summaries.iter() {
            summary.add_to_experiment(experiment);
        }
        if network_summaries.is_some() {
            network_summaries
                .unwrap()
                .iter()
                .for_each(|n| n.report(experiment));
        }
        // From `recv` we get an entry for each timestamp, containing a one-element vector with the
        // count of output pairs for a given timestamp. We sum across all the timestamps, so we need to
        // remove the duplicates
        let count: u64 = recv
            .extract()
            .iter()
            .map(|pair| pair.1.clone().iter().sum::<usize>())
            .sum::<usize>() as u64;

        count as usize
    } else {
        0
    }
}

pub fn source_hashed_two_round<G, T, K, D, F, S>(
    scope: &G,
    global_vecs: Arc<ChunkedDataset<K, D>>,
    sketcher: Arc<S>,
    hash_fns: Arc<TensorCollection<F>>,
    hash_fns2: Arc<TensorCollection<F>>,
    matrix: MatrixDescription,
    direction: MatrixDirection,
) -> Stream<G, ((usize, u32), (TensorPool, TensorPool, S::Output, (K, D)))>
// ) -> Stream<G, (u32, (K, D))>
where
    G: Scope<Timestamp = T>,
    T: Timestamp + Succ,
    D: ExchangeData + Debug,
    F: LSHFunction<Input = D, Output = u32> + Sync + Send + Clone + 'static,
    S: Sketcher<Input = D> + Clone + 'static,
    S::Output: SketchData + Debug,
    K: KeyData + Debug,
{
    let worker: u64 = scope.index() as u64;
    let logger = scope.danny_logger();
    let repetitions = hash_fns.repetitions();
    let vecs = Arc::clone(&global_vecs);
    let mut stopwatch = RepetitionStopWatch::new("repetition", worker == 0, logger);
    let mut bit_pools: HashMap<K, TensorPool> = HashMap::new();
    let mut bit_pools_intern: HashMap<K, TensorPool> = HashMap::new();
    info!("Computing the bit pools");
    let start = Instant::now();
    for (k, v) in vecs.iter_stripe(matrix, direction, worker) {
        bit_pools.insert(*k, hash_fns.pool(v));
        bit_pools_intern.insert(*k, hash_fns2.pool(v));
    }
    let end = Instant::now();
    info!(
        "Computed the bit pools ({:?}, {})",
        end - start,
        proc_mem!()
    );

    source(scope, "hashed source two round", move |capability| {
        let mut cap = Some(capability);
        move |output| {
            let mut done = false;
            if let Some(cap) = cap.as_mut() {
                for current_repetition in 0..repetitions {
                    stopwatch.maybe_stop();
                    stopwatch.start();
                    if worker == 0 {
                        debug!("Repetition {} (Hu et al. baseline)", current_repetition,);
                    }
                    let mut session = output.session(&cap);
                    for (k, v) in vecs.iter_stripe(matrix, direction, worker) {
                        let h = hash_fns.hash(&bit_pools[k], current_repetition as usize);
                        let s = sketcher.sketch(v);
                        session.give((
                            (current_repetition, h),
                            (
                                bit_pools[k].clone(),
                                hash_fns2.pool(v),
                                s.clone(),
                                (k.clone(), v.clone()),
                            ),
                        ));
                        // for inner_rep in 0..repetitions_inner {
                        //     let h2 = hash_fns2.hash(&bit_pools_intern[k], inner_rep as usize);
                        //     session.give(((current_repetition, h), ((inner_rep, h2), (k.clone(), v.clone()))));
                        // }
                    }
                }
                done = true;
            }

            if done {
                // Drop the capability to signal that we will send no more data
                cap = None;
            }
        }
    })
}
