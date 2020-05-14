use crate::config::*;
use crate::dataset::ChunkedDataset;
use crate::experiment::Experiment;
use crate::io::*;
use crate::join::Joiner;
use crate::logging::init_event_logging;
use crate::logging::*;
use crate::operators::*;
use danny_base::lsh::*;
use danny_base::sketch::*;
use danny_base::types::*;
use rand::{Rng, SeedableRng};
use serde::de::Deserialize;
use std::clone::Clone;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use timely::communication::Allocator;
use timely::dataflow::channels::pact::Pipeline as PipelinePact;
use timely::dataflow::operators::capture::{Event as TimelyEvent, Extract};
use timely::dataflow::operators::generic::source;
use timely::dataflow::operators::*;
use timely::dataflow::*;
use timely::worker::Worker;
use timely::ExchangeData;

fn simple_source<G, K, F, D, S>(
    scope: &G,
    vecs: Arc<ChunkedDataset<K, D>>,
    sketcher: Arc<S>,
    hash_fns: Arc<TensorCollection<F>>,
    worker: u64,
    matrix: MatrixDescription,
    direction: MatrixDirection,
) -> Stream<G, (K, (D, TensorPool, S::Output))>
where
    G: Scope,
    G::Timestamp: Succ,
    K: KeyData + Debug,
    D: ExchangeData + SketchEstimate + Debug,
    S: Sketcher<Input = D> + Clone + 'static,
    S::Output: SketchData + Debug,
    F: LSHFunction<Input = D, Output = u32> + Sync + Send + Clone + 'static,
{
    let logger = scope.danny_logger();
    source(scope, "hashed source", move |capability| {
        let mut cap = Some(capability);
        move |output| {
            if let Some(cap) = cap.take() {
                let _pg = ProfileGuard::new(logger.clone(), 0, 0, "sketching_hashing");
                let start = Instant::now();
                let mut session = output.session(&cap);
                for (k, v) in vecs.iter_stripe(matrix, direction, worker) {
                    let pool = hash_fns.pool(v);
                    let s = sketcher.sketch(v);
                    let output_element = (k.clone(), (v.clone(), pool.clone(), s.clone()));
                    match direction {
                        MatrixDirection::Columns => {
                            let col = (k.route() % u64::from(matrix.columns)) as u8;
                            for row in 0..matrix.rows {
                                session.give(((row, col), output_element.clone()));
                            }
                        }
                        MatrixDirection::Rows => {
                            let row = (k.route() % u64::from(matrix.rows)) as u8;
                            for col in 0..matrix.columns {
                                session.give(((row, col), output_element.clone()));
                            }
                        }
                    };
                }
                let end = Instant::now();
                info!("Distributed sketches and pools in {:?}", end - start);
            }
        }
    })
    .exchange(move |tuple| matrix.row_major((tuple.0).0, (tuple.0).1))
    .map(|pair| pair.1)
}

pub fn one_round_lsh<D, F, H, S, V, B, R>(
    worker: &mut Worker<Allocator>,
    left_path: &str,
    right_path: &str,
    range: f64,
    k: usize,
    hash_function_builder: B,
    sketcher: S,
    sketch_predicate: SketchPredicate<V>,
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
    let no_dedup = config.no_dedup;
    let no_verify = config.no_verify;
    let network = NetworkGauge::start();
    // This channel is used to get the results
    let (output_send_ch, recv) = channel();
    // let output_send_ch = Arc::new(Mutex::new(output_send_ch));

    let (send_exec_summary, recv_exec_summary) = channel();
    let send_exec_summary = Arc::new(Mutex::new(send_exec_summary));

    let hasher = Arc::new(TensorCollection::new(
        k,
        range,
        config.recall,
        hash_function_builder,
        rng,
    ));

    info!(
        "Left dataset has {} points, right has {}",
        D::num_elements(left_path.into()),
        D::num_elements(right_path.into())
    );
    let (global_left, global_right) = load_vectors::<D>(worker, left_path, right_path, &config);

    let global_left = Arc::clone(&global_left);
    let global_right = Arc::clone(&global_right);
    // let bloom_filter = Arc::clone(&bloom_filter);
    let hasher = Arc::clone(&hasher);
    let execution_summary = init_event_logging(&worker);
    // let output_send_ch = output_send_ch
    //     .lock()
    //     .expect("Cannot get lock on output channel")
    //     .clone();
    let sim_pred = sim_pred.clone();
    let sketch_predicate = sketch_predicate.clone();
    let sketcher = sketcher.clone();
    let sketcher = Arc::new(sketcher);
    let worker_index = worker.index() as u64;
    let matrix = MatrixDescription::for_workers(worker.peers());
    let (worker_row, worker_col) = matrix.row_major_to_pair(worker_index);

    let probe = worker.dataflow::<u32, _, _>(move |scope| {
        let mut probe = ProbeHandle::<u32>::new();
        let global_left = Arc::clone(&global_left);
        let global_right = Arc::clone(&global_right);
        let logger = scope.danny_logger();
        let mut pl = progress_logger::ProgressLogger::builder()
            .with_items_name("repetitions")
            .with_expected_updates(hasher.repetitions() as u64)
            .start();

        let left = simple_source(
            scope,
            Arc::clone(&global_left),
            Arc::clone(&sketcher),
            Arc::clone(&hasher),
            worker_index,
            matrix,
            MatrixDirection::Rows,
        );
        let right = simple_source(
            scope,
            Arc::clone(&global_right),
            Arc::clone(&sketcher),
            Arc::clone(&hasher),
            worker_index,
            matrix,
            MatrixDirection::Columns,
        );

        left.binary_frontier(&right, PipelinePact, PipelinePact, "bucket", move |_, _| {
            let mut notificator = FrontierNotificator::new();
            let mut left_data = HashMap::new();
            let mut right_data = HashMap::new();
            let mut left_vectors = HashMap::new();
            let mut right_vectors = HashMap::new();
            move |left_in, right_in, output| {
                left_in.for_each(|t, data| {
                    log_event!(logger, LogEvent::Load(t.time().to_step_id(), data.len()));
                    let local_data = left_data.entry(t.time().clone()).or_insert_with(|| {
                        Vec::with_capacity(global_left.chunk_len(worker_row as usize))
                    });
                    let local_vectors = left_vectors
                        .entry(t.time().clone())
                        .or_insert_with(HashMap::new);
                    for (k, (v, p, s)) in data.replace(Vec::new()).drain(..) {
                        local_data.push((k, p, s));
                        local_vectors.entry(k).or_insert(v);
                    }
                    notificator.notify_at(t.retain());
                });
                right_in.for_each(|t, data| {
                    log_event!(logger, LogEvent::Load(t.time().to_step_id(), data.len()));
                    let local_data = right_data.entry(t.time().clone()).or_insert_with(|| {
                        Vec::with_capacity(global_right.chunk_len(worker_col as usize))
                    });
                    let local_vectors = right_vectors
                        .entry(t.time().clone())
                        .or_insert_with(HashMap::<ElementId, D>::new);
                    for (k, (v, p, s)) in data.replace(Vec::new()).drain(..) {
                        local_data.push((k, p, s));
                        local_vectors.entry(k).or_insert(v);
                    }
                    notificator.notify_at(t.retain());
                });
                notificator.for_each(&[left_in.frontier(), &right_in.frontier()], |t, _| {
                    if let Some(left_data) = left_data.remove(&t) {
                        let right_data = right_data.remove(&t).expect("missing right data");
                        let left_vectors = left_vectors.remove(&t).expect("missing left vectors");
                        let right_vectors =
                            right_vectors.remove(&t).expect("missing right vectors");
                        let repetitions = hasher.repetitions();
                        let mut cnt = 0;
                        info!("Starting {} repetitions", repetitions);
                        for rep in 0..repetitions {
                            let _pg = ProfileGuard::new(logger.clone(), rep, 0, "repetition");
                            let start = Instant::now();
                            // let mut bucket = Bucket::default();
                            let mut joiner = Joiner::default();
                            for (k, pool, sketch) in left_data.iter() {
                                joiner.push_left(hasher.hash(pool, rep), (*k, *sketch, pool));
                            }
                            for (k, pool, sketch) in right_data.iter() {
                                joiner.push_right(hasher.hash(pool, rep), (*k, *sketch, pool));
                            }
                            let mut sketch_discarded = 0;
                            let mut duplicates_discarded = 0;
                            let mut examined_pairs = 0;
                            joiner.join_map(
                                |_hash, (lk, l_sketch, l_pool), (rk, r_sketch, r_pool)| {
                                    examined_pairs += 1;
                                    if sketch_predicate.eval(l_sketch, r_sketch) {
                                        if no_verify
                                            || sim_pred(&left_vectors[lk], &right_vectors[rk])
                                        {
                                            if no_dedup || !hasher.already_seen(l_pool, r_pool, rep)
                                            {
                                                cnt += 1;
                                            } else {
                                                duplicates_discarded += 1;
                                            }
                                        }
                                    } else {
                                        sketch_discarded += 1;
                                    }
                                },
                            );
                            let end = Instant::now();
                            info!("Repetition {} ended in {:?}", rep, end - start);
                            pl.update(1u64);
                            log_event!(logger, LogEvent::GeneratedPairs(rep, examined_pairs));
                            log_event!(logger, LogEvent::SketchDiscarded(rep, sketch_discarded));
                            log_event!(
                                logger,
                                LogEvent::DuplicatesDiscarded(rep, duplicates_discarded)
                            );
                        }
                        output.session(&t).give(cnt);
                    }
                });
            }
        })
        .stream_sum()
        .exchange(|_| 0) // Bring all the counts to the first worker
        .inspect_time(|t, cnt| println!("count at {}: {}", t, cnt))
        .probe_with(&mut probe)
        .capture_into(output_send_ch);

        probe
    });

    worker.step_while(|| !probe.done());

    info!("Collect execution summaries");
    collect_execution_summaries(execution_summary, send_exec_summary.clone(), worker);

    info!("Collect network summaries");
    let network_summaries = network.map(|n| n.measure().collect_from_workers(worker, &config));

    if config.is_master() {
        // let mut exec_summaries = Vec::new();
        // for summary in recv_exec_summary.iter() {
        //     if let TimelyEvent::Messages(_, msgs) = summary {
        //         exec_summaries.extend(msgs);
        //     }
        // }
        // for summary in exec_summaries.iter() {
        //     summary.add_to_experiment(experiment);
        // }
        // if network_summaries.is_some() {
        //     network_summaries
        //         .unwrap()
        //         .iter()
        //         .for_each(|n| n.report(experiment));
        // }
        info!("Get elements out of count");
        // From `recv` we get an entry for each timestamp, containing a one-element vector with the
        // count of output pairs for a given timestamp. We sum across all the timestamps, so we need to
        // remove the duplicates
        let count: u64 = recv
            .extract()
            .iter()
            .map(|pair: &(u32, Vec<u64>)| {
                let cnt = pair.1.clone().iter().sum::<u64>();
                println!("Time {}, count {}", pair.0, cnt);
                cnt
            })
            .sum();

        count as usize
    } else {
        0
    }
}
