use crate::config::*;
use crate::dataset::ChunkedDataset;
use crate::experiment::Experiment;
use crate::io::*;
use crate::logging::init_event_logging;
use crate::logging::*;
use crate::lsh::operators::*;
use crate::operators::*;
use danny_base::bloom::*;
use danny_base::bucket::*;
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
use std::time::Duration;
use std::time::Instant;
use timely::dataflow::channels::pact::Exchange as ExchangePact;
use timely::dataflow::channels::pact::Pipeline as PipelinePact;
use timely::dataflow::operators::capture::{Event as TimelyEvent, Extract};
use timely::dataflow::operators::generic::source;
use timely::dataflow::operators::*;
use timely::dataflow::*;
use timely::progress::timestamp::Timestamp;
use timely::ExchangeData;

#[allow(clippy::too_many_arguments)]
pub fn distributed_lsh<D, F, H, S, V, B, R>(
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
    let network = NetworkGauge::start();
    let timely_builder = config.get_timely_builder();
    // This channel is used to get the results
    let (output_send_ch, recv) = channel();
    let output_send_ch = Arc::new(Mutex::new(output_send_ch));

    let (send_exec_summary, recv_exec_summary) = channel();
    let send_exec_summary = Arc::new(Mutex::new(send_exec_summary));

    let hasher = TensorCollection::new(k, range, hash_function_builder, rng);
    let hasher = Arc::new(hasher);

    let rng = rng.clone();

    debug!(
        "Left dataset has {} points, right has {}",
        D::num_elements(left_path.into()),
        D::num_elements(right_path.into())
    );
    let (global_left, global_right) = load_vectors(left_path, right_path, &config);

    let bloom_filter = Arc::new(AtomicBloomFilter::<ElementId>::new(
        config.get_bloom_bits(),
        config.get_bloom_k(),
        rng.clone(),
    ));
    let bloom_filter_pre_communication = Arc::new(AtomicBloomFilter::<ElementId>::new(
        config.get_bloom_bits(),
        config.get_bloom_k(),
        rng.clone(),
    ));

    timely::execute::execute_from(timely_builder.0, timely_builder.1, move |mut worker| {
        let global_left = Arc::clone(&global_left);
        let global_right = Arc::clone(&global_right);
        let bloom_filter = Arc::clone(&bloom_filter);
        let hasher = Arc::clone(&hasher);
        let bloom_filter_pre_communication = Arc::clone(&bloom_filter_pre_communication);
        let mut rng = rng.clone();
        let execution_summary = init_event_logging(&worker);
        let output_send_ch = output_send_ch
            .lock()
            .expect("Cannot get lock on output channel")
            .clone();
        let sim_pred = sim_pred.clone();
        let sketch_predicate = sketch_predicate.clone();
        let sketcher = sketcher.clone();

        let probe = worker.dataflow::<u32, _, _>(move |scope| {
            let mut probe = ProbeHandle::new();

            let candidates = generate_candidates_global_k(
                Arc::clone(&global_left),
                Arc::clone(&global_right),
                range,
                k,
                scope.clone(),
                hasher,
                sketcher,
                sketch_predicate,
                Arc::clone(&bloom_filter_pre_communication),
                probe.clone(),
                &mut rng,
            );

            candidates_filter_count(
                candidates,
                Arc::clone(&global_left),
                Arc::clone(&global_right),
                sim_pred,
                Arc::clone(&bloom_filter),
            )
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
            .map(|pair| pair.1.clone().iter().sum::<u64>())
            .sum();

        count as usize
    } else {
        0
    }
}

#[allow(clippy::too_many_arguments)]
fn generate_candidates_global_k<K, D, G, T, F, S, SV, R>(
    left: Arc<ChunkedDataset<K, D>>,
    right: Arc<ChunkedDataset<K, D>>,
    _range: f64,
    _k: usize,
    scope: G,
    hasher: Arc<TensorCollection<F>>,
    sketcher: S,
    sketch_predicate: SketchPredicate<SV>,
    _filter: Arc<AtomicBloomFilter<K>>,
    probe: ProbeHandle<T>,
    _rng: &mut R,
) -> Stream<G, (K, K)>
where
    K: KeyData + Debug + Into<u64>,
    D: ExchangeData + Debug,
    G: Scope<Timestamp = T>,
    T: Timestamp + Succ + ToStepId,
    F: LSHFunction<Input = D, Output = u32> + Sync + Send + Clone + 'static,
    S: Sketcher<Input = D, Output = SV> + Send + Sync + Clone + 'static,
    SV: SketchData + Debug,
    R: Rng + SeedableRng + Send + Sync + Clone + 'static,
{
    let worker = scope.index() as u64;
    let peers = scope.peers();
    let matrix = MatrixDescription::for_workers(peers as usize);
    let sketcher = Arc::new(sketcher);

    let sketches_left = build_sketches(
        Arc::clone(&left),
        Arc::clone(&sketcher),
        worker,
        matrix,
        MatrixDirection::Rows,
    );
    let sketches_right = build_sketches(
        Arc::clone(&right),
        Arc::clone(&sketcher),
        worker,
        matrix,
        MatrixDirection::Columns,
    );

    let left_hashes = source_hashed_sketched(
        &scope,
        Arc::clone(&left),
        Arc::clone(&hasher),
        sketches_left,
        matrix,
        MatrixDirection::Rows,
        probe.clone(),
    );
    let right_hashes = source_hashed_sketched(
        &scope,
        Arc::clone(&right),
        Arc::clone(&hasher),
        sketches_right,
        matrix,
        MatrixDirection::Columns,
        probe.clone(),
    );
    left_hashes.bucket_pred(
        &right_hashes,
        move |a, b| sketch_predicate.eval(&a.0, &b.0),
        // move |a, b| !filter.test_and_insert(&(a.1, b.1)),
        |_, _| true,
        |x| x.1,
    )
}

fn build_sketches<D, K, S, SV>(
    vectors: Arc<ChunkedDataset<K, D>>,
    sketcher: Arc<S>,
    worker: u64,
    matrix: MatrixDescription,
    direction: MatrixDirection,
) -> Arc<HashMap<K, SV>>
where
    D: ExchangeData,
    K: KeyData + Debug,
    S: Sketcher<Input = D, Output = SV> + Clone + 'static,
    SV: SketchData + Debug,
{
    let mut sketches: HashMap<K, SV> =
        HashMap::with_capacity(vectors.stripe_len(matrix, direction, worker));
    debug!("Computing sketches");
    let start_sketch = Instant::now();
    for (k, v) in vectors.iter_stripe(matrix, direction, worker) {
        let s = sketcher.sketch(v);
        sketches.insert(k.clone(), s);
    }
    let end_sketch = Instant::now();
    debug!("Sketches computed in {:?}", end_sketch - start_sketch);
    Arc::new(sketches)
}

fn candidates_filter_count<G, T, K, D, F>(
    candidates: Stream<G, (K, K)>,
    global_left: Arc<ChunkedDataset<K, D>>,
    global_right: Arc<ChunkedDataset<K, D>>,
    sim_pred: F,
    bloom_filter: Arc<AtomicBloomFilter<K>>,
) -> Stream<G, u64>
where
    G: Scope<Timestamp = T>,
    T: Timestamp + Succ + ToStepId,
    K: KeyData + Debug + Into<u64>,
    D: ExchangeData + Debug,
    F: Fn(&D, &D) -> bool + Send + Clone + Sync + 'static,
{
    let peers = candidates.scope().peers();
    let matrix = MatrixDescription::for_workers(peers as usize);
    let logger = candidates.scope().danny_logger();

    candidates
        .approximate_distinct_atomic(
            ExchangePact::new(move |pair: &(K, K)| {
                let row = pair.0.route() % u64::from(matrix.rows);
                let col = pair.1.route() % u64::from(matrix.columns);
                matrix.row_major(row as u8, col as u8)
            }),
            Arc::clone(&bloom_filter),
        )
        .unary(PipelinePact, "count-matching", move |_, _| {
            let mut pl =
                ProgressLogger::new(Duration::from_secs(60), "comparisons".to_owned(), None);
            move |input, output| {
                input.for_each(|t, d| {
                    let _pg = ProfileGuard::new(
                        logger.clone(),
                        t.time().to_step_id(),
                        1,
                        "distance_computation",
                    );
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

fn simple_source<G, K, F, D, S>(
    scope: &G,
    vecs: Arc<ChunkedDataset<K, D>>,
    sketcher: Arc<S>,
    hash_fns: Arc<TensorCollection<F>>,
    throttle: Option<ProbeHandle<G::Timestamp>>,
    worker: u64,
    matrix: MatrixDescription,
    direction: MatrixDirection,
) -> Stream<G, (K, (TensorPool, S::Output))>
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
                    let output_element = (k.clone(), (pool.clone(), s.clone()));
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

/// A simple take on the fixed parameter algorithm.
pub fn simple_fixed<D, F, H, S, V, B, R>(
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
    let network = NetworkGauge::start();
    let timely_builder = config.get_timely_builder();
    // This channel is used to get the results
    let (output_send_ch, recv) = channel();
    let output_send_ch = Arc::new(Mutex::new(output_send_ch));

    let (send_exec_summary, recv_exec_summary) = channel();
    let send_exec_summary = Arc::new(Mutex::new(send_exec_summary));

    let hasher = Arc::new(TensorCollection::new(k, range, hash_function_builder, rng));

    let rng = rng.clone();

    debug!(
        "Left dataset has {} points, right has {}",
        D::num_elements(left_path.into()),
        D::num_elements(right_path.into())
    );
    let (global_left, global_right) = load_vectors::<D>(left_path, right_path, &config);

    let bloom_filter = Arc::new(AtomicBloomFilter::<ElementId>::new(
        config.get_bloom_bits(),
        config.get_bloom_k(),
        rng.clone(),
    ));

    timely::execute::execute_from(timely_builder.0, timely_builder.1, move |mut worker| {
        let global_left = Arc::clone(&global_left);
        let global_right = Arc::clone(&global_right);
        // let bloom_filter = Arc::clone(&bloom_filter);
        let hasher = Arc::clone(&hasher);
        let rng = rng.clone();
        let execution_summary = init_event_logging(&worker);
        let output_send_ch = output_send_ch
            .lock()
            .expect("Cannot get lock on output channel")
            .clone();
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

            let left = simple_source(
                scope,
                Arc::clone(&global_left),
                Arc::clone(&sketcher),
                Arc::clone(&hasher),
                Some(probe.clone()),
                worker_index,
                matrix,
                MatrixDirection::Rows,
            );
            let right = simple_source(
                scope,
                Arc::clone(&global_right),
                Arc::clone(&sketcher),
                Arc::clone(&hasher),
                Some(probe.clone()),
                worker_index,
                matrix,
                MatrixDirection::Columns,
            );

            left.binary_frontier(&right, PipelinePact, PipelinePact, "bucket", move |_, _| {
                let mut notificator = FrontierNotificator::new();
                let mut left_data = HashMap::new();
                let mut right_data = HashMap::new();
                move |left_in, right_in, output| {
                    left_in.for_each(|t, data| {
                        let local_data = left_data.entry(t.time().clone()).or_insert_with(|| {
                            Vec::with_capacity(global_left.chunk_len(worker_row as usize))
                        });
                        for (k, (p, s)) in data.replace(Vec::new()).drain(..) {
                            local_data.push((k, p, s));
                        }
                        notificator.notify_at(t.retain());
                    });
                    right_in.for_each(|t, data| {
                        let local_data = right_data.entry(t.time().clone()).or_insert_with(|| {
                            Vec::with_capacity(global_right.chunk_len(worker_col as usize))
                        });
                        for (k, (p, s)) in data.replace(Vec::new()).drain(..) {
                            local_data.push((k, p, s));
                        }
                        notificator.notify_at(t.retain());
                    });
                    notificator.for_each(&[left_in.frontier(), &right_in.frontier()], |t, _| {
                        if let Some(left_data) = left_data.remove(&t) {
                            let right_data = right_data.remove(&t).expect("missing right data");
                            let repetitions = hasher.repetitions();
                            let mut cnt = 0;
                            for rep in 0..repetitions {
                                let _pg = ProfileGuard::new(logger.clone(), rep, 0, "repetition");
                                let start = Instant::now();
                                let mut bucket = Bucket::default();
                                for (k, pool, sketch) in left_data.iter() {
                                    bucket.push_left(hasher.hash(pool, rep), (*k, *sketch, pool));
                                }
                                for (k, pool, sketch) in right_data.iter() {
                                    bucket.push_right(hasher.hash(pool, rep), (*k, *sketch, pool));
                                }
                                let mut sketch_discarded = 0;
                                let mut duplicates_discarded = 0;
                                let mut examined_pairs = 0;
                                bucket.for_all(|(lk, l_sketch, l_pool), (rk, r_sketch, r_pool)| {
                                    examined_pairs += 1;
                                    if !hasher.already_seen(l_pool, r_pool, rep) {
                                        if sketch_predicate.eval(l_sketch, r_sketch) {
                                            if sim_pred(&global_left[lk], &global_right[rk]) {
                                                cnt += 1;
                                            }
                                        } else {
                                            sketch_discarded += 1;
                                        }
                                    } else {
                                        duplicates_discarded += 1;
                                    }
                                });
                                let end = Instant::now();
                                info!("Repetition {} ended in {:?}", rep, end - start);
                                log_event!(logger, LogEvent::GeneratedPairs(rep, examined_pairs));
                                log_event!(
                                    logger,
                                    LogEvent::SketchDiscarded(rep, sketch_discarded)
                                );
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

#[allow(clippy::too_many_arguments)]
pub fn hu_baseline<D, F, H, B, R>(
    left_path: &str,
    right_path: &str,
    range: f64,
    k: usize,
    hash_function_builder: B,
    sim_pred: F,
    rng: &mut R,
    config: &Config,
    experiment: &mut Experiment,
) -> usize
where
    for<'de> D: ReadBinaryFile + Deserialize<'de> + ExchangeData + Debug + SketchEstimate,
    F: Fn(&D, &D) -> bool + Send + Clone + Sync + 'static,
    H: LSHFunction<Input = D, Output = u32> + Sync + Send + Clone + 'static,
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

    let rng = rng.clone();

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
        let mut rng = rng.clone();
        let execution_summary = init_event_logging(&worker);
        let output_send_ch = output_send_ch
            .lock()
            .expect("Cannot get lock on output channel")
            .clone();
        let sim_pred = sim_pred.clone();

        let probe = worker.dataflow::<u32, _, _>(move |scope| {
            let mut probe = ProbeHandle::new();
            let matrix = MatrixDescription::for_workers(scope.peers() as usize);

            let left_hashes = source_hashed_one_round(
                scope,
                Arc::clone(&global_left),
                Arc::clone(&hasher),
                matrix,
                MatrixDirection::Rows,
            );
            let right_hashes = source_hashed_one_round(
                scope,
                Arc::clone(&global_right),
                Arc::clone(&hasher),
                matrix,
                MatrixDirection::Columns,
            );
            left_hashes
                .bucket_pred_count(&right_hashes, move |l, r| sim_pred(&l.1, &r.1))
                .inspect(|cnt| info!("Count before exchange {}", cnt))
                .exchange(|_| 0) // Bring all the counts to the first worker
                .inspect(|cnt| info!("Count after exchange {}", cnt))
                .probe_with(&mut probe)
                .capture_into(output_send_ch);

            probe
        });

        // Do the stepping even though it's not strictly needed: we use it to wait for the dataflow
        // to finish
        // worker.step_while(|| probe.less_than(&(repetitions as u32)));
        worker.step_while(|| probe.with_frontier(|f| !f.is_empty()));

        info!("Finished stepping");

        collect_execution_summaries(execution_summary, send_exec_summary.clone(), &mut worker);
    })
    .expect("Problems with the dataflow");

    info!("Collecting summaries");

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
