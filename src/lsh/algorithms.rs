use crate::bloom::*;
use crate::config::*;
use crate::dataset::ChunkedDataset;
use crate::experiment::Experiment;
use crate::io::*;
use crate::logging::init_event_logging;
use crate::logging::*;
use crate::lsh::adaptive::*;
use crate::lsh::functions::*;
use crate::lsh::operators::*;
use crate::lsh::prefix_hash::*;
use crate::operators::*;
use crate::sketch::*;
use crate::types::*;
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
use timely::dataflow::operators::*;
use timely::dataflow::*;
use timely::progress::timestamp::Timestamp;
use timely::ExchangeData;

#[allow(clippy::too_many_arguments)]
pub fn fixed_param_lsh<D, F, H, O, S, V, B, R>(
    left_path: &str,
    right_path: &str,
    k: ParamK,
    hash_collection_builder: B,
    sketcher: S,
    sketch_predicate: SketchPredicate<V>,
    sim_pred: F,
    rng: &mut R,
    config: &Config,
    experiment: &mut Experiment,
) -> usize
where
    for<'de> D: ReadBinaryFile + Deserialize<'de> + ExchangeData + Debug,
    F: Fn(&D, &D) -> bool + Send + Clone + Sync + 'static,
    H: LSHFunction<Input = D, Output = O> + Sync + Send + Clone + 'static,
    O: HashData + PrefixHash + Debug,
    S: Sketcher<Input = D, Output = V> + Send + Sync + Clone + 'static,
    V: SketchData + Debug,
    R: Rng + SeedableRng + Send + Sync + Clone + 'static,
    B: Fn(usize, &mut R) -> LSHCollection<H> + Sized + Send + Sync + Clone + 'static,
{
    let network = NetworkGauge::start();
    let timely_builder = config.get_timely_builder();
    // This channel is used to get the results
    let (output_send_ch, recv) = channel();
    let output_send_ch = Arc::new(Mutex::new(output_send_ch));

    let (send_exec_summary, recv_exec_summary) = channel();
    let send_exec_summary = Arc::new(Mutex::new(send_exec_summary));

    let hash_collection_builder = hash_collection_builder.clone();
    let rng = rng.clone();

    info!(
        "Left dataset has {} points, right has {}",
        D::num_elements(left_path.into()),
        D::num_elements(right_path.into())
    );
    let (global_left, global_right) = load_vectors(left_path, right_path, &config);

    let bloom_filter = Arc::new(AtomicBloomFilter::<u32>::from_config(&config, rng.clone()));
    let bloom_filter_pre_communication =
        Arc::new(AtomicBloomFilter::<u32>::from_config(&config, rng.clone()));

    timely::execute::execute_from(timely_builder.0, timely_builder.1, move |mut worker| {
        let global_left = Arc::clone(&global_left);
        let global_right = Arc::clone(&global_right);
        let bloom_filter = Arc::clone(&bloom_filter);
        let bloom_filter_pre_communication = Arc::clone(&bloom_filter_pre_communication);
        let hash_collection_builder = hash_collection_builder.clone();
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

            let candidates = match k {
                ParamK::Adaptive(min_k, max_k) => generate_candidates_adaptive(
                    Arc::clone(&global_left),
                    Arc::clone(&global_right),
                    min_k,
                    max_k,
                    scope.clone(),
                    hash_collection_builder,
                    sketcher,
                    sketch_predicate,
                    Arc::clone(&bloom_filter_pre_communication),
                    probe.clone(),
                    &mut rng,
                ),
                k => generate_candidates_global_k(
                    Arc::clone(&global_left),
                    Arc::clone(&global_right),
                    k,
                    scope.clone(),
                    hash_collection_builder,
                    sketcher,
                    sketch_predicate,
                    Arc::clone(&bloom_filter_pre_communication),
                    probe.clone(),
                    &mut rng,
                ),
            };

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
fn generate_candidates_global_k<K, D, G, T, F, H, S, SV, R, B>(
    left: Arc<ChunkedDataset<K, D>>,
    right: Arc<ChunkedDataset<K, D>>,
    k: ParamK,
    scope: G,
    hash_collection_builder: B,
    sketcher: S,
    sketch_predicate: SketchPredicate<SV>,
    filter: Arc<AtomicBloomFilter<K>>,
    probe: ProbeHandle<T>,
    rng: &mut R,
) -> Stream<G, (K, K)>
where
    K: KeyData + Debug + Into<u64>,
    D: ExchangeData + Debug,
    G: Scope<Timestamp = T>,
    T: Timestamp + Succ + ToStepId,
    F: LSHFunction<Input = D, Output = H> + Sync + Send + Clone + 'static,
    H: HashData + Debug,
    S: Sketcher<Input = D, Output = SV> + Send + Sync + Clone + 'static,
    SV: SketchData + Debug,
    R: Rng + SeedableRng + Send + Sync + Clone + 'static,
    B: Fn(usize, &mut R) -> LSHCollection<F> + Sized + Send + Sync + Clone + 'static,
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

    let hash_fn = match k {
        ParamK::Fixed(k) => hash_collection_builder(k, rng),
        ParamK::Adaptive(_, _) => panic!("You should not be here!!"),
    };

    let left_hashes = source_hashed_sketched(
        &scope,
        Arc::clone(&left),
        hash_fn.clone(),
        sketches_left,
        matrix,
        MatrixDirection::Rows,
        probe.clone(),
    );
    let right_hashes = source_hashed_sketched(
        &scope,
        Arc::clone(&right),
        hash_fn.clone(),
        sketches_right,
        matrix,
        MatrixDirection::Columns,
        probe.clone(),
    );
    left_hashes
        .bucket_pred(
            &right_hashes,
            move |a, b| sketch_predicate.eval(&a.0, &b.0),
            move |a, b| !filter.test_and_insert(&(a.1, b.1)),
        )
        .map(|(l, r)| (l.1, r.1))
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
    info!("Computing sketches");
    let start_sketch = Instant::now();
    for (k, v) in vectors.iter_stripe(matrix, direction, worker) {
        let s = sketcher.sketch(v);
        sketches.insert(k.clone(), s);
    }
    let end_sketch = Instant::now();
    info!("Sketches computed in {:?}", end_sketch - start_sketch);
    Arc::new(sketches)
}

#[allow(clippy::too_many_arguments)]
fn generate_candidates_adaptive<K, D, G, T, F, H, S, SV, R, B>(
    left: Arc<ChunkedDataset<K, D>>,
    right: Arc<ChunkedDataset<K, D>>,
    min_k: usize,
    max_k: usize,
    scope: G,
    hash_collection_builder: B,
    sketcher: S,
    sketch_predicate: SketchPredicate<SV>,
    filter: Arc<AtomicBloomFilter<K>>,
    probe: ProbeHandle<T>,
    rng: &mut R,
) -> Stream<G, (K, K)>
where
    K: KeyData + Debug + Into<u64>,
    D: ExchangeData + Debug,
    G: Scope<Timestamp = T>,
    T: Timestamp + Succ + ToStepId,
    F: LSHFunction<Input = D, Output = H> + Sync + Send + Clone + 'static,
    H: HashData + Debug + PrefixHash,
    S: Sketcher<Input = D, Output = SV> + Send + Sync + Clone + 'static,
    SV: SketchData + Debug,
    R: Rng + SeedableRng + Send + Sync + Clone + 'static,
    B: Fn(usize, &mut R) -> LSHCollection<F> + Sized + Send + Sync + Clone + 'static,
{
    let peers = scope.peers();
    let matrix = MatrixDescription::for_workers(peers as usize);
    let _logger = scope.danny_logger();
    let worker: u64 = scope.index() as u64;
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

    let multihash = Arc::new(MultilevelHasher::new(
        min_k,
        max_k,
        hash_collection_builder,
        rng,
    ));
    let min_level = multihash.min_level();

    let (levels_left, levels_right) = find_best_level(
        scope.clone(),
        Arc::clone(&left),
        Arc::clone(&right),
        Arc::clone(&multihash),
        Arc::clone(&sketches_left),
        Arc::clone(&sketches_right),
        matrix,
        rng.clone(),
    );
    let levels_left = levels_left
        .matrix_distribute(MatrixDirection::Rows, matrix)
        .map(|triplet| (triplet.1, triplet.2));
    let levels_right = levels_right
        .matrix_distribute(MatrixDirection::Columns, matrix)
        .map(|triplet| (triplet.1, triplet.2));

    let left_hashes = source_hashed_adaptive_sketched(
        &scope,
        &levels_left,
        Arc::clone(&left),
        Arc::clone(&multihash),
        Arc::clone(&sketches_left),
        matrix,
        MatrixDirection::Rows,
        probe.clone(),
    );
    let right_hashes = source_hashed_adaptive_sketched(
        &scope,
        &levels_right,
        Arc::clone(&right),
        Arc::clone(&multihash),
        Arc::clone(&sketches_right),
        matrix,
        MatrixDirection::Columns,
        probe.clone(),
    );
    left_hashes
        .bucket_prefixes(
            &right_hashes,
            min_level,
            move |l, r| sketch_predicate.eval(&l.1, &r.1),
            move |l: &(K, SV), r: &(K, SV)| !filter.test_and_insert(&(l.0, r.0)),
        )
        .map(|(l, r)| (l.0, r.0))
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
