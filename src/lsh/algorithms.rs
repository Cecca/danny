use crate::bloom::*;
use crate::config::*;
use crate::dataset::ChunkedDataset;
use crate::experiment::Experiment;
use crate::io::*;
use crate::logging::init_event_logging;
use crate::logging::*;
use crate::lsh::functions::*;
use crate::lsh::operators::*;
use crate::lsh::prefix_hash::*;
use crate::operators::Route;
use crate::operators::*;
use crate::sketch::*;
use abomonation::Abomonation;
use rand::{Rng, SeedableRng};
use serde::de::Deserialize;
use timely::dataflow::operators::concat::Concatenate;

use std::clone::Clone;

use std::fmt::Debug;
use std::hash::Hash;

use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex};

use std::time::Duration;

use timely::dataflow::channels::pact::Exchange as ExchangePact;
use timely::dataflow::channels::pact::Pipeline as PipelinePact;
use timely::dataflow::operators::capture::{Event as TimelyEvent, Extract};
use timely::dataflow::operators::*;

use timely::dataflow::*;

use timely::progress::timestamp::Timestamp;
use timely::Data;
use timely::ExchangeData;

#[allow(clippy::too_many_arguments)]
pub fn fixed_param_lsh<D, F, H, O, S, V, B, R>(
    left_path: &str,
    right_path: &str,
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
    for<'a> O:
        Data + Sync + Send + Clone + Abomonation + Debug + Route + Eq + Hash + Ord + PrefixHash<'a>,
    S: Sketcher<Input = D, Output = V> + Send + Sync + Clone + 'static,
    V: Data + Debug + Sync + Send + Clone + Abomonation + SketchEstimate + BitBasedSketch,
    R: Rng + SeedableRng + Send + Sync + Clone + 'static,
    B: Fn(usize, &mut R) -> LSHCollection<H, O> + Sized + Send + Sync + Clone + 'static,
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

    let estimator_samples = config.get_estimator_samples();
    let cost_balance = config.get_cost_balance();

    let bloom_filter = Arc::new(AtomicBloomFilter::<u32>::from_config(&config, rng.clone()));

    timely::execute::execute_from(timely_builder.0, timely_builder.1, move |mut worker| {
        let global_left = Arc::clone(&global_left);
        let global_right = Arc::clone(&global_right);
        let bloom_filter = Arc::clone(&bloom_filter);
        let hash_collection_builder = hash_collection_builder.clone();
        let mut rng = rng.clone();
        let execution_summary = init_event_logging(&worker);
        let output_send_ch = output_send_ch
            .lock()
            .expect("Cannot get lock on output channel")
            .clone();
        let sim_pred = sim_pred.clone();

        let sketcher_pair = sketcher_pair.clone();

        let probe = worker.dataflow::<u32, _, _>(move |scope| {
            let mut probe = ProbeHandle::new();
            let sketcher_pair = sketcher_pair;

            let candidates = match k {
                ParamK::Adaptive(min_k, max_k) => generate_candidates_adaptive(
                    Arc::clone(&global_left),
                    Arc::clone(&global_right),
                    min_k,
                    max_k,
                    estimator_samples,
                    cost_balance,
                    scope.clone(),
                    hash_collection_builder,
                    sketcher_pair,
                    probe.clone(),
                    &mut rng,
                ),
                k => generate_candidates_global_k(
                    Arc::clone(&global_left),
                    Arc::clone(&global_right),
                    k,
                    scope.clone(),
                    hash_collection_builder,
                    sketcher_pair,
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

        // info!(
        //     "Execution summary for worker {}: {:?}",
        //     index, execution_summary
        // );
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

        // let precision = count as f64 / global_summary.distinct_pairs as f64;
        // let _potential_pairs =
        //     D::num_elements(left_path.into()) * D::num_elements(right_path.into());
        // let fraction_distinct = global_summary.distinct_pairs as f64 / potential_pairs as f64;
        // info!(
        //     "Evaluated fraction of the potential pairs: {} ({}/{})",
        //     fraction_distinct, global_summary.distinct_pairs, potential_pairs
        // );
        // info!("Precision: {}", precision);

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
    sketcher_pair: Option<(S, SketchPredicate<SV>)>,
    probe: ProbeHandle<T>,
    rng: &mut R,
) -> Stream<G, (K, K)>
where
    K: ExchangeData + Debug + Route + Hash + Eq + Copy + Into<u64>,
    D: Data + Sync + Send + Clone + Abomonation + Debug,
    G: Scope<Timestamp = T>,
    T: Timestamp + Succ + ToStepId,
    F: LSHFunction<Input = D, Output = H> + Sync + Send + Clone + 'static,
    H: Data + Sync + Send + Clone + Abomonation + Debug + Route + Eq + Hash + Ord,
    S: Sketcher<Input = D, Output = SV> + Send + Sync + Clone + 'static,
    SV: Data + Debug + Sync + Send + Clone + Abomonation + SketchEstimate + BitBasedSketch,
    R: Rng + SeedableRng + Send + Sync + Clone + 'static,
    B: Fn(usize, &mut R) -> LSHCollection<F, H> + Sized + Send + Sync + Clone + 'static,
{
    let peers = scope.peers();
    let matrix = MatrixDescription::for_workers(peers as usize);

    let hash_fn = match k {
        ParamK::Exact(k) => hash_collection_builder(k, rng),
        ParamK::Adaptive(_, _) => panic!("You should not be here!!"),
    };

    match sketcher_pair {
        Some((sketcher, sketch_predicate)) => {
            let left_hashes = source_hashed_sketched(
                &scope,
                Arc::clone(&left),
                hash_fn.clone(),
                sketcher.clone(),
                matrix,
                MatrixDirection::Rows,
                probe.clone(),
            );
            let right_hashes = source_hashed_sketched(
                &scope,
                Arc::clone(&right),
                hash_fn.clone(),
                sketcher.clone(),
                matrix,
                MatrixDirection::Rows,
                probe.clone(),
            );
            left_hashes
                .bucket(&right_hashes)
                .filter_sketches(sketch_predicate)
        }
        None => {
            let left_hashes = source_hashed(
                &scope,
                Arc::clone(&left),
                hash_fn.clone(),
                matrix,
                MatrixDirection::Rows,
                probe.clone(),
            );
            let right_hashes = source_hashed(
                &scope,
                Arc::clone(&right),
                hash_fn.clone(),
                matrix,
                MatrixDirection::Columns,
                probe.clone(),
            );
            left_hashes.bucket(&right_hashes)
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn generate_candidates_adaptive<K, D, G, T, F, H, S, SV, R, B>(
    left: Arc<ChunkedDataset<K, D>>,
    right: Arc<ChunkedDataset<K, D>>,
    min_k: usize,
    max_k: usize,
    sample_size: usize,
    cost_balance: f64,
    scope: G,
    hash_collection_builder: B,
    sketcher_pair: Option<(S, SketchPredicate<SV>)>,
    probe: ProbeHandle<T>,
    rng: &mut R,
) -> Stream<G, (K, K)>
where
    K: Data + Sync + Send + Clone + Abomonation + Debug + Route + Hash + Eq + Ord,
    D: Data + Sync + Send + Clone + Abomonation + Debug,
    G: Scope<Timestamp = T>,
    T: Timestamp + Succ + ToStepId,
    F: LSHFunction<Input = D, Output = H> + Sync + Send + Clone + 'static,
    for<'a> H:
        Data + Sync + Send + Clone + Abomonation + Debug + Route + Eq + Hash + Ord + PrefixHash<'a>,
    S: Sketcher<Input = D, Output = SV> + Send + Sync + Clone + 'static,
    SV: Data + Debug + Sync + Send + Clone + Abomonation + SketchEstimate + BitBasedSketch,
    R: Rng + SeedableRng + Send + Sync + Clone + 'static,
    B: Fn(usize, &mut R) -> LSHCollection<F, H> + Sized + Send + Sync + Clone + 'static,
{
    let peers = scope.peers();
    let matrix = MatrixDescription::for_workers(peers as usize);

    let multihash = Arc::new(MultilevelHasher::new(
        min_k,
        max_k,
        hash_collection_builder,
        rng,
    ));

    let (levels_left, levels_right) = find_best_level(
        scope.clone(),
        Arc::clone(&left),
        Arc::clone(&right),
        Arc::clone(&multihash),
        matrix,
        cost_balance,
    );
    let levels_left = levels_left
        .matrix_distribute(MatrixDirection::Rows, matrix)
        .map(|triplet| (triplet.1, triplet.2));
    let levels_right = levels_right
        .matrix_distribute(MatrixDirection::Columns, matrix)
        .map(|triplet| (triplet.1, triplet.2));

    match sketcher_pair {
        Some((sketcher, sketch_predicate)) => {
            let left_hashes = source_hashed_adaptive_sketched(
                &scope,
                &levels_left,
                Arc::clone(&left),
                Arc::clone(&multihash),
                sketcher.clone(),
                matrix,
                MatrixDirection::Rows,
                probe.clone(),
                rng.clone(),
            );
            let right_hashes = source_hashed_adaptive_sketched(
                &scope,
                &levels_right,
                Arc::clone(&right),
                Arc::clone(&multihash),
                sketcher.clone(),
                matrix,
                MatrixDirection::Columns,
                probe.clone(),
                rng.clone(),
            );
            left_hashes
                .bucket_prefixes(&right_hashes, move |l, r| sketch_predicate.eval(&l.1, &r.1))
                .map(|(l, r)| (l.0, r.0))
        }
        None => {
            let left_hashes = source_hashed_adaptive(
                &scope,
                &levels_left,
                Arc::clone(&left),
                Arc::clone(&multihash),
                matrix,
                MatrixDirection::Rows,
                probe.clone(),
                rng.clone(),
            );
            let right_hashes = source_hashed_adaptive(
                &scope,
                &levels_right,
                Arc::clone(&right),
                Arc::clone(&multihash),
                matrix,
                MatrixDirection::Columns,
                probe.clone(),
                rng.clone(),
            );
            left_hashes.bucket_prefixes(&right_hashes, |_, _| true)
        }
    }
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
    K: Data + Route + Sync + Send + Clone + Abomonation + Debug + Hash + Into<u64> + Copy,
    D: Data + Sync + Send + Clone + Abomonation + Debug,
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
