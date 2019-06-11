use crate::dataset::*;
use crate::logging::*;
use crate::lsh::*;
use crate::operators::*;
use crate::sketch::*;
use abomonation::Abomonation;
use rand::Rng;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use timely::communication::Push;
use timely::dataflow::channels::pact::{Exchange as ExchangePact, Pipeline};
use timely::dataflow::operators::aggregation::Aggregate;
use timely::dataflow::operators::aggregation::StateMachine;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::generic::source;
use timely::dataflow::operators::generic::{FrontieredInputHandle, OutputHandle};
use timely::dataflow::operators::Capability;
use timely::dataflow::operators::Leave;
use timely::dataflow::operators::*;
use timely::dataflow::scopes::Child;
use timely::dataflow::*;
use timely::logging::Logger;
use timely::order::Product;
use timely::progress::timestamp::PathSummary;
use timely::progress::Timestamp;
use timely::Data;
use timely::ExchangeData;

#[allow(dead_code, clippy::too_many_arguments)]
fn sample_sketches<G, K, D, H, F, V, R>(
    scope: &G,
    vecs: Arc<ChunkedDataset<K, D>>,
    sample_probability: f64,
    hasher: Arc<MultilevelHasher<D, H, F>>,
    sketches: Arc<HashMap<K, V>>,
    matrix: MatrixDescription,
    direction: MatrixDirection,
    mut rng: R,
) -> Stream<G, V>
where
    G: Scope,
    D: ExchangeData + Debug,
    H: ExchangeData + Route + Debug + Eq + Hash + Ord,
    K: ExchangeData + Debug + Route + Eq + Hash,
    F: LSHFunction<Input = D, Output = H> + Send + Clone + Sync + 'static,
    V: ExchangeData + Debug,
    R: Rng + Clone + 'static,
{
    let worker = scope.index() as u64;
    let vecs = Arc::clone(&vecs);
    let mut done = false;
    let max_level = hasher.max_level();
    let num_repetitions = hasher.repetitions_at_level(max_level);
    source(&scope, "all-sketches", move |cap| {
        let logger = scope.danny_logger();
        let hasher = Arc::clone(&hasher);
        let mut cap = Some(cap);
        move |output| {
            let logger = logger.clone();
            if let Some(cap) = cap.as_mut() {
                let _pg = ProfileGuard::new(logger.clone(), 0, 0, "cost_estimation_sketching");
                let mut session = output.session(cap);
                let mut cnt = 0;
                for (k, v) in vecs
                    .iter_stripe(matrix, direction, worker)
                    .filter(|_| rng.gen_bool(sample_probability))
                {
                    let sketch = sketches.get(k).expect("Missing sketch for key");
                    session.give(sketch.clone());
                    cnt += 1;
                }
                info!("Sampled {} points", cnt);
                log_event!(logger, LogEvent::AdaptiveSampledPoints(cnt));
                done = true;
            }

            if done {
                cap = None;
            }
        }
    })
}

#[allow(dead_code, clippy::too_many_arguments)]
fn compute_best_level<G, K, D, H, F, V>(
    sample: &Stream<G, V>,
    vecs: Arc<ChunkedDataset<K, D>>,
    hasher: Arc<MultilevelHasher<D, H, F>>,
    sketches: Arc<HashMap<K, V>>,
    weight: f64,
    matrix: MatrixDescription,
    direction: MatrixDirection,
) -> Stream<G, (K, usize)>
where
    G: Scope,
    D: ExchangeData + Debug,
    K: ExchangeData + Debug + Route + Hash + Ord,
    for<'a> H: ExchangeData + Route + Debug + Hash + Ord + PrefixHash<'a>,
    F: LSHFunction<Input = D, Output = H> + Send + Clone + Sync + 'static,
    V: ExchangeData + Debug + SketchEstimate,
{
    let worker = sample.scope().index() as u64;
    let min_level = hasher.min_level();
    let max_level = hasher.max_level();
    let logger = sample.scope().danny_logger();
    sample
        .broadcast()
        .unary_frontier(Pipeline, "best_level_computation", move |_, _| {
            let mut notificator = FrontierNotificator::new();
            let mut stash = HashMap::new();
            move |input, output| {
                input.for_each(|t, data| {
                    stash
                        .entry(t.time().clone())
                        .or_insert_with(Vec::new)
                        .extend(data.replace(Vec::new()));
                    notificator.notify_at(t.retain());
                });

                notificator.for_each(&[input.frontier()], |t, _| {
                    let _pg =
                        ProfileGuard::new(logger.clone(), 0, 0, "cost_estimation_computation");
                    let mut histogram = HashMap::new();
                    let sampled_sketches = stash
                        .remove(&t)
                        .expect("There should be the entry for this time!");
                    // Find the best level for each point
                    let mut session = output.session(&t);
                    for (k, v) in vecs.iter_stripe(matrix, direction, worker) {
                        let sketch_v = sketches
                            .get(k)
                            .expect("Missing sketch for key (estimation)");
                        let probabilities: Vec<f64> = sampled_sketches
                            .iter()
                            .map(|s| {
                                let estimated_distance = SketchEstimate::estimate(sketch_v, s);
                                F::probability_at_range(estimated_distance)
                            })
                            .collect();

                        // Try the different levels
                        let mut best_level = min_level;
                        let mut min_cost = std::f64::INFINITY;
                        for level in min_level..=max_level {
                            let repetitions = hasher.repetitions_at_level(level) as f64;
                            let prob_sum: f64 =
                                probabilities.iter().map(|&p| p.powi(level as i32)).sum();
                            let cost = repetitions * (1.0 + weight * prob_sum);
                            if cost < min_cost {
                                min_cost = cost;
                                best_level = level;
                            }
                        }
                        *histogram.entry(best_level).or_insert(0) += 1;
                        session.give((k.clone(), best_level));
                    }
                    for (level, count) in histogram {
                        log_event!(logger, LogEvent::AdaptiveLevelHistogram(level, count));
                    }
                });
            }
        })
}

pub fn find_best_level<G, T, K, D, H, F, V, R>(
    scope: G,
    left: Arc<ChunkedDataset<K, D>>,
    right: Arc<ChunkedDataset<K, D>>,
    hasher: Arc<MultilevelHasher<D, H, F>>,
    sketches_left: Arc<HashMap<K, V>>,
    sketches_right: Arc<HashMap<K, V>>,
    matrix: MatrixDescription,
    rng: R,
) -> (Stream<G, (K, usize)>, Stream<G, (K, usize)>)
where
    G: Scope<Timestamp = T>,
    T: Timestamp + Succ + ToStepId + Debug,
    D: ExchangeData + Debug,
    K: ExchangeData + Debug + Route + Hash + Ord,
    for<'a> H: ExchangeData + Route + Debug + Hash + Ord + PrefixHash<'a>,
    F: LSHFunction<Input = D, Output = H> + Send + Clone + Sync + 'static,
    V: ExchangeData + Debug + SketchEstimate,
    R: Rng + Clone + 'static,
{
    let prob_left = 4.0 / (left.global_n as f64).sqrt();
    let weight_left = 1.0 / prob_left;
    let prob_right = 4.0 / (right.global_n as f64).sqrt();
    let weight_right = 1.0 / prob_right;

    let sample_left = sample_sketches(
        &scope,
        Arc::clone(&left),
        prob_left,
        Arc::clone(&hasher),
        Arc::clone(&sketches_left),
        matrix,
        MatrixDirection::Rows,
        rng.clone(),
    );
    let sample_right = sample_sketches(
        &scope,
        Arc::clone(&right),
        prob_right,
        Arc::clone(&hasher),
        Arc::clone(&sketches_right),
        matrix,
        MatrixDirection::Columns,
        rng.clone(),
    );

    let best_left = compute_best_level(
        &sample_right,
        Arc::clone(&left),
        Arc::clone(&hasher),
        Arc::clone(&sketches_left),
        weight_right,
        matrix,
        MatrixDirection::Rows,
    );
    let best_right = compute_best_level(
        &sample_left,
        Arc::clone(&right),
        Arc::clone(&hasher),
        Arc::clone(&sketches_right),
        weight_left,
        matrix,
        MatrixDirection::Columns,
    );

    (best_left, best_right)
}
