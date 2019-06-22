use crate::dataset::*;
use crate::logging::*;
use crate::lsh::*;
use crate::operators::*;
use crate::sketch::*;

use rand::Rng;

use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

use timely::dataflow::channels::pact::Pipeline;

use timely::dataflow::operators::generic::source;

use timely::dataflow::operators::*;

use timely::dataflow::*;

use timely::progress::Timestamp;

use timely::ExchangeData;

#[allow(dead_code, clippy::too_many_arguments)]
fn sample_sketches<G, K, D, V, R>(
    scope: &G,
    vecs: Arc<ChunkedDataset<K, D>>,
    sample_probability: f64,
    sketches: Arc<HashMap<K, V>>,
    matrix: MatrixDescription,
    direction: MatrixDirection,
    mut rng: R,
) -> Stream<G, V>
where
    G: Scope,
    D: ExchangeData + Debug,
    K: ExchangeData + Debug + Route + Eq + Hash,
    V: ExchangeData + Debug,
    R: Rng + Clone + 'static,
{
    let worker = scope.index() as u64;
    let vecs = Arc::clone(&vecs);
    let mut done = false;
    source(&scope, "all-sketches", move |cap| {
        let logger = scope.danny_logger();
        let mut cap = Some(cap);
        move |output| {
            let logger = logger.clone();
            if let Some(cap) = cap.as_mut() {
                let _pg = ProfileGuard::new(logger.clone(), 0, 0, "cost_estimation_sketching");
                let mut session = output.session(cap);
                let mut cnt = 0;
                for (k, _) in vecs
                    .iter_stripe(matrix, direction, worker)
                    .filter(|_| rng.gen_bool(sample_probability))
                {
                    let sketch = sketches
                        .get(k)
                        .expect("Missing sketch for key (during sampling)");
                    session.give(sketch.clone());
                    cnt += 1;
                }
                debug!("Sampled {} points", cnt);
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
    balance: f64,
    matrix: MatrixDescription,
    direction: MatrixDirection,
) -> Stream<G, (K, usize)>
where
    G: Scope,
    D: ExchangeData + Debug + SketchEstimate,
    K: ExchangeData + Debug + Route + Hash + Ord,
    H: ExchangeData + Route + Debug + Hash + Ord + PrefixHash,
    F: LSHFunction<Input = D, Output = H> + Send + Clone + Sync + 'static,
    V: ExchangeData + Debug + BitBasedSketch,
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
                    let mut cnt = 0;
                    for (k, _v) in vecs.iter_stripe(matrix, direction, worker) {
                        let sketch_v = sketches
                            .get(k)
                            .expect("Missing sketch for key (estimation)");
                        let probabilities: Vec<f64> = sampled_sketches
                            .iter()
                            .map(|s| {
                                let estimated_distance = D::sketch_estimate(sketch_v, s);
                                F::probability_at_range(estimated_distance)
                            })
                            .collect();

                        // Try the different levels
                        let mut best_level = min_level;
                        let mut min_cost = std::f64::INFINITY;
                        let mut best_sum_probabilities = 0.0;
                        let mut best_repetitions = 0.0;
                        for level in min_level..=max_level {
                            let repetitions = hasher.repetitions_at_level(level) as f64;
                            let prob_sum: f64 =
                                probabilities.iter().map(|&p| p.powi(level as i32)).sum();
                            let cost =
                                repetitions * (1.0 * balance + (1.0 - balance) * weight * prob_sum);
                            info!(
                                "Estimating cost for point {:?} at level {}: {:.2} [{:.2} + {:.2}*{:.2} ({:.2} + {:.2})]",
                                k,
                                level,
                                cost,
                                repetitions,
                                weight,
                                prob_sum,
                                balance * repetitions,
                                (1.0 - balance) * weight * prob_sum,
                            );
                            if cost < min_cost {
                                min_cost = cost;
                                best_level = level;
                                best_sum_probabilities = prob_sum;
                                best_repetitions = repetitions;
                            }
                        }
                        info!(
                            "Cost for point {:?}: {} [{} + {} ({} + {})], assigned level {}",
                            k,
                            min_cost,
                            best_repetitions,
                            best_sum_probabilities,
                            balance * best_repetitions,
                            (1.0 - balance) * best_sum_probabilities,
                            best_level
                        );
                        *histogram.entry(best_level).or_insert(0) += 1;
                        session.give((k.clone(), best_level));
                        cnt += 1;
                    }
                    info!(
                        "Estimated best level for {} points out of {}",
                        cnt, vecs.global_n
                    );
                    for (level, count) in histogram {
                        log_event!(logger, LogEvent::AdaptiveLevelHistogram(level, count));
                    }
                });
            }
        })
}

/// A balance towards 0 penalizes collisions, a balance towards 1 penalizes repetitions
#[allow(clippy::too_many_arguments)]
pub fn find_best_level<G, T, K, D, H, F, V, R>(
    scope: G,
    left: Arc<ChunkedDataset<K, D>>,
    right: Arc<ChunkedDataset<K, D>>,
    balance: f64,
    hasher: Arc<MultilevelHasher<D, H, F>>,
    sketches_left: Arc<HashMap<K, V>>,
    sketches_right: Arc<HashMap<K, V>>,
    matrix: MatrixDescription,
    rng: R,
) -> (Stream<G, (K, usize)>, Stream<G, (K, usize)>)
where
    G: Scope<Timestamp = T>,
    T: Timestamp + Succ + ToStepId + Debug,
    D: ExchangeData + Debug + SketchEstimate,
    K: ExchangeData + Debug + Route + Hash + Ord,
    H: ExchangeData + Route + Debug + Hash + Ord + PrefixHash,
    F: LSHFunction<Input = D, Output = H> + Send + Clone + Sync + 'static,
    V: ExchangeData + Debug + BitBasedSketch,
    R: Rng + Clone + 'static,
{
    assert!(
        balance >= 0.0 && balance <= 1.0,
        "Balance should be between 0 and 1"
    );
    let prob_left = 4.0 / (left.global_n as f64).sqrt();
    let weight_left = 1.0 / prob_left;
    let prob_right = 4.0 / (right.global_n as f64).sqrt();
    let weight_right = 1.0 / prob_right;

    let sample_left = sample_sketches(
        &scope,
        Arc::clone(&left),
        prob_left,
        Arc::clone(&sketches_left),
        matrix,
        MatrixDirection::Rows,
        rng.clone(),
    );
    let sample_right = sample_sketches(
        &scope,
        Arc::clone(&right),
        prob_right,
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
        balance,
        matrix,
        MatrixDirection::Rows,
    );
    let best_right = compute_best_level(
        &sample_left,
        Arc::clone(&right),
        Arc::clone(&hasher),
        Arc::clone(&sketches_right),
        weight_left,
        balance,
        matrix,
        MatrixDirection::Columns,
    );

    (best_left, best_right)
}
