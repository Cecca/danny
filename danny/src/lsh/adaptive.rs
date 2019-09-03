use crate::config::*;
use crate::dataset::*;
use crate::logging::*;

use crate::operators::*;
use danny_base::bucket::*;
use danny_base::bloom::*;
use danny_base::lsh::*;
use danny_base::sketch::*;

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

#[derive(Debug, Clone, Copy)]
pub struct AdaptiveParams {
    pub sampling_factor: f64,
    pub balance: f64,
    pub weight: f64,
    pub bucket_size: u32,
    pub repetition_cost: f64,
}

impl Default for AdaptiveParams {
    fn default() -> Self {
        Self {
            sampling_factor: 1.0,
            balance: 0.5,
            weight: 1.0,
            bucket_size: 0,
            repetition_cost: 1.0,
        }
    }
}

impl AdaptiveParams {
    pub fn from_config(config: &Config) -> Self {
        let cost_balance = config.get_cost_balance();
        let sampling_factor = config.get_sampling_factor();
        let bucket_size = config.get_desired_bucket_size();
        let repetition_cost = config.get_repetition_cost();
        assert!(
            cost_balance >= 0.0 && cost_balance <= 1.0,
            "Balance should be between 0 and 1"
        );
        Self {
            sampling_factor,
            balance: cost_balance,
            bucket_size,
            weight: 1.0,
            repetition_cost,
        }
    }

    pub fn with_weight(&self, weight: f64) -> Self {
        Self {
            weight,
            sampling_factor: self.sampling_factor,
            balance: self.balance,
            bucket_size: self.bucket_size,
            repetition_cost: self.repetition_cost,
        }
    }
}

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

pub fn estimate_best_level<'a, D, I, S, H>(
    sketches: I,
    sketch: &S,
    min_level: usize,
    max_level: usize,
    params: AdaptiveParams,
    hasher: Arc<DKTCollection<H>>,
) -> usize
where
    D: SketchEstimate,
    I: Iterator<Item = &'a S>,
    S: BitBasedSketch + 'static,
    H: LSHFunction<Input = D, Output = u32> + Send + Clone + Sync + 'static,
{
    let probabilities: Vec<f64> = sketches
        .map(|s| D::collision_probability_estimate(sketch, s))
        .collect();
    let mut powers = vec![1.0; probabilities.len()];
    for _ in 1..min_level {
        // Start from 1 beause otherwise the we are elevating the power to on too much
        for i in 0..powers.len() {
            powers[i] *= probabilities[i];
        }
    }

    // Try the different levels
    let mut best_level = min_level;
    let mut min_cost = std::f64::INFINITY;
    for level in min_level..=max_level {
        let repetitions = hasher.repetitions_at(level) as f64;
        let mut prob_sum = 0.0;
        for i in 0..powers.len() {
            powers[i] *= probabilities[i];
            prob_sum += powers[i];
        }
        // let prob_sum: f64 =
        //     probabilities.iter().map(|&p| p.powi(level as i32)).sum();
        let estimated_collisions: f64 = params.weight * prob_sum;
        let cost = repetitions
            * (params.repetition_cost * params.balance
                + (1.0 - params.balance) * estimated_collisions);
        if cost < min_cost {
            min_cost = cost;
            best_level = level;
        }
        if estimated_collisions <= f64::from(params.bucket_size) {
            // Early break if we are happy with this number of collisions
            best_level = level;
            break;
        }
    }
    best_level
}

#[allow(dead_code, clippy::too_many_arguments)]
fn compute_best_level<G, K, D, F, V>(
    sample: &Stream<G, V>,
    vecs: Arc<ChunkedDataset<K, D>>,
    hasher: Arc<DKTCollection<F>>,
    sketches: Arc<HashMap<K, V>>,
    params: AdaptiveParams,
    matrix: MatrixDescription,
    direction: MatrixDirection,
) -> Stream<G, (K, usize)>
where
    G: Scope,
    D: ExchangeData + Debug + SketchEstimate,
    K: ExchangeData + Debug + Route + Hash + Ord,
    F: LSHFunction<Input = D, Output = u32> + Send + Clone + Sync + 'static,
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
                        let best_level = estimate_best_level(
                            sampled_sketches.iter(),
                            sketch_v,
                            min_level,
                            max_level,
                            params,
                            Arc::clone(&hasher),
                        );
                        *histogram.entry(best_level).or_insert(0) += 1;
                        session.give((k.clone(), best_level));
                        cnt += 1;
                    }
                    info!(
                        "Estimated best level for {} points out of {} ({:?})\n{:?}",
                        cnt, vecs.global_n, params, histogram
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
pub fn find_best_level<G, T, K, D, F, V, R>(
    scope: G,
    left: Arc<ChunkedDataset<K, D>>,
    right: Arc<ChunkedDataset<K, D>>,
    params: AdaptiveParams,
    hasher: Arc<DKTCollection<F>>,
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
    F: LSHFunction<Input = D, Output = u32> + Send + Clone + Sync + 'static,
    V: ExchangeData + Debug + BitBasedSketch,
    R: Rng + Clone + 'static,
{
    let prob_left = params.sampling_factor / (left.global_n as f64).sqrt();
    let prob_left = if prob_left > 1.0 {
        warn!("Capping the sampling probability to 1");
        1.0
    } else {
        prob_left
    };
    let weight_left = 1.0 / prob_left;
    let prob_right = params.sampling_factor / (right.global_n as f64).sqrt();
    let prob_right = if prob_right > 1.0 {
        warn!("Capping the sampling probability to 1");
        1.0
    } else {
        prob_right
    };
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
        params.with_weight(weight_right),
        matrix,
        MatrixDirection::Rows,
    );
    let best_right = compute_best_level(
        &sample_left,
        Arc::clone(&right),
        Arc::clone(&hasher),
        Arc::clone(&sketches_right),
        params.with_weight(weight_left),
        matrix,
        MatrixDirection::Columns,
    );

    (best_left, best_right)
}

pub fn adaptive_local_solve<K, D, S, H, P>(
    left: Arc<ChunkedDataset<K, D>>,
    right: Arc<ChunkedDataset<K, D>>,
    left_sketches: &HashMap<K, S>,
    right_sketches: &HashMap<K, S>,
    left_pools: &HashMap<K, DKTPool>,
    right_pools: &HashMap<K, DKTPool>,
    hasher: Arc<DKTCollection<H>>,
    sketch_predicate: &SketchPredicate<S>,
    sim_pred: &P,
    filter: Arc<AtomicBloomFilter<K>>,
    max_level: usize,
    worker: usize,
    matrix: MatrixDescription,
) -> u64
where
    K: Hash + Route + Eq + Debug + Copy + Into<u64>,
    D: SketchEstimate,
    S: BitBasedSketch + 'static,
    H: LSHFunction<Input = D, Output = u32> + Send + Clone + Sync + 'static,
    P: Fn(&D, &D) -> bool + Send + Clone + Sync + 'static,
{
    let params = AdaptiveParams::default();
    let (worker_row, worker_col) = matrix.row_major_to_pair(worker as u64);
    let mut bucket = AdaptiveBucket::default();
    let left_levels: HashMap<K, usize> = left.iter_chunk(worker_row as usize).map(|(k, _)| {
        let level = estimate_best_level(
            left_sketches.values(),
            &left_sketches[k],
            1,
            max_level,
            params,
            Arc::clone(&hasher),
        );
        (*k, level)
    }).collect();
    let right_levels: HashMap<K, usize> = right.iter_chunk(worker_col as usize).map(|(k, _)| {
        let level = estimate_best_level(
            right_sketches.values(),
            &right_sketches[k],
            1,
            max_level,
            params,
            Arc::clone(&hasher),
        );
        (*k, level)
    }).collect();

    let mut cnt = 0;
    for rep in 0..hasher.repetitions() {
        for (k, _) in left.iter_chunk(worker_row as usize) {
            let level = left_levels[k];
            if hasher.repetitions_at(level) < rep {
                bucket.push_left(level as u8, hasher.hash(&left_pools[k], rep), *k);
            }
        }
        for (k, _) in right.iter_chunk(worker_row as usize) {
            let level = right_levels[k];
            if hasher.repetitions_at(level) < rep {
                bucket.push_right(level as u8, hasher.hash(&right_pools[k], rep), *k);
            }
        }

        bucket.for_prefixes(|l, r|{
            if sketch_predicate.eval(&left_sketches[l], &right_sketches[r])
            {
                if !filter.test_and_insert(&(*l, *r)) {
                    if sim_pred(&left[&l], &right[&r]) {
                        cnt += 1;
                    }
                }
            }
        });
    }

    cnt
}
