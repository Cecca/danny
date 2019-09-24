use crate::config::*;
use crate::dataset::*;
use crate::logging::*;

use crate::operators::*;
use danny_base::bloom::*;
use danny_base::bucket::*;
use danny_base::lsh::*;
use danny_base::sketch::*;

use rand::Rng;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Instant;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::capture::event::Event as TimelyEvent;
use timely::dataflow::operators::generic::source;
use timely::dataflow::operators::*;
use timely::dataflow::*;
use timely::logging::Logger;
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

    pub fn probability_and_weight(&self, n: usize) -> (f64, f64) {
        let prob = self.sampling_factor / (n as f64).sqrt();
        let prob = if prob > 1.0 {
            warn!("Capping the sampling probability to 1");
            1.0
        } else {
            prob
        };
        let weight = 1.0 / prob;
        (prob, weight)
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

pub fn adaptive_local_solve<K, D, S, H, P, R>(
    left: Arc<ChunkedDataset<K, D>>,
    right: Arc<ChunkedDataset<K, D>>,
    mut left_data: Vec<(K, DKTPool, S)>,
    mut right_data: Vec<(K, DKTPool, S)>,
    hasher: Arc<DKTCollection<H>>,
    sketch_predicate: &SketchPredicate<S>,
    sim_pred: &P,
    filter: Arc<AtomicBloomFilter<K>>,
    max_level: usize,
    logger: Option<Logger<LogEvent>>,
    params: AdaptiveParams,
    rng: &mut R,
) -> u64
where
    K: Hash + Route + Eq + Debug + Copy + Into<u64>,
    D: SketchEstimate,
    S: BitBasedSketch + Debug + 'static,
    H: LSHFunction<Input = D, Output = u32> + Send + Clone + Sync + 'static,
    P: Fn(&D, &D) -> bool + Send + Clone + Sync + 'static,
    R: Rng + 'static,
{
    let (prob_left, weight_left) = params.probability_and_weight(left.global_n);
    let (prob_right, weight_right) = params.probability_and_weight(right.global_n);
    info!(
        "Sampling probability left {} and right {}",
        prob_left, prob_right
    );
    let sample_left: Vec<S> = left_data
        .iter()
        .filter(|_| rng.gen_bool(prob_left))
        .map(|triplet| triplet.2.clone())
        .collect();
    let sample_right: Vec<S> = right_data
        .iter()
        .filter(|_| rng.gen_bool(prob_right))
        .map(|triplet| triplet.2.clone())
        .collect();
    let pg = ProfileGuard::new(logger.clone(), 0, 0, "best_level_computation");
    info!("Computing left best levels");
    let mut left_hist = vec![0; max_level + 1];
    let left_data: Vec<(K, DKTPool, S, u8)> = left_data
        .drain(..)
        .map(|(k, pool, sketch)| {
            let level = estimate_best_level(
                sample_left.iter(),
                &sketch,
                1,
                max_level,
                params.with_weight(weight_right),
                Arc::clone(&hasher),
            );
            left_hist[level] += 1;
            (k, pool, sketch, level as u8)
        })
        .collect();
    info!("Computing right best levels");
    let mut right_hist = vec![0; max_level + 1];
    let right_data: Vec<(K, DKTPool, S, u8)> = right_data
        .drain(..)
        .map(|(k, pool, sketch)| {
            let level = estimate_best_level(
                sample_right.iter(),
                &sketch,
                1,
                max_level,
                params.with_weight(weight_left),
                Arc::clone(&hasher),
            );
            right_hist[level] += 1;
            (k, pool, sketch, level as u8)
        })
        .collect();
    info!("Left histogram of levels {:?}", left_hist);
    drop(pg);
    for (level, count) in left_hist.drain(..).enumerate() {
        if count > 0 {
            log_event!(logger, LogEvent::AdaptiveLevelHistogram(level, count));
        }
    }
    for (level, count) in right_hist.drain(..).enumerate() {
        if count > 0 {
            log_event!(logger, LogEvent::AdaptiveLevelHistogram(level, count));
        }
    }

    let mut cnt = 0;
    for rep in 0..hasher.repetitions() {
        let _pg = ProfileGuard::new(logger.clone(), rep, 0, "repetition");
        let mut bucket = AdaptiveBucket::default();
        info!("Starting repetition {}", rep);
        let start = Instant::now();
        let mut left_active = 0;
        for (k, pool, sketch, level) in left_data.iter() {
            if rep < hasher.repetitions_at(*level as usize) {
                bucket.push_left(*level, hasher.hash(pool, rep), (*k, *sketch));
                left_active += 1;
            }
        }
        let mut right_active = 0;
        for (k, pool, sketch, level) in right_data.iter() {
            if rep < hasher.repetitions_at(*level as usize) {
                bucket.push_right(*level, hasher.hash(pool, rep), (*k, *sketch));
                right_active += 1;
            }
        }
        info!(
            "Points active at repetition {}: {} x {}",
            rep, left_active, right_active
        );

        let mut examined_pairs = 0;
        let mut sketch_discarded = 0;
        let mut duplicates_discarded = 0;
        bucket.for_prefixes(|l, r| {
            examined_pairs += 1;
            if sketch_predicate.eval(&l.1, &r.1) {
                if !filter.test_and_insert(&(l.0, r.0)) {
                    if sim_pred(&left[&l.0], &right[&r.0]) {
                        cnt += 1;
                    }
                } else {
                    duplicates_discarded += 1;
                }
            } else {
                sketch_discarded += 1;
            }
        });
        log_event!(logger, LogEvent::SketchDiscarded(rep, sketch_discarded));
        log_event!(
            logger,
            LogEvent::DuplicatesDiscarded(rep, duplicates_discarded)
        );
        log_event!(logger, LogEvent::GeneratedPairs(rep, examined_pairs));
        let end = Instant::now();
        info!("Repetition {} completed in {:?}", rep, end - start);
    }

    cnt
}
