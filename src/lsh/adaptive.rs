use crate::dataset::*;
use crate::logging::*;
use crate::lsh::*;
use crate::operators::*;
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

type RepetitionId = usize;

#[allow(dead_code)]
fn sample_hashes<G, K, D, H, F, R>(
    scope: &G,
    vecs: Arc<ChunkedDataset<K, D>>,
    sample_probability: f64,
    hasher: Arc<MultilevelHasher<D, H, F>>,
    matrix: MatrixDescription,
    direction: MatrixDirection,
    mut rng: R,
) -> Stream<G, (RepetitionId, H, f64)>
where
    G: Scope,
    D: ExchangeData + Debug,
    H: ExchangeData + Route + Debug + Eq + Hash + Ord,
    K: ExchangeData + Debug + Route,
    F: LSHFunction<Input = D, Output = H> + Send + Clone + Sync + 'static,
    R: Rng + Clone + 'static,
{
    let worker = scope.index() as u64;
    let vecs = Arc::clone(&vecs);
    let mut done = false;
    let max_level = hasher.max_level();
    let num_repetitions = hasher.repetitions_at_level(max_level);
    source(&scope, "all-hashes", move |cap| {
        let logger = scope.danny_logger();
        let hasher = Arc::clone(&hasher);
        let mut cap = Some(cap);
        move |output| {
            let logger = logger.clone();
            if let Some(cap) = cap.as_mut() {
                let _pg = ProfileGuard::new(logger.clone(), 0, 0, "cost_estimation_hashing");
                let mut session = output.session(cap);
                let mut accum = HashMap::new();
                let mut cnt = 0;
                for (k, v) in vecs
                    .iter_stripe(matrix, direction, worker)
                    .filter(|_| rng.gen_bool(sample_probability))
                {
                    for rep in 0..num_repetitions {
                        let h = hasher.hash(v, max_level, rep);
                        *accum.entry((rep, h)).or_insert(0.0) += 1.0 / sample_probability;
                    }
                    cnt += 1;
                }
                info!("Sampled {} points", cnt);
                log_event!(logger, LogEvent::AdaptiveSampledPoints(cnt));
                for ((rep, h), weight) in accum.drain() {
                    session.give((rep, h, weight));
                }
                done = true;
            }

            if done {
                cap = None;
            }
        }
    })
}

#[allow(dead_code)]
fn compute_best_level<G, K, D, H, F>(
    sample: &Stream<G, (RepetitionId, H, f64)>,
    vecs: Arc<ChunkedDataset<K, D>>,
    hasher: Arc<MultilevelHasher<D, H, F>>,
    matrix: MatrixDescription,
    direction: MatrixDirection,
    balance: f64,
) -> Stream<G, (K, usize)>
where
    G: Scope,
    D: ExchangeData + Debug,
    K: ExchangeData + Debug + Route + Hash + Ord,
    for<'a> H: ExchangeData + Route + Debug + Hash + Ord + PrefixHash<'a>,
    F: LSHFunction<Input = D, Output = H> + Send + Clone + Sync + 'static,
{
    let worker = sample.scope().index() as u64;
    let num_repetitions = hasher.repetitions_at_level(hasher.max_level());
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
                    let entry = stash.entry(t.time().clone()).or_insert_with(HashMap::new);
                    for (rep, h, weight) in data.replace(Vec::new()).drain(..) {
                        entry.entry(rep).or_insert_with(Vec::new).push((h, weight));
                    }
                    notificator.notify_at(t.retain());
                });

                notificator.for_each(&[input.frontier()], |t, _| {
                    let _pg =
                        ProfileGuard::new(logger.clone(), 0, 0, "cost_estimation_computation");
                    // Arrange the sampled hashes
                    let mut sampled_hashes = stash
                        .remove(&t)
                        .expect("There should be the entry for this time!");
                    for (_, entries) in sampled_hashes.iter_mut() {
                        entries.sort_unstable_by(|p1, p2| p1.0.lex_cmp(&p2.0));
                    }
                    // Accumulate the weights of the collisions, for all levels across repetitions
                    let mut accumulator: HashMap<K, HashMap<usize, f64>> = HashMap::new();
                    for rep in 0..num_repetitions {
                        let scratch: Vec<(H, K)> = {
                            let mut s =
                                Vec::with_capacity(vecs.stripe_len(matrix, direction, worker));
                            for (k, v) in vecs.iter_stripe(matrix, direction, worker) {
                                let h = hasher.hash(v, max_level, rep);
                                s.push((h, k.clone()));
                            }
                            s.sort_unstable_by(|p1, p2| p1.0.lex_cmp(&p2.0));
                            s
                        };
                        for level in hasher.levels_at_repetition(rep) {
                            let bucket_iter = BucketsPrefixIter::new(
                                &scratch,
                                sampled_hashes.get(&rep).expect("Missing repetition!"),
                                level,
                            );
                            for (l_bucket, r_bucket) in bucket_iter {
                                for (_, k) in l_bucket {
                                    for (_, weight) in r_bucket {
                                        *accumulator
                                            .entry(k.clone())
                                            .or_insert_with(HashMap::new)
                                            .entry(level)
                                            .or_insert(0.0) += weight;
                                    }
                                }
                            }
                        }
                    }

                    // Find the best level for each point
                    let mut histogram = HashMap::new();
                    let mut session = output.session(&t);
                    let mut count_not_colliding_points = 0;
                    for (k, _) in vecs.iter_stripe(matrix, direction, worker) {
                        if let Some(collisions) = accumulator.get(k) {
                            let mut min_work = std::f64::INFINITY;
                            let mut best_level = 0;
                            for (&level, &weight) in collisions {
                                let reps = hasher.repetitions_at_level(level as usize);
                                let work = balance * reps as f64 + (1.0 - balance) * weight;
                                if work < min_work {
                                    min_work = work;
                                    best_level = level;
                                }
                            }
                            *histogram.entry(best_level).or_insert(0) += 1;
                            session.give((k.clone(), best_level));
                        } else {
                            // No collisions in any level
                            *histogram.entry(min_level).or_insert(0) += 1;
                            count_not_colliding_points += 1;
                            session.give((k.clone(), min_level));
                        }
                    }
                    info!(
                        "There are {} points colliding with nothing",
                        count_not_colliding_points
                    );

                    log_event!(
                        logger,
                        LogEvent::AdaptiveNoCollision(count_not_colliding_points)
                    );
                    for (level, count) in histogram {
                        log_event!(logger, LogEvent::AdaptiveLevelHistogram(level, count));
                    }
                });
            }
        })
}

pub fn find_best_level<G, T, K, D, H, F, R>(
    scope: G,
    left: Arc<ChunkedDataset<K, D>>,
    right: Arc<ChunkedDataset<K, D>>,
    hasher: Arc<MultilevelHasher<D, H, F>>,
    matrix: MatrixDescription,
    balance: f64,
    rng: R,
) -> (Stream<G, (K, usize)>, Stream<G, (K, usize)>)
where
    G: Scope<Timestamp = T>,
    T: Timestamp + Succ + ToStepId + Debug,
    D: ExchangeData + Debug,
    K: ExchangeData + Debug + Route + Hash + Ord,
    for<'a> H: ExchangeData + Route + Debug + Hash + Ord + PrefixHash<'a>,
    F: LSHFunction<Input = D, Output = H> + Send + Clone + Sync + 'static,
    R: Rng + Clone + 'static,
{
    let prob_left = 4.0 / (left.global_n as f64).sqrt();
    let prob_right = 4.0 / (right.global_n as f64).sqrt();

    let sample_left = sample_hashes(
        &scope,
        Arc::clone(&left),
        prob_left,
        Arc::clone(&hasher),
        matrix,
        MatrixDirection::Rows,
        rng.clone(),
    );
    let sample_right = sample_hashes(
        &scope,
        Arc::clone(&right),
        prob_right,
        Arc::clone(&hasher),
        matrix,
        MatrixDirection::Columns,
        rng.clone(),
    );

    let best_left = compute_best_level(
        &sample_right,
        Arc::clone(&left),
        Arc::clone(&hasher),
        matrix,
        MatrixDirection::Rows,
        balance,
    );
    let best_right = compute_best_level(
        &sample_left,
        Arc::clone(&right),
        Arc::clone(&hasher),
        matrix,
        MatrixDirection::Columns,
        balance,
    );

    (best_left, best_right)
}
