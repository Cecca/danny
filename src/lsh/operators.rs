use crate::dataset::*;
use crate::logging::*;
use crate::lsh::bucket::*;
use crate::lsh::functions::*;
use crate::lsh::prefix_hash::*;
use crate::operators::Route;
use crate::operators::*;
use crate::types::*;
use abomonation::Abomonation;
use std::clone::Clone;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Instant;
use timely::dataflow::channels::pact::{Exchange as ExchangePact, Pipeline};
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::generic::source;
use timely::dataflow::operators::generic::FrontieredInputHandle;
use timely::dataflow::operators::Capability;
use timely::dataflow::operators::*;
use timely::dataflow::*;
use timely::logging::Logger;
use timely::progress::Timestamp;
use timely::Data;
use timely::ExchangeData;

pub trait BucketStream<G, T, H, K>
where
    G: Scope<Timestamp = T>,
    T: Timestamp,
    H: Data + Route + Debug + Send + Sync + Abomonation + Clone + Eq + Hash + Ord,
    K: Data + Debug + Send + Sync + Abomonation + Clone,
{
    fn bucket_pred<P, PD, R, O>(
        &self,
        right: &Stream<G, (H, K)>,
        pre: P,
        distinct_pre: PD,
        result: R,
    ) -> Stream<G, (O, O)>
    where
        P: FnMut(&K, &K) -> bool + 'static,
        PD: FnMut(&K, &K) -> bool + 'static,
        R: Fn(K) -> O + 'static,
        O: ExchangeData;
}

impl<G, T, H, K> BucketStream<G, T, H, K> for Stream<G, (H, K)>
where
    G: Scope<Timestamp = T>,
    T: Timestamp + ToStepId,
    H: Data + Route + Debug + Send + Sync + Abomonation + Clone + Eq + Hash + Ord,
    K: Data + Debug + Send + Sync + Abomonation + Clone,
{
    #[allow(clippy::explicit_counter_loop)]
    fn bucket_pred<P, PD, R, O>(
        &self,
        right: &Stream<G, (H, K)>,
        mut pred: P,
        mut distinct_pred: PD,
        result: R,
    ) -> Stream<G, (O, O)>
    where
        P: FnMut(&K, &K) -> bool + 'static,
        PD: FnMut(&K, &K) -> bool + 'static,
        R: Fn(K) -> O + 'static,
        O: ExchangeData,
    {
        let mut buckets = HashMap::new();
        let mut pool = BucketPool::default();
        let logger = self.scope().danny_logger();

        self.binary_frontier(
            &right,
            ExchangePact::new(|pair: &(H, K)| pair.0.route()),
            ExchangePact::new(|pair: &(H, K)| pair.0.route()),
            "bucket",
            move |_, _| {
                move |left_in, right_in, output| {
                    left_in.for_each(|t, d| {
                        let _pg = ProfileGuard::new(
                            logger.clone(),
                            t.time().to_step_id(),
                            1,
                            "bucket_receive",
                        );
                        debug!(
                            "Received batch of left messages for time {:?}:\n\t{:?}",
                            t.time(),
                            d.iter()
                        );
                        let mut data = d.replace(Vec::new());
                        log_event!(
                            logger,
                            LogEvent::ReceivedHashes(t.time().to_step_id(), data.len())
                        );
                        let rep_entry = buckets.entry(t.retain()).or_insert_with(|| pool.get());
                        for (h, k) in data.drain(..) {
                            rep_entry.push_left(h, k);
                        }
                    });
                    right_in.for_each(|t, d| {
                        let _pg = ProfileGuard::new(
                            logger.clone(),
                            t.time().to_step_id(),
                            1,
                            "bucket_receive",
                        );
                        debug!(
                            "Received batch of right messages for time {:?}:\n\t{:?}",
                            t.time(),
                            d.iter()
                        );
                        let mut data = d.replace(Vec::new());
                        log_event!(
                            logger,
                            LogEvent::ReceivedHashes(t.time().to_step_id(), data.len())
                        );
                        let rep_entry = buckets.entry(t.retain()).or_insert_with(|| pool.get());
                        for (h, k) in data.drain(..) {
                            rep_entry.push_right(h, k);
                        }
                    });
                    let frontiers = &[left_in.frontier(), right_in.frontier()];
                    for (time, buckets) in buckets.iter_mut() {
                        if frontiers.iter().all(|f| !f.less_equal(time)) {
                            let _pg = ProfileGuard::new(
                                logger.clone(),
                                time.time().to_step_id(),
                                1,
                                "candidate_emission",
                            );
                            let mut session = output.session(time);
                            let mut cnt = 0;
                            let mut sketch_cnt = 0;
                            let mut bloom_cnt = 0;
                            let start = Instant::now();
                            if !buckets.is_one_side_empty() {
                                buckets.for_all(|l, r| {
                                    if pred(l, r) {
                                        if distinct_pred(l, r) {
                                            session.give((result(l.clone()), result(r.clone())));
                                            cnt += 1;
                                        } else {
                                            bloom_cnt += 1;
                                        }
                                    } else {
                                        sketch_cnt += 1;
                                    }
                                });
                            }
                            let total_pairs = cnt + bloom_cnt + sketch_cnt;
                            buckets.clear();
                            let end = Instant::now();
                            log_event!(
                                logger,
                                LogEvent::GeneratedPairs(time.time().to_step_id(), cnt)
                            );
                            log_event!(
                                logger,
                                LogEvent::SketchDiscarded(time.time().to_step_id(), sketch_cnt)
                            );
                            log_event!(
                                logger,
                                LogEvent::DuplicatesDiscarded(time.time().to_step_id(), bloom_cnt)
                            );
                            info!(
                                "Candidates {}: Emitted {} / Discarded {} / Duplicates {} in {:?} ({}) (repetition {:?})",
                                total_pairs,
                                cnt,
                                sketch_cnt,
                                bloom_cnt,
                                end - start,
                                proc_mem!(),
                                time.time()
                            );
                        }
                    }

                    // Cleanup exhausted buckets, returning buckets to the pool,
                    // so to reuse the allocated memory in the future
                    let cleanup_times: Vec<Capability<T>> = buckets
                        .iter()
                        .filter(|(_, b)| b.is_empty())
                        .map(|p| p.0)
                        .cloned()
                        .collect();
                    for t in cleanup_times.iter() {
                        let bucket = buckets.remove(t).unwrap();
                        // put it back into the pool
                        pool.give_back(bucket);
                    }
                }
            },
        )
    }
}

pub trait BucketPrefixesStream<G, T, H, K>
where
    G: Scope<Timestamp = T>,
    T: Timestamp,
    H: HashData + PrefixHash,
    K: ExchangeData,
{
    fn bucket_prefixes<P, PD>(
        &self,
        right: &Self,
        routing_prefix: usize,
        proximity_predicate: P,
        distinct_predicate: PD,
    ) -> Stream<G, (K, K)>
    where
        P: FnMut(&K, &K) -> bool + 'static,
        PD: FnMut(&K, &K) -> bool + 'static;
}

impl<G, T, H, K> BucketPrefixesStream<G, T, H, K> for Stream<G, (H, (K, u8))>
where
    G: Scope<Timestamp = T>,
    T: Timestamp + ToStepId,
    H: HashData + Debug + PrefixHash,
    K: ExchangeData + Debug,
{
    #[allow(clippy::explicit_counter_loop)]
    fn bucket_prefixes<P, PD>(
        &self,
        right: &Self,
        routing_prefix: usize,
        mut sketch_pred: P,
        mut distinct_predicate: PD,
    ) -> Stream<G, (K, K)>
    where
        P: FnMut(&K, &K) -> bool + 'static,
        PD: FnMut(&K, &K) -> bool + 'static,
    {
        let mut buckets = HashMap::new();
        let logger = self.scope().danny_logger();

        self.binary_frontier(
            &right,
            ExchangePact::new(move |pair: &(H, (K, u8))| pair.0.prefix(routing_prefix).route()),
            ExchangePact::new(move |pair: &(H, (K, u8))| pair.0.prefix(routing_prefix).route()),
            "bucket",
            move |_, _| {
                move |left_in, right_in, output| {
                    left_in.for_each(|t, d| {
                        let _pg = ProfileGuard::new(
                            logger.clone(),
                            t.time().to_step_id(),
                            1,
                            "bucket_receive",
                        );
                        debug!(
                            "Received batch of left messages for time {:?}:\n\t{:?}",
                            t.time(),
                            d.iter()
                        );
                        let mut data = d.replace(Vec::new());
                        log_event!(
                            logger,
                            LogEvent::ReceivedHashes(t.time().to_step_id(), data.len())
                        );
                        let rep_entry = buckets.entry(t.retain()).or_insert_with(|| AdaptiveBucket::default());
                        for (h, (k, level)) in data.drain(..) {
                            rep_entry.push_left(level, h, k);
                        }
                    });
                    right_in.for_each(|t, d| {
                        let _pg = ProfileGuard::new(
                            logger.clone(),
                            t.time().to_step_id(),
                            1,
                            "bucket_receive",
                        );
                        debug!(
                            "Received batch of right messages for time {:?}:\n\t{:?}",
                            t.time(),
                            d.iter()
                        );
                        let mut data = d.replace(Vec::new());
                        log_event!(
                            logger,
                            LogEvent::ReceivedHashes(t.time().to_step_id(), data.len())
                        );
                        let rep_entry = buckets.entry(t.retain()).or_insert_with(|| AdaptiveBucket::default());
                        for (h, (k, level)) in data.drain(..) {
                            rep_entry.push_right(level, h, k);
                        }
                    });
                    let frontiers = &[left_in.frontier(), right_in.frontier()];
                    for (time, buckets) in buckets.iter_mut() {
                        if frontiers.iter().all(|f| !f.less_equal(time)) {
                            let _pg = ProfileGuard::new(
                                logger.clone(),
                                time.time().to_step_id(),
                                1,
                                "candidate_emission",
                            );
                            // Emit some output pairs
                            debug!(
                                "Outputting pairs out of buckets from {} left and {} right vectors",
                                buckets.len_left(),
                                buckets.len_right()
                            );
                            let mut session = output.session(time);
                            let mut cnt = 0;
                            let mut bloom_discarded = 0;
                            let mut sketch_discarded = 0;
                            debug!("Starting candidate emission ({})", proc_mem!());
                            let start = Instant::now();
                            if !buckets.is_one_side_empty() {
                                buckets.for_prefixes(|l, r| {
                                    if sketch_pred(l, r) {
                                        if distinct_predicate(l, r) {
                                            session.give((l.clone(), r.clone()));
                                            cnt += 1;
                                        } else {
                                            bloom_discarded += 1;
                                        }
                                    } else {
                                        sketch_discarded += 1;
                                    }
                                });
                            }
                            let end = Instant::now();
                            log_event!(
                                logger,
                                LogEvent::GeneratedPairs(time.time().to_step_id(), cnt)
                            );
                            log_event!(
                                logger,
                                LogEvent::SketchDiscarded(
                                    time.time().to_step_id(),
                                    sketch_discarded
                                )
                            );
                            log_event!(
                                logger,
                                LogEvent::DuplicatesDiscarded(
                                    time.time().to_step_id(),
                                    bloom_discarded
                                )
                            );
                            info!(
                                "Candidates: Emitted {} / Discarded {} / Duplicates {} in {:?} ({}) (repetition {:?})",
                                cnt,
                                sketch_discarded,
                                bloom_discarded,
                                end - start,
                                proc_mem!(),
                                time.time()
                            );
                        }
                    }

                    // Cleanup exhausted buckets, returning buckets to the pool,
                    // so to reuse the allocated memory in the future
                    let cleanup_times: Vec<Capability<T>> = buckets
                        .iter()
                        .filter(|(_, b)| b.is_empty())
                        .map(|p| p.0)
                        .cloned()
                        .collect();
                    for t in cleanup_times.iter() {
                        let bucket = buckets.remove(t).unwrap();
                    }
                }
            },
        )
    }
}

struct RepetitionStopWatch {
    start: Option<Instant>,
    counter: usize,
    name: String,
    logger: Option<Logger<LogEvent>>,
    verbose: bool,
}

impl RepetitionStopWatch {
    pub fn new(name: &str, verbose: bool, logger: Option<Logger<LogEvent>>) -> Self {
        Self {
            start: None,
            counter: 0usize,
            name: name.to_owned(),
            logger: logger,
            verbose,
        }
    }

    pub fn start(&mut self) {
        self.start.replace(Instant::now());
    }

    pub fn maybe_stop(&mut self) {
        if let Some(start) = self.start.take() {
            let elapsed = Instant::now() - start;
            if self.verbose {
                info!("{} {} ended in {:?}", self.name, self.counter, elapsed);
            }
            log_event!(
                self.logger,
                LogEvent::Profile(self.counter, 0, self.name.clone(), elapsed)
            );
            self.counter += 1;
        }
    }
}

pub fn source_hashed_sketched<G, T, K, D, F, V>(
    scope: &G,
    global_vecs: Arc<ChunkedDataset<K, D>>,
    hash_fns: Arc<DKTCollection<F>>,
    sketches: Arc<HashMap<K, V>>,
    matrix: MatrixDescription,
    direction: MatrixDirection,
    throttling_probe: ProbeHandle<G::Timestamp>,
) -> Stream<G, (u32, (V, K))>
where
    G: Scope<Timestamp = T>,
    T: Timestamp + Succ,
    D: Data + Sync + Send + Clone + Abomonation + Debug,
    F: LSHFunction<Input = D, Output = u32> + Sync + Send + Clone + 'static,
    K: KeyData + Debug,
    V: SketchData + Debug,
{
    let worker: u64 = scope.index() as u64;
    let logger = scope.danny_logger();
    let repetitions = hash_fns.repetitions();
    let mut current_repetition = 0usize;
    let vecs = Arc::clone(&global_vecs);
    let mut stopwatch = RepetitionStopWatch::new("repetition", worker == 0, logger);
    let mut bit_pools: HashMap<K, DKTPool> = HashMap::new();
    info!("Computing the bit pools");
    let start = Instant::now();
    for (k, v) in vecs.iter_stripe(matrix, direction, worker) {
        bit_pools.insert(*k, hash_fns.pool(v));
    }
    let end = Instant::now();
    info!(
        "Computed the bit pools ({:?}, {})",
        end - start,
        proc_mem!()
    );

    source(scope, "hashed source", move |capability| {
        let mut cap = Some(capability);
        move |output| {
            let mut done = false;
            if let Some(cap) = cap.as_mut() {
                if !throttling_probe.less_than(cap.time()) {
                    stopwatch.maybe_stop();
                    stopwatch.start();
                    if worker == 0 {
                        debug!("Repetition {} with sketches", current_repetition,);
                    }
                    let mut session = output.session(&cap);
                    for (k, v) in vecs.iter_stripe(matrix, direction, worker) {
                        let h = hash_fns.hash(&bit_pools[k], current_repetition as usize);
                        let s = sketches.get(k).expect("Missing sketch");
                        session.give((h, (s.clone(), k.clone())));
                    }
                    current_repetition += 1;
                    cap.downgrade(&cap.time().succ());
                    done = current_repetition >= repetitions;
                }
            }

            if done {
                // Drop the capability to signal that we will send no more data
                cap = None;
            }
        }
    })
}

#[allow(clippy::too_many_arguments, clippy::type_complexity)]
pub fn source_hashed_adaptive_sketched<G, T, K, D, F, SV>(
    scope: &G,
    best_levels: &Stream<G, (K, usize)>,
    global_vecs: Arc<ChunkedDataset<K, D>>,
    hasher: Arc<DKTCollection<F>>,
    sketches: Arc<HashMap<K, SV>>,
    matrix: MatrixDescription,
    direction: MatrixDirection,
    throttling_probe: ProbeHandle<G::Timestamp>,
) -> Stream<G, (u32, ((K, SV), u8))>
where
    G: Scope<Timestamp = T>,
    T: Timestamp + Succ + ToStepId,
    D: Data + Sync + Send + Clone + Abomonation + Debug,
    F: LSHFunction<Input = D, Output = u32> + Sync + Send + Clone + 'static,
    K: KeyData + Debug,
    SV: SketchData + Debug,
{
    let worker = scope.index() as u64;
    let max_level = hasher.max_level();
    let hasher = Arc::clone(&hasher);
    let global_vecs_2 = Arc::clone(&global_vecs);
    let logger = scope.danny_logger();
    let num_repetitions = hasher.repetitions_at(max_level);
    let mut bit_pools: HashMap<K, DKTPool> = HashMap::new();
    info!("Computing the bit pools");
    let start = Instant::now();
    for (k, v) in global_vecs.iter_stripe(matrix, direction, worker) {
        bit_pools.insert(*k, hasher.pool(v));
    }
    let end = Instant::now();
    info!(
        "Computed the bit pools ({:?}, {})",
        end - start,
        proc_mem!()
    );

    let mut builder = OperatorBuilder::new("adaptive-source".to_owned(), best_levels.scope());
    let mut input_best_levels = builder.new_input(&best_levels, Pipeline);
    let (mut output, output_stream) = builder.new_output();
    builder.build(move |mut capabilities| {
        let mut capability = Some(capabilities.pop().unwrap());

        let mut stopwatch = RepetitionStopWatch::new("repetition", worker == 0, logger.clone());
        let mut best_levels: HashMap<K, usize> = HashMap::new();
        // We start from the starting level of the hasher, even though the minimum level might be higher.
        // This is to synchronize the left and right generators. They might have different
        // minimum levels, and this is the simplest way to ensure that they both are always in the
        // same round. Performance-wise it doesn't hurt much to run through some empty levels.
        let mut current_repetition = 0;
        let mut done = false;
        let vecs = Arc::clone(&global_vecs_2);

        move |frontiers| {
            let mut best_levels_input =
                FrontieredInputHandle::new(&mut input_best_levels, &frontiers[0]);
            let mut output = output.activate();

            best_levels_input.for_each(|_t, data| {
                let mut data = data.replace(Vec::new());
                for (key, level) in data.drain(..) {
                    best_levels.insert(key, level);
                }
            });
            if let Some(capability) = capability.as_mut() {
                if !best_levels_input.frontier().less_equal(capability.time())
                    && !throttling_probe.less_than(capability.time())
                {
                    stopwatch.maybe_stop();
                    stopwatch.start();
                    if worker == 0 {
                        debug!(
                            "Repetition {}/{} (current memory {})",
                            current_repetition,
                            num_repetitions,
                            proc_mem!(),
                        );
                    }
                    let _pg = ProfileGuard::new(
                        logger.clone(),
                        capability.time().to_step_id(),
                        1,
                        "hash_generation",
                    );

                    let mut session = output.session(&capability);
                    let mut cnt = 0;
                    for (key, _) in vecs.iter_stripe(matrix, direction, worker) {
                        // Here we have that some vectors may not not have a best level. This is because
                        // those vectors didn't collide with anything in the estimatio of the cost.
                        // Since we use the same hasher for both the estimation and the actual computation
                        // we can simply avoid emitting the vectors for which we have no entry in the map.
                        if let Some(&this_best_level) = best_levels.get(key) {
                            if hasher.is_active(current_repetition, this_best_level) {
                                // We hash to the max level because the levelling will be taken care of in the buckets
                                let h = hasher.hash(&bit_pools[key], current_repetition);
                                let s = *sketches
                                    .get(key)
                                    .expect("Missing sketch for key in repetition");
                                session.give((h, ((*key, s), this_best_level as u8)));
                                cnt += 1;
                            }
                        }
                    }
                    log_event!(
                        logger,
                        LogEvent::GeneratedHashes(capability.time().to_step_id(), cnt)
                    );
                    capability.downgrade(&capability.time().succ());
                    current_repetition += 1;
                    if current_repetition >= num_repetitions {
                        done = true;
                    }
                }
            }
            if done {
                // Drop the capability to signal that we will send no more data
                capability = None;
            }
        }
    });

    output_stream
}
