use crate::dataset::*;
use crate::logging::*;
use crate::operators::Route;
use crate::operators::*;
use danny_base::bucket::*;
use danny_base::lsh::*;
use danny_base::prefix_hash::*;
use danny_base::sketch::*;

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

    fn bucket_pred_count<P>(&self, right: &Stream<G, (H, K)>, pre: P) -> Stream<G, usize>
    where
        P: FnMut(&K, &K) -> bool + 'static;


}    



pub trait BucketTwoStream<G, T, H, K, S>
where
    G: Scope<Timestamp = T>,
    T: Timestamp,
    H: Data + Route + Debug + Send + Sync + Abomonation + Clone + Eq + Hash + Ord,
    K: Data + Debug + Send + Sync + Abomonation + Clone,
    S: SketchData + Debug, 
{
    fn bucket_pred_lsh<P, PD, R, O, F, D, SP>(
            &self,
            right: &Stream<G, (H, (DKTPool, S, K))>,
            hasher: Arc<DKTCollection<F>>,
            pre: P,
            sketch_pred: SP, 
            distinct_pre: PD,
            result: R,
        ) -> Stream<G, usize>
        where
            P: FnMut(&K, &K) -> bool + 'static,
            PD: FnMut(&K, &K) -> bool + 'static,
            SP: FnMut(&S, &S) -> bool + 'static,
            R: Fn(K) -> O + 'static,
            F: LSHFunction<Input = D, Output = u32> + Sync + Send + Clone + 'static,
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
                            d.iter().size_hint()
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
                            d.iter().size_hint()
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
                        } else {
                            debug!("Don't do anything at {:?}\n{:?}", time, frontiers)
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

    #[allow(clippy::explicit_counter_loop)]
    fn bucket_pred_count<P>(&self, right: &Stream<G, (H, K)>, mut pred: P) -> Stream<G, usize>
    where
        P: FnMut(&K, &K) -> bool + 'static,
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
                                "candidate_verification",
                            );
                            let mut session = output.session(time);
                            let mut cnt = 0;
                            let mut total_pairs = 0;
                            let start = Instant::now();
                            info!("Left: {}, right: {}", buckets.len_left(), buckets.len_right());
                            if !buckets.is_one_side_empty() {
                                buckets.for_all(|l, r| {
                                    total_pairs += 1;
                                    if pred(l, r) {
                                        cnt += 1;
                                    }
                                });
                            }
                            buckets.clear();
                            let end = Instant::now();
                            session.give(cnt);
                            info!(
                                "Candidates {}: Passing predicate: {} // in {:?} ({})",
                                total_pairs,
                                cnt,
                                end - start,
                                proc_mem!()
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

impl<G, T, H, K, S> BucketTwoStream<G, T, H, K, S> for Stream<G, (H, (DKTPool, S, K))>
where
    G: Scope<Timestamp = T>,
    T: Timestamp + ToStepId,
    H: Data + Route + Debug + Send + Sync + Abomonation + Clone + Eq + Hash + Ord,
    K: Data + Debug + Send + Sync + Abomonation + Clone,
    S: SketchData + Debug,
{
    #[allow(clippy::explicit_counter_loop)]
    fn bucket_pred_lsh<P, PD, R, O, F, D, SP>(
        &self,
        right: &Stream<G, (H, (DKTPool, S, K))>,
        hasher: Arc<DKTCollection<F>>,
        mut pred: P,
        mut sketch_pred: SP, 
        mut distinct_pred: PD,
        result: R,
    ) -> Stream<G, usize>
    where
        P: FnMut(&K, &K) -> bool + 'static,
        PD: FnMut(&K, &K) -> bool + 'static,
        SP: FnMut(&S, &S) -> bool + 'static,
        R: Fn(K) -> O + 'static,
        F: LSHFunction<Input = D, Output = u32> + Sync + Send + Clone + 'static,
        O: ExchangeData,
    {
        let mut buckets = HashMap::new();
        let mut pool = BucketPool::default();
        let logger = self.scope().danny_logger();

        self.binary_frontier(
            &right,
            ExchangePact::new(|pair: &(H, (DKTPool, S, K))| pair.0.route()),
            ExchangePact::new(|pair: &(H, (DKTPool, S, K))| pair.0.route()),
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
                        info!(
                            "Received batch of left messages for time {:?}:\n\t{:?}",
                            t.time(),
                            d.iter().size_hint()
                        );
                        let mut data = d.replace(Vec::new());
                        log_event!(
                            logger,
                            LogEvent::ReceivedHashes(t.time().to_step_id(), data.len())
                        );
                        let rep_entry = buckets.entry(t.retain()).or_insert_with(|| pool.get());
                        for (h, v) in data.drain(..) {
                            rep_entry.push_left(h, v);
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
                            d.iter().size_hint()
                        );
                        let mut data = d.replace(Vec::new());
                        log_event!(
                            logger,
                            LogEvent::ReceivedHashes(t.time().to_step_id(), data.len())
                        );
                        let rep_entry = buckets.entry(t.retain()).or_insert_with(|| pool.get());
                        for (h, v) in data.drain(..) {
                            rep_entry.push_right(h, v);
                        }
                    });
                    let frontiers = &[left_in.frontier(), right_in.frontier()];
                    for (time, bucket) in buckets.iter_mut() {
                        if frontiers.iter().all(|f| !f.less_equal(time)) {
                            let _pg = ProfileGuard::new(
                                logger.clone(),
                                time.time().to_step_id(),
                                1,
                                "candidate_emission",
                            );
                            let mut session = output.session(time);
                            let mut total = 0;
                            let mut cnt = 0;
                            let mut sketch_cnt = 0;
                            let mut bloom_cnt = 0;
                            let repetitions = hasher.repetitions();
                            let start = Instant::now();
                            info!("Left: {}, right: {}", bucket.len_left(), bucket.len_right());
                            if !bucket.is_one_side_empty() {                                
                                debug!("Doing {:?} local repetitions", repetitions);
                                for rep in 0..repetitions {
                                    debug!("In repetition {}", rep);
                                    bucket.for_all_buckets(|x: &[(H, (DKTPool, S, K))], y: &[(H, (DKTPool, S, K))]| {
                                        let mut bucket = Bucket::default();
                                        // split up to buckets and do all to all within buckets.
                                        for (_, (pool, s, v)) in x.iter() {
                                            bucket.push_left(hasher.hash(pool, rep), (s, v))
                                        }
                                        for (_, (pool, s, v)) in y.iter() {
                                            bucket.push_right(hasher.hash(pool, rep), (s, v))
                                        }
                                        // TODO add sketches and duplicate counting
                                        bucket.for_all(|l, r| {
                                            total += 1;
                                            if (sketch_pred(l.0, r.0)) {
                                                if pred(l.1, r.1) {
                                                    cnt += 1;
                                                }
                                            } else {
                                                sketch_cnt += 1;
                                            }

                                        });
                                        bucket.clear();
                                    });
                                }
                            }
                            let total_pairs = cnt + bloom_cnt + sketch_cnt;
                            bucket.clear();
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
                                total,
                                cnt,
                                sketch_cnt,
                                bloom_cnt,
                                end - start,
                                proc_mem!(),
                                time.time()
                            );
                            session.give(cnt);
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

pub fn source_hashed_one_round<G, T, K, D, F>(
    scope: &G,
    global_vecs: Arc<ChunkedDataset<K, D>>,
    hash_fns: Arc<DKTCollection<F>>,
    matrix: MatrixDescription,
    direction: MatrixDirection,
) -> Stream<G, ((usize, u32), (K, D))>
// ) -> Stream<G, (u32, (K, D))>
where
    G: Scope<Timestamp = T>,
    T: Timestamp + Succ,
    D: ExchangeData + Debug,
    F: LSHFunction<Input = D, Output = u32> + Sync + Send + Clone + 'static,
    K: KeyData + Debug,
{
    let worker: u64 = scope.index() as u64;
    let logger = scope.danny_logger();
    let repetitions = hash_fns.repetitions();
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

    source(scope, "hashed source one round", move |capability| {
        let mut cap = Some(capability);
        move |output| {
            let mut done = false;
            if let Some(cap) = cap.as_mut() {
                for current_repetition in 0..repetitions {
                    stopwatch.maybe_stop();
                    stopwatch.start();
                    if worker == 0 {
                        debug!("Repetition {} (Hu et al. baseline)", current_repetition,);
                    }
                    let mut session = output.session(&cap);
                    for (k, v) in vecs.iter_stripe(matrix, direction, worker) {
                        let h = hash_fns.hash(&bit_pools[k], current_repetition as usize);
                        session.give(((current_repetition, h), (k.clone(), v.clone())));
                    }
                }
                done = true;
            }

            if done {
                // Drop the capability to signal that we will send no more data
                cap = None;
            }
        }
    })
}


pub fn source_hashed_two_round<G, T, K, D, F, S>(
    scope: &G,
    global_vecs: Arc<ChunkedDataset<K, D>>,
    sketcher: Arc<S>,
    hash_fns: Arc<DKTCollection<F>>,
    hash_fns2: Arc<DKTCollection<F>>,
    matrix: MatrixDescription,
    direction: MatrixDirection,
) -> Stream<G, ((usize, u32), (DKTPool, S::Output, (K, D)))>
// ) -> Stream<G, (u32, (K, D))>
where
    G: Scope<Timestamp = T>,
    T: Timestamp + Succ,
    D: ExchangeData + Debug,
    F: LSHFunction<Input = D, Output = u32> + Sync + Send + Clone + 'static,
    S: Sketcher<Input = D> + Clone + 'static,
    S::Output: SketchData + Debug,
    K: KeyData + Debug,
{
    let worker: u64 = scope.index() as u64;
    let logger = scope.danny_logger();
    let repetitions = hash_fns.repetitions();
    let repetitions_inner = hash_fns2.repetitions();
    let vecs = Arc::clone(&global_vecs);
    let mut stopwatch = RepetitionStopWatch::new("repetition", worker == 0, logger);
    let mut bit_pools: HashMap<K, DKTPool> = HashMap::new();
    let mut bit_pools_intern: HashMap<K, DKTPool> = HashMap::new();
    info!("Computing the bit pools");
    let start = Instant::now();
    for (k, v) in vecs.iter_stripe(matrix, direction, worker) {
        bit_pools.insert(*k, hash_fns.pool(v));
        bit_pools_intern.insert(*k, hash_fns2.pool(v));
    }
    let end = Instant::now();
    info!(
        "Computed the bit pools ({:?}, {})",
        end - start,
        proc_mem!()
    );

    source(scope, "hashed source two round", move |capability| {
        let mut cap = Some(capability);
        move |output| {
            let mut done = false;
            if let Some(cap) = cap.as_mut() {
                for current_repetition in 0..repetitions {
                    stopwatch.maybe_stop();
                    stopwatch.start();
                    if worker == 0 {
                        debug!("Repetition {} (Hu et al. baseline)", current_repetition,);
                    }
                    let mut session = output.session(&cap);
                    for (k, v) in vecs.iter_stripe(matrix, direction, worker) {
                        let h = hash_fns.hash(&bit_pools[k], current_repetition as usize);
                        let s = sketcher.sketch(v);
                        session.give(((current_repetition, h), (hash_fns2.pool(v), s.clone(), (k.clone(), v.clone()))));
                        // for inner_rep in 0..repetitions_inner {
                        //     let h2 = hash_fns2.hash(&bit_pools_intern[k], inner_rep as usize);
                        //     session.give(((current_repetition, h), ((inner_rep, h2), (k.clone(), v.clone()))));
                        // }
                    }
                }
                done = true;
            }

            if done {
                // Drop the capability to signal that we will send no more data
                cap = None;
            }
        }
    })
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
                    for (k, _v) in vecs.iter_stripe(matrix, direction, worker) {
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

