use crate::dataset::*;
use crate::logging::*;
use crate::lsh::bucket::*;
use crate::lsh::functions::*;
use crate::lsh::prefix_hash::*;
use crate::operators::Route;
use crate::operators::*;
use crate::sketch::*;
use abomonation::Abomonation;
use rand::{Rng, SeedableRng};
use std::clone::Clone;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Instant;
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

pub struct PairGenerator<H, K>
where
    H: Hash + Eq + Ord,
{
    buckets: BTreeMap<H, (Vec<K>, Vec<K>)>,
    cur_left: Vec<K>,
    cur_right: Vec<K>,
    cur_left_idx: usize,
    cur_right_idx: usize,
}

impl<H: Hash + Eq + Clone + Ord, K: Clone> PairGenerator<H, K> {
    pub fn new(buckets: BTreeMap<H, (Vec<K>, Vec<K>)>) -> Self {
        PairGenerator {
            buckets,
            cur_left: Vec::new(),
            cur_right: Vec::new(),
            cur_left_idx: 0,
            cur_right_idx: 0,
        }
    }

    pub fn done(&self) -> bool {
        self.buckets.is_empty() && self.cur_left.is_empty() && self.cur_right.is_empty()
    }
}

impl<H: Hash + Eq + Clone + Ord, K: Clone> Iterator for PairGenerator<H, K> {
    type Item = (K, K);

    fn next(&mut self) -> Option<(K, K)> {
        if self.done() {
            return None;
        }
        // info!("Iter");
        // dbg!(&self.cur_left);
        // dbg!(&self.cur_right);
        if self.cur_left.is_empty() {
            assert!(
                self.cur_right.is_empty(),
                "left vector is empty, but right one is not"
            );
            loop {
                // Early return if there is no key, i.e. if the map is empty
                let key = self.buckets.keys().next().cloned()?;
                let buckets = self.buckets.remove(&key).unwrap();
                // Consider only non empty buckets
                if !buckets.0.is_empty() && !buckets.1.is_empty() {
                    self.cur_left = buckets.0;
                    self.cur_right = buckets.1;
                    self.cur_left_idx = 0;
                    self.cur_right_idx = 0;
                    // info!("Using key {:?}", key);
                    break;
                }
            }
        }
        // dbg!(self.cur_left.len());
        // dbg!(self.cur_right.len());
        let left_elem = self.cur_left[self.cur_left_idx].clone();
        let right_elem = self.cur_right[self.cur_right_idx].clone();
        let pair = (left_elem, right_elem);
        // Move the index
        if self.cur_right_idx + 1 >= self.cur_right.len() {
            self.cur_right_idx = 0;
            if self.cur_left_idx + 1 >= self.cur_left.len() {
                // We are done for this key
                // info!("Clearing both vectors");
                self.cur_left.clear();
                self.cur_right.clear();
            } else {
                self.cur_left_idx += 1;
            }
        } else {
            self.cur_right_idx += 1;
        }

        Some(pair)
    }
}

pub trait BucketStream<G, T, H, K>
where
    G: Scope<Timestamp = T>,
    T: Timestamp,
    H: Data + Route + Debug + Send + Sync + Abomonation + Clone + Eq + Hash + Ord,
    K: Data + Debug + Send + Sync + Abomonation + Clone,
{
    fn bucket(&self, right: &Stream<G, (H, K)>) -> Stream<G, (K, K)>;
    fn bucket_pred<P>(&self, right: &Stream<G, (H, K)>, pre: P) -> Stream<G, (K, K)>
    where
        P: FnMut(&K, &K) -> bool + 'static;
}

impl<G, T, H, K> BucketStream<G, T, H, K> for Stream<G, (H, K)>
where
    G: Scope<Timestamp = T>,
    T: Timestamp + ToStepId,
    H: Data + Route + Debug + Send + Sync + Abomonation + Clone + Eq + Hash + Ord,
    K: Data + Debug + Send + Sync + Abomonation + Clone,
{
    fn bucket(&self, right: &Stream<G, (H, K)>) -> Stream<G, (K, K)> {
        self.bucket_pred(right, |_, _| true)
    }

    #[allow(clippy::explicit_counter_loop)]
    fn bucket_pred<P>(&self, right: &Stream<G, (H, K)>, pred: P) -> Stream<G, (K, K)>
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
                let mut pred = pred;
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
                            // Emit some output pairs
                            info!(
                                "Outputting pairs out of buckets from {} left and {} right vectors",
                                buckets.len_left(),
                                buckets.len_right()
                            );
                            let mut session = output.session(time);
                            let mut cnt = 0;
                            buckets.for_all(|l, r| {
                                if pred(l, r) {
                                    session.give((l.clone(), r.clone()));
                                    cnt += 1;
                                }
                            });
                            buckets.clear();
                            log_event!(
                                logger,
                                LogEvent::GeneratedPairs(time.time().to_step_id(), cnt)
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
                        let bucket = buckets
                            .remove(t)
                            .expect("No buckets at the requested time!");
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
    for<'a> H:
        Data + Route + Debug + Send + Sync + Abomonation + Clone + Eq + Hash + Ord + PrefixHash<'a>,
    K: Data + Debug + Send + Sync + Abomonation + Clone,
{
    fn bucket_prefixes<P, F>(
        &self,
        right: &Self,
        routing_prefix: usize,
        predicate: P,
        report: F,
    ) -> Stream<G, (K, K)>
    where
        P: FnMut(&K, &K) -> bool + 'static,
        F: FnMut(&T, usize) + 'static;
}

impl<G, T, H, K> BucketPrefixesStream<G, T, H, K> for Stream<G, (H, (K, u8))>
where
    G: Scope<Timestamp = T>,
    T: Timestamp + ToStepId,
    for<'a> H:
        Data + Route + Debug + Send + Sync + Abomonation + Clone + Eq + Hash + Ord + PrefixHash<'a>,
    K: Data + Debug + Send + Sync + Abomonation + Clone,
{
    #[allow(clippy::explicit_counter_loop)]
    fn bucket_prefixes<P, F>(
        &self,
        right: &Self,
        routing_prefix: usize,
        pred: P,
        mut report: F,
    ) -> Stream<G, (K, K)>
    where
        P: FnMut(&K, &K) -> bool + 'static,
        F: FnMut(&T, usize) + 'static,
    {
        let mut buckets = HashMap::new();
        let mut pool = BucketPool::default();
        let logger = self.scope().danny_logger();

        self.binary_frontier(
            &right,
            ExchangePact::new(move |pair: &(H, (K, u8))| pair.0.prefix(routing_prefix).route()),
            ExchangePact::new(move |pair: &(H, (K, u8))| pair.0.prefix(routing_prefix).route()),
            "bucket",
            move |_, _| {
                let mut pred = pred;
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
                            // Emit some output pairs
                            info!(
                                "Outputting pairs out of buckets from {} left and {} right vectors",
                                buckets.len_left(),
                                buckets.len_right()
                            );
                            let mut session = output.session(time);
                            let mut cnt = 0;
                            let mut discarded = 0;
                            info!("Starting candidate emission ({})", proc_mem!());
                            let start = Instant::now();
                            buckets.for_prefixes(|l, r| {
                                if pred(l, r) {
                                    session.give((l.clone(), r.clone()));
                                    cnt += 1;
                                } else {
                                    discarded += 1;
                                }
                            });
                            buckets.clear();
                            report(time.time(), discarded);
                            let end = Instant::now();
                            info!(
                                "Emitted {} candidate pairs in {:?} ({}) (repetition {:?})",
                                cnt,
                                end - start,
                                proc_mem!(),
                                time.time()
                            );
                            log_event!(
                                logger,
                                LogEvent::GeneratedPairs(time.time().to_step_id(), cnt)
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
                        let bucket = buckets.remove(t).expect("No buckets at the requested time");
                        // put it back into the pool
                        pool.give_back(bucket);
                    }
                }
            },
        )
    }
}

pub trait FilterSketches<G, T, K, V>
where
    G: Scope<Timestamp = T>,
    T: Timestamp + Succ,
    K: Data + Sync + Send + Clone + Abomonation + Debug,
    V: SketchEstimate + Data + Debug + Send + Sync + Abomonation + Clone + BitBasedSketch,
{
    fn filter_sketches(&self, sketch_predicate: SketchPredicate<V>) -> Stream<G, (K, K)>;
}

impl<G, T, K, V> FilterSketches<G, T, K, V> for Stream<G, ((V, K), (V, K))>
where
    G: Scope<Timestamp = T>,
    T: Timestamp + Succ + ToStepId,
    K: Data + Sync + Send + Clone + Abomonation + Debug,
    V: SketchEstimate + Data + Debug + Send + Sync + Abomonation + Clone + BitBasedSketch,
{
    fn filter_sketches(&self, sketch_predicate: SketchPredicate<V>) -> Stream<G, (K, K)> {
        let logger = self.scope().danny_logger();
        self.unary(Pipeline, "sketch filtering", move |_, _| {
            move |input, output| {
                input.for_each(|t, data| {
                    let _pg = ProfileGuard::new(
                        logger.clone(),
                        t.time().to_step_id(),
                        1,
                        "sketch_filtering",
                    );
                    let mut discarded = 0;
                    let t = t.retain();
                    let mut session = output.session(&t);
                    let mut data = data.replace(Vec::new());
                    for (p1, p2) in data.drain(..) {
                        if sketch_predicate.eval(&p1.0, &p2.0) {
                            session.give((p1.1, p2.1));
                        } else {
                            discarded += 1;
                        }
                    }
                    log_event!(
                        logger,
                        LogEvent::SketchDiscarded(t.time().to_step_id(), discarded)
                    );
                });
            }
        })
    }
}

struct RepetitionStopWatch {
    start: Option<Instant>,
    counter: usize,
    name: String,
    logger: Option<Logger<LogEvent>>,
}

impl RepetitionStopWatch {
    pub fn new(name: &str, logger: Option<Logger<LogEvent>>) -> Self {
        Self {
            start: None,
            counter: 0usize,
            name: name.to_owned(),
            logger: logger,
        }
    }

    pub fn start(&mut self) {
        self.start.replace(Instant::now());
    }

    pub fn maybe_stop(&mut self, verbose: bool) {
        if let Some(start) = self.start.take() {
            let elapsed = Instant::now() - start;
            if verbose {
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

pub fn source_hashed<G, T, K, D, F, H>(
    scope: &G,
    global_vecs: Arc<ChunkedDataset<K, D>>,
    hash_fns: LSHCollection<F, H>,
    matrix: MatrixDescription,
    direction: MatrixDirection,
    throttling_probe: ProbeHandle<G::Timestamp>,
) -> Stream<G, (H, K)>
where
    G: Scope<Timestamp = T>,
    T: Timestamp + Succ,
    D: Data + Sync + Send + Clone + Abomonation + Debug,
    F: LSHFunction<Input = D, Output = H> + Sync + Send + Clone + 'static,
    H: Data + Route + Debug + Send + Sync + Abomonation + Clone + Eq + Hash,
    K: Data + Debug + Send + Sync + Abomonation + Clone + Eq + Hash + Route,
{
    let worker: u64 = scope.index() as u64;
    let logger = scope.danny_logger();
    let repetitions = hash_fns.repetitions();
    let mut current_repetition = 0usize;
    let mut stopwatch = RepetitionStopWatch::new("repetition", logger);
    source(scope, "hashed source", move |capability| {
        let mut cap = Some(capability);
        let vecs = Arc::clone(&global_vecs);
        move |output| {
            let mut done = false;
            if let Some(cap) = cap.as_mut() {
                if !throttling_probe.less_than(cap.time()) {
                    stopwatch.maybe_stop(worker == 0);
                    stopwatch.start();
                    if worker == 0 {
                        info!("Repetition {}", current_repetition);
                    }
                    let mut session = output.session(&cap);
                    for (k, v) in vecs.iter_stripe(matrix, direction, worker) {
                        let h = hash_fns.hash(v, current_repetition as usize);
                        session.give((h, k.clone()));
                    }
                    current_repetition += 1;
                    cap.downgrade(&cap.time().succ());
                    done = current_repetition >= repetitions;
                }
            }

            if done {
                // Drop the capability to signal that we will send no more data
                cap = None;
                info!("Generated all repetitions");
            }
        }
    })
}

pub fn source_hashed_sketched<G, T, K, D, F, S, H, V>(
    scope: &G,
    global_vecs: Arc<ChunkedDataset<K, D>>,
    hash_fns: LSHCollection<F, H>,
    sketcher: S,
    matrix: MatrixDescription,
    direction: MatrixDirection,
    throttling_probe: ProbeHandle<G::Timestamp>,
) -> Stream<G, (H, (V, K))>
where
    // G: Scope<Timestamp = Product<u32, u32>>,
    G: Scope<Timestamp = T>,
    T: Timestamp + Succ,
    D: Data + Sync + Send + Clone + Abomonation + Debug,
    F: LSHFunction<Input = D, Output = H> + Sync + Send + Clone + 'static,
    S: Sketcher<Input = D, Output = V> + Clone + 'static,
    H: Data + Route + Debug + Send + Sync + Abomonation + Clone + Eq + Hash,
    K: Data + Debug + Send + Sync + Abomonation + Clone + Eq + Hash + Route,
    V: Data + Debug + Send + Sync + Abomonation + Clone,
{
    let worker: u64 = scope.index() as u64;
    let logger = scope.danny_logger();
    let repetitions = hash_fns.repetitions();
    let mut current_repetition = 0usize;
    let vecs = Arc::clone(&global_vecs);
    let mut sketches: HashMap<K, V> = HashMap::new();
    info!("Computing sketches");
    let start_sketch = Instant::now();
    for (k, v) in vecs.iter_stripe(matrix, direction, worker) {
        let s = sketcher.sketch(v);
        sketches.insert(k.clone(), s);
    }
    let end_sketch = Instant::now();
    info!("Sketches computed in {:?}", end_sketch - start_sketch);
    let mut stopwatch = RepetitionStopWatch::new("repetition", logger);

    source(scope, "hashed source", move |capability| {
        let mut cap = Some(capability);
        move |output| {
            let mut done = false;
            if let Some(cap) = cap.as_mut() {
                if !throttling_probe.less_than(cap.time()) {
                    stopwatch.maybe_stop(worker == 0);
                    stopwatch.start();
                    if worker == 0 {
                        info!("Repetition {} with sketches", current_repetition,);
                    }
                    let mut session = output.session(&cap);
                    for (k, v) in vecs.iter_stripe(matrix, direction, worker) {
                        let h = hash_fns.hash(v, current_repetition as usize);
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_pair_iterator() {
        let mut buckets = BTreeMap::new();
        buckets.insert(0, (vec![1, 2, 3], vec![10, 11, 12]));
        buckets.insert(1, (vec![], vec![19]));
        buckets.insert(2, (vec![1, 2, 3, 4], vec![]));
        buckets.insert(3, (vec![3, 4], vec![30]));

        let mut expected = HashSet::new();
        for (_k, (lks, rks)) in buckets.iter() {
            for lk in lks.iter() {
                for rk in rks.iter() {
                    expected.insert((lk.clone(), rk.clone()));
                }
            }
        }
        let mut iterator = PairGenerator::new(buckets);
        let mut actual = HashSet::new();
        assert!(!iterator.done());
        while let Some(pair) = iterator.next() {
            actual.insert(pair);
        }

        assert_eq!(expected, actual);
        assert!(iterator.done());
    }
}

pub fn collect_sample<G, K, D, F, H, R>(
    scope: &G,
    global_vecs: Arc<ChunkedDataset<K, D>>,
    n: usize,
    matrix: MatrixDescription,
    direction: MatrixDirection,
    rng: R,
) -> Stream<G, (K, D)>
where
    G: Scope<Timestamp = u32>,
    // G: Scope<Timestamp = Product<u32, u32>>,
    D: Data + Sync + Send + Clone + Abomonation + Debug,
    F: LSHFunction<Input = D, Output = H> + Sync + Send + Clone + 'static,
    H: Data + Debug + Send + Sync + Abomonation + Clone + Eq + Hash,
    K: Data + Debug + Send + Sync + Abomonation + Clone + Eq + Hash + Route,
    R: Rng + SeedableRng + Clone + ?Sized + 'static,
{
    let worker: u64 = scope.index() as u64;
    let mut seeder = rng.clone();
    for _ in 0..worker {
        seeder.gen::<f64>();
    }

    let rng = R::from_rng(seeder).expect("Error initializing random number generator");

    source(scope, "hashed source", move |capability| {
        let mut rng = rng;
        let mut cap = Some(capability);
        let vecs = Arc::clone(&global_vecs);
        move |output| {
            let mut done = false;
            if let Some(cap) = cap.as_mut() {
                let mut session = output.session(&cap);
                let p = n as f64 / vecs.stripe_len(matrix, direction, worker) as f64;
                info!("Sampling with probability {}", p);
                for (k, v) in vecs.iter_stripe(matrix, direction, worker) {
                    if rng.gen_bool(p) {
                        session.give((k.clone(), v.clone()));
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

#[allow(clippy::too_many_arguments, clippy::type_complexity)]
pub fn source_hashed_adaptive<G, T, K, D, F, H, R>(
    scope: &G,
    best_levels: &Stream<G, (K, usize)>,
    global_vecs: Arc<ChunkedDataset<K, D>>,
    multilevel_hasher: Arc<MultilevelHasher<D, H, F>>,
    matrix: MatrixDescription,
    direction: MatrixDirection,
    throttling_probe: ProbeHandle<G::Timestamp>,
    rng: R,
) -> Stream<G, (H, (K, u8))>
where
    G: Scope<Timestamp = T>,
    T: Timestamp + Succ + ToStepId,
    D: Data + Sync + Send + Clone + Abomonation + Debug,
    F: LSHFunction<Input = D, Output = H> + Sync + Send + Clone + 'static,
    H: Data + Route + Debug + Send + Sync + Abomonation + Clone + Eq + Hash + Ord,
    K: Data + Debug + Send + Sync + Abomonation + Clone + Eq + Hash + Route + Ord,
    R: Rng + SeedableRng + Clone + ?Sized + 'static + Sync + Send,
{
    let worker: u64 = scope.index() as u64;
    let min_level = multilevel_hasher.min_level();
    let max_level = multilevel_hasher.max_level();
    let num_repetitions = multilevel_hasher.repetitions_at_level(max_level);
    let multilevel_hasher = Arc::clone(&multilevel_hasher);
    let global_vecs_2 = Arc::clone(&global_vecs);
    let logger = scope.danny_logger();

    let mut builder = OperatorBuilder::new("adaptive-source".to_owned(), best_levels.scope());
    let mut input_best_levels = builder.new_input(&best_levels, Pipeline);
    let (mut output, output_stream) = builder.new_output();
    builder.build(move |mut capabilities| {
        let mut capability = Some(capabilities.pop().expect("No capability to pop"));

        let mut best_levels: BTreeMap<K, usize> = BTreeMap::new();
        let mut current_repetition = 0;
        let mut done = false;
        let vecs = Arc::clone(&global_vecs_2);
        let mut stopwatch = RepetitionStopWatch::new("repetition", logger.clone());

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
                    stopwatch.maybe_stop(worker == 0);
                    stopwatch.start();
                    if worker == 0 {
                        info!(
                            "Repetition {}/{} (current memory {}, previous iter)",
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

                    let mut cnt = 0;
                    let mut session = output.session(&capability);
                    let active_levels = multilevel_hasher.levels_at_repetition(current_repetition);
                    for (key, v) in vecs.iter_stripe(matrix, direction, worker) {
                        let &this_best_level = best_levels.get(key).unwrap_or(&min_level);
                        if active_levels.contains(&this_best_level) {
                            // We hash to the max level because the levelling will be taken care of in the buckets
                            let h = multilevel_hasher.hash(v, max_level, current_repetition);
                            session.give((h, (key.clone(), this_best_level as u8)));
                            cnt += 1;
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

#[allow(clippy::too_many_arguments, clippy::type_complexity)]
pub fn source_hashed_adaptive_sketched<G, T, K, D, F, H, R, S, SV>(
    scope: &G,
    best_levels: &Stream<G, (K, usize)>,
    global_vecs: Arc<ChunkedDataset<K, D>>,
    multilevel_hasher: Arc<MultilevelHasher<D, H, F>>,
    sketcher: S,
    matrix: MatrixDescription,
    direction: MatrixDirection,
    throttling_probe: ProbeHandle<G::Timestamp>,
    rng: R,
) -> Stream<G, (H, ((K, SV), u8))>
where
    G: Scope<Timestamp = T>,
    T: Timestamp + Succ + ToStepId,
    D: Data + Sync + Send + Clone + Abomonation + Debug,
    F: LSHFunction<Input = D, Output = H> + Sync + Send + Clone + 'static,
    H: Data + Route + Debug + Send + Sync + Abomonation + Clone + Eq + Hash + Ord,
    K: Data + Debug + Send + Sync + Abomonation + Clone + Eq + Hash + Route,
    R: Rng + SeedableRng + Clone + ?Sized + 'static + Sync + Send,
    S: Sketcher<Input = D, Output = SV> + Clone + 'static,
    SV: ExchangeData + Debug,
{
    let worker: u64 = scope.index() as u64;
    let mut sketches: HashMap<K, SV> = HashMap::new();
    info!("Computing sketches");
    let start_sketch = Instant::now();
    for (k, v) in global_vecs.iter_stripe(matrix, direction, worker) {
        let s = sketcher.sketch(v);
        sketches.insert(k.clone(), s);
    }
    let end_sketch = Instant::now();
    info!("Sketches computed in {:?}", end_sketch - start_sketch);

    let min_level = multilevel_hasher.min_level();
    let max_level = multilevel_hasher.max_level();
    let multilevel_hasher = Arc::clone(&multilevel_hasher);
    let multilevel_hasher_2 = Arc::clone(&multilevel_hasher);
    let global_vecs_2 = Arc::clone(&global_vecs);
    let logger = scope.danny_logger();
    let starting_level = multilevel_hasher.min_level();
    let num_repetitions = multilevel_hasher.repetitions_at_level(max_level);

    let mut builder = OperatorBuilder::new("adaptive-source".to_owned(), best_levels.scope());
    let mut input_best_levels = builder.new_input(&best_levels, Pipeline);
    let (mut output, output_stream) = builder.new_output();
    builder.build(move |mut capabilities| {
        let mut capability = Some(capabilities.pop().expect("No capability to pop"));

        let mut stopwatch = RepetitionStopWatch::new("repetition", logger.clone());
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
                    stopwatch.maybe_stop(worker == 0);
                    stopwatch.start();
                    if worker == 0 {
                        info!(
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
                    let active_levels = multilevel_hasher.levels_at_repetition(current_repetition);
                    let mut cnt = 0;
                    for (key, v) in vecs.iter_stripe(matrix, direction, worker) {
                        let &this_best_level = best_levels.get(key).unwrap_or(&min_level);
                        if active_levels.contains(&this_best_level) {
                            // We hash to the max level because the levelling will be taken care of in the buckets
                            let h = multilevel_hasher.hash(v, max_level, current_repetition);
                            let s = sketches[key].clone();
                            session.give((h, ((key.clone(), s), this_best_level as u8)));
                            cnt += 1;
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

type EstimationTimestamp<T: Timestamp> = Product<T, usize>;

fn all_hashes_source<G, T, K, D, H, F, R>(
    scope: &G,
    vecs: Arc<ChunkedDataset<K, D>>,
    sampling_probability: f64,
    hasher: Arc<MultilevelHasher<D, H, F>>,
    matrix: MatrixDescription,
    direction: MatrixDirection,
    throttling_probe: ProbeHandle<EstimationTimestamp<T>>,
    mut rng: R,
) -> Stream<G, (H, K)>
where
    G: Scope<Timestamp = EstimationTimestamp<T>>,
    T: Timestamp + Debug + Succ + ToStepId,
    D: ExchangeData + Debug,
    H: Data + Route + Debug + Send + Sync + Abomonation + Clone + Eq + Hash + Ord,
    K: Data + Debug + Send + Sync + Abomonation + Clone + Route,
    F: LSHFunction<Input = D, Output = H> + Send + Clone + Sync + 'static,
    R: Rng + 'static,
{
    let worker = scope.index() as u64;
    let logger = scope.danny_logger();
    let vecs = Arc::clone(&vecs);

    let mut done = false;
    let max_level = hasher.max_level();
    let mut current_repetition = 0;
    let num_repetitions = hasher.repetitions_at_level(max_level);
    let mut stopwatch = RepetitionStopWatch::new("cost_estimation", logger.clone());
    source(&scope, "all-hashes", move |cap| {
        let hasher = Arc::clone(&hasher);
        let mut cap = Some(cap);
        move |output| {
            if let Some(cap) = cap.as_mut() {
                if !throttling_probe.less_than(cap.time()) {
                    stopwatch.maybe_stop(false);
                    stopwatch.start();
                    let mut session = output.session(cap);
                    let mut cnt = 0;
                    for (k, v) in vecs
                        .iter_stripe(matrix, direction, worker)
                        .filter(|_| rng.gen_bool(sampling_probability))
                    {
                        let h = hasher.hash(v, max_level, current_repetition);
                        session.give((h, k.clone()));
                        cnt += 1;
                    }
                    // info!(
                    //     "Output just {} hashed points over {} (sample probability {})",
                    //     cnt,
                    //     vecs.stripe_len(matrix, direction, worker),
                    //     sampling_probability
                    // );
                    current_repetition += 1;
                    cap.downgrade(&cap.time().succ());
                    if current_repetition >= num_repetitions {
                        done = true;
                    }
                }
            }

            if done {
                cap = None;
            }
        }
    })
}

fn count_collisions<G, T, H, K, D, F>(
    left: &Stream<G, (H, K)>,
    right: &Stream<G, (H, K)>,
    left_sampling_probability: f64,
    right_sampling_probability: f64,
    hasher: Arc<MultilevelHasher<D, H, F>>,
    probe: ProbeHandle<G::Timestamp>,
) -> (Stream<G, (K, (u8, f64))>, Stream<G, (K, (u8, f64))>)
where
    G: Scope<Timestamp = EstimationTimestamp<T>>,
    T: Timestamp,
    K: ExchangeData + Hash + Eq + Debug + Ord,
    for<'a> H: ExchangeData + Ord + Route + Debug + PrefixHash<'a> + Hash + Eq,
    F: LSHFunction<Input = D, Output = H> + Send + Clone + Sync + 'static,
    D: ExchangeData + Debug,
{
    let worker = left.scope().index();
    let routing_prefix = hasher.min_level();
    let l_weight = 1.0 / left_sampling_probability;
    let r_weight = 1.0 / right_sampling_probability;

    let mut builder = OperatorBuilder::new("collision-counter".to_owned(), left.scope());

    let mut input_left = builder.new_input(
        &left,
        ExchangePact::new(move |pair: &(H, K)| pair.0.prefix(routing_prefix).route()),
    );
    let mut input_right = builder.new_input(
        &right,
        ExchangePact::new(move |pair: &(H, K)| pair.0.prefix(routing_prefix).route()),
    );
    let (mut output_left, stream_left) = builder.new_output();
    let (mut output_right, stream_right) = builder.new_output();

    let mut pool = BucketPool::default();
    let mut buckets = HashMap::new();
    let mut caps_left = HashMap::new();
    let mut caps_right = HashMap::new();

    builder.build(move |_| {
        let mut notificator = FrontierNotificator::new();
        move |frontiers| {
            let mut input_left = FrontieredInputHandle::new(&mut input_left, &frontiers[0]);
            let mut input_right = FrontieredInputHandle::new(&mut input_right, &frontiers[1]);
            let mut output_left = output_left.activate();
            let mut output_right = output_right.activate();

            input_left.for_each(|t, data| {
                let mut data = data.replace(Vec::new());
                caps_left
                    .entry(t.time().clone())
                    .or_insert_with(|| t.delayed_for_output(t.time(), 0));
                let rep_entry = buckets
                    .entry(t.time().clone())
                    .or_insert_with(|| pool.get());
                for (h, k) in data.drain(..) {
                    rep_entry.push_left(h, k);
                }
                notificator.notify_at(t.retain());
            });
            input_right.for_each(|t, data| {
                let mut data = data.replace(Vec::new());
                caps_right
                    .entry(t.time().clone())
                    .or_insert_with(|| t.delayed_for_output(t.time(), 1));
                let rep_entry = buckets
                    .entry(t.time().clone())
                    .or_insert_with(|| pool.get());
                for (h, k) in data.drain(..) {
                    rep_entry.push_right(h, k);
                }
                notificator.notify_at(t.retain());
            });

            notificator.for_each(
                &[input_left.frontier(), input_right.frontier()],
                |time, _| {
                    if let Some(mut buckets) = buckets.remove(&time) {
                        // Because of the sampling, either the left or the right side of the bucket might be empty.
                        // in this case we skip the block altogether, also because otherwise the capabilities might not be there.
                        if buckets.len_left() > 0 && buckets.len_right() > 0 {
                            let mut collisions_left = HashMap::new();
                            let mut collisions_right = HashMap::new();
                            let cap_left = caps_left.remove(&time.time()).unwrap_or_else(|| {
                                panic!(
                                    "[{}] Could not find time {:?} (left) {:?}",
                                    worker,
                                    time.time(),
                                    caps_left
                                )
                            });
                            let cap_right = caps_right.remove(&time.time()).unwrap_or_else(|| {
                                panic!(
                                    "[{}] Could not find time {:?} (right) {:?}",
                                    worker,
                                    time.time(),
                                    caps_right
                                )
                            });
                            let mut session_left = output_left.session(&cap_left);
                            let mut session_right = output_right.session(&cap_right);
                            let actual_min_level = *hasher.levels_at_repetition(time.inner).start();
                            // It is OK for a point not to collide on all levels if we
                            // are not doing a self join
                            buckets.for_all_prefixes(
                                actual_min_level,
                                hasher.max_level(),
                                |level, lb, rb| {
                                    for (_h, l) in lb {
                                        *collisions_left
                                            .entry((l.clone(), level))
                                            .or_insert(0.0) += rb.len() as f64 * r_weight;
                                    }
                                    for (_h, r) in rb {
                                        *collisions_right
                                            .entry((r.clone(), level))
                                            .or_insert(0.0) += lb.len() as f64 * l_weight;
                                    }
                                },
                            );
                            buckets.clear();
                            pool.give_back(buckets);
                            for ((k, level), weight) in collisions_left.drain() {
                                session_left.give((k, (level as u8, weight)));
                            }
                            for ((k, level), weight) in collisions_right.drain() {
                                session_right.give((k, (level as u8, weight)));
                            }
                        } else {
                            // Note that we still have to remove the capabilities from their stashes.
                            caps_left.remove(&time.time());
                            caps_right.remove(&time.time());
                        }
                    }
                },
            );
        }
    });
    (
        stream_left.probe_with(&mut probe.clone()),
        stream_right.probe_with(&mut probe.clone()),
    )
}

/// A balance greater than 0.5 penalizes repetitions
fn select_minimum<G, T, K, D, H, F>(
    counts: &Stream<Child<G, EstimationTimestamp<T>>, (K, (u8, f64))>,
    hasher: Arc<MultilevelHasher<D, H, F>>,
    balance: f64,
    iteration_cost: f64,
) -> Stream<G, (K, usize)>
where
    G: Scope<Timestamp = T>,
    T: Timestamp,
    D: ExchangeData + Debug,
    K: ExchangeData + Route + Hash + Eq + Debug,
    H: Data + Route + Debug + Send + Sync + Abomonation + Clone + Eq + Hash + Ord,
    F: LSHFunction<Input = D, Output = H> + Send + Clone + Sync + 'static,
{
    let logger = counts.scope().danny_logger();
    assert!(balance >= 0.0 && balance <= 1.0);
    let mut trigger = false;
    counts
        .leave()
        .aggregate(
            |_key, val: (u8, f64), agg: &mut HashMap<u8, f64>| {
                *agg.entry(val.0).or_insert(0.0) += val.1;
            },
            move |key, agg: HashMap<u8, f64>| {
                let mut min_work = std::f64::INFINITY;
                let mut best_level = 0;
                for (&level, &collisions) in agg.iter() {
                    let reps = hasher.repetitions_at_level(level as usize);
                    let work = balance * iteration_cost * reps as f64
                        + (1.0 - balance) * collisions as f64;
                    if work < min_work {
                        min_work = work;
                        best_level = level;
                    }
                }
                (key, best_level as usize)
            },
            Route::route,
        )
        .inspect_batch(move |_, d| {
            if trigger {
                info!("Selected best levels (at least some of them)");
                trigger = false;
            }
            let mut levels = HashMap::new();
            for (_, level) in d.iter() {
                *levels.entry(level).or_insert(0) += 1;
            }
            for (&level, count) in levels {
                log_event!(logger, LogEvent::AdaptiveLevelHistogram(level, count));
            }
        })
}

pub fn find_best_level<G, T, K, D, H, F, R>(
    mut scope: G,
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
    K: Data + Debug + Send + Sync + Abomonation + Clone + Route + Hash + Eq + Ord,
    for<'a> H:
        Data + Route + Debug + Send + Sync + Abomonation + Clone + Eq + Hash + Ord + PrefixHash<'a>,
    F: LSHFunction<Input = D, Output = H> + Send + Clone + Sync + 'static,
    R: Rng + Clone + 'static,
{
    // 1. Generate all hash values for all repetitions
    // 2. In each level/repetition pair, build the buckets
    // 3. Accumulate the cost
    // 4. Accumulate the minimum cost in a distributed fashion

    // let iteration_cost = (left.global_n + right.global_n) as f64;
    let iteration_cost = 1.0;
    info!("The cost of every iteration is {}", iteration_cost);
    let probe = ProbeHandle::new();
    let left_sampling_probability = 1.0 / (left.global_n as f64).sqrt();
    let right_sampling_probability = 1.0 / (right.global_n as f64).sqrt();

    scope.scoped::<EstimationTimestamp<T>, _, _>("estimation scope", |inner| {
        let l_hashes = all_hashes_source(
            inner,
            left,
            left_sampling_probability,
            Arc::clone(&hasher),
            matrix,
            MatrixDirection::Rows,
            probe.clone(),
            rng.clone(),
        );
        let r_hashes = all_hashes_source(
            inner,
            right,
            right_sampling_probability,
            Arc::clone(&hasher),
            matrix,
            MatrixDirection::Columns,
            probe.clone(),
            rng.clone(),
        );

        let (left_collisions, right_collisions) = count_collisions(
            &l_hashes,
            &r_hashes,
            left_sampling_probability,
            right_sampling_probability,
            Arc::clone(&hasher),
            probe.clone(),
        );
        (
            select_minimum(
                &left_collisions,
                Arc::clone(&hasher),
                balance,
                iteration_cost,
            ),
            select_minimum(
                &right_collisions,
                Arc::clone(&hasher),
                balance,
                iteration_cost,
            ),
        )
    })
}
