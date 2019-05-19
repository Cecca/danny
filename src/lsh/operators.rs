use crate::dataset::*;
use crate::logging::*;
use crate::lsh::bucket::*;
use crate::lsh::functions::*;
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
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::generic::source;
use timely::dataflow::operators::generic::{FrontieredInputHandle, OutputHandle};
use timely::dataflow::operators::Capability;
use timely::dataflow::operators::Leave;
use timely::dataflow::operators::*;
use timely::dataflow::scopes::Child;
use timely::dataflow::*;
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
    H: Data + Route + Debug + Send + Sync + Abomonation + Clone + Eq + Hash + Ord + Copy,
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
                        let bucket = buckets.remove(t).unwrap();
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
    let repetitions = hash_fns.repetitions() as u32;
    let mut current_repetition = 0u32;
    source(scope, "hashed source", move |capability| {
        let mut cap = Some(capability);
        let vecs = Arc::clone(&global_vecs);
        move |output| {
            let mut done = false;
            if let Some(cap) = cap.as_mut() {
                if !throttling_probe.less_than(cap.time()) {
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
    let repetitions = hash_fns.repetitions() as u32;
    let mut current_repetition = 0u32;
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

    source(scope, "hashed source", move |capability| {
        let mut cap = Some(capability);
        move |output| {
            let mut done = false;
            if let Some(cap) = cap.as_mut() {
                if !throttling_probe.less_than(cap.time()) {
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
) -> Stream<G, (H, (K, bool))>
where
    G: Scope<Timestamp = T>,
    T: Timestamp + Succ + ToStepId,
    D: Data + Sync + Send + Clone + Abomonation + Debug,
    F: LSHFunction<Input = D, Output = H> + Sync + Send + Clone + 'static,
    H: Data + Route + Debug + Send + Sync + Abomonation + Clone + Eq + Hash + Ord,
    K: Data + Debug + Send + Sync + Abomonation + Clone + Eq + Hash + Route,
    R: Rng + SeedableRng + Clone + ?Sized + 'static + Sync + Send,
{
    let worker: u64 = scope.index() as u64;
    let max_level = multilevel_hasher.max_level();
    let multilevel_hasher = Arc::clone(&multilevel_hasher);
    let multilevel_hasher_2 = Arc::clone(&multilevel_hasher);
    let global_vecs_2 = Arc::clone(&global_vecs);
    let logger = scope.danny_logger();
    let starting_level = multilevel_hasher.min_level();

    // Find the minimum among the levels
    let min_level = best_levels
        .broadcasted_min()
        .inspect(|m| info!("Min level is {}", m));

    let mut builder = OperatorBuilder::new("adaptive-source".to_owned(), best_levels.scope());
    let mut input_best_levels = builder.new_input(&best_levels, Pipeline);
    let mut input_min_level = builder.new_input(&min_level, Pipeline);
    let (mut output, output_stream) = builder.new_output();
    builder.build(move |mut capabilities| {
        let mut capability = Some(capabilities.pop().unwrap());

        let mut rep_start = Instant::now();
        let mut min_level: Option<usize> = None;
        let mut best_levels: HashMap<K, usize> = HashMap::new();
        // We start from the starting level of the hasher, even though the minimum level might be higher. 
        // This is to synchronize the left and right generators. They might have different
        // minimum levels, and this is the simplest way to ensure that they both are always in the
        // same round. Performance-wise it doesn't hurt much to run through some empty levels.
        let mut current_level = starting_level;
        let mut current_repetition = 0;
        let mut current_max_repetitions = 0;
        let mut done = false;
        let vecs = Arc::clone(&global_vecs_2);

        move |frontiers| {
            let mut best_levels_input =
                FrontieredInputHandle::new(&mut input_best_levels, &frontiers[0]);
            let mut min_level_input =
                FrontieredInputHandle::new(&mut input_min_level, &frontiers[1]);
            let mut output = output.activate();

            min_level_input.for_each(|_t, data| {
                assert!(min_level.is_none());
                assert!(data.len() == 1);
                min_level.replace(data[0]);
                current_max_repetitions = multilevel_hasher_2.repetitions_at_level(current_level);
            });
            best_levels_input.for_each(|_t, data| {
                let mut data = data.replace(Vec::new());
                for (key, level) in data.drain(..) {
                    best_levels.insert(key, level);
                }
            });
            if let Some(min_level) = min_level {
                if let Some(capability) = capability.as_mut() {
                    if true { // !throttling_probe.less_than(capability.time()) {
                        if worker == 0 {
                            info!(
                                "Level {}/{} repetition {}/{} (current memory {}, previous iter: {:?})",
                                current_level,
                                max_level,
                                current_repetition,
                                current_max_repetitions,
                                proc_mem!(),
                                Instant::now() - rep_start
                            );
                        }
                        log_event!(logger, LogEvent::Profile(
                            capability.time().to_step_id(), 0, "repetition".to_owned(), Instant::now() - rep_start));
                        rep_start = Instant::now();
                        if current_level >= min_level {
                            let _pg = ProfileGuard::new(
                                logger.clone(), capability.time().to_step_id(), 1, "hash_generation");
                            let start = Instant::now();

                            let mut session = output.session(&capability);
                            let mut cnt_best = 0;
                            let mut cnt_current = 0;
                            for (key, v) in vecs.iter_stripe(matrix, direction, worker) {
                                let this_best_level = *best_levels.get(key).expect("Missing best level for vector");
                                if current_level <= this_best_level {
                                    let h = multilevel_hasher.hash(v, current_level, current_repetition);
                                    if current_level == this_best_level {
                                        session.give((h, (key.clone(), true)));
                                        cnt_best += 1;
                                    } else {
                                        session.give((h, (key.clone(), false)));
                                        cnt_current += 1;
                                    }
                                }
                            }
                            log_event!(
                                logger,
                                LogEvent::AdaptiveBestGenerated(current_level, cnt_best)
                            );
                            log_event!(
                                logger,
                                LogEvent::AdaptiveCurrentGenerated(current_level, cnt_current)
                            );
                            info!(
                                "Emitted all {} + {} hashed values in {:?}",
                                cnt_best, cnt_current,
                                Instant::now() - start
                            );
                        }
                        capability.downgrade(&capability.time().succ());
                        current_repetition += 1;
                        if current_repetition >= current_max_repetitions {
                            current_level += 1;
                            done = current_level > max_level;
                            if !done {
                                current_repetition = 0;
                                current_max_repetitions =
                                    multilevel_hasher_2.repetitions_at_level(current_level)
                            }
                        }
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
) -> Stream<G, (H, (K, SV, bool))>
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

    let max_level = multilevel_hasher.max_level();
    let multilevel_hasher = Arc::clone(&multilevel_hasher);
    let multilevel_hasher_2 = Arc::clone(&multilevel_hasher);
    let global_vecs_2 = Arc::clone(&global_vecs);
    let logger = scope.danny_logger();
    let starting_level = multilevel_hasher.min_level();

    // Find the minimum among the levels
    let min_level = best_levels
        .broadcasted_min()
        .inspect(|m| info!("Min level is {}", m));

    let mut builder = OperatorBuilder::new("adaptive-source".to_owned(), best_levels.scope());
    let mut input_best_levels = builder.new_input(&best_levels, Pipeline);
    let mut input_min_level = builder.new_input(&min_level, Pipeline);
    let (mut output, output_stream) = builder.new_output();
    builder.build(move |mut capabilities| {
        let mut capability = Some(capabilities.pop().unwrap());

        let mut rep_start = Instant::now();
        let mut min_level: Option<usize> = None;
        let mut best_levels: HashMap<K, usize> = HashMap::new();
        // We start from the starting level of the hasher, even though the minimum level might be higher. 
        // This is to synchronize the left and right generators. They might have different
        // minimum levels, and this is the simplest way to ensure that they both are always in the
        // same round. Performance-wise it doesn't hurt much to run through some empty levels.
        let mut current_level = starting_level;
        let mut current_repetition = 0;
        let mut current_max_repetitions = 0;
        let mut done = false;
        let vecs = Arc::clone(&global_vecs_2);

        move |frontiers| {
            let mut best_levels_input =
                FrontieredInputHandle::new(&mut input_best_levels, &frontiers[0]);
            let mut min_level_input =
                FrontieredInputHandle::new(&mut input_min_level, &frontiers[1]);
            let mut output = output.activate();

            min_level_input.for_each(|_t, data| {
                assert!(min_level.is_none());
                assert!(data.len() == 1);
                min_level.replace(data[0]);
                current_max_repetitions = multilevel_hasher_2.repetitions_at_level(current_level);
            });
            best_levels_input.for_each(|_t, data| {
                let mut data = data.replace(Vec::new());
                for (key, level) in data.drain(..) {
                    best_levels.insert(key, level);
                }
            });
            if let Some(min_level) = min_level {
                if let Some(capability) = capability.as_mut() {
                    if !throttling_probe.less_than(capability.time()) {
                        if worker == 0 {
                            info!(
                                "Level {}/{} repetition {}/{} (current memory {}, previous iter: {:?})",
                                current_level,
                                max_level,
                                current_repetition,
                                current_max_repetitions,
                                proc_mem!(),
                                Instant::now() - rep_start
                            );
                        }
                        log_event!(logger, LogEvent::Profile(
                            capability.time().to_step_id(), 0, "repetition".to_owned(), Instant::now() - rep_start));
                        rep_start = Instant::now();
                        if current_level >= min_level {
                            let _pg = ProfileGuard::new(
                                logger.clone(), capability.time().to_step_id(), 1, "hash_generation");
                            let start = Instant::now();

                            let mut session = output.session(&capability);
                            let mut cnt_best = 0;
                            let mut cnt_current = 0;
                            for (key, v) in vecs.iter_stripe(matrix, direction, worker) {
                                let this_best_level = *best_levels.get(key).expect("Missing best level for vector");
                                if current_level <= this_best_level {
                                    let h = multilevel_hasher.hash(v, current_level, current_repetition);
                                    let s = sketches[key].clone();
                                    if current_level == this_best_level {
                                        session.give((h, (key.clone(), s, true)));
                                        cnt_best += 1;
                                    } else {
                                        session.give((h, (key.clone(), s, false)));
                                        cnt_current += 1;
                                    }
                                }
                            }
                            log_event!(
                                logger,
                                LogEvent::AdaptiveBestGenerated(current_level, cnt_best)
                            );
                            log_event!(
                                logger,
                                LogEvent::AdaptiveCurrentGenerated(current_level, cnt_current)
                            );
                            info!(
                                "Emitted all {} + {} hashed values in {:?}",
                                cnt_best, cnt_current,
                                Instant::now() - start
                            );
                        }
                        capability.downgrade(&capability.time().succ());
                        current_repetition += 1;
                        if current_repetition >= current_max_repetitions {
                            current_level += 1;
                            done = current_level > max_level;
                            if !done {
                                current_repetition = 0;
                                current_max_repetitions =
                                    multilevel_hasher_2.repetitions_at_level(current_level)
                            }
                        }
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

// The timestamp to be used in the product estimation
#[derive(Ord, PartialOrd, Eq, PartialEq, Clone, Copy, Debug, Abomonation, Default, Hash)]
struct CostTimestamp {
    level: usize,
    repetition: usize,
}

impl PathSummary<CostTimestamp> for CostTimestamp {
    #[inline]
    fn results_in(&self, product: &CostTimestamp) -> Option<CostTimestamp> {
        self.level.results_in(&product.level).and_then(|level| {
            self.repetition
                .results_in(&product.repetition)
                .map(|repetition| Self { level, repetition })
        })
    }
    #[inline]
    fn followed_by(&self, other: &CostTimestamp) -> Option<CostTimestamp> {
        self.level.followed_by(&other.level).and_then(|level| {
            self.repetition
                .followed_by(&other.repetition)
                .map(|repetition| Self { level, repetition })
        })
    }
}

impl timely::PartialOrder for CostTimestamp {
    fn less_equal(&self, other: &Self) -> bool {
        self <= other
    }
}

impl CostTimestamp {
    fn advance_level(&self) -> Self {
        Self {
            level: self.level + 1,
            repetition: 0,
        }
    }
    fn advance_repetition(&self) -> Self {
        Self {
            level: self.level,
            repetition: self.repetition + 1,
        }
    }
}

impl Timestamp for CostTimestamp {
    type Summary = CostTimestamp;
}

fn all_hashes_source<G, T, K, D, H, F>(
    scope: &G,
    vecs: Arc<ChunkedDataset<K, D>>,
    hasher: Arc<MultilevelHasher<D, H, F>>,
    matrix: MatrixDescription,
    direction: MatrixDirection,
) -> Stream<G, (H, K)>
where
    G: Scope<Timestamp = Product<T, CostTimestamp>>,
    T: Timestamp + Debug,
    D: ExchangeData + Debug,
    H: Data + Route + Debug + Send + Sync + Abomonation + Clone + Eq + Hash + Ord,
    K: Data + Debug + Send + Sync + Abomonation + Clone + Route,
    F: LSHFunction<Input = D, Output = H> + Send + Clone + Sync + 'static,
{
    let worker = scope.index() as u64;
    let vecs = Arc::clone(&vecs);
    let mut done = false;
    let mut current_level = hasher.min_level();
    let mut current_repetition = 0;
    let mut current_max_repetition = hasher.repetitions_at_level(current_level);
    source(&scope, "all-hashes", move |cap| {
        let hasher = Arc::clone(&hasher);
        let mut cap = Some(cap);
        if let Some(cap) = cap.as_mut() {
            while cap.time().inner.level < current_level {
                cap.downgrade(&Product::new(
                    cap.time().outer.clone(),
                    cap.time().inner.advance_level(),
                ));
            }
        }
        move |output| {
            if let Some(cap) = cap.as_mut() {
                assert!(cap.time().inner.level == current_level);
                assert!(cap.time().inner.repetition == current_repetition);
                let mut session = output.session(cap);
                for (k, v) in vecs.iter_stripe(matrix, direction, worker) {
                    let h = hasher.hash(v, current_level, current_repetition);
                    session.give((h, k.clone()));
                }

                current_repetition += 1;
                cap.downgrade(&Product::new(
                    cap.time().outer.clone(),
                    cap.time().inner.advance_repetition(),
                ));
                if current_repetition >= current_max_repetition {
                    current_level += 1;
                    if current_level > hasher.max_level() {
                        done = true;
                    } else {
                        current_repetition = 0;
                        current_max_repetition = hasher.repetitions_at_level(current_level);
                        cap.downgrade(&Product::new(
                            cap.time().outer.clone(),
                            cap.time().inner.advance_level(),
                        ));
                    }
                }
            }

            if done {
                cap = None;
            }
        }
    })
}

fn count_collisions<G, H, K>(
    left: &Stream<G, (H, K)>,
    right: &Stream<G, (H, K)>,
) -> (Stream<G, (K, usize)>, Stream<G, (K, usize)>)
where
    G: Scope,
    K: ExchangeData,
    H: ExchangeData + Ord + Copy + Route,
{
    let mut builder = OperatorBuilder::new("collision-counter".to_owned(), left.scope());
    let mut input_left = builder.new_input(&left, ExchangePact::new(|p: &(H, K)| p.0.route()));
    let mut input_right = builder.new_input(&right, ExchangePact::new(|p: &(H, K)| p.0.route()));
    let (mut output_left, stream_left) = builder.new_output();
    let (mut output_right, stream_right) = builder.new_output();

    let mut pool = BucketPool::default();
    let mut buckets = HashMap::new();
    let mut caps_left = HashMap::new();
    let mut caps_right = HashMap::new();

    builder.build(move |capabilities| {
        move |frontiers| {
            let mut input_left = FrontieredInputHandle::new(&mut input_left, &frontiers[0]);
            let mut input_right = FrontieredInputHandle::new(&mut input_right, &frontiers[0]);
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
            });

            let frontiers = &[input_left.frontier(), input_right.frontier()];
            for (time, buckets) in buckets.iter_mut() {
                if frontiers.iter().all(|f| !f.less_equal(time)) {
                    let mut session_left = output_left.session(&caps_left[time]);
                    let mut session_right = output_right.session(&caps_right[time]);
                    buckets.for_all_buckets(|lb, rb| {
                        for (_, l) in lb {
                            session_left.give((l.clone(), rb.len()));
                        }
                        for (_, r) in rb {
                            session_right.give((r.clone(), lb.len()));
                        }
                    });
                    buckets.clear();
                }
            }

            // Cleanup exhausted buckets, returning buckets to the pool,
            // so to reuse the allocated memory in the future
            let cleanup_times: Vec<G::Timestamp> = buckets
                .iter()
                .filter(|(_, b)| b.is_empty())
                .map(|p| p.0)
                .cloned()
                .collect();
            for t in cleanup_times.iter() {
                caps_left.remove(t).unwrap();
                caps_right.remove(t).unwrap();
                let bucket = buckets.remove(t).unwrap();
                // put it back into the pool
                pool.give_back(bucket);
            }
        }
    });
    (stream_left, stream_right)
}

/// A balance greater than 0.5 penalizes repetitions
fn select_minimum<GOutput, T, K, D, H, F>(
    counts: &Stream<Child<GOutput, Product<T, CostTimestamp>>, (K, usize)>,
    hasher: Arc<MultilevelHasher<D, H, F>>,
    balance: f64,
    iteration_cost: f64,
) -> Stream<GOutput, (K, usize)>
where
    GOutput: Scope<Timestamp = T>,
    T: Timestamp,
    D: ExchangeData + Debug,
    K: ExchangeData + Route + Hash + Eq,
    H: Data + Route + Debug + Send + Sync + Abomonation + Clone + Eq + Hash + Ord + Copy,
    F: LSHFunction<Input = D, Output = H> + Send + Clone + Sync + 'static,
{
    let logger = counts.scope().danny_logger();
    let mut partial_aggregator = HashMap::new();
    assert!(balance >= 0.0 && balance <= 1.0);
    counts
        .unary_frontier(Pipeline, "", move |_, _| {
            move |input, output| {
                input.for_each(|t, data| {
                    let mut data = data.replace(Vec::new());
                    let level = t.inner.level;
                    let time_entry = partial_aggregator
                        .entry(t.retain())
                        .or_insert_with(HashMap::new);
                    for (k, collisions) in data.drain(..) {
                        *time_entry.entry((k, level)).or_insert(0usize) += collisions;
                        // output.session(&t).give((k, (t.inner.level, collisions)));
                    }
                });
                for (t, aggregator) in partial_aggregator.iter_mut() {
                    if !input.frontier().less_equal(t) {
                        let mut session = output.session(&t);
                        for ((k, level), collisions) in aggregator.drain() {
                            session.give((k, (level, collisions)))
                        }
                    }
                }
                partial_aggregator.retain(|_, d| !d.is_empty());
            }
        })
        .leave()
        .aggregate(
            |_key, val: (usize, usize), agg: &mut HashMap<usize, usize>| {
                *agg.entry(val.0).or_insert(0usize) += val.1;
            },
            move |key, agg: HashMap<usize, usize>| {
                let mut min_work = std::f64::INFINITY;
                let mut best_level = 0;
                for (&level, &collisions) in agg.iter() {
                    let reps = hasher.repetitions_at_level(level);
                    let work = balance * iteration_cost * reps as f64
                        + (1.0 - balance) * collisions as f64;
                    if work < min_work {
                        min_work = work;
                        best_level = level;
                    }
                }
                (key, best_level)
            },
            Route::route,
        )
        .inspect_batch(move |_, d| {
            let mut levels = HashMap::new();
            for (_, level) in d.iter() {
                *levels.entry(level).or_insert(0) += 1;
            }
            for (&level, count) in levels {
                log_event!(logger, LogEvent::AdaptiveLevelHistogram(level, count));
            }
        })
}

pub fn find_best_level<G, T, K, D, H, F>(
    mut scope: G,
    left: Arc<ChunkedDataset<K, D>>,
    right: Arc<ChunkedDataset<K, D>>,
    hasher: Arc<MultilevelHasher<D, H, F>>,
    matrix: MatrixDescription,
    balance: f64,
) -> (Stream<G, (K, usize)>, Stream<G, (K, usize)>)
where
    G: Scope<Timestamp = T>,
    T: Timestamp + Succ + ToStepId + Debug,
    D: ExchangeData + Debug,
    K: Data + Debug + Send + Sync + Abomonation + Clone + Route + Hash + Eq,
    H: Data + Route + Debug + Send + Sync + Abomonation + Clone + Eq + Hash + Ord + Copy,
    F: LSHFunction<Input = D, Output = H> + Send + Clone + Sync + 'static,
{
    // 1. Generate all hash values for all repetitions
    // 2. In each level/repetition pair, build the buckets
    // 3. Accumulate the cost
    // 4. Accumulate the minimum cost in a distributed fashion

    // let iteration_cost = (left.global_n + right.global_n) as f64;
    let iteration_cost = 1.0;
    info!("The cost of every iteration is {}", iteration_cost);

    scope.scoped::<Product<T, CostTimestamp>, _, _>("scope_level", move |inner| {
        let l_hashes = all_hashes_source(
            inner,
            left,
            Arc::clone(&hasher),
            matrix,
            MatrixDirection::Rows,
        );
        let r_hashes = all_hashes_source(
            inner,
            right,
            Arc::clone(&hasher),
            matrix,
            MatrixDirection::Columns,
        );

        let (left_collisions, right_collisions) = count_collisions(&l_hashes, &r_hashes);
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
