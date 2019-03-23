use crate::config::Config;
use crate::dataset::*;
use crate::experiment::Experiment;
use crate::io::*;
use crate::logging::init_event_logging;
use crate::logging::*;
use crate::lsh::functions::*;
use crate::measure::InnerProduct;
use crate::operators::Route;
use crate::operators::*;
use crate::sketch::*;
use crate::types::*;
use abomonation::Abomonation;
use rand::distributions::{Distribution, Normal, Uniform};
use rand::{Rng, SeedableRng};
use serde::de::Deserialize;
use std::clone::Clone;
use std::collections::hash_map::Drain;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Barrier, Mutex, RwLock};
use std::thread;
use std::time::Instant;
use timely::dataflow::channels::pact::{Exchange as ExchangePact, Pipeline};
use timely::dataflow::operators::capture::{Event as TimelyEvent, Extract};
use timely::dataflow::operators::generic::source;
use timely::dataflow::operators::*;
use timely::dataflow::*;
use timely::order::Product;
use timely::progress::Timestamp;
use timely::Data;

pub struct PairGenerator<H, K>
where
    H: Hash + Eq + Ord,
{
    buckets: HashMap<H, (Vec<K>, Vec<K>)>,
    cur_left: Vec<K>,
    cur_right: Vec<K>,
    cur_left_idx: usize,
    cur_right_idx: usize,
}

impl<H: Hash + Eq + Clone + Ord, K: Clone> PairGenerator<H, K> {
    pub fn new(buckets: HashMap<H, (Vec<K>, Vec<K>)>) -> Self {
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
    T: Timestamp + Succ,
    H: Data + Route + Debug + Send + Sync + Abomonation + Clone + Eq + Hash + Ord,
    K: Data + Debug + Send + Sync + Abomonation + Clone,
{
    fn bucket(&self, right: &Stream<G, (H, K)>, batch_size: usize) -> Stream<G, (K, K)>;
    fn bucket_pred<P, R, O>(
        &self,
        right: &Stream<G, (H, K)>,
        pred: P,
        result: R,
        batch_size: usize,
    ) -> Stream<G, (O, O)>
    where
        O: Data,
        P: FnMut(&(K, K)) -> bool + 'static,
        R: Fn(K) -> O + 'static;
}

impl<G, T, H, K> BucketStream<G, T, H, K> for Stream<G, (H, K)>
where
    G: Scope<Timestamp = T>,
    T: Timestamp + Succ,
    H: Data + Route + Debug + Send + Sync + Abomonation + Clone + Eq + Hash + Ord,
    K: Data + Debug + Send + Sync + Abomonation + Clone,
{
    fn bucket(&self, right: &Stream<G, (H, K)>, batch_size: usize) -> Stream<G, (K, K)> {
        self.bucket_pred(right, |_| true, |k| k, batch_size)
    }

    fn bucket_pred<P, R, O>(
        &self,
        right: &Stream<G, (H, K)>,
        pred: P,
        result: R,
        batch_size: usize,
    ) -> Stream<G, (O, O)>
    where
        O: Data,
        P: FnMut(&(K, K)) -> bool + 'static,
        R: Fn(K) -> O + 'static,
    {
        let mut buckets = HashMap::new();
        let mut generators = Vec::new();
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
                        debug!(
                            "Received batch of left messages for time {:?}:\n\t{:?}",
                            t.time(),
                            d.iter()
                        );
                        let rep_entry = buckets.entry(t.retain()).or_insert_with(HashMap::new);
                        let mut data = d.replace(Vec::new());
                        log_event!(logger, LogEvent::ReceivedHashes(data.len()));
                        for (h, k) in data.drain(..) {
                            let bucket = rep_entry
                                .entry(h)
                                .or_insert_with(|| (Vec::new(), Vec::new()));
                            bucket.0.push(k);
                        }
                    });
                    right_in.for_each(|t, d| {
                        debug!(
                            "Received batch of right messages for time {:?}:\n\t{:?}",
                            t.time(),
                            d.iter()
                        );
                        let rep_entry = buckets.entry(t.retain()).or_insert_with(HashMap::new);
                        let mut data = d.replace(Vec::new());
                        log_event!(logger, LogEvent::ReceivedHashes(data.len()));
                        for (h, k) in data.drain(..) {
                            let bucket = rep_entry
                                .entry(h)
                                .or_insert_with(|| (Vec::new(), Vec::new()));
                            bucket.1.push(k);
                        }
                    });
                    let frontiers = &[left_in.frontier(), right_in.frontier()];
                    let time = buckets
                        .keys()
                        .cloned()
                        .find(|t| frontiers.iter().all(|f| !f.less_equal(t)));
                    if let Some(time) = time {
                        // We got all data for the repetition at `time`
                        // Enqueue the pairs generator
                        let buckets = buckets
                            .remove(&time)
                            .expect("Cannot find the required time in the right buckets");
                        let generator = PairGenerator::new(buckets);
                        generators.push((time.clone(), generator));
                    }
                    for (time, generator) in generators.iter_mut() {
                        // Emit some output pairs
                        let mut session = output.session(time);
                        let mut cnt = 0;
                        for (l, r) in generator.filter(|p| pred(p)).take(batch_size) {
                            session.give((result(l), result(r)));
                            cnt += 1;
                        }
                        log_event!(logger, LogEvent::GeneratedPairs(cnt));
                        time.downgrade(&time.time().succ());
                    }

                    // Cleanup exhausted generators
                    generators.retain(|(_, gen)| !gen.done());
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
    T: Timestamp + Succ,
    K: Data + Sync + Send + Clone + Abomonation + Debug,
    V: SketchEstimate + Data + Debug + Send + Sync + Abomonation + Clone + BitBasedSketch,
{
    fn filter_sketches(&self, sketch_predicate: SketchPredicate<V>) -> Stream<G, (K, K)> {
        let logger = self.scope().danny_logger();
        self.unary(Pipeline, "sketch filtering", move |_, _| {
            move |input, output| {
                let mut discarded = 0;
                let mut cnt = 0;
                input.for_each(|t, data| {
                    let t = t.retain();
                    let mut session = output.session(&t);
                    let mut data = data.replace(Vec::new());
                    for (p1, p2) in data.drain(..) {
                        if sketch_predicate.eval(&p1.0, &p2.0) {
                            cnt += 1;
                            session.give((p1.1, p2.1));
                        } else {
                            discarded += 1;
                        }
                    }
                });
                log_event!(logger, LogEvent::SketchDiscarded(discarded));
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
                    for (k, v) in vecs.iter_stripe(&matrix, direction, worker) {
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
    for (k, v) in vecs.iter_stripe(&matrix, direction, worker) {
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
                    for (k, v) in vecs.iter_stripe(&matrix, direction, worker) {
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
    use crate::measure::*;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    #[test]
    fn test_pair_iterator() {
        let mut buckets = HashMap::new();
        buckets.insert(0, (vec![1, 2, 3], vec![10, 11, 12]));
        buckets.insert(1, (vec![], vec![19]));
        buckets.insert(2, (vec![1, 2, 3, 4], vec![]));
        buckets.insert(3, (vec![3, 4], vec![30]));

        let mut expected = HashSet::new();
        for (k, (lks, rks)) in buckets.iter() {
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
                let p = n as f64 / vecs.stripe_len(&matrix, direction, worker) as f64;
                info!("Sampling with probability {}", p);
                for (k, v) in vecs.iter_stripe(&matrix, direction, worker) {
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

#[allow(clippy::too_many_arguments)]
pub fn source_hashed_adaptive<G, T, K, D, F, H, R>(
    scope: &G,
    global_vecs: Arc<ChunkedDataset<K, D>>,
    multilevel_hasher: Arc<MultilevelHasher<D, H, F>>,
    matrix: MatrixDescription,
    direction: MatrixDirection,
    n: usize,
    throttling_probe: ProbeHandle<G::Timestamp>,
    rng: R,
) -> Stream<G, (H, (K, LevelInfo))>
where
    G: Scope<Timestamp = T>,
    T: Timestamp + Succ,
    D: Data + Sync + Send + Clone + Abomonation + Debug,
    F: LSHFunction<Input = D, Output = H> + Sync + Send + Clone + 'static,
    H: Data + Route + Debug + Send + Sync + Abomonation + Clone + Eq + Hash,
    K: Data + Debug + Send + Sync + Abomonation + Clone + Eq + Hash + Route,
    R: Rng + SeedableRng + Clone + ?Sized + 'static + Sync + Send,
{
    let worker: u64 = scope.index() as u64;
    let max_level = multilevel_hasher.max_level();
    let multilevel_hasher = Arc::clone(&multilevel_hasher);
    let multilevel_hasher_2 = Arc::clone(&multilevel_hasher);
    let global_vecs_2 = Arc::clone(&global_vecs);

    let collisions = BestLevelEstimator::stream_collisions(
        scope,
        Arc::clone(&multilevel_hasher),
        Arc::clone(&global_vecs),
        n,
        matrix,
        direction,
        rng.clone(),
    );

    // First, find the best k value for all the points
    let best_levels = collisions.unary_frontier(Pipeline, "best-level-finder", move |_, _| {
        let vecs = Arc::clone(&global_vecs);
        let mut collisions = HashMap::new();
        move |input, output| {
            input.for_each(|t, data| {
                let mut data = data.replace(Vec::new());
                collisions
                    .entry(t.retain())
                    .or_insert_with(Vec::new)
                    .append(&mut data);
            });

            for (time, counts) in collisions.iter_mut() {
                if !input.frontier().less_equal(time) {
                    info!("Finding best level for each and every vector");
                    let estimator = BestLevelEstimator::from_counts(&multilevel_hasher, &counts);
                    info!("Built estimator: {}", estimator.describe());
                    let mut session = output.session(&time);
                    for (key, v) in vecs.iter_stripe(&matrix, direction, worker) {
                        let best_level = estimator.get_best_level(&multilevel_hasher, v);
                        session.give((key.clone(), best_level));
                    }
                    info!(
                        "Found best level for each and every vector, clearing counts (memory {})",
                        proc_mem!()
                    );
                    counts.clear();
                }
            }

            collisions.retain(|_, counts| !counts.is_empty());
        }
    });

    // Find the minimum among the levels
    let min_level = best_levels
        .map(|p| p.1)
        // Find the minimum in each worker
        .accumulate(std::usize::MAX, |min_level, data| {
            for &x in data.iter() {
                *min_level = std::cmp::min(*min_level, x);
            }
        })
        // Find the minimum of the minimum
        .exchange(|_| 0)
        .accumulate(std::usize::MAX, |min_level, data| {
            for &x in data.iter() {
                *min_level = std::cmp::min(*min_level, x);
            }
        })
        // Send the overall minimum to everybody
        .broadcast()
        .inspect(|d| info!("Minimum level {:?}", d));

    best_levels.binary_frontier(
        &min_level,
        Pipeline,
        Pipeline,
        "adaptive-source",
        move |default_cap, _| {
            let mut cap = Some(default_cap);
            let mut min_level: Option<usize> = None;
            let mut best_levels: HashMap<K, usize> = HashMap::new();
            let mut current_level = 0;
            let mut current_repetition = 0;
            let mut current_max_repetitions =
                multilevel_hasher_2.repetitions_at_level(current_level);
            let mut done = false;
            let vecs = Arc::clone(&global_vecs_2);
            move |best_levels_input, min_level_input, output| {
                min_level_input.for_each(|_t, data| {
                    assert!(min_level.is_none());
                    assert!(data.len() == 1);
                    min_level.replace(data[0]);
                    current_level = data[0];
                });
                best_levels_input.for_each(|_t, data| {
                    let mut data = data.replace(Vec::new());
                    for (key, level) in data.drain(..) {
                        best_levels.insert(key, level);
                    }
                });
                if let Some(_min_level) = min_level {
                    if let Some(cap) = cap.as_mut() {
                        if !throttling_probe.less_than(cap.time()) {
                            info!(
                                "Level {}/{} repetition {} (current memory {})",
                                current_level,
                                max_level,
                                current_repetition,
                                proc_mem!()
                            );
                            let mut session = output.session(&cap);
                            for (key, v) in vecs.iter_stripe(&matrix, direction, worker) {
                                let this_best_level = best_levels[key];
                                if current_level <= this_best_level {
                                    let h = multilevel_hasher_2.hash(
                                        v,
                                        current_level,
                                        current_repetition,
                                    );
                                    session.give((
                                        h,
                                        (
                                            key.clone(),
                                            LevelInfo::new(this_best_level, current_level),
                                        ),
                                    ));
                                }
                            }
                            info!("Emitted all pairs (current memory {})", proc_mem!());
                            cap.downgrade(&cap.time().succ());
                            current_repetition += 1;
                            if current_repetition >= current_max_repetitions {
                                current_level += 1;
                                done = current_level >= max_level;
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
                    cap = None;
                }
            }
        },
    )
}
