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
use rand::Rng;
use serde::de::Deserialize;
use std::clone::Clone;
use std::collections::{HashMap, HashSet};
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
    H: Hash + Eq,
{
    left: HashMap<H, Vec<K>>,
    right: HashMap<H, Vec<K>>,
    cur_left: Vec<K>,
    cur_right: Vec<K>,
    cur_left_idx: usize,
    cur_right_idx: usize,
}

impl<H: Hash + Eq + Clone, K: Clone> PairGenerator<H, K> {
    pub fn new(left: HashMap<H, Vec<K>>, right: HashMap<H, Vec<K>>) -> Self {
        PairGenerator {
            left,
            right,
            cur_left: Vec::new(),
            cur_right: Vec::new(),
            cur_left_idx: 0,
            cur_right_idx: 0,
        }
    }

    pub fn done(&self) -> bool {
        self.left.is_empty()
            && self.right.is_empty()
            && self.cur_left.is_empty()
            && self.cur_right.is_empty()
    }
}

impl<H: Hash + Eq + Clone, K: Clone> Iterator for PairGenerator<H, K> {
    type Item = (K, K);

    fn next(&mut self) -> Option<(K, K)> {
        if self.done() {
            return None;
        }
        if self.cur_left.is_empty() {
            assert!(
                self.cur_right.is_empty(),
                "left vector is empty, but right one is not"
            );
            let k = self
                .left
                .keys()
                .cloned()
                .filter(|k| self.right.contains_key(k))
                .next();
            if k.is_none() {
                // No more common keys between left and right, cleanup and return
                self.left.clear();
                self.right.clear();
                return None;
            }
            let key = k.unwrap();
            self.cur_left = self
                .left
                .remove(&key)
                .expect("This left key should be present");
            self.cur_right = self
                .right
                .remove(&key)
                .expect("This right key should be present");
            self.cur_left_idx = 0;
            self.cur_right_idx = 0;
        }
        let left_elem = self.cur_left[self.cur_left_idx].clone();
        let right_elem = self.cur_right[self.cur_right_idx].clone();
        let pair = (left_elem, right_elem);
        // Move the index
        if self.cur_right_idx + 1 >= self.cur_right.len() {
            self.cur_right_idx = 0;
            if self.cur_left_idx + 1 >= self.cur_left.len() {
                // We are done for this key
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
    H: Data + Route + Debug + Send + Sync + Abomonation + Clone + Eq + Hash,
    K: Data + Debug + Send + Sync + Abomonation + Clone,
{
    fn bucket(&self, right: &Stream<G, (H, K)>) -> Stream<G, (K, K)>;
    fn bucket_batched(
        &self,
        right: &Stream<G, (H, K)>,
        throttling_probe: ProbeHandle<T>,
    ) -> Stream<G, (K, K)>;
}

impl<G, T, H, K> BucketStream<G, T, H, K> for Stream<G, (H, K)>
where
    G: Scope<Timestamp = T>,
    T: Timestamp + Succ,
    H: Data + Route + Debug + Send + Sync + Abomonation + Clone + Eq + Hash,
    K: Data + Debug + Send + Sync + Abomonation + Clone,
{
    fn bucket_batched(
        &self,
        right: &Stream<G, (H, K)>,
        throttling_probe: ProbeHandle<T>,
    ) -> Stream<G, (K, K)> {
        let mut left_buckets = HashMap::new();
        let mut right_buckets = HashMap::new();
        let mut generators = Vec::new();

        self.binary_frontier(
            &right,
            ExchangePact::new(|pair: &(H, K)| pair.0.route()),
            ExchangePact::new(|pair: &(H, K)| pair.0.route()),
            "bucket",
            move |_, _| {
                move |left_in, right_in, output| {
                    left_in.for_each(|t, d| {
                        debug!(
                            "Received batch of left messages for time {:?}:\n\t{:?}",
                            t.time(),
                            d.iter()
                        );
                        let rep_entry = left_buckets.entry(t.retain()).or_insert(HashMap::new());
                        let mut data = d.replace(Vec::new());
                        for (h, k) in data.drain(..) {
                            rep_entry.entry(h).or_insert(Vec::new()).push(k);
                        }
                    });
                    right_in.for_each(|t, d| {
                        debug!(
                            "Received batch of right messages for time {:?}:\n\t{:?}",
                            t.time(),
                            d.iter()
                        );
                        let rep_entry = right_buckets.entry(t.retain()).or_insert(HashMap::new());
                        let mut data = d.replace(Vec::new());
                        for (h, k) in data.drain(..) {
                            rep_entry.entry(h).or_insert(Vec::new()).push(k);
                        }
                    });
                    let frontiers = &[left_in.frontier(), right_in.frontier()];
                    let time = left_buckets
                        .keys()
                        .cloned()
                        .filter(|t| {
                            right_buckets.contains_key(t)
                                && frontiers.iter().all(|f| !f.less_equal(t))
                        })
                        .next();
                    match time {
                        Some(time) => {
                            // We got all data for the repetition at `time`
                            // Enqueue the pairs generator
                            let left_buckets = left_buckets.remove(&time).unwrap();
                            let right_buckets = right_buckets.remove(&time).unwrap();
                            let generator = PairGenerator::new(left_buckets, right_buckets);
                            generators.push((time.clone(), generator));
                        }
                        None => (),
                    }
                    for (time, generator) in generators.iter_mut() {
                        if !throttling_probe.less_than(time) {
                            // Emit some output pairs
                            let mut session = output.session(time);
                            for pair in generator.take(100000) {
                                session.give(pair);
                            }
                            time.downgrade(&time.time().succ());
                        }
                    }

                    // Cleanup exhausted generators
                    generators.retain(|(_, gen)| !gen.done());
                }
            },
        )
    }

    fn bucket(&self, right: &Stream<G, (H, K)>) -> Stream<G, (K, K)> {
        let mut left_buckets = HashMap::new();
        let mut right_buckets = HashMap::new();
        let logger = self.scope().danny_logger();

        self.binary_frontier(
            &right,
            ExchangePact::new(|pair: &(H, K)| pair.0.route()),
            ExchangePact::new(|pair: &(H, K)| pair.0.route()),
            "bucket",
            move |_, _| {
                move |left_in, right_in, output| {
                    left_in.for_each(|t, d| {
                        debug!(
                            "Received batch of left messages for time {:?}:\n\t{:?}",
                            t.time(),
                            d.iter()
                        );
                        let rep_entry = left_buckets.entry(t.retain()).or_insert(HashMap::new());
                        let mut data = d.replace(Vec::new());
                        for (h, k) in data.drain(..) {
                            rep_entry.entry(h).or_insert(Vec::new()).push(k);
                        }
                    });
                    right_in.for_each(|t, d| {
                        debug!(
                            "Received batch of right messages for time {:?}:\n\t{:?}",
                            t.time(),
                            d.iter()
                        );
                        let rep_entry = right_buckets.entry(t.retain()).or_insert(HashMap::new());
                        let mut data = d.replace(Vec::new());
                        for (h, k) in data.drain(..) {
                            rep_entry.entry(h).or_insert(Vec::new()).push(k);
                        }
                    });
                    let frontiers = &[left_in.frontier(), right_in.frontier()];
                    for (time, left_buckets) in left_buckets.iter_mut() {
                        if frontiers.iter().all(|f| !f.less_equal(time)) {
                            let mut session = output.session(&time);
                            // We got all data for the repetition at `time`
                            if let Some(right_buckets) = right_buckets.get_mut(time) {
                                let mut cnt = 0;
                                for (h, left_keys) in left_buckets.drain() {
                                    if let Some(right_keys) = right_buckets.get(&h) {
                                        for kl in left_keys.iter() {
                                            for kr in right_keys.iter() {
                                                //  do output
                                                let out_pair = (kl.clone(), kr.clone());
                                                session.give(out_pair);
                                                cnt += 1;
                                            }
                                        }
                                    }
                                }
                                log_event!(logger, LogEvent::GeneratedPairs(cnt));
                            }
                            // Remove the repetition from the right buckets
                            right_buckets.remove(time);
                        }
                    }
                    // Clean up the entries with empty buckets from the left (from the right we
                    // already did it)
                    left_buckets.retain(|_t, buckets| {
                        let to_keep = buckets.len() > 0;
                        to_keep
                    });
                    right_buckets.retain(|_t, buckets| {
                        let to_keep = buckets.len() > 0;
                        to_keep
                    });
                    // TODO: cleanup the duplicates filter when we are done with bucketing. We need
                    // this to free memory.
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
                log_event!(logger, LogEvent::GeneratedPairs(cnt));
            }
        })
    }
}

pub fn source_hashed<G, K, D, F, H>(
    scope: &G,
    global_vecs: Arc<RwLock<Arc<ChunkedDataset<K, D>>>>,
    hash_fns: LSHCollection<F, H>,
    matrix: MatrixDescription,
    direction: MatrixDirection,
    throttling_probe: ProbeHandle<G::Timestamp>,
) -> Stream<G, (H, K)>
where
    G: Scope<Timestamp = Product<u32, u32>>,
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
        let vecs = Arc::clone(&global_vecs.read().unwrap());
        move |output| {
            let mut done = false;
            if let Some(cap) = cap.as_mut() {
                if !throttling_probe.less_than(cap.time()) {
                    info!(
                        "Worker {} repetition {} (memory {})",
                        worker,
                        current_repetition,
                        proc_mem!()
                    );
                    let mut session = output.session(&cap);
                    let mut pl = ProgressLogger::new(
                        std::time::Duration::from_secs(60),
                        "hashes".to_owned(),
                        Some(vecs.stripe_len(&matrix, direction, worker) as u64),
                    );
                    for (k, v) in vecs.iter_stripe(&matrix, direction, worker) {
                        let h = hash_fns.hash(v, current_repetition as usize);
                        session.give((h, k.clone()));
                        pl.add(1);
                    }
                    pl.done();
                    current_repetition += 1;
                    cap.downgrade(&Product::new(current_repetition, 0));
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

pub fn source_hashed_sketched<G, K, D, F, S, H, V>(
    scope: &G,
    global_vecs: Arc<RwLock<Arc<ChunkedDataset<K, D>>>>,
    hash_fns: LSHCollection<F, H>,
    sketcher: S,
    matrix: MatrixDescription,
    direction: MatrixDirection,
    throttling_probe: ProbeHandle<G::Timestamp>,
) -> Stream<G, (H, (V, K))>
where
    G: Scope<Timestamp = Product<u32, u32>>,
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
    let vecs = Arc::clone(&global_vecs.read().unwrap());
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
                    info!(
                        "worker {} Repetition {} with sketches (memory {})",
                        worker,
                        current_repetition,
                        proc_mem!()
                    );
                    let mut session = output.session(&cap);
                    for (k, v) in vecs.iter_stripe(&matrix, direction, worker) {
                        let h = hash_fns.hash(v, current_repetition as usize);
                        let s = sketches.get(k).expect("Missing sketch");
                        session.give((h, (s.clone(), k.clone())));
                    }
                    current_repetition += 1;
                    cap.downgrade(&Product::new(current_repetition, 0));
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
        let mut left = HashMap::new();
        left.insert(0, vec![1, 2, 3]);
        left.insert(2, vec![1, 2, 3, 4]);
        left.insert(3, vec![3, 4]);
        let mut right = HashMap::new();
        right.insert(0, vec![10, 11, 12]);
        right.insert(1, vec![19]);
        right.insert(3, vec![30]);

        let mut expected = HashSet::new();
        for l in left.get(&0).unwrap() {
            for r in right.get(&0).unwrap() {
                expected.insert((l.clone(), r.clone()));
            }
        }
        for l in left.get(&3).unwrap() {
            for r in right.get(&3).unwrap() {
                expected.insert((l.clone(), r.clone()));
            }
        }

        let mut iterator = PairGenerator::new(left, right);
        let mut actual = HashSet::new();
        assert!(!iterator.done());
        while let Some(pair) = iterator.next() {
            actual.insert(pair);
        }

        assert_eq!(expected, actual);
        assert!(iterator.done());
    }
}
