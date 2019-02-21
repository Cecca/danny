use crate::config::Config;
use crate::experiment::Experiment;
use crate::io::*;
use crate::logging::init_event_logging;
use crate::logging::*;
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
use timely::dataflow::operators::*;
use timely::dataflow::*;
use timely::order::Product;
use timely::progress::Timestamp;
use timely::Data;

pub trait LSHFunction {
    type Input;
    type Output;
    fn hash(&self, v: &Self::Input) -> Self::Output;
    fn probability_at_range(range: f64) -> f64;

    fn repetitions_at_range(range: f64, k: usize) -> usize {
        let p = Self::probability_at_range(range);
        let reps = (1_f64 / p).powi(k as i32).ceil() as usize;
        info!("Probability at range {} is {} (reps: {})", range, p, reps);
        reps
    }
}

#[derive(Clone)]
pub struct LSHCollection<F, H>
where
    F: LSHFunction<Output = H> + Clone,
    H: Clone,
{
    functions: Vec<F>,
}

impl<F, H> LSHCollection<F, H>
where
    F: LSHFunction<Output = H> + Clone,
    H: Clone,
{
    pub fn for_each_hash<L>(&self, v: &F::Input, mut logic: L) -> ()
    where
        L: FnMut(usize, F::Output),
    {
        let mut repetition = 0;
        self.functions.iter().for_each(|f| {
            let h = f.hash(v);
            logic(repetition, h);
            repetition += 1;
        });
    }

    pub fn hash(&self, v: &F::Input, repetition: usize) -> F::Output {
        self.functions[repetition].hash(v)
    }

    pub fn repetitions(&self) -> usize {
        self.functions.len()
    }
}

#[derive(Clone)]
pub struct Hyperplane {
    k: usize,
    planes: Vec<UnitNormVector>,
}

impl Hyperplane {
    pub fn new<R>(k: usize, dim: usize, rng: &mut R) -> Hyperplane
    where
        R: Rng + ?Sized,
    {
        assert!(
            k < 32,
            "Only k<32 is supported so to be able to pack hases in words"
        );
        let mut planes = Vec::with_capacity(k);
        let gaussian = Normal::new(0.0, 1.0);
        for _ in 0..k {
            let mut plane = Vec::with_capacity(dim);
            for _ in 0..dim {
                plane.push(gaussian.sample(rng) as f32);
            }
            let plane = UnitNormVector::new(plane);
            planes.push(plane);
        }
        Hyperplane { k, planes }
    }

    pub fn collection<R>(
        k: usize,
        repetitions: usize,
        dim: usize,
        rng: &mut R,
    ) -> LSHCollection<Hyperplane, u32>
    where
        R: Rng + ?Sized,
    {
        let mut functions = Vec::with_capacity(repetitions);
        for _ in 0..repetitions {
            functions.push(Hyperplane::new(k, dim, rng));
        }
        LSHCollection { functions }
    }
}

impl LSHFunction for Hyperplane {
    type Input = UnitNormVector;
    type Output = u32;

    fn hash(&self, v: &UnitNormVector) -> u32 {
        let mut h = 0u32;
        for plane in self.planes.iter() {
            if InnerProduct::inner_product(plane, v) >= 0_f64 {
                h = (h << 1) | 1;
            } else {
                h = h << 1;
            }
        }
        h
    }

    fn probability_at_range(range: f64) -> f64 {
        1_f64 - range.acos() / std::f64::consts::PI
    }
}

/// Produces 64 bit hashes of 32 bits values
#[derive(Clone)]
pub struct TabulatedHasher {
    table0: [u64; 256],
    table1: [u64; 256],
    table2: [u64; 256],
    table3: [u64; 256],
}

impl TabulatedHasher {
    pub fn new<R>(rng: &mut R) -> TabulatedHasher
    where
        R: Rng + ?Sized,
    {
        let uniform = Uniform::new(0u64, std::u64::MAX);
        let mut table0 = [0_u64; 256];
        let mut table1 = [0_u64; 256];
        let mut table2 = [0_u64; 256];
        let mut table3 = [0_u64; 256];
        for i in 0..256 {
            table0[i] = uniform.sample(rng);
        }
        for i in 0..256 {
            table1[i] = uniform.sample(rng);
        }
        for i in 0..256 {
            table2[i] = uniform.sample(rng);
        }
        for i in 0..256 {
            table3[i] = uniform.sample(rng);
        }
        TabulatedHasher {
            table0,
            table1,
            table2,
            table3,
        }
    }

    pub fn hash(&self, x: u32) -> u64 {
        let mut h = self.table0[(x & 0xFF) as usize];
        h ^= self.table1[((x >> 8) & 0xFF) as usize];
        h ^= self.table2[((x >> 16) & 0xFF) as usize];
        h ^= self.table3[((x >> 24) & 0xFF) as usize];
        h
    }
}

#[derive(Clone)]
pub struct MinHash {
    k: usize,
    hashers: Vec<TabulatedHasher>,
    coeffs: Vec<u64>,
}

impl MinHash {
    fn new<R>(k: usize, rng: &mut R) -> MinHash
    where
        R: Rng + ?Sized,
    {
        let mut hashers = Vec::with_capacity(k);
        let uniform = Uniform::new(0u64, std::u64::MAX);
        let mut coeffs = Vec::new();
        for _ in 0..k {
            hashers.push(TabulatedHasher::new(rng));
            coeffs.push(uniform.sample(rng));
        }
        MinHash { k, hashers, coeffs }
    }

    pub fn collection<R>(k: usize, repetitions: usize, rng: &mut R) -> LSHCollection<MinHash, u32>
    where
        R: Rng + ?Sized,
    {
        let mut functions = Vec::with_capacity(repetitions);
        for _ in 0..repetitions {
            functions.push(MinHash::new(k, rng));
        }
        LSHCollection { functions }
    }
}

impl LSHFunction for MinHash {
    type Input = BagOfWords;
    type Output = u32;

    fn hash(&self, v: &BagOfWords) -> u32 {
        let mut h = 0u64;
        for (hasher, coeff) in self.hashers.iter().zip(self.coeffs.iter()) {
            let min_w = v.words().iter().map(|w| hasher.hash(*w)).min().unwrap();
            h = h.wrapping_add(coeff.wrapping_mul(min_w));
        }
        (h >> 32) as u32
    }

    fn probability_at_range(range: f64) -> f64 {
        range
    }
}

pub trait HashStream<G, K, D>
where
    G: Scope,
    K: Data + Debug,
    D: Data + Debug,
{
    fn hash_buffered<F, H>(&self, hash_coll: &LSHCollection<F, H>) -> Stream<G, (H, K)>
    where
        F: LSHFunction<Input = D, Output = H> + Clone + 'static,
        H: Data + Clone;

    fn hash<F, H>(&self, hash_coll: &LSHCollection<F, H>) -> Stream<G, (H, K)>
    where
        F: LSHFunction<Input = D, Output = H> + Clone + 'static,
        H: Data + Clone;
}

impl<G, T, K, D> HashStream<G, K, D> for Stream<G, (K, D)>
where
    G: Scope<Timestamp = T>,
    T: Succ + Clone + Timestamp,
    K: Data + Debug,
    D: Data + Debug,
{
    fn hash_buffered<F, H>(&self, hash_coll: &LSHCollection<F, H>) -> Stream<G, (H, K)>
    where
        F: LSHFunction<Input = D, Output = H> + Clone + 'static,
        H: Data + Clone,
    {
        let repetitions = hash_coll.repetitions();
        let mut probe1 = ProbeHandle::new();
        let probe2 = probe1.clone();
        let hash_coll = hash_coll.clone();
        let mut stash: Vec<Option<(Capability<G::Timestamp>, Vec<(K, D)>)>> =
            vec![None; repetitions + 1]; // The last cell is always None: it is just to simplify the code below
        self.unary(Pipeline, "hash", move |_, _| {
            move |input, output| {
                let hash_coll = hash_coll.clone();
                // Accumulate the vectors in the stash area. This stash vector has an entry per
                // repetition, containing the vectors that need to be hashed for that repetition.
                // A given vector starts at index 0 and then moves forward to the next cells,
                // dropping out at the end of the vector. When a cell has no more associated
                // vectors, its associated capability is dropped.
                input.for_each(|t, data| {
                    let mut data = data.replace(Vec::new());
                    stash[0]
                        .get_or_insert((t.retain(), Vec::new()))
                        .1
                        .append(&mut data)
                });

                for i in 0..repetitions {
                    let mut to_move: Vec<(K, D)> =
                        if let Some((time, data)) = stash[i].iter_mut().next() {
                            // TODO: maybe here there is room for optimization
                            if !probe2.less_than(time.time()) {
                                let mut session = output.session(&time);
                                let mut cnt = 0;
                                for (k, v) in data.iter() {
                                    let h = hash_coll.hash(&v, i);
                                    session.give((h, k.clone()));
                                }
                                debug!("Emitted {} hashes for repetition {}", cnt, i);
                                data.drain(..).collect()
                            } else {
                                Vec::new()
                            }
                        } else {
                            Vec::new()
                        };
                    if to_move.len() > 0 && i + 1 < repetitions {
                        let time = stash[i]
                            .as_ref()
                            .expect(
                                "At this point we should be sure that there are vectors to move",
                            )
                            .0
                            .clone();
                        let time = time.delayed(&(time.time().succ()));
                        stash[i + 1]
                            .get_or_insert((time, Vec::new()))
                            .1
                            .append(&mut to_move);
                    }
                }

                // Remove items whose time is past the last repetition
                for entry in stash.iter_mut() {
                    *entry = entry.take().filter(|pair| pair.1.len() > 0);
                }
            }
        })
        .probe_with(&mut probe1)
    }

    fn hash<F, H>(&self, hash_coll: &LSHCollection<F, H>) -> Stream<G, (H, K)>
    where
        F: LSHFunction<Input = D, Output = H> + Clone + 'static,
        H: Data + Clone,
    {
        let hash_coll = hash_coll.clone();
        self.unary(Pipeline, "hash", move |_, _| {
            move |input, output| {
                let hash_coll = hash_coll.clone();
                input.for_each(move |t, data| {
                    let t = t.retain();
                    let mut data = data.replace(Vec::new());
                    for (id, v) in data.drain(..) {
                        hash_coll.for_each_hash(&v, |rep, h| {
                            let t_out = t.delayed(&(t.time().succs(rep)));
                            debug!("Outputting hashes at time {:?} ", t_out.time());
                            output.session(&t_out).give((h, id.clone()));
                        });
                    }
                });
            }
        })
    }
}

pub trait HashSketchStream<G, K, D, S, V>
where
    G: Scope,
    K: Data + Debug,
    D: Data + Debug,
    S: Sketcher<Input = D, Output = V>,
    V: Data + SketchEstimate + Clone,
{
    fn hash_sketch_buffered<F, H>(
        &self,
        hash_coll: &LSHCollection<F, H>,
        sketcher: &S,
    ) -> Stream<G, (H, (V, K))>
    where
        F: LSHFunction<Input = D, Output = H> + Clone + 'static,
        H: Data + Clone;
}

impl<G, T, K, D, S, V> HashSketchStream<G, K, D, S, V> for Stream<G, (K, D)>
where
    G: Scope<Timestamp = T>,
    T: Succ + Clone + Timestamp,
    K: Data + Debug,
    D: Data + Debug,
    S: Sketcher<Input = D, Output = V> + Clone + 'static,
    V: Data + SketchEstimate + Clone,
{
    fn hash_sketch_buffered<F, H>(
        &self,
        hash_coll: &LSHCollection<F, H>,
        sketcher: &S,
    ) -> Stream<G, (H, (V, K))>
    where
        F: LSHFunction<Input = D, Output = H> + Clone + 'static,
        H: Data + Clone,
    {
        let sketcher = sketcher.clone();
        let repetitions = hash_coll.repetitions();
        let mut probe1 = ProbeHandle::new();
        let probe2 = probe1.clone();
        let hash_coll = hash_coll.clone();
        let mut stash: Vec<Option<(Capability<G::Timestamp>, Vec<(K, V, D)>)>> =
            vec![None; repetitions + 1]; // The last cell is always None: it is just to simplify the code below
        self.unary(Pipeline, "hash", move |_, _| {
            move |input, output| {
                let hash_coll = hash_coll.clone();
                // Accumulate the vectors in the stash area. This stash vector has an entry per
                // repetition, containing the vectors that need to be hashed for that repetition.
                // A given vector starts at index 0 and then moves forward to the next cells,
                // dropping out at the end of the vector. When a cell has no more associated
                // vectors, its associated capability is dropped.
                //
                // While we are at it, we also sketch each vector
                input.for_each(|t, data| {
                    let mut data = data.replace(Vec::new());
                    let mut data2 = Vec::with_capacity(data.len());
                    for (k, v) in data.drain(..) {
                        let s = sketcher.sketch(&v);
                        data2.push((k, s, v));
                    }
                    stash[0]
                        .get_or_insert((t.retain(), Vec::new()))
                        .1
                        .append(&mut data2)
                });

                for i in 0..repetitions {
                    let mut to_move: Vec<(K, V, D)> =
                        if let Some((time, data)) = stash[i].iter_mut().next() {
                            // TODO: maybe here there is room for optimization
                            if !probe2.less_than(time.time()) {
                                let mut session = output.session(&time);
                                let mut cnt = 0;
                                for (k, s, v) in data.iter() {
                                    let h = hash_coll.hash(&v, i);
                                    session.give((h, (s.clone(), k.clone())));
                                }
                                debug!("Emitted {} hashes for repetition {}", cnt, i);
                                data.drain(..).collect()
                            } else {
                                Vec::new()
                            }
                        } else {
                            Vec::new()
                        };
                    if to_move.len() > 0 && i + 1 < repetitions {
                        let time = stash[i]
                            .as_ref()
                            .expect(
                                "At this point we should be sure that there are vectors to move",
                            )
                            .0
                            .clone();
                        let time = time.delayed(&(time.time().succ()));
                        stash[i + 1]
                            .get_or_insert((time, Vec::new()))
                            .1
                            .append(&mut to_move);
                    }
                }

                // Remove items whose time is past the last repetition
                for entry in stash.iter_mut() {
                    *entry = entry.take().filter(|pair| pair.1.len() > 0);
                }
            }
        })
        .probe_with(&mut probe1)
    }
}

struct PairGenerator<H, K>
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
    fn new(left: HashMap<H, Vec<K>>, right: HashMap<H, Vec<K>>) -> Self {
        PairGenerator {
            left,
            right,
            cur_left: Vec::new(),
            cur_right: Vec::new(),
            cur_left_idx: 0,
            cur_right_idx: 0,
        }
    }

    fn done(&self) -> bool {
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

trait BucketStream<G, H, K>
where
    G: Scope,
    H: Data + Route + Debug + Send + Sync + Abomonation + Clone + Eq + Hash,
    K: Data + Debug + Send + Sync + Abomonation + Clone,
{
    fn bucket(&self, right: &Stream<G, (H, K)>) -> Stream<G, (K, K)>;
    fn bucket_batched(&self, right: &Stream<G, (H, K)>) -> Stream<G, (K, K)>;
}

impl<G, H, K> BucketStream<G, H, K> for Stream<G, (H, K)>
where
    G: Scope,
    H: Data + Route + Debug + Send + Sync + Abomonation + Clone + Eq + Hash,
    K: Data + Debug + Send + Sync + Abomonation + Clone,
    // K: Data + Debug + Send + Sync + Abomonation + Clone + Eq + Hash,
{
    fn bucket_batched(&self, right: &Stream<G, (H, K)>) -> Stream<G, (K, K)> {
        let mut left_buckets = HashMap::new();
        let mut right_buckets = HashMap::new();
        let mut generators = HashMap::new();

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
                            generators.insert(time.clone(), generator);
                        }
                        None => (),
                    }
                    // Emit some output pairs
                    for (time, mut generator) in generators.iter_mut() {
                        let mut session = output.session(time);
                        for pair in generator.take(10000) {
                            session.give(pair);
                        }
                    }

                    // Cleanup exhausted generators
                    generators.retain(|_, gen| !gen.done());
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

trait CollidingPairs<G, T, K, D, F, H>
where
    G: Scope<Timestamp = T>,
    T: Timestamp + Succ,
    K: Data + Sync + Send + Clone + Abomonation + Debug + Route,
    D: Data + Sync + Send + Clone + Abomonation + Debug,
    F: LSHFunction<Input = D, Output = H> + Sync + Send + Clone + 'static,
    H: Data + Sync + Send + Clone + Abomonation + Debug + Route + Eq + Hash,
{
    fn colliding_pairs(
        &self,
        right: &Stream<G, (K, D)>,
        hash_coll: &LSHCollection<F, H>,
    ) -> Stream<G, (K, K)>;

    fn colliding_pairs_sketch<S, V>(
        &self,
        right: &Stream<G, (K, D)>,
        hash_coll: &LSHCollection<F, H>,
        sketch: &S,
        sketch_predicate: SketchPredicate<V>,
    ) -> Stream<G, (K, K)>
    where
        S: Sketcher<Input = D, Output = V> + Clone + 'static,
        V: SketchEstimate + Data + Debug + Send + Sync + Abomonation + Clone + BitBasedSketch;
}

impl<G, T, K, D, F, H> CollidingPairs<G, T, K, D, F, H> for Stream<G, (K, D)>
where
    G: Scope<Timestamp = T>,
    T: Timestamp + Succ,
    D: Data + Sync + Send + Clone + Abomonation + Debug,
    F: LSHFunction<Input = D, Output = H> + Sync + Send + Clone + 'static,
    H: Data + Route + Debug + Send + Sync + Abomonation + Clone + Eq + Hash,
    K: Data + Debug + Send + Sync + Abomonation + Clone + Eq + Hash + Route,
{
    fn colliding_pairs(
        &self,
        right: &Stream<G, (K, D)>,
        hash_coll: &LSHCollection<F, H>,
    ) -> Stream<G, (K, K)> {
        let logger = self.scope().danny_logger();
        self.scope()
            .scoped::<Product<_, u32>, _, _>("candidate generation", |inner| {
                // TODO: Reconsider if you have to buffer the repetitions: maybe batching the pairs
                // is sufficient.
                let left_hashes = self.enter(inner).hash_buffered(&hash_coll);
                let right_hashes = right.enter(inner).hash_buffered(&hash_coll);
                let candidate_pairs =
                    left_hashes
                        .bucket_batched(&right_hashes)
                        .inspect_batch(move |_, data| {
                            log_event!(logger, LogEvent::GeneratedPairs(data.len()));
                        });
                candidate_pairs.leave()
            })
    }

    fn colliding_pairs_sketch<S, V>(
        &self,
        right: &Stream<G, (K, D)>,
        hash_coll: &LSHCollection<F, H>,
        sketcher: &S,
        sketch_predicate: SketchPredicate<V>,
    ) -> Stream<G, (K, K)>
    where
        S: Sketcher<Input = D, Output = V> + Clone + 'static,
        V: SketchEstimate + Data + Debug + Send + Sync + Abomonation + Clone + BitBasedSketch,
    {
        let logger = self.scope().danny_logger();
        let sketch_predicate = sketch_predicate;
        self.scope()
            .scoped::<Product<_, u32>, _, _>("candidate generation", |inner| {
                // TODO: Reconsider if you have to buffer the repetitions: maybe batching the pairs
                // is sufficient.
                let left_hashes = self.enter(inner).hash_sketch_buffered(&hash_coll, sketcher);
                let right_hashes = right
                    .enter(inner)
                    .hash_sketch_buffered(&hash_coll, sketcher);
                let candidate_pairs = left_hashes.bucket_batched(&right_hashes).unary(
                    Pipeline,
                    "sketch filtering",
                    move |_, _| {
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
                    },
                );
                candidate_pairs.leave()
            })
    }
}

pub fn load_global_vecs<D>(
    left_path_main: String,
    right_path_main: String,
    config: &Config,
) -> (
    Arc<RwLock<Arc<HashMap<u32, D>>>>,
    Arc<RwLock<Arc<HashMap<u32, D>>>>,
    Arc<Mutex<Sender<(u8, u8)>>>,
    Arc<Barrier>,
    std::thread::JoinHandle<()>,
)
where
    for<'de> D: Deserialize<'de> + ReadBinaryFile + Sync + Send + Clone + 'static,
{
    // These two maps hold the vectors that need to be accessed by all threads in this machine.
    let global_left_write: Arc<RwLock<Arc<HashMap<u32, D>>>> =
        Arc::new(RwLock::new(Arc::new(HashMap::new())));
    let global_right_write: Arc<RwLock<Arc<HashMap<u32, D>>>> =
        Arc::new(RwLock::new(Arc::new(HashMap::new())));
    let global_left_read = global_left_write.clone();
    let global_right_read = global_right_write.clone();

    let (send_coords, recv_coords) = channel();
    let send_coords = Arc::new(Mutex::new(send_coords));

    let total_workers = config.get_total_workers();
    let worker_threads = config.get_threads();
    let waiting_threads = worker_threads + 1;
    let io_barrier = Arc::new(std::sync::Barrier::new(waiting_threads));
    let io_barrier_reader = io_barrier.clone();

    let reader_handle = thread::spawn(move || {
        let start = Instant::now();
        let matrix_desc = MatrixDescription::for_workers(total_workers);
        info!(
            "Data partitioned according to {} x {}",
            matrix_desc.rows, matrix_desc.columns
        );
        let mut row_set = HashSet::new();
        let mut column_set = HashSet::new();
        let mut global_left = global_left_write.write().unwrap();
        let global_left =
            Arc::get_mut(&mut global_left).expect("This should be the only reference");
        let mut global_right = global_right_write.write().unwrap();
        let global_right =
            Arc::get_mut(&mut global_right).expect("This should be the only reference");
        debug!("Getting the pairs on the main thread");
        for _ in 0..worker_threads {
            // We know we will receive exactly that many messages
            let (i, j): (u8, u8) = recv_coords.recv().expect("Problem receiving coordinate");
            row_set.insert(i);
            column_set.insert(j);
        }
        debug!("This machine is responsible for rows: {:?}", row_set);
        debug!("This machine is responsible for columns: {:?}", column_set);
        debug!("Memory before reading data {}", proc_mem!());
        ReadBinaryFile::read_binary(
            left_path_main.into(),
            |l| row_set.contains(&((l % matrix_desc.rows as usize) as u8)),
            |c, v| {
                global_left.insert(c as u32, v);
            },
        );
        ReadBinaryFile::read_binary(
            right_path_main.into(),
            |l| column_set.contains(&((l % matrix_desc.columns as usize) as u8)),
            |c, v| {
                global_right.insert(c as u32, v);
            },
        );
        debug!("Memory after reading data {}", proc_mem!());
        let end = Instant::now();
        let elapsed = end - start;
        info!(
            "Loaded {} left vectors and {} right vectors (in {:?})",
            global_left.len(),
            global_right.len(),
            elapsed
        );

        debug!("Reader is calling wait on the main barrier");
        io_barrier_reader.wait();
        debug!("After reader barrier!");
    });

    (
        global_left_read,
        global_right_read,
        send_coords,
        io_barrier,
        reader_handle,
    )
}

pub fn fixed_param_lsh<D, F, H, O, S, V>(
    left_path: &String,
    right_path: &String,
    hash_fn: LSHCollection<H, O>,
    sketcher_pair: Option<(S, SketchPredicate<V>)>,
    sim_pred: F,
    config: &Config,
    experiment: &mut Experiment,
) -> usize
where
    for<'de> D:
        ReadBinaryFile + Deserialize<'de> + Data + Sync + Send + Clone + Abomonation + Debug,
    F: Fn(&D, &D) -> bool + Send + Clone + Sync + 'static,
    H: LSHFunction<Input = D, Output = O> + Sync + Send + Clone + 'static,
    O: Data + Sync + Send + Clone + Abomonation + Debug + Route + Eq + Hash,
    S: Sketcher<Input = D, Output = V> + Send + Sync + Clone + 'static,
    V: Data + Debug + Sync + Send + Clone + Abomonation + SketchEstimate + BitBasedSketch,
{
    let timely_builder = config.get_timely_builder();
    // This channel is used to get the results
    let (output_send_ch, recv) = channel();
    let output_send_ch = Arc::new(Mutex::new(output_send_ch));

    let (send_exec_summary, recv_exec_summary) = channel();
    let send_exec_summary = Arc::new(Mutex::new(send_exec_summary));

    let left_path = left_path.clone();
    let right_path = right_path.clone();
    let left_path_final = left_path.clone();
    let right_path_final = right_path.clone();
    let repetitions = hash_fn.repetitions();

    let (global_left_read, global_right_read, send_coords, io_barrier, reader_handle) =
        load_global_vecs(left_path.clone(), right_path.clone(), config);

    timely::execute::execute_from(timely_builder.0, timely_builder.1, move |mut worker| {
        let execution_summary = init_event_logging(&worker);
        let output_send_ch = output_send_ch.lock().unwrap().clone();
        let sim_pred = sim_pred.clone();
        let index = worker.index();
        let peers = worker.peers() as u64;

        let send_coords = send_coords.lock().unwrap().clone();
        let matrix_coords =
            MatrixDescription::for_workers(peers as usize).row_major_to_pair(index as u64);
        debug!("Sending coordinates {:?}", matrix_coords);
        send_coords
            .send(matrix_coords)
            .expect("Error while pushing into coordinates channel");
        debug!("Waiting for input to be loaded");
        io_barrier.wait();
        debug!("After worker barrier!");

        let global_left_read = global_left_read.clone();
        let global_right_read = global_right_read.clone();

        let hash_fn = hash_fn.clone();
        let sketcher_pair = sketcher_pair.clone();
        let sketcher_pair_1 = sketcher_pair.clone();
        debug!(
            "Started worker {}/{} (machine memory used {})",
            index,
            peers,
            proc_mem!()
        );
        let (mut left, mut right, probe) = worker.dataflow::<u32, _, _>(move |scope| {
            let logger = scope.danny_logger();
            let global_left = Arc::clone(&global_left_read.read().unwrap());
            let global_right = Arc::clone(&global_right_read.read().unwrap());

            let (left_in, left_hashes) = scope.new_input::<(O, (V, u32))>();
            let (right_in, right_hashes) = scope.new_input::<(O, (V, u32))>();
            let mut probe = ProbeHandle::new();
            // let hash_fn = hash_fn;
            let sketcher_pair = sketcher_pair;
            let (_sketcher, sketch_predicate) = sketcher_pair.unwrap();

            let matrix = MatrixDescription::for_workers(peers as usize);
            // let candidates = match sketcher_pair {
            //     Some((sketcher, sketch_predicate)) => left_stream.colliding_pairs_sketch(
            //         &right_stream,
            //         &hash_fn,
            //         &sketcher,
            //         sketch_predicate,
            //     ),
            //     None => left_stream.colliding_pairs(&right_stream, &hash_fn),
            // };
            let candidates = left_hashes.bucket_batched(&right_hashes).unary(
                Pipeline,
                "sketch filtering",
                move |_, _| {
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
                },
            );
            candidates
                .pair_route(matrix)
                .map(|pair| pair.1)
                .approximate_distinct(1 << 30, 0.05, 123123123)
                .filter(move |(lk, rk)| {
                    let lv = global_left.get(lk).unwrap();
                    let rv = global_right.get(rk).unwrap();
                    sim_pred(lv, rv)
                })
                .count()
                .exchange(|_| 0)
                .probe_with(&mut probe)
                .capture_into(output_send_ch);

            (left_in, right_in, probe)
        });

        // Push data into the dataflow graph. Each worker will read some of the lines of the input
        debug!("Reading data files:\n\t{:?}\n\t{:?}", left_path, right_path);
        let left_path = left_path.clone();
        let right_path = right_path.clone();
        let sketcher = sketcher_pair_1.unwrap().0;
        for repetition in 0..repetitions {
            info!("Starting repetition {}", repetition);
            let start = Instant::now();
            let rep_start = Instant::now();
            ReadBinaryFile::read_binary(
                left_path.clone().into(),
                |l| l % peers as usize == index,
                |c, v| {
                    let s = sketcher.sketch(&v);
                    let h = hash_fn.hash(&v, repetition);
                    left.send((h, (s, c as u32)));
                },
            );
            ReadBinaryFile::read_binary(
                right_path.clone().into(),
                |l| l % peers as usize == index,
                |c, v| {
                    let s = sketcher.sketch(&v);
                    let h = hash_fn.hash(&v, repetition);
                    right.send((h, (s, c as u32)));
                },
            );
            left.advance_to(repetition as u32);
            right.advance_to(repetition as u32);
            let end = Instant::now();
            let elapsed = end - start;
            info!(
                "Time to feed the input to the dataflow graph: {:?}",
                elapsed
            );
            worker.step_while(|| probe.less_than(&(repetition as u32)));
            let rep_end = Instant::now();
            info!(
                "Time for repetition {}: {:?}",
                repetition,
                (rep_end - rep_start)
            );
        }

        info!(
            "Execution summary for worker {}: {:?}",
            index, execution_summary
        );
        collect_execution_summaries(execution_summary, send_exec_summary.clone(), &mut worker);
    })
    .unwrap();

    reader_handle
        .join()
        .expect("Problem joining the reader thread");

    if config.is_master() {
        let mut exec_summaries = Vec::new();
        for summary in recv_exec_summary.iter() {
            match summary {
                TimelyEvent::Messages(_, msgs) => exec_summaries.extend(msgs),
                _ => (),
            }
        }
        let global_summary = exec_summaries
            .iter()
            .fold(FrozenExecutionSummary::zero(), |a, b| a.sum(b));
        // From `recv` we get an entry for each timestamp, containing a one-element vector with the
        // count of output pairs for a given timestamp. We sum across all the timestamps, so we need to
        // remove the duplicates
        let count: usize = recv
            .extract()
            .iter()
            .map(|pair| pair.1.clone().iter().sum::<usize>())
            .sum();

        let precision = count as f64 / global_summary.distinct_pairs as f64;
        let potential_pairs =
            D::num_elements(left_path_final.into()) * D::num_elements(right_path_final.into());
        let fraction_distinct = global_summary.distinct_pairs as f64 / potential_pairs as f64;
        global_summary.add_to_experiment("execution_summary", experiment);
        info!(
            "Evaluated fraction of the potential pairs: {} ({}/{})",
            fraction_distinct, global_summary.distinct_pairs, potential_pairs
        );
        info!("Precision: {}", precision);
        info!("Global summary {:?}", global_summary);

        count
    } else {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::measure::*;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    #[test]
    fn test_hyperplane() {
        let mut rng = StdRng::seed_from_u64(123);
        let k = 20;
        let hasher = Hyperplane::new(k, 3, &mut rng);
        let a = UnitNormVector::new(vec![0.0, 1.0, 3.0]);
        let ha = hasher.hash(&a);
        let b = UnitNormVector::new(vec![1.0, 1.0, 3.0]);
        let hb = hasher.hash(&b);

        println!("{:?}", ha);
        println!("{:?}", hb);

        assert!(ha != hb);

        let dim = 300;
        for _ in 0..10 {
            let a = UnitNormVector::random_normal(dim, &mut rng);
            let b = UnitNormVector::random_normal(dim, &mut rng);
            let cos = InnerProduct::cosine(&a, &b);
            println!("Cosine between the vectors is {}", cos);
            assert!(cos >= -1.0 && cos <= 1.0);
            let acos = cos.acos();
            assert!(!acos.is_nan());
            let expected = 1.0 - acos / std::f64::consts::PI;
            let mut collisions = 0;
            let samples = 10000;
            for _ in 0..samples {
                let h = Hyperplane::new(1, dim, &mut rng);
                if h.hash(&a) == h.hash(&b) {
                    collisions += 1;
                }
            }
            let p = collisions as f64 / samples as f64;
            assert!(
                (p - expected).abs() <= 0.01,
                "estimated p={}, expected={}",
                p,
                expected
            );
        }
    }

    #[test]
    fn test_minhash() {
        let mut rng = StdRng::seed_from_u64(1232);
        let hasher = MinHash::new(20, &mut rng);
        let a = BagOfWords::new(10, vec![1, 3, 4]);
        let ha = hasher.hash(&a);
        let b = BagOfWords::new(10, vec![0, 1]);
        let hb = hasher.hash(&b);
        let c = BagOfWords::new(10, vec![0, 1]);
        let hc = hasher.hash(&c);

        println!("{:?}", ha);
        println!("{:?}", hb);
        println!("{:?}", hc);

        assert!(ha != hb);
        assert!(hc == hb);

        for _ in 0..10 {
            let a = BagOfWords::random(3000, 0.01, &mut rng);
            let b = BagOfWords::random(3000, 0.01, &mut rng);
            let similarity = Jaccard::jaccard(&a, &b);
            let mut collisions = 0;
            let samples = 10000;
            for _ in 0..samples {
                let h = MinHash::new(1, &mut rng);
                if h.hash(&a) == h.hash(&b) {
                    collisions += 1;
                }
            }
            let p = collisions as f64 / samples as f64;
            assert!(
                (p - similarity).abs() <= 0.05,
                "estimated p={}, expected={}",
                p,
                similarity
            );
        }
    }

    #[test]
    fn test_minhash_2() {
        let mut rng = StdRng::seed_from_u64(1232);
        let k = 3;
        for _ in 0..10 {
            let a = BagOfWords::random(3000, 0.01, &mut rng);
            let b = BagOfWords::random(3000, 0.01, &mut rng);
            let similarity = Jaccard::jaccard(&a, &b);
            let expected = similarity.powi(k as i32);
            let mut collisions = 0;
            let samples = 1000;
            for _ in 0..samples {
                let h = MinHash::new(k, &mut rng);
                if h.hash(&a) == h.hash(&b) {
                    collisions += 1;
                }
            }
            let p = collisions as f64 / samples as f64;
            assert!(
                (p - expected).abs() <= 0.02,
                "estimated p={}, expected={}",
                p,
                expected
            );
        }
    }

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
