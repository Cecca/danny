use crate::config::Config;
use crate::dataset::*;
use crate::experiment::Experiment;
use crate::io::*;
use crate::logging::init_event_logging;
use crate::logging::*;
use crate::lsh::operators::collect_sample;
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
use std::collections::BTreeMap;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Barrier, Mutex, RwLock};
use std::thread;
use std::time::Instant;
use timely::communication::allocator::Allocate;
use timely::dataflow::channels::pact::{Exchange as ExchangePact, Pipeline};
use timely::dataflow::operators::aggregation::Aggregate;
use timely::dataflow::operators::capture::{Event as TimelyEvent, Extract};
use timely::dataflow::operators::generic::source;
use timely::dataflow::operators::*;
use timely::dataflow::*;
use timely::order::Product;
use timely::progress::Timestamp;
use timely::worker::Worker;
use timely::Data;

pub trait LSHFunction {
    type Input;
    type Output;
    fn hash(&self, v: &Self::Input) -> Self::Output;
    fn probability_at_range(range: f64) -> f64;

    fn repetitions_at_range(range: f64, k: usize) -> usize {
        let p = Self::probability_at_range(range);
        let reps = (2.0 * (1_f64 / p).powi(k as i32)).ceil() as usize;
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

    pub fn collection_builder<R>(
        threshold: f64,
        dim: usize,
    ) -> impl Fn(usize, &mut R) -> LSHCollection<Hyperplane, u32> + Clone
    where
        R: Rng + ?Sized,
    {
        let threshold = threshold;
        let dim = dim;
        move |k: usize, rng: &mut R| {
            let repetitions = Hyperplane::repetitions_at_range(threshold, k);
            Self::collection(k, repetitions, dim, rng)
        }
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

    pub fn collection_builder<R>(
        threshold: f64,
    ) -> impl Fn(usize, &mut R) -> LSHCollection<MinHash, u32> + Clone
    where
        R: Rng + ?Sized,
    {
        let threshold = threshold;
        move |k: usize, rng: &mut R| {
            let repetitions = MinHash::repetitions_at_range(threshold, k);
            Self::collection(k, repetitions, rng)
        }
    }
}

impl LSHFunction for MinHash {
    type Input = BagOfWords;
    type Output = u32;

    fn hash(&self, v: &BagOfWords) -> u32 {
        let mut h = 0u64;
        for (hasher, coeff) in self.hashers.iter().zip(self.coeffs.iter()) {
            assert!(!v.words().is_empty(), "The collection of words is empty");
            let min_w = v
                .words()
                .iter()
                .map(|w| hasher.hash(*w))
                .min()
                .expect("No minimum");
            h = h.wrapping_add(coeff.wrapping_mul(min_w));
        }
        (h >> 32) as u32
    }

    fn probability_at_range(range: f64) -> f64 {
        range
    }
}

pub fn estimate_best_k<K, D, H, F, B, R>(
    left: Vec<(K, D)>,
    right: Vec<(K, D)>,
    max_k: usize,
    builder: B,
    rng: &mut R,
) -> usize
where
    K: Send + Sync + 'static,
    D: Clone + Send + Sync + 'static,
    H: Clone + Hash + Eq + Send + Sync + 'static,
    F: LSHFunction<Input = D, Output = H> + Clone + Send + Sync + 'static,
    B: Fn(usize, &mut R) -> LSHCollection<F, H>,
    R: Rng + ?Sized,
{
    let left = Arc::new(left);
    let right = Arc::new(right);
    let mut best_k = 0;
    let mut min_cost = std::usize::MAX;
    for k in 1..=max_k {
        info!("Estimating collisions for k={}", k);
        let hasher = builder(k, rng);
        let hasher = Arc::new(hasher);
        let mut comparisons_count = 0usize;
        for rep in 0..hasher.repetitions() {
            let hasher_1 = Arc::clone(&hasher);
            let hasher_2 = Arc::clone(&hasher);
            let left = Arc::clone(&left);
            let right = Arc::clone(&right);
            let left_buckets = Arc::new(Mutex::new(HashMap::new()));
            let right_buckets = Arc::new(Mutex::new(HashMap::new()));
            let left_buckets_2 = Arc::clone(&left_buckets);
            let right_buckets_2 = Arc::clone(&right_buckets);
            let h1 = thread::spawn(move || {
                let mut left_buckets = left_buckets.lock().unwrap();
                for (_, v) in left.iter() {
                    let h = hasher_1.hash(v, rep);
                    *left_buckets.entry(h).or_insert(0usize) += 1;
                }
            });
            let h2 = thread::spawn(move || {
                let mut right_buckets = right_buckets.lock().unwrap();
                for (_, v) in right.iter() {
                    let h = hasher_2.hash(v, rep);
                    *right_buckets.entry(h).or_insert(0usize) += 1;
                }
            });
            h1.join().unwrap();
            h2.join().unwrap();
            for (k, l_cnt) in left_buckets_2.lock().unwrap().drain() {
                if let Some(r_cnt) = right_buckets_2.lock().unwrap().get(&k) {
                    comparisons_count += r_cnt * l_cnt;
                }
            }
        }
        let cost = comparisons_count + hasher.repetitions() * (left.len() + right.len());
        info!("Cost for k={} -> {}", k, cost);
        if cost < min_cost {
            min_cost = cost;
            best_k = k;
        } else {
            info!("Cost is now raising, breaking the loop");
            break;
        }
    }
    best_k
}

/// Structure that allows to hash a vector at the desired level k. In this respect,
/// it is somehow the multi-level counterpart of LSHCollection
pub struct MultilevelHasher<D, H, F>
where
    D: Clone + Data + Debug + Abomonation + Send + Sync,
    H: Clone + Hash + Eq + Debug + Send + Sync + Data + Abomonation,
    F: LSHFunction<Input = D, Output = H> + Clone + Sync + Send + 'static,
{
    hashers: HashMap<usize, LSHCollection<F, H>>,
}

impl<D, H, F> MultilevelHasher<D, H, F>
where
    D: Clone + Data + Debug + Abomonation + Send + Sync,
    H: Clone + Hash + Eq + Debug + Send + Sync + Data + Abomonation,
    F: LSHFunction<Input = D, Output = H> + Clone + Sync + Send + 'static,
{
    pub fn new<B, R>(min_level: usize, max_level: usize, builder: B, rng: &mut R) -> Self
    where
        B: Fn(usize, &mut R) -> LSHCollection<F, H>,
        R: Rng + SeedableRng + Send + Clone + ?Sized + 'static,
    {
        info!(
            "Building MultiLevelHasher with minimum level {} and maximum level {}",
            min_level, max_level
        );
        let mut hashers = HashMap::new();
        for level in min_level..=max_level {
            hashers.insert(level, builder(level, rng));
        }
        Self { hashers }
    }

    pub fn max_level(&self) -> usize {
        self.hashers.len()
    }

    pub fn repetitions_at_level(&self, level: usize) -> usize {
        self.hashers[&level].repetitions()
    }

    pub fn hash(&self, v: &D, level: usize, repetition: usize) -> H {
        self.hashers[&level].hash(v, repetition)
    }
}

#[derive(Clone, Copy, Abomonation, Debug)]
pub struct LevelInfo {
    best_level: usize,
    current_level: usize,
}

impl LevelInfo {
    pub fn new(best_level: usize, current_level: usize) -> Self {
        Self {
            best_level,
            current_level,
        }
    }

    pub fn is_match(&self, other: &Self) -> bool {
        if self.current_level == other.current_level {
            self.current_level == std::cmp::min(self.best_level, other.current_level)
        } else {
            false
        }
    }
}

pub struct BestLevelEstimator<H>
where
    H: Clone + Hash + Eq + Ord + Debug + Send + Sync + Data + Abomonation,
{
    /// One hashmap for each level of k, the values are a vector with a position
    /// for each repetition, and each repetition is a map between hash value
    /// and count of things hashing to it
    buckets: HashMap<usize, Vec<BTreeMap<H, usize>>>,
}

impl<H> BestLevelEstimator<H>
where
    H: Clone + Hash + Eq + Ord + Debug + Send + Sync + Data + Abomonation,
{
    pub fn stream_collisions<G, T, K, D, F, R>(
        scope: &G,
        multilevel_hasher: Arc<MultilevelHasher<D, H, F>>,
        global_vecs: Arc<ChunkedDataset<K, D>>,
        n: usize,
        matrix: MatrixDescription,
        direction: MatrixDirection,
        rng: R,
    ) -> Stream<G, ((usize, usize, H), usize)>
    where
        G: Scope<Timestamp = T>,
        T: Timestamp + Succ,
        K: Data + Debug + Send + Sync + Abomonation + Clone + Eq + Hash + Route,
        D: Clone + Data + Debug + Abomonation + Send + Sync,
        F: LSHFunction<Input = D, Output = H> + Clone + Sync + Send + 'static,
        R: Rng + SeedableRng + Send + Clone + ?Sized + 'static,
    {
        let worker = scope.index() as u64;
        let multilevel_hasher = multilevel_hasher;
        source(scope, "collisions-stream", move |cap| {
            let multilevel_hasher = multilevel_hasher;
            let vecs = Arc::clone(&global_vecs);
            let mut cap = Some(cap);
            let mut rng = rng;
            move |output| {
                if let Some(cap) = cap.take() {
                    let mut session = output.session(&cap);
                    let p = n as f64 / vecs.stripe_len(&matrix, direction, worker) as f64;
                    info!("Sampling with probability {} from each block", p);
                    let mut accumulator = HashMap::new();
                    for (_, v) in vecs.iter_stripe(&matrix, direction, worker) {
                        if rng.gen_bool(p) {
                            for (level, hasher) in multilevel_hasher.hashers.iter() {
                                for repetition in 0..multilevel_hasher.repetitions_at_level(*level)
                                {
                                    let h = hasher.hash(v, repetition);
                                    *accumulator.entry((*level, repetition, h)).or_insert(0) += 1;
                                }
                            }
                        }
                    }
                    info!("Outputting {} collision messages", accumulator.len());
                    for e in accumulator.drain() {
                        session.give(e);
                    }
                    info!(
                        "Sampled vectors and generated collisions (memory {})",
                        proc_mem!()
                    );
                }
            }
        })
        .aggregate(
            |_key, val, agg| {
                *agg += val;
            },
            |key, agg: usize| (key, agg),
            |key| (key.0 * 31 + key.1) as u64,
        )
        .broadcast()
    }

    pub fn from_counts<D, F>(
        multilevel_hasher: &MultilevelHasher<D, H, F>,
        counts: &[((usize, usize, H), usize)],
    ) -> Self
    where
        D: Clone + Data + Debug + Abomonation + Send + Sync,
        F: LSHFunction<Input = D, Output = H> + Clone + Sync + Send + 'static,
    {
        let mut buckets = HashMap::new();
        for (level, hasher) in multilevel_hasher.hashers.iter() {
            let mut repetitions_maps = Vec::new();
            for _ in 0..hasher.repetitions() {
                let rep_map = BTreeMap::new();
                repetitions_maps.push(rep_map);
            }
            buckets.insert(*level, repetitions_maps);
        }
        for ((level, repetition, h), count) in counts {
            buckets.get_mut(level).unwrap()[*repetition].insert(h.clone(), *count);
        }
        BestLevelEstimator { buckets }
    }

    pub fn cost_str(&self) -> String {
        let mut res = String::new();
        for (level, repetitions) in self.buckets.iter() {
            let cost = repetitions
                .iter()
                .map(|buckets_map| buckets_map.values().sum::<usize>())
                .sum::<usize>();
            let overall_buckets = repetitions
                .iter()
                .map(|buckets_map| buckets_map.len())
                .sum::<usize>();
            res.push_str(&format!(
                " ({}): {} [{}]",
                level,
                cost,
                cost as f64 / overall_buckets as f64
            ));
        }
        res
    }

    pub fn bucket_detail(&self) -> String {
        let mut res = String::new();
        for (level, repetitions) in self.buckets.iter() {
            res.push_str(&format!("  level {} ", level));
            for buckets_map in repetitions.iter() {
                res.push_str(&format!("\n     {:?}", buckets_map));
            }
            res.push('\n');
        }
        res
    }

    pub fn get_best_level<D, F>(
        &self,
        multilevel_hasher: &MultilevelHasher<D, H, F>,
        v: &D,
    ) -> usize
    where
        D: Clone + Data + Debug + Abomonation + Send + Sync,
        F: LSHFunction<Input = D, Output = H> + Clone + Sync + Send + 'static,
    {
        let mut min_work = std::usize::MAX;
        let mut best_level = 0;
        for (idx, hasher) in multilevel_hasher.hashers.iter() {
            let mut work = hasher.repetitions();
            for rep in 0..hasher.repetitions() {
                let h = hasher.hash(v, rep);
                let collisions_count = self.buckets[idx][rep].get(&h).unwrap_or(&0usize);
                work += 1 + collisions_count * collisions_count;
            }
            if work < min_work {
                min_work = work;
                best_level = *idx;
            }
        }
        best_level
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
        let hasher = Hyperplane::new(k, 4, &mut rng);
        let a = UnitNormVector::new(vec![0.0, 1.0, 3.0, 1.0]);
        let ha = hasher.hash(&a);
        let b = UnitNormVector::new(vec![1.0, 1.0, 3.0, 1.0]);
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

}
