use crate::dataset::*;
use crate::logging::*;
use crate::measure::InnerProduct;
use crate::operators::Route;
use crate::operators::*;
use crate::types::*;
use abomonation::Abomonation;
use packed_simd::u32x8;
use rand::distributions::{Distribution, Normal, Uniform};
use rand::{Rng, SeedableRng};
use std::clone::Clone;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::aggregation::Aggregate;
use timely::dataflow::operators::generic::source;
use timely::dataflow::operators::*;
use timely::dataflow::scopes::Child;
use timely::dataflow::*;
use timely::order::Product;
use timely::progress::Timestamp;
use timely::Data;
use timely::ExchangeData;

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
    pub fn for_each_hash<L>(&self, v: &F::Input, mut logic: L)
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
                h <<= 1;
            }
        }
        h
    }

    fn probability_at_range(range: f64) -> f64 {
        1_f64 - range.acos() / std::f64::consts::PI
    }
}

#[derive(Clone)]
pub struct MinHash {
    k: usize,
    alphas: Vec<u64>,
    betas: Vec<u64>,
}

impl MinHash {
    fn new<R>(k: usize, rng: &mut R) -> MinHash
    where
        R: Rng + ?Sized,
    {
        let uniform = Uniform::new(0u64, std::u64::MAX);
        let mut alphas = Vec::with_capacity(k);
        let mut betas = Vec::with_capacity(k);
        for _ in 0..k {
            alphas.push(uniform.sample(rng));
            betas.push(uniform.sample(rng));
        }
        MinHash { k, alphas, betas }
    }

    pub fn collection<R>(
        k: usize,
        repetitions: usize,
        rng: &mut R,
    ) -> LSHCollection<MinHash, Vec<u32>>
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
    ) -> impl Fn(usize, &mut R) -> LSHCollection<MinHash, Vec<u32>> + Clone
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
    type Output = Vec<u32>;

    fn hash(&self, v: &BagOfWords) -> Vec<u32> {
        let mut result = Vec::with_capacity(self.k);
        for (alpha, beta) in self.alphas.iter().zip(self.betas.iter()) {
            assert!(!v.words().is_empty(), "The collection of words is empty");
            let min_w = v
                .words()
                .iter()
                .map(|w| (alpha.wrapping_mul(u64::from(*w))).wrapping_add(*beta) >> 32)
                .min()
                .expect("No minimum");
            // The hash value has already been masked to 32 bits at this point
            result.push(min_w as u32);
        }
        result
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
        *self.hashers.keys().max().unwrap()
    }

    pub fn min_level(&self) -> usize {
        *self.hashers.keys().min().unwrap()
    }

    pub fn repetitions_at_level(&self, level: usize) -> usize {
        self.hashers
            .get(&level)
            .expect("no entry for requested level")
            .repetitions()
    }

    pub fn levels_at_repetition(&self, repetition: usize) -> std::ops::RangeInclusive<usize> {
        let min_level = self.min_level();
        let max_level = self.max_level();
        let start = (min_level..=max_level)
            .find(|&l| self.repetitions_at_level(l) >= repetition)
            .unwrap();
        start..=max_level
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
    buckets: HashMap<usize, Vec<HashMap<H, usize>>>,
    /// A balance less than 0.5 gives more weight to the collisions in the computation of the cost,
    /// a value greater than 0.5 gives more weight to the repetitions
    balance: f64,
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
        let logger = scope.danny_logger();
        let worker = scope.index() as u64;
        let multilevel_hasher = multilevel_hasher;
        source(scope, "collisions-stream", move |cap| {
            let multilevel_hasher = multilevel_hasher;
            let vecs = Arc::clone(&global_vecs);
            let mut cap = Some(cap);
            let mut rng = rng;
            move |output| {
                if let Some(cap) = cap.take() {
                    let _pg = ProfileGuard::new(logger.clone(), 0, 0, "best_k_estimation");
                    let mut session = output.session(&cap);
                    let p = n as f64 / vecs.stripe_len(matrix, direction, worker) as f64;
                    let p = if p > 1.0 { 1.0 } else { p };
                    let weight: usize = (1.0 / p).ceil() as usize;
                    info!("Sampling with probability {} from each block", p);
                    let mut accumulator = HashMap::new();
                    for (_, v) in vecs.iter_stripe(matrix, direction, worker) {
                        if rng.gen_bool(p) {
                            for level in
                                multilevel_hasher.min_level()..=multilevel_hasher.max_level()
                            {
                                for repetition in 0..multilevel_hasher.repetitions_at_level(level) {
                                    let h = multilevel_hasher.hash(v, level, repetition);
                                    *accumulator.entry((level, repetition, h)).or_insert(0) +=
                                        weight;
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

    pub fn best_levels<G, T, K, D, F>(
        collisions_stream: &Stream<G, ((usize, usize, H), usize)>,
        multilevel_hasher: Arc<MultilevelHasher<D, H, F>>,
        global_vecs: Arc<ChunkedDataset<K, D>>,
        matrix: MatrixDescription,
        direction: MatrixDirection,
        balance: f64,
    ) -> Stream<G, (K, usize)>
    where
        G: Scope<Timestamp = T>,
        T: Timestamp + Succ,
        K: Data + Debug + Send + Sync + Abomonation + Clone + Eq + Hash + Route,
        D: Clone + Data + Debug + Abomonation + Send + Sync,
        F: LSHFunction<Input = D, Output = H> + Clone + Sync + Send + 'static,
    {
        let worker = collisions_stream.scope().index() as u64;
        let logger = collisions_stream.scope().danny_logger();
        collisions_stream.unary_frontier(Pipeline, "best-level-finder", move |_, _| {
            let vecs = Arc::clone(&global_vecs);
            let mut collisions = HashMap::new();
            let mut start_receiving = None;
            move |input, output| {
                input.for_each(|t, data| {
                    if start_receiving.is_none() {
                        debug!(
                            "Start to receive simulated collisions (memory {})",
                            proc_mem!()
                        );
                        start_receiving = Some(Instant::now());
                    }
                    let mut data = data.replace(Vec::new());
                    collisions
                        .entry(t.retain())
                        .or_insert_with(Vec::new)
                        .append(&mut data);
                });

                for (time, counts) in collisions.iter_mut() {
                    if !input.frontier().less_equal(time) {
                        let end_receiving = Instant::now();
                        log_event!(
                            logger,
                            LogEvent::Profile(
                                0,
                                0,
                                "receive_simulated_collisions".to_owned(),
                                end_receiving - start_receiving.unwrap()
                            )
                        );
                        debug!(
                            "Time to receive {} simulated collisions {:?} (memory {:?})",
                            counts.len(),
                            end_receiving - start_receiving.unwrap(),
                            proc_mem!()
                        );
                        debug!("Finding best level for each and every vector");
                        let _pg = ProfileGuard::new(logger.clone(), 0, 0, "best_k_estimation");
                        let start_estimation = Instant::now();
                        let estimator =
                            BestLevelEstimator::from_counts(&multilevel_hasher, &counts, balance);
                        debug!("Built estimator (total mem {})", proc_mem!(),);
                        let mut session = output.session(&time);
                        let mut level_stats = std::collections::BTreeMap::new();
                        for (key, v) in vecs.iter_stripe(matrix, direction, worker) {
                            let best_level = estimator.get_best_level(&multilevel_hasher, v);
                            session.give((key.clone(), best_level));
                            *level_stats.entry(best_level).or_insert(0usize) += 1;
                        }
                        let end_estimation = Instant::now();
                        info!(
                            "Distribution of counts ({:.2?}) {:#?}",
                            end_estimation - start_estimation,
                            level_stats
                        );
                        counts.clear();
                    }
                }

                collisions.retain(|_, counts| !counts.is_empty());
            }
        })
    }

    pub fn from_counts<D, F>(
        multilevel_hasher: &MultilevelHasher<D, H, F>,
        counts: &[((usize, usize, H), usize)],
        balance: f64,
    ) -> Self
    where
        D: Clone + Data + Debug + Abomonation + Send + Sync,
        F: LSHFunction<Input = D, Output = H> + Clone + Sync + Send + 'static,
    {
        let mut buckets = HashMap::new();
        for (level, hasher) in multilevel_hasher.hashers.iter() {
            let mut repetitions_maps = Vec::new();
            for _ in 0..hasher.repetitions() {
                let rep_map = HashMap::new();
                repetitions_maps.push(rep_map);
            }
            buckets.insert(*level, repetitions_maps);
        }
        for ((level, repetition, h), count) in counts {
            buckets.get_mut(level).unwrap()[*repetition].insert(h.clone(), *count);
        }
        BestLevelEstimator { buckets, balance }
    }

    pub fn cost_str(&self) -> String {
        let mut res = String::new();
        for (level, repetitions) in self.buckets.iter() {
            let cost = repetitions
                .iter()
                .map(|buckets_map| buckets_map.values().sum::<usize>())
                .sum::<usize>();
            let overall_buckets = repetitions.iter().map(HashMap::len).sum::<usize>();
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
        let mut min_work = std::f64::INFINITY;
        let mut best_level = 0;
        let mut largest_bucket = 0;
        let rep_factor = self.balance;
        let collision_factor = 1.0 - self.balance;
        for (idx, hasher) in multilevel_hasher.hashers.iter() {
            let mut collisions_work = 0.0;
            let mut largest = 0;
            for rep in 0..hasher.repetitions() {
                let h = hasher.hash(v, rep);
                let collisions_count = self.buckets[idx][rep].get(&h).unwrap_or(&0usize);
                largest = std::cmp::max(largest, *collisions_count);
                collisions_work += (*collisions_count as f64).powf(1.0);
            }
            let work = (rep_factor * hasher.repetitions() as f64)
                + (collision_factor * collisions_work as f64);
            if work < min_work {
                min_work = work;
                best_level = *idx;
                largest_bucket = largest;
            }
        }
        assert!(min_work > 0.0);
        info!(
            "Level {}, work {}, largest bucket in level {}",
            best_level, min_work, largest_bucket
        );
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
