use crate::measure::InnerProduct;
use crate::types::*;
use abomonation::Abomonation;
use rand::distributions::{Distribution, Normal, Uniform};
use rand::{Rng, SeedableRng};
use std::clone::Clone;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
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
pub struct LSHCollection<F>
where
    F: LSHFunction + Clone,
    F::Output: Clone,
{
    functions: Vec<F>,
}

impl<F> LSHCollection<F>
where
    F: LSHFunction + Clone,
    F::Output: Clone,
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
    ) -> LSHCollection<Hyperplane>
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
    ) -> impl Fn(usize, &mut R) -> LSHCollection<Hyperplane> + Clone
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
pub struct OneBitMinHash {
    k: usize,
    alphas: Vec<u64>,
    betas: Vec<u64>,
}

impl OneBitMinHash {
    pub fn new<R>(k: usize, rng: &mut R) -> Self
    where
        R: Rng + ?Sized,
    {
        assert!(k <= 32);
        let uniform = Uniform::new(0u64, std::u64::MAX);
        let mut alphas = Vec::with_capacity(k);
        let mut betas = Vec::with_capacity(k);
        for _ in 0..k {
            alphas.push(uniform.sample(rng));
            betas.push(uniform.sample(rng));
        }
        OneBitMinHash { k, alphas, betas }
    }

    pub fn collection<R>(k: usize, repetitions: usize, rng: &mut R) -> LSHCollection<OneBitMinHash>
    where
        R: Rng + ?Sized,
    {
        let mut functions = Vec::with_capacity(repetitions);
        for _ in 0..repetitions {
            functions.push(OneBitMinHash::new(k, rng));
        }
        LSHCollection { functions }
    }

    pub fn collection_builder<R>(
        threshold: f64,
    ) -> impl Fn(usize, &mut R) -> LSHCollection<OneBitMinHash> + Clone
    where
        R: Rng + ?Sized,
    {
        let threshold = threshold;
        move |k: usize, rng: &mut R| {
            let repetitions = OneBitMinHash::repetitions_at_range(threshold, k);
            Self::collection(k, repetitions, rng)
        }
    }
}

impl LSHFunction for OneBitMinHash {
    type Input = BagOfWords;
    type Output = u32;

    fn hash(&self, v: &BagOfWords) -> u32 {
        let mut hash_value = 0u32;
        for (alpha, beta) in self.alphas.iter().zip(self.betas.iter()) {
            let h = v
                .words()
                .iter()
                .map(|w| (alpha.wrapping_mul(u64::from(*w))).wrapping_add(*beta) >> 32)
                .min()
                .unwrap();
            hash_value = (hash_value << 1) | (1 & h) as u32;
        }
        hash_value
    }

    fn probability_at_range(range: f64) -> f64 {
        (range + 1.0) / 2.0
    }
}

/// Structure that allows to hash a vector at the desired level k. In this respect,
/// it is somehow the multi-level counterpart of LSHCollection
pub struct MultilevelHasher<D, H, F>
where
    D: Clone + Data + Debug + Abomonation + Send + Sync,
    H: Clone + Hash + Eq + Debug + Send + Sync + Data + Abomonation,
    F: LSHFunction<Input = D, Output = H> + Clone + Sync + Send + 'static,
{
    hashers: HashMap<usize, LSHCollection<F>>,
}

impl<D, H, F> MultilevelHasher<D, H, F>
where
    D: Clone + Data + Debug + Abomonation + Send + Sync,
    H: Clone + Hash + Eq + Debug + Send + Sync + Data + Abomonation,
    F: LSHFunction<Input = D, Output = H> + Clone + Sync + Send + 'static,
{
    pub fn new<B, R>(min_level: usize, max_level: usize, builder: B, rng: &mut R) -> Self
    where
        B: Fn(usize, &mut R) -> LSHCollection<F>,
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
        let hasher = OneBitMinHash::new(20, &mut rng);
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
            let expected = OneBitMinHash::probability_at_range(similarity);
            let mut collisions = 0;
            let samples = 10000;
            for _ in 0..samples {
                let h = OneBitMinHash::new(1, &mut rng);
                if h.hash(&a) == h.hash(&b) {
                    collisions += 1;
                }
            }
            let p = collisions as f64 / samples as f64;
            assert!(
                (p - expected).abs() <= 0.05,
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
            let expected = OneBitMinHash::probability_at_range(similarity).powi(k as i32);
            let mut collisions = 0;
            let samples = 10000;
            for _ in 0..samples {
                let h = OneBitMinHash::new(k, &mut rng);
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
