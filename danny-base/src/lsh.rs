use crate::measure::InnerProduct;
use crate::types::*;
use rand::distributions::{Distribution, Normal, Uniform};
use rand::Rng;
use std::clone::Clone;

pub trait LSHFunction {
    type Input;
    type Output;
    fn hash(&self, v: &Self::Input) -> Self::Output;
    fn probability_at_range(range: f64) -> f64;

    fn repetitions_at_range(range: f64, k: usize) -> usize {
        let p = Self::probability_at_range(range);
        let reps = (2.0 * (1_f64 / p).powi(k as i32)).ceil() as usize;
        debug!("Probability at range {} is {} (reps: {})", range, p, reps);
        reps
    }
}

#[derive(Clone, Abomonation, Debug, Hash)]
pub struct TensorPool {
    left: Vec<u16>,
    right: Vec<u16>,
}

pub struct TensorCollection<F>
where
    F: LSHFunction<Output = u32> + Clone,
{
    k_left: usize,
    k_right: usize,
    mask_left: u32,
    mask_right: u32,
    repetitions: usize,
    hashers: Vec<F>,
}

impl<F> TensorCollection<F>
where
    F: LSHFunction<Output = u32> + Clone,
{
    fn get_mask(k: usize) -> u32 {
        assert!(k <= 16);
        let mut m = 0u32;
        for _ in 0..k {
            m = (m << 1) | 1;
        }
        m
    }

    pub fn new<B, R: Rng>(k: usize, range: f64, recall: f64, mut builder: B, rng: &mut R) -> Self
    where
        B: FnMut(usize, &mut R) -> F,
    {
        let k_left = ((k as f64) / 2.0).ceil() as usize;
        let k_right = ((k as f64) / 2.0).floor() as usize;
        let coll_prob_left = F::probability_at_range(range).powi(k_left as i32);
        let coll_prob_right = F::probability_at_range(range).powi(k_right as i32);
        let repetitions = ((1_f64 - recall.sqrt()).ln().powi(2)
            / ((1_f64 - coll_prob_left).ln() * (1_f64 - coll_prob_right).ln()))
        .ceil() as usize;
        assert!(repetitions > 0);
        let part_repetitions = (repetitions as f64).sqrt().ceil() as usize;
        let mut hashers = Vec::new();
        for _ in 0..part_repetitions {
            hashers.push(builder(k, rng));
        }

        Self {
            k_left,
            k_right,
            mask_left: Self::get_mask(k_left),
            mask_right: Self::get_mask(k_right),
            repetitions,
            hashers,
        }
    }

    pub fn pool(&self, v: &F::Input) -> TensorPool {
        let mut left = Vec::new();
        let mut right = Vec::new();
        for hasher in self.hashers.iter() {
            let h: u32 = hasher.hash(v);
            let bits_left = (h & self.mask_left) as u16;
            let bits_right = ((h >> self.k_left) & self.mask_right) as u16;
            left.push(bits_left);
            right.push(bits_right);
        }

        TensorPool { left, right }
    }

    pub fn hash(&self, pool: &TensorPool, repetition: usize) -> u32 {
        let idx_left = repetition / self.hashers.len();
        let idx_right = repetition % self.hashers.len();
        let left = pool.left[idx_left];
        let right = pool.right[idx_right];
        ((left as u32) << self.k_right) | (right as u32)
    }

    /// Tells whether a collision was already seen
    pub fn already_seen(&self, a: &TensorPool, b: &TensorPool, repetition: usize) -> bool {
        let idx_left = repetition / self.hashers.len();
        let idx_right = repetition % self.hashers.len();
        for i in 0..idx_left {
            if a.left[i] == b.left[i] {
                for j in 0..a.right.len() {
                    if a.right[j] == b.right[j] {
                        return true;
                    }
                }
            }
        }
        if a.left[idx_left] == b.left[idx_left] {
            for j in 0..idx_right {
                if a.right[j] == b.right[j] {
                    return true;
                }
            }
        }
        false
    }

    pub fn repetitions(&self) -> usize {
        self.repetitions
    }
}

#[derive(Clone, Abomonation, Debug, Hash)]
pub struct DKTPool {
    bits: Vec<BitVector>,
}

#[derive(Clone, Abomonation, Default, Debug, Hash)]
pub struct BitVector {
    bits: Vec<u32>,
}

impl BitVector {
    fn from_vec(vec: Vec<u32>) -> Self {
        Self { bits: vec }
    }
    pub fn get(&self, index: usize) -> bool {
        let word_index = index / 32;
        let bit_index = index % 32;
        let mask = 1u32 << bit_index;
        self.bits[word_index] & mask != 0
    }
}

#[derive(Clone)]
pub struct DKTCollection<F>
where
    F: LSHFunction<Output = u32> + Clone,
{
    k: usize,
    repetitions: usize,
    num_bits: usize,
    alphas: Vec<u64>,
    betas: Vec<u64>,
    hashers: Vec<Vec<F>>,
}

impl<F> DKTCollection<F>
where
    F: LSHFunction<Output = u32> + Clone,
{
    pub fn new<B, R: Rng>(k: usize, range: f64, mut builder: B, rng: &mut R) -> Self
    where
        B: FnMut(usize, &mut R) -> F,
    {
        let p = F::probability_at_range(range);
        let num_bits = (5.0 * k as f64 / p).ceil() as usize;
        let alphas: Vec<u64> = (0..k).map(|_| rng.next_u64()).collect();
        let betas: Vec<u64> = (0..k).map(|_| rng.next_u64()).collect();
        let mut hashers = Vec::new();
        let full_32 = num_bits / 32;
        let rem_32 = num_bits % 32;
        for _ in 0..k {
            let mut hs = Vec::new();
            for _ in 0..full_32 {
                hs.push(builder(32, rng));
            }
            if rem_32 > 0 {
                hs.push(builder(rem_32, rng));
            }
            hashers.push(hs);
        }
        let repetitions = F::repetitions_at_range(range, k);
        Self {
            k,
            repetitions,
            num_bits,
            alphas,
            betas,
            hashers,
        }
    }

    pub fn pool(&self, v: &F::Input) -> DKTPool {
        let mut all_bits = Vec::new();
        for hashers_row in &self.hashers {
            let mut bits = Vec::new();
            for f in hashers_row {
                bits.push(f.hash(v));
            }
            let bits = BitVector::from_vec(bits);
            all_bits.push(bits);
        }
        DKTPool { bits: all_bits }
    }

    #[inline]
    fn get_bit_index(&self, k: usize, repetition: usize) -> usize {
        self.alphas[k]
            .wrapping_mul(repetition as u64)
            .wrapping_add(self.betas[k]) as usize
            % self.num_bits
    }

    pub fn hash(&self, pool: &DKTPool, repetition: usize) -> u32 {
        let mut h = 0u32;
        for (i, bits) in pool.bits.iter().enumerate() {
            if bits.get(self.get_bit_index(i, repetition)) {
                h = (h << 1) | 1;
            } else {
                h <<= 1;
            }
        }
        h
    }

    pub fn repetitions(&self) -> usize {
        self.repetitions
    }
}

#[derive(Clone)]
pub struct Hyperplane {
    k: usize,
    planes: Vec<Vector>,
}

impl Hyperplane {
    pub fn new<R>(k: usize, dim: usize, rng: &mut R) -> Hyperplane
    where
        R: Rng + ?Sized,
    {
        assert!(
            k <= 32,
            "Only k<=32 is supported so to be able to pack hashes in words"
        );
        let mut planes = Vec::with_capacity(k);
        let gaussian = Normal::new(0.0, 1.0);
        for _ in 0..k {
            let mut plane = Vec::with_capacity(dim);
            for _ in 0..dim {
                plane.push(gaussian.sample(rng) as f32);
            }
            let plane = Vector::new(plane);
            planes.push(plane);
        }
        Hyperplane { k, planes }
    }

    pub fn builder<R>(dim: usize) -> impl Fn(usize, &mut R) -> Hyperplane + Clone
    where
        R: Rng + ?Sized,
    {
        let dim = dim;
        move |k: usize, rng: &mut R| Hyperplane::new(k, dim, rng)
    }
}

impl LSHFunction for Hyperplane {
    type Input = Vector;
    type Output = u32;

    fn hash(&self, v: &Vector) -> u32 {
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

    pub fn builder<R>() -> impl Fn(usize, &mut R) -> OneBitMinHash + Clone
    where
        R: Rng + ?Sized,
    {
        move |k: usize, rng: &mut R| OneBitMinHash::new(k, rng)
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
        let a = Vector::new(vec![0.0, 1.0, 3.0, 1.0]);
        let ha = hasher.hash(&a);
        let b = Vector::new(vec![1.0, 1.0, 3.0, 1.0]);
        let hb = hasher.hash(&b);

        println!("{:?}", ha);
        println!("{:?}", hb);

        assert!(ha != hb);

        let dim = 300;
        for _ in 0..10 {
            let a = Vector::random_normal(dim, &mut rng).normalize();
            let b = Vector::random_normal(dim, &mut rng).normalize();
            let ip = InnerProduct::inner_product(&a, &b);
            println!("Inner product between the vectors is {}", ip);
            assert!(ip >= -1.0 && ip <= 1.0, "inner product is {}", ip);
            let acos = ip.acos();
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

        for _ in 0..100 {
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

    #[test]
    fn test_tensor_1() {
        let mut rng = StdRng::seed_from_u64(1223132);
        let a = BagOfWords::random(3000, 0.01, &mut rng);
        let coll = TensorCollection::new(4, 0.5, 0.8, OneBitMinHash::builder(), &mut rng);
        let pool = coll.pool(&a);
        for rep in 0..coll.repetitions() {
            let h = coll.hash(&pool, rep);
            println!("{:32b}", h);
        }
        // TODO: come up with a way to actually test this
    }
}
