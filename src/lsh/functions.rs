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
use timely::dataflow::operators::generic::source;
use timely::dataflow::operators::*;
use timely::dataflow::*;
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
