use crate::lsh::*;
use crate::measure::*;
use crate::types::*;
use packed_simd::u32x4;
use packed_simd::u32x8;
use rand::distributions::{Distribution, Normal, Uniform};
use rand::Rng;
use std::collections::HashMap;
use std::iter::Iterator;

pub trait BitBasedSketch {
    fn bits(&self) -> &Vec<u32>;
}

#[derive(Clone, Debug)]
pub struct SketchPredicate<T>
where
    T: BitBasedSketch,
{
    /// The number of bits that can be different between two sketches without having to reject the
    /// pair.
    bit_threshold: usize,
    _marker: std::marker::PhantomData<T>,
}

impl<T> SketchPredicate<T>
where
    T: BitBasedSketch,
{
    pub fn from_probability(k: usize, p: f64, epsilon: f64) -> Self {
        let k = k as f64;
        let delta = (3.0 / (p * k) * (1.0 / epsilon).ln()).sqrt();
        let bit_threshold = ((1.0 + delta) * p * k).ceil() as usize;
        info!(
            "Using bit thresold {} of different bits to reject (delta {} epsilon {})",
            bit_threshold, delta, epsilon
        );
        SketchPredicate {
            bit_threshold,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn jaccard(k: usize, similarity: f64, epsilon: f64) -> Self {
        let p = (1.0 - similarity) / 2.0; // The probability that two corresponding bits are different
        Self::from_probability(k, p, epsilon)
    }

    pub fn cosine(k: usize, similarity: f64, epsilon: f64) -> Self {
        let p = similarity.acos() / std::f64::consts::PI;
        Self::from_probability(k, p, epsilon)
    }

    /// Return true if the two sketches represent vectors associated with vectors
    /// within the similarity threshold this predicate was built with
    pub fn eval(&self, a: &T, b: &T) -> bool {
        let threshold = self.bit_threshold;
        let ca = a.bits().chunks_exact(4);
        let cb = b.bits().chunks_exact(4);
        let rem = ca
            .remainder()
            .iter()
            .zip(cb.remainder().iter())
            .map(|(x, y)| (x ^ y).count_ones())
            .sum::<u32>();
        let different_bits = rem as usize
            + ca.map(u32x4::from_slice_unaligned)
                .zip(cb.map(u32x4::from_slice_unaligned))
                .map(|(x, y)| (x ^ y).count_ones())
                .sum::<u32x4>()
                .wrapping_sum() as usize;

        different_bits <= threshold
    }
}

pub trait SketchEstimate {
    fn estimate(a: &Self, b: &Self) -> f64;
}

impl SketchEstimate for SimHashValue {
    #[allow(clippy::cast_lossless)]
    fn estimate(a: &SimHashValue, b: &SimHashValue) -> f64 {
        let p = a
            .bits
            .iter()
            .zip(b.bits.iter())
            .map(|(x, y)| (x ^ y).count_zeros())
            .sum::<u32>() as f64
            / (32.0 * a.bits.len() as f64);
        (std::f64::consts::PI * (1.0 - p)).cos()
    }
}

impl BitBasedSketch for OneBitMinHashValue {
    fn bits(&self) -> &Vec<u32> {
        &self.bits
    }
}

impl BitBasedSketch for SimHashValue {
    fn bits(&self) -> &Vec<u32> {
        &self.bits
    }
}

impl SketchEstimate for OneBitMinHashValue {
    #[allow(clippy::cast_lossless)]
    fn estimate(a: &OneBitMinHashValue, b: &OneBitMinHashValue) -> f64 {
        let p = a
            .bits
            .iter()
            .zip(b.bits.iter())
            .map(|(x, y)| (x ^ y).count_zeros())
            .sum::<u32>() as f64
            / (32.0 * a.bits.len() as f64);
        2.0 * p - 1.0
    }
}

pub trait Sketcher {
    type Input;
    type Output;

    fn sketch(&self, v: &Self::Input) -> Self::Output;
}

impl Sketcher for Hyperplane {
    type Input = UnitNormVector;
    type Output = u32;

    fn sketch(&self, v: &UnitNormVector) -> u32 {
        self.hash(v)
    }
}

#[derive(Abomonation, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SimHashValue {
    bits: Vec<u32>,
}

#[derive(Clone)]
pub struct LongSimHash {
    k: usize,
    planes: Vec<UnitNormVector>,
}

impl LongSimHash {
    pub fn new<R>(k: usize, dim: usize, rng: &mut R) -> Self
    where
        R: Rng + ?Sized,
    {
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
        LongSimHash { k, planes }
    }
}

impl Sketcher for LongSimHash {
    type Input = UnitNormVector;
    type Output = SimHashValue;

    fn sketch(&self, v: &UnitNormVector) -> SimHashValue {
        let num_elems = (self.k as f64 / 4.0).ceil() as usize;
        let mut sketch = Vec::with_capacity(num_elems);
        let mut part = 0u32;
        for (i, plane) in self.planes.iter().enumerate() {
            if i != 0 && i % 32 == 0 {
                sketch.push(part);
                part = 0;
            }
            if InnerProduct::inner_product(plane, v) >= 0_f64 {
                part = (part << 1) | 1;
            } else {
                part <<= 1;
            }
        }
        if sketch.len() != num_elems {
            sketch.push(part);
        }
        SimHashValue { bits: sketch }
    }
}

#[derive(Clone)]
pub struct OneBitMinHash {
    k: usize,
    alphas: Vec<u64>,
    betas: Vec<u64>,
}

#[derive(Abomonation, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OneBitMinHashValue {
    bits: Vec<u32>,
}

impl OneBitMinHash {
    pub fn new<R>(k: usize, rng: &mut R) -> Self
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
        OneBitMinHash { k, alphas, betas }
    }
}

impl Sketcher for OneBitMinHash {
    type Input = BagOfWords;
    type Output = OneBitMinHashValue;

    fn sketch(&self, v: &BagOfWords) -> OneBitMinHashValue {
        let num_elems = (self.k as f32 / 32.0).ceil() as usize;
        let mut bits = Vec::with_capacity(num_elems);
        let mut part = 0u32;

        for (i, (alpha, beta)) in self.alphas.iter().zip(self.betas.iter()).enumerate() {
            if i % 32 == 0 && i > 0 {
                bits.push(part);
                part = 0;
            }
            let h = v
                .words()
                .iter()
                .map(|w| (alpha.wrapping_mul(u64::from(*w))).wrapping_add(*beta) >> 32)
                .min()
                .unwrap();
            part = (part << 1) | (1 & h) as u32;
        }
        if bits.len() < num_elems {
            bits.push(part);
        }

        OneBitMinHashValue { bits }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand_xorshift::XorShiftRng;

    #[test]
    fn test_one_bit_minhash() {
        let samples = 1000;
        let mut rng = XorShiftRng::seed_from_u64(123);
        let s1 = BagOfWords::new(10000, vec![0, 1, 2, 4, 6, 7, 13, 22, 53]);
        let s2 = BagOfWords::new(10000, vec![0, 1, 2, 4, 6, 7, 13, 22, 53, 54]);
        let similarity = Jaccard::jaccard(&s1, &s2);
        let k = 64;
        let mut sum_squared_error = 0.0;
        let mut sum_preds = 0.0;

        for _ in 0..samples {
            let sketcher = OneBitMinHash::new(k, &mut rng);
            let h1 = sketcher.sketch(&s1);
            let h2 = sketcher.sketch(&s2);
            let predicted = SketchEstimate::estimate(&h1, &h2);
            let error = predicted - similarity;
            sum_squared_error += error * error;
            sum_preds += predicted
        }

        let mean_squared_error = sum_squared_error / samples as f64;
        let prediction = sum_preds / samples as f64;

        println!("Pred: {}, MSE: {}", prediction, mean_squared_error);
        assert!((prediction - similarity).abs() < 0.01);
    }

    #[test]
    fn test_simhash() {
        let samples = 1000;
        let mut rng = XorShiftRng::seed_from_u64(123);
        let s1 = UnitNormVector::new(vec![1.0, 0.2, 0.3, 0.7]);
        let s2 = UnitNormVector::new(vec![4.0, 0.2, 2.3, 0.7]);
        let similarity = InnerProduct::cosine(&s1, &s2);
        let k = 512;
        let mut sum_squared_error = 0.0;
        let mut sum_preds = 0.0;

        for _ in 0..samples {
            let sketcher = LongSimHash::new(k, s1.dim(), &mut rng);
            let h1 = sketcher.sketch(&s1);
            let h2 = sketcher.sketch(&s2);
            let predicted = SketchEstimate::estimate(&h1, &h2);
            let error = predicted - similarity;
            sum_squared_error += error * error;
            sum_preds += predicted
        }

        let mean_squared_error = sum_squared_error / samples as f64;
        let prediction = sum_preds / samples as f64;

        println!(
            "actual: {}, predicted: {}, MSE: {}",
            similarity, prediction, mean_squared_error
        );
        assert!((prediction - similarity).abs() < 0.01);
    }
}
