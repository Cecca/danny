use crate::lsh::*;
use crate::measure::*;
use crate::types::*;
use packed_simd::u32x4;
use packed_simd::u64x2;

use rand::distributions::{Distribution, Normal, Uniform};
use rand::Rng;

use std::iter::Iterator;

pub trait BitBasedSketch: Clone + Copy {
    fn different_bits(&self, other: &Self) -> u32;
    fn same_bits(&self, other: &Self) -> u32;
    fn num_bits(&self) -> usize;
}

pub trait FromJaccard: Sized {
    type SketcherType: Sketcher<Input = BagOfWords, Output = Self> + Send + Sync + Clone + 'static;
    fn from_jaccard<R: Rng>(rng: &mut R) -> Self::SketcherType;
}

pub trait FromCosine: Sized {
    type SketcherType: Sketcher<Input = UnitNormVector, Output = Self>
        + Send
        + Sync
        + Clone
        + 'static;
    fn from_cosine<R: Rng>(dim: usize, rng: &mut R) -> Self::SketcherType;
}

#[derive(Debug, Clone, Copy, Abomonation, Hash, Eq, PartialEq)]
pub struct Sketch64 {
    data: u64,
}

impl BitBasedSketch for Sketch64 {
    fn different_bits(&self, other: &Self) -> u32 {
        (self.data ^ other.data).count_ones()
    }
    fn same_bits(&self, other: &Self) -> u32 {
        (self.data ^ other.data).count_zeros()
    }
    fn num_bits(&self) -> usize {
        64
    }
}

#[derive(Clone)]
pub struct Sketcher64<T, F>
where
    F: LSHFunction<Input = T, Output = u32>,
{
    lsh_functions: [F; 2],
}

impl FromCosine for Sketch64 {
    type SketcherType = Sketcher64<UnitNormVector, Hyperplane>;
    fn from_cosine<R: Rng>(dim: usize, rng: &mut R) -> Self::SketcherType {
        Sketcher64 {
            lsh_functions: [Hyperplane::new(32, dim, rng), Hyperplane::new(32, dim, rng)],
        }
    }
}

impl FromJaccard for Sketch64 {
    type SketcherType = Sketcher64<BagOfWords, OneBitMinHash>;
    fn from_jaccard<R: Rng>(rng: &mut R) -> Self::SketcherType {
        Sketcher64 {
            lsh_functions: [OneBitMinHash::new(32, rng), OneBitMinHash::new(32, rng)],
        }
    }
}

impl<T, F> Sketcher for Sketcher64<T, F>
where
    F: LSHFunction<Input = T, Output = u32>,
{
    type Input = T;
    type Output = Sketch64;
    fn sketch(&self, v: &Self::Input) -> Self::Output {
        let a = self.lsh_functions[0].hash(v) as u64;
        let b = self.lsh_functions[1].hash(v) as u64;
        Sketch64 {
            data: (a << 32) | b,
        }
    }
}

#[derive(Debug, Clone, Copy, Abomonation, Hash, Eq, PartialEq)]
pub struct Sketch128 {
    data: [u64; 2],
}

impl BitBasedSketch for Sketch128 {
    fn different_bits(&self, other: &Self) -> u32 {
        (u64x2::from_slice_unaligned(&self.data) ^ u64x2::from_slice_unaligned(&other.data))
            .count_ones()
            .wrapping_sum() as u32
    }

    fn same_bits(&self, other: &Self) -> u32 {
        (u64x2::from_slice_unaligned(&self.data) ^ u64x2::from_slice_unaligned(&other.data))
            .count_zeros()
            .wrapping_sum() as u32
    }
    fn num_bits(&self) -> usize {
        128
    }
}

#[derive(Clone)]
pub struct Sketcher128<T, F>
where
    F: LSHFunction<Input = T, Output = u32>,
{
    lsh_functions: [F; 4],
}

impl FromCosine for Sketch128 {
    type SketcherType = Sketcher128<UnitNormVector, Hyperplane>;
    fn from_cosine<R: Rng>(dim: usize, rng: &mut R) -> Self::SketcherType {
        Sketcher128 {
            lsh_functions: [
                Hyperplane::new(32, dim, rng),
                Hyperplane::new(32, dim, rng),
                Hyperplane::new(32, dim, rng),
                Hyperplane::new(32, dim, rng),
            ],
        }
    }
}

impl FromJaccard for Sketch128 {
    type SketcherType = Sketcher128<BagOfWords, OneBitMinHash>;
    fn from_jaccard<R: Rng>(rng: &mut R) -> Self::SketcherType {
        Sketcher128 {
            lsh_functions: [
                OneBitMinHash::new(32, rng),
                OneBitMinHash::new(32, rng),
                OneBitMinHash::new(32, rng),
                OneBitMinHash::new(32, rng),
            ],
        }
    }
}

impl<T, F> Sketcher for Sketcher128<T, F>
where
    F: LSHFunction<Input = T, Output = u32>,
{
    type Input = T;
    type Output = Sketch128;
    fn sketch(&self, v: &Self::Input) -> Self::Output {
        Sketch128 {
            data: [
                ((self.lsh_functions[0].hash(v) as u64) << 32)
                    | (self.lsh_functions[1].hash(v) as u64),
                ((self.lsh_functions[2].hash(v) as u64) << 32)
                    | (self.lsh_functions[3].hash(v) as u64),
            ],
        }
    }
}

#[derive(Clone, Debug)]
pub struct SketchPredicate<T>
where
    T: BitBasedSketch,
{
    /// The number of bits that can be different between two sketches without having to reject the
    /// pair.
    bit_threshold: u32,
    _marker: std::marker::PhantomData<T>,
}

impl<T> SketchPredicate<T>
where
    T: BitBasedSketch,
{
    pub fn from_probability(k: usize, p: f64, epsilon: f64) -> Self {
        let k = k as f64;
        let delta = (3.0 / (p * k) * (1.0 / epsilon).ln()).sqrt();
        let bit_threshold = ((1.0 + delta) * p * k).ceil() as u32;
        debug!(
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

    /// Return true if the two sketches are associated with vectors
    /// within the similarity threshold this predicate was built with
    pub fn eval(&self, a: &T, b: &T) -> bool {
        a.different_bits(b) <= self.bit_threshold
    }
}

pub trait SketchEstimate {
    fn sketch_estimate<S: BitBasedSketch>(a: &S, b: &S) -> f64;
}

impl SketchEstimate for UnitNormVector {
    fn sketch_estimate<S: BitBasedSketch>(a: &S, b: &S) -> f64 {
        let p = f64::from(a.same_bits(b)) / (a.num_bits() as f64);
        (std::f64::consts::PI * (1.0 - p)).cos()
    }
}

impl SketchEstimate for BagOfWords {
    fn sketch_estimate<S: BitBasedSketch>(a: &S, b: &S) -> f64 {
        let p = f64::from(a.same_bits(b)) / (a.num_bits() as f64);
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
            let sketcher = LongOneBitMinHash::new(k, &mut rng);
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
