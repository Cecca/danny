use crate::lsh::*;
use crate::types::*;
use packed_simd::u64x2;
use packed_simd::u64x4;
use packed_simd::u64x8;
use rand::Rng;
use statrs::distribution::Binomial;
use statrs::distribution::Discrete;
use std::marker::PhantomData;

pub trait Sketcher {
    type Input;
    type Output;

    fn sketch(&self, v: &Self::Input) -> Self::Output;
}

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

/// A 0-bits sketch
#[derive(Debug, Clone, Copy, Abomonation, Hash, Eq, PartialEq)]
pub struct Sketch0;

impl BitBasedSketch for Sketch0 {
    fn different_bits(&self, _other: &Self) -> u32 {
        0
    }
    fn same_bits(&self, _other: &Self) -> u32 {
        0
    }
    fn num_bits(&self) -> usize {
        0
    }
}

#[derive(Clone)]
pub struct Sketcher0<T, F> {
    _mark_a: PhantomData<T>,
    _mark_b: PhantomData<F>,
}

impl FromCosine for Sketch0 {
    type SketcherType = Sketcher0<UnitNormVector, Hyperplane>;
    fn from_cosine<R: Rng>(dim: usize, rng: &mut R) -> Self::SketcherType {
        Sketcher0 {
            _mark_a: PhantomData::<UnitNormVector>,
            _mark_b: PhantomData::<Hyperplane>,
        }
    }
}

impl FromJaccard for Sketch0 {
    type SketcherType = Sketcher0<BagOfWords, OneBitMinHash>;
    fn from_jaccard<R: Rng>(rng: &mut R) -> Self::SketcherType {
        Sketcher0 {
            _mark_a: PhantomData::<BagOfWords>,
            _mark_b: PhantomData::<OneBitMinHash>,
        }
    }
}

impl<T, F> Sketcher for Sketcher0<T, F>
where
    F: LSHFunction<Input = T, Output = u32>,
{
    type Input = T;
    type Output = Sketch0;
    fn sketch(&self, v: &Self::Input) -> Self::Output {
        Sketch0
    }
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
        let distr = Binomial::new(p, k as u64).expect("Error creating the distribution");
        let mut prob_different_bits = 0.0;
        let mut bit_threshold = 0;
        while prob_different_bits < 1.0 - epsilon {
            prob_different_bits += distr.pmf(bit_threshold);
            bit_threshold += 1;
        }
        debug!(
            "Using bit thresold {} of different bits to reject (epsilon {}, mean {}",
            bit_threshold,
            epsilon,
            k as f64 * p,
        );
        SketchPredicate {
            bit_threshold: bit_threshold as u32,
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

#[derive(Debug, Clone, Copy, Abomonation, Hash, Eq, PartialEq)]
pub struct Sketch256 {
    data: [u64; 4],
}

impl BitBasedSketch for Sketch256 {
    fn different_bits(&self, other: &Self) -> u32 {
        (u64x4::from_slice_unaligned(&self.data) ^ u64x4::from_slice_unaligned(&other.data))
            .count_ones()
            .wrapping_sum() as u32
    }
    fn same_bits(&self, other: &Self) -> u32 {
        (u64x4::from_slice_unaligned(&self.data) ^ u64x4::from_slice_unaligned(&other.data))
            .count_zeros()
            .wrapping_sum() as u32
    }
    fn num_bits(&self) -> usize {
        256
    }
}

#[derive(Clone)]
pub struct Sketcher256<T, F>
where
    F: LSHFunction<Input = T, Output = u32>,
{
    lsh_functions: [F; 8],
}

impl FromCosine for Sketch256 {
    type SketcherType = Sketcher256<UnitNormVector, Hyperplane>;
    fn from_cosine<R: Rng>(dim: usize, rng: &mut R) -> Self::SketcherType {
        Sketcher256 {
            lsh_functions: [
                Hyperplane::new(32, dim, rng),
                Hyperplane::new(32, dim, rng),
                Hyperplane::new(32, dim, rng),
                Hyperplane::new(32, dim, rng),
                Hyperplane::new(32, dim, rng),
                Hyperplane::new(32, dim, rng),
                Hyperplane::new(32, dim, rng),
                Hyperplane::new(32, dim, rng),
            ],
        }
    }
}

impl FromJaccard for Sketch256 {
    type SketcherType = Sketcher256<BagOfWords, OneBitMinHash>;
    fn from_jaccard<R: Rng>(rng: &mut R) -> Self::SketcherType {
        Sketcher256 {
            lsh_functions: [
                OneBitMinHash::new(32, rng),
                OneBitMinHash::new(32, rng),
                OneBitMinHash::new(32, rng),
                OneBitMinHash::new(32, rng),
                OneBitMinHash::new(32, rng),
                OneBitMinHash::new(32, rng),
                OneBitMinHash::new(32, rng),
                OneBitMinHash::new(32, rng),
            ],
        }
    }
}

impl<T, F> Sketcher for Sketcher256<T, F>
where
    F: LSHFunction<Input = T, Output = u32>,
{
    type Input = T;
    type Output = Sketch256;
    fn sketch(&self, v: &Self::Input) -> Self::Output {
        Sketch256 {
            data: [
                ((self.lsh_functions[0].hash(v) as u64) << 32)
                    | (self.lsh_functions[1].hash(v) as u64),
                ((self.lsh_functions[2].hash(v) as u64) << 32)
                    | (self.lsh_functions[3].hash(v) as u64),
                ((self.lsh_functions[4].hash(v) as u64) << 32)
                    | (self.lsh_functions[5].hash(v) as u64),
                ((self.lsh_functions[6].hash(v) as u64) << 32)
                    | (self.lsh_functions[7].hash(v) as u64),
            ],
        }
    }
}

#[derive(Debug, Clone, Copy, Abomonation, Hash, Eq, PartialEq)]
pub struct Sketch512 {
    data: [u64; 8],
}

impl BitBasedSketch for Sketch512 {
    fn different_bits(&self, other: &Self) -> u32 {
        (u64x8::from_slice_unaligned(&self.data) ^ u64x8::from_slice_unaligned(&other.data))
            .count_ones()
            .wrapping_sum() as u32
    }
    fn same_bits(&self, other: &Self) -> u32 {
        (u64x8::from_slice_unaligned(&self.data) ^ u64x8::from_slice_unaligned(&other.data))
            .count_zeros()
            .wrapping_sum() as u32
    }
    fn num_bits(&self) -> usize {
        512
    }
}

#[derive(Clone)]
pub struct Sketcher512<T, F>
where
    F: LSHFunction<Input = T, Output = u32>,
{
    lsh_functions: [F; 16],
}

impl FromCosine for Sketch512 {
    type SketcherType = Sketcher512<UnitNormVector, Hyperplane>;
    fn from_cosine<R: Rng>(dim: usize, rng: &mut R) -> Self::SketcherType {
        Sketcher512 {
            lsh_functions: [
                Hyperplane::new(32, dim, rng),
                Hyperplane::new(32, dim, rng),
                Hyperplane::new(32, dim, rng),
                Hyperplane::new(32, dim, rng),
                Hyperplane::new(32, dim, rng),
                Hyperplane::new(32, dim, rng),
                Hyperplane::new(32, dim, rng),
                Hyperplane::new(32, dim, rng),
                Hyperplane::new(32, dim, rng),
                Hyperplane::new(32, dim, rng),
                Hyperplane::new(32, dim, rng),
                Hyperplane::new(32, dim, rng),
                Hyperplane::new(32, dim, rng),
                Hyperplane::new(32, dim, rng),
                Hyperplane::new(32, dim, rng),
                Hyperplane::new(32, dim, rng),
            ],
        }
    }
}

impl FromJaccard for Sketch512 {
    type SketcherType = Sketcher512<BagOfWords, OneBitMinHash>;
    fn from_jaccard<R: Rng>(rng: &mut R) -> Self::SketcherType {
        Sketcher512 {
            lsh_functions: [
                OneBitMinHash::new(32, rng),
                OneBitMinHash::new(32, rng),
                OneBitMinHash::new(32, rng),
                OneBitMinHash::new(32, rng),
                OneBitMinHash::new(32, rng),
                OneBitMinHash::new(32, rng),
                OneBitMinHash::new(32, rng),
                OneBitMinHash::new(32, rng),
                OneBitMinHash::new(32, rng),
                OneBitMinHash::new(32, rng),
                OneBitMinHash::new(32, rng),
                OneBitMinHash::new(32, rng),
                OneBitMinHash::new(32, rng),
                OneBitMinHash::new(32, rng),
                OneBitMinHash::new(32, rng),
                OneBitMinHash::new(32, rng),
            ],
        }
    }
}

impl<T, F> Sketcher for Sketcher512<T, F>
where
    F: LSHFunction<Input = T, Output = u32>,
{
    type Input = T;
    type Output = Sketch512;
    fn sketch(&self, v: &Self::Input) -> Self::Output {
        Sketch512 {
            data: [
                ((self.lsh_functions[0].hash(v) as u64) << 32)
                    | (self.lsh_functions[1].hash(v) as u64),
                ((self.lsh_functions[2].hash(v) as u64) << 32)
                    | (self.lsh_functions[3].hash(v) as u64),
                ((self.lsh_functions[4].hash(v) as u64) << 32)
                    | (self.lsh_functions[5].hash(v) as u64),
                ((self.lsh_functions[6].hash(v) as u64) << 32)
                    | (self.lsh_functions[7].hash(v) as u64),
                ((self.lsh_functions[8].hash(v) as u64) << 32)
                    | (self.lsh_functions[9].hash(v) as u64),
                ((self.lsh_functions[10].hash(v) as u64) << 32)
                    | (self.lsh_functions[11].hash(v) as u64),
                ((self.lsh_functions[12].hash(v) as u64) << 32)
                    | (self.lsh_functions[13].hash(v) as u64),
                ((self.lsh_functions[14].hash(v) as u64) << 32)
                    | (self.lsh_functions[15].hash(v) as u64),
            ],
        }
    }
}

pub trait SketchEstimate {
    fn sketch_estimate<S: BitBasedSketch>(a: &S, b: &S) -> f64;
    fn collision_probability_estimate<S: BitBasedSketch>(a: &S, b: &S) -> f64;
}

impl SketchEstimate for UnitNormVector {
    fn sketch_estimate<S: BitBasedSketch>(a: &S, b: &S) -> f64 {
        let p = f64::from(a.same_bits(b)) / (a.num_bits() as f64);
        (std::f64::consts::PI * (1.0 - p)).cos()
    }
    fn collision_probability_estimate<S: BitBasedSketch>(a: &S, b: &S) -> f64 {
        f64::from(a.same_bits(b)) / (a.num_bits() as f64)
    }
}

impl SketchEstimate for BagOfWords {
    fn sketch_estimate<S: BitBasedSketch>(a: &S, b: &S) -> f64 {
        let p = f64::from(a.same_bits(b)) / (a.num_bits() as f64);
        2.0 * p - 1.0
    }
    fn collision_probability_estimate<S: BitBasedSketch>(a: &S, b: &S) -> f64 {
        f64::from(a.same_bits(b)) / (a.num_bits() as f64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::measure::*;
    use rand::SeedableRng;
    use rand_xorshift::XorShiftRng;

    #[test]
    fn test_one_bit_minhash() {
        let samples = 1000;
        let mut rng = XorShiftRng::seed_from_u64(123);
        let s1 = BagOfWords::new(10000, vec![0, 1, 2, 4, 6, 7, 13, 22, 53]);
        let s2 = BagOfWords::new(10000, vec![0, 1, 2, 4, 6, 7, 13, 22, 53, 54]);
        let similarity = Jaccard::jaccard(&s1, &s2);
        let _k = 64;
        let mut sum_squared_error = 0.0;
        let mut sum_preds = 0.0;

        for _ in 0..samples {
            let sketcher = Sketch64::from_jaccard(&mut rng);
            let h1 = sketcher.sketch(&s1);
            let h2 = sketcher.sketch(&s2);
            let predicted = BagOfWords::sketch_estimate(&h1, &h2);
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
        let mut sum_squared_error = 0.0;
        let mut sum_preds = 0.0;

        for _ in 0..samples {
            let sketcher = Sketch64::from_cosine(s1.dim(), &mut rng);
            let h1 = sketcher.sketch(&s1);
            let h2 = sketcher.sketch(&s2);
            let predicted = UnitNormVector::sketch_estimate(&h1, &h2);
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
