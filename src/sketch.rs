use crate::lsh::*;
use crate::measure::*;
use crate::types::*;
use rand::distributions::{Distribution, Normal};
use rand::Rng;

pub trait SketchEstimate {
    fn estimate(a: &Self, b: &Self) -> f64;
}

impl SketchEstimate for SimHashValue {
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

impl SketchEstimate for OneBitMinHashValue {
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
                part = part << 1;
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
    hashers: Vec<TabulatedHasher>,
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
        let mut hashers = Vec::with_capacity(k);
        for _ in 0..k {
            hashers.push(TabulatedHasher::new(rng));
        }
        OneBitMinHash { k, hashers }
    }
}

impl Sketcher for OneBitMinHash {
    type Input = BagOfWords;
    type Output = OneBitMinHashValue;

    fn sketch(&self, v: &BagOfWords) -> OneBitMinHashValue {
        let num_elems = (self.k as f32 / 32.0).ceil() as usize;
        let mut bits = Vec::with_capacity(num_elems);
        let mut part = 0u32;

        for (i, hasher) in self.hashers.iter().enumerate() {
            if i % 32 == 0 && i > 0 {
                bits.push(part);
                part = 0;
            }
            let h = v.words().iter().map(|w| hasher.hash(*w)).min().unwrap();
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
    use rand::Rng;
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
