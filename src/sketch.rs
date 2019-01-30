use crate::lsh::Hyperplane;
use crate::lsh::LSHFunction;
use crate::measure::*;
use crate::types::*;
use rand::distributions::{Distribution, Normal, Uniform};
use rand::Rng;

// Marker structs
pub struct SketchCosine {}
pub struct SketchJaccard {}

pub trait SketchEstimate<T> {
    fn estimate(a: Self, b: Self) -> f64;
}

impl SketchEstimate<SketchCosine> for u32 {
    fn estimate(a: u32, b: u32) -> f64 {
        (a ^ b).count_zeros() as f64 / 32.0
    }
}

impl SketchEstimate<SketchCosine> for Vec<u32> {
    fn estimate(a: Vec<u32>, b: Vec<u32>) -> f64 {
        a.iter()
            .zip(b.iter())
            .map(|(x, y)| (x ^ y).count_zeros())
            .sum::<u32>() as f64
            / (32.0 * a.len() as f64)
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
    type Output = Vec<u32>;

    fn sketch(&self, v: &UnitNormVector) -> Vec<u32> {
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
        sketch
    }
}
