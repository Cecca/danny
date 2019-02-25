extern crate packed_simd;

use crate::types::*;
use packed_simd::f32x4;

pub trait InnerProduct {
    fn inner_product(a: &Self, b: &Self) -> f64;
    fn norm_2(a: &Self) -> f64 {
        Self::inner_product(a, a).sqrt()
    }
    fn cosine(a: &Self, b: &Self) -> f64 {
        Self::inner_product(a, b) / (Self::norm_2(a) * Self::norm_2(b))
    }
}

impl InnerProduct for Vec<f32> {
    fn inner_product(a: &Vec<f32>, b: &Vec<f32>) -> f64 {
        // a.iter().zip(b.iter()).map(|(x, y)| x * y).sum::<f32>() as f64
        a.chunks_exact(4)
            .map(f32x4::from_slice_unaligned)
            .zip(b.chunks_exact(4).map(f32x4::from_slice_unaligned))
            .map(|(a, b)| a * b)
            .sum::<f32x4>()
            .sum() as f64
    }
    fn norm_2(a: &Vec<f32>) -> f64 {
        let squared_sum: f64 = a.iter().map(|a| a * a).sum::<f32>() as f64;
        squared_sum.sqrt()
    }
}

impl InnerProduct for VectorWithNorm {
    fn inner_product(a: &VectorWithNorm, b: &VectorWithNorm) -> f64 {
        InnerProduct::inner_product(a.data(), &b.data())
    }

    fn norm_2(a: &VectorWithNorm) -> f64 {
        a.norm()
    }
}

impl InnerProduct for UnitNormVector {
    fn inner_product(a: &UnitNormVector, b: &UnitNormVector) -> f64 {
        InnerProduct::inner_product(a.data(), &b.data())
    }

    fn norm_2(_a: &UnitNormVector) -> f64 {
        1.0
    }

    fn cosine(a: &UnitNormVector, b: &UnitNormVector) -> f64 {
        Self::inner_product(a, b)
    }
}

pub trait Jaccard {
    fn jaccard(a: &Self, b: &Self) -> f64;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn generic_dist<T, F>(a: &T, b: &T, dist: F) -> f64
    where
        F: Fn(&T, &T) -> f64,
    {
        dist(a, b)
    }

    #[test]
    fn test1() {
        let a: Vec<f32> = vec![1f32, 2.0];
        let b: Vec<f32> = vec![1f32, 2.0];
        let expected = 1.0;
        let actual = generic_dist(&a, &b, InnerProduct::cosine);
        assert!((expected - actual).abs() < 10e-10_f64)
    }

}
