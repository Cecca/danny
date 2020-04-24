extern crate packed_simd;

use crate::types::*;
use packed_simd::f32x8;

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
    #[allow(clippy::cast_lossless)]
    fn inner_product(a: &Vec<f32>, b: &Vec<f32>) -> f64 {
        let ac = a.chunks_exact(8);
        let bc = b.chunks_exact(8);
        let rem = ac
            .remainder()
            .iter()
            .zip(bc.remainder().iter())
            .map(|(a, b)| a * b)
            .sum::<f32>() as f64;
        let part = ac
            .map(f32x8::from_slice_unaligned)
            .zip(bc.map(f32x8::from_slice_unaligned))
            .map(|(a, b)| a * b)
            .sum::<f32x8>()
            .sum() as f64;
        part + rem
    }

    #[allow(clippy::cast_lossless)]
    fn norm_2(a: &Vec<f32>) -> f64 {
        let squared_sum: f64 = a.iter().map(|a| a * a).sum::<f32>() as f64;
        squared_sum.sqrt()
    }
}

impl InnerProduct for Vector {
    fn inner_product(a: &Vector, b: &Vector) -> f64 {
        InnerProduct::inner_product(a.data(), &b.data())
    }

    fn norm_2(_a: &Vector) -> f64 {
        1.0
    }

    fn cosine(a: &Vector, b: &Vector) -> f64 {
        Self::inner_product(a, b)
    }
}

pub trait Jaccard {
    fn jaccard(a: &Self, b: &Self) -> f64;
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::distributions::{Distribution, Normal};
    use rand::SeedableRng;
    use rand_xorshift::XorShiftRng;

    fn generic_dist<T, F>(a: &T, b: &T, dist: F) -> f64
    where
        F: Fn(&T, &T) -> f64,
    {
        dist(a, b)
    }

    #[test]
    fn test1() {
        let a: Vec<f32> = vec![1f32, 2.0, 1f32, 1f32];
        let b: Vec<f32> = vec![1f32, 2.0, 1f32, 1f32];
        let expected = 1.0;
        let actual = generic_dist(&a, &b, InnerProduct::cosine);
        println!("Actual distance is {}", actual);
        assert!((expected - actual).abs() < 10e-10_f64)
    }

    #[test]
    fn check_simd() {
        let mut rng = XorShiftRng::seed_from_u64(121245);
        for dim in [3, 300, 301].iter() {
            let dim = *dim;
            let dist = Normal::new(0.0, 1.0);
            let a: Vec<f32> = dist
                .sample_iter(&mut rng)
                .take(dim)
                .map(|x| x as f32)
                .collect();
            let b: Vec<f32> = dist
                .sample_iter(&mut rng)
                .take(dim)
                .map(|x| x as f32)
                .collect();
            let expected: f64 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum::<f32>() as f64;
            let actual = InnerProduct::inner_product(&a, &b);
            println!(
                "{} != {} (diff {})",
                expected,
                actual,
                (expected - actual).abs()
            );
            assert!((expected - actual).abs() < 10e-5_f64);
        }
    }
}
