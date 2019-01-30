use crate::types::*;

pub trait Measure {
    fn similarity(a: &Self, b: &Self) -> f64;
}

pub trait InnerProduct {
    fn inner_product(a: &Self, b: &Self) -> f64;
    fn norm_2(a: &Self) -> f64 {
        Self::inner_product(a, a).sqrt()
    }
}

impl InnerProduct for Vec<f32> {
    fn inner_product(a: &Vec<f32>, b: &Vec<f32>) -> f64 {
        a.iter().zip(b.iter()).map(|(x, y)| x * y).sum::<f32>() as f64
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

    fn norm_2(a: &UnitNormVector) -> f64 {
        1.0
    }
}

pub trait Cosine {
    fn cosine(a: &Self, b: &Self) -> f64;
}

impl<T> Cosine for T
where
    T: InnerProduct,
{
    fn cosine(a: &T, b: &T) -> f64 {
        T::inner_product(a, b) / (T::norm_2(a) * T::norm_2(b))
    }
}

impl<T> Measure for T
where
    T: Cosine,
{
    fn similarity(a: &Self, b: &Self) -> f64 {
        Self::cosine(a, b)
    }
}

pub trait Jaccard {
    fn jaccard(a: &Self, b: &Self) -> f64;
}

#[cfg(test)]
mod tests {

    use crate::measure::Cosine;

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
        let actual = generic_dist(&a, &b, Cosine::cosine);
        assert!((expected - actual).abs() < 10e-10_f64)
    }

}
