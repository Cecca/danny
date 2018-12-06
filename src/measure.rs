pub trait Measure {
    fn similarity(a: &Self, b: &Self) -> f64;
}

pub trait InnerProduct {
    fn inner_product(a: &Self, b: &Self) -> f64;
    fn norm_2(a: &Self) -> f64 {
        Self::inner_product(a, a).sqrt()
    }
}

impl InnerProduct for Vec<f64> {
    fn inner_product(a: &Vec<f64>, b: &Vec<f64>) -> f64 {
        a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
    }
    fn norm_2(a: &Vec<f64>) -> f64 {
        let squared_sum: f64 = a.iter().map(|a| a * a).sum();
        squared_sum.sqrt()
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

    use measure::Cosine;

    fn generic_dist<T, F>(a: &T, b: &T, dist: F) -> f64
    where
        F: Fn(&T, &T) -> f64,
    {
        dist(a, b)
    }

    #[test]
    fn test1() {
        let a: Vec<f64> = vec![1f64, 2f64];
        let b: Vec<f64> = vec![1f64, 2f64];
        let expected = 1.0;
        let actual = generic_dist(&a, &b, Cosine::cosine);
        assert!((expected - actual).abs() < 10e-10_f64)
    }

}
