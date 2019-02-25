#[macro_use]
extern crate criterion;
extern crate danny;

use criterion::Criterion;
use danny::lsh::functions::*;
use danny::measure::InnerProduct;
use danny::types::UnitNormVector;
use rand::SeedableRng;
use rand_xorshift::XorShiftRng;

fn bench_inner_product(c: &mut Criterion) {
    c.bench_function("inner product 300 dimensions", |bencher| {
        let mut rng = XorShiftRng::seed_from_u64(124);
        let a = UnitNormVector::random_normal(300, &mut rng);
        let b = UnitNormVector::random_normal(300, &mut rng);
        bencher.iter(|| UnitNormVector::inner_product(&a, &b));
    });

    c.bench_function_over_inputs(
        "several dimensions",
        |bencher, &&dim| {
            let mut rng = XorShiftRng::seed_from_u64(124);
            let a = UnitNormVector::random_normal(dim, &mut rng);
            let b = UnitNormVector::random_normal(dim, &mut rng);
            bencher.iter(|| UnitNormVector::inner_product(&a, &b));
        },
        &[2, 4, 5, 32, 33, 300, 303],
    );
}

fn bench_hyperplane(c: &mut Criterion) {
    c.bench_function("hyperplane 300 dimensions, k=16", |bencher| {
        let mut rng = XorShiftRng::seed_from_u64(124);
        let a = UnitNormVector::random_normal(300, &mut rng);
        let hasher = Hyperplane::new(16, 300, &mut rng);
        bencher.iter(|| hasher.hash(&a));
    });
}

criterion_group!(benches, bench_inner_product, bench_hyperplane);
criterion_main!(benches);
