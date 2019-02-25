#[macro_use]
extern crate criterion;
extern crate danny;

use criterion::Criterion;
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
}

criterion_group!(benches, bench_inner_product);
criterion_main!(benches);
