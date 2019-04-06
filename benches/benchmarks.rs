#[macro_use]
extern crate criterion;
extern crate danny;

use criterion::Criterion;
use danny::bloom::*;
use danny::lsh::functions::*;
use danny::measure::InnerProduct;
use danny::types::UnitNormVector;
use rand::RngCore;
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

fn bench_bloom(c: &mut Criterion) {
    let k = 5;
    let bits = 6_695_021_038;
    let elements = 100_000;
    c.bench_function("Atomic Bloom filter", move |bencher| {
        let mut rng = XorShiftRng::seed_from_u64(124);
        let bloom = AtomicBloomFilter::new(bits, k, &mut rng);
        let mut elems = Vec::with_capacity(elements);
        for _ in 0..elements {
            elems.push(rng.next_u64());
        }
        bencher.iter(|| {
            for x in elems.iter() {
                let x = (*x, *x);
                let _already_in = bloom.test_and_insert(&x);
            }
        });
    });

    c.bench_function("Standard Bloom filter", move |bencher| {
        let mut rng = XorShiftRng::seed_from_u64(124);
        let mut bloom = BloomFilter::from_params(elements, bits, k, &mut rng);
        let mut elems = Vec::with_capacity(elements);
        for _ in 0..elements {
            elems.push(rng.next_u64());
        }
        bencher.iter(|| {
            for x in elems.iter() {
                let x = (*x, *x);
                bloom.insert(&x);
            }
        });
    });
}

criterion_group!(benches, bench_inner_product, bench_hyperplane, bench_bloom);
criterion_main!(benches);
