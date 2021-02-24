// Benchmark to evaluate the performance of different
// components of the verification component

use danny::io::*;
use danny_base::sketch::*;
use danny_base::types::*;
use danny_base::{lsh::*, sketch::Sketcher512};
use rand::prelude::*;
use rand_xorshift::XorShiftRng;
use serde::Deserialize;
use std::time::{Duration, Instant};

fn bench_similarity<T, P>(pairs: &[(T, T)], samples: usize, predicate: P) -> Vec<Duration>
where
    T: Clone,
    P: Fn(&T, &T) -> bool,
{
    let mut sink = false;
    let mut estimates: Vec<Duration> = pairs
        .iter()
        .map(|(q, t)| {
            let timer = Instant::now();
            for _ in 0..samples {
                sink ^= predicate(q, t);
            }
            timer.elapsed() / samples as u32
        })
        .collect();
    eprintln!("sink {}", sink);

    estimates.sort_unstable();
    estimates
}

fn bench_sketching<T, H>(
    pairs: &[(T, T)],
    samples: usize,
    sketcher: &Sketcher512<T, H>,
    predicate: &SketchPredicate<Sketch512>,
) -> Vec<Duration>
where
    T: Clone,
    H: LSHFunction<Input = T, Output = u32> + Clone,
{
    let mut sink = false;
    let mut estimates: Vec<Duration> = pairs
        .iter()
        .map(|(q, t)| {
            let q_sketch = sketcher.sketch(q);
            let t_sketch = sketcher.sketch(t);
            let timer = Instant::now();
            for _ in 0..samples {
                sink ^= predicate.eval(&q_sketch, &t_sketch);
            }
            timer.elapsed() / samples as u32
        })
        .collect();
    eprintln!("sink {}", sink);

    estimates.sort_unstable();
    estimates
}

fn bench_deduplicate<T, H>(
    pairs: &[(T, T)],
    samples: usize,
    hasher: &TensorCollection<H>,
) -> Vec<Duration>
where
    T: Clone,
    H: LSHFunction<Input = T, Output = u32> + Clone,
{
    let mut sink = false;
    let mut estimates: Vec<Duration> = pairs
        .iter()
        .map(|(q, t)| {
            let q_pool = hasher.pool(q);
            let t_pool = hasher.pool(t);
            let repetitions = hasher.repetitions();
            let mut duration = Duration::from_nanos(0);
            for repetition in 0..repetitions {
                let timer = Instant::now();
                for _ in 0..samples {
                    sink ^= hasher.already_seen(&q_pool, &t_pool, repetition);
                }
                duration += timer.elapsed();
            }
            duration / (repetitions * samples) as u32
        })
        .collect();
    eprintln!("sink {}", sink);

    estimates.sort_unstable();
    estimates
}

fn bench<T, H, P, B, R>(
    path: String,
    samples: usize,
    n_targets: usize,
    predicate: P,
    builder: B,
    sketcher: Sketcher512<T, H>,
    sketch_predicate: SketchPredicate<Sketch512>,
    rng: &mut R,
) where
    for<'de> T: ReadBinaryFile + Deserialize<'de> + SketchEstimate + Clone,
    H: LSHFunction<Input = T, Output = u32> + Clone,
    P: Fn(&T, &T) -> bool + Copy,
    R: Rng + SeedableRng + Send + Sync + Clone + 'static,
    B: Fn(usize, &mut R) -> H + Sized + Send + Sync + Clone + 'static,
{
    let hasher = TensorCollection::new(8, 0.5, 0.8, builder, rng);

    eprintln!("reading data... ");
    let mut data = Vec::new();
    ReadBinaryFile::read_binary(path.clone().into(), |_| true, |_, v: T| data.push(v));
    data.shuffle(rng);
    eprintln!("loaded and shuffled data");

    eprintln!("building close and far lists... ");
    let queries = &data[..n_targets];
    let close: Vec<(T, T)> = queries
        .iter()
        .flat_map(|q| {
            let q = q.clone();
            let q1 = q.clone();
            data.iter()
                .filter(move |x| predicate(&q, x))
                .cloned()
                .take(n_targets)
                .map(move |t| (q1.clone(), t))
        })
        .collect();
    let far: Vec<(T, T)> = queries
        .iter()
        .flat_map(|q| {
            let q = q.clone();
            let q1 = q.clone();
            data.iter()
                .filter(move |x| !predicate(&q, x))
                .cloned()
                .take(n_targets)
                .map(move |t| (q1.clone(), t))
        })
        .collect();
    eprintln!("done");

    eprintln!("Estimating cost of similarity predicate for close points");
    let similarity_close_estimates = bench_similarity(&close, samples, &predicate);
    eprintln!(
        "median time to verify similarity of close points {:?}",
        similarity_close_estimates[similarity_close_estimates.len() / 2]
    );
    eprintln!("Estimating cost of similarity predicate for far points");
    let similarity_far_estimates = bench_similarity(&far, samples, &predicate);
    eprintln!(
        "average time to verify similarity of far points {:?}",
        similarity_far_estimates[similarity_far_estimates.len() / 2]
    );

    eprintln!("Estimating cost of sketch predicate for close points");
    let sketch_close_estimates = bench_sketching(&close, samples, &sketcher, &sketch_predicate);
    eprintln!(
        "median time to verify sketch of close points {:?}",
        sketch_close_estimates[sketch_close_estimates.len() / 2]
    );
    eprintln!("Estimating cost of sketch predicate for far points");
    let sketch_far_estimates = bench_sketching(&far, samples, &sketcher, &sketch_predicate);
    eprintln!(
        "average time to verify sketch of far points {:?}",
        sketch_far_estimates[sketch_far_estimates.len() / 2]
    );

    eprintln!("Estimating cost of dedup predicate for close points");
    let dedup_close_estimates = bench_deduplicate(&close, samples, &hasher);
    eprintln!(
        "median time to dedup close points {:?}",
        dedup_close_estimates[dedup_close_estimates.len() / 2]
    );

    eprintln!("Estimating cost of dedup predicate for far points");
    let dedup_far_estimates = bench_deduplicate(&far, samples, &hasher);
    eprintln!(
        "median time to dedup far points {:?}",
        dedup_far_estimates[dedup_far_estimates.len() / 2]
    );

    for (sketch, (dedup, verify)) in sketch_close_estimates.iter().zip(
        dedup_close_estimates
            .iter()
            .zip(similarity_close_estimates.iter()),
    ) {
        println!(
            "{},close,{},{},{}",
            path,
            sketch.as_nanos(),
            dedup.as_nanos(),
            verify.as_nanos()
        );
    }

    for (sketch, (dedup, verify)) in sketch_far_estimates.iter().zip(
        dedup_far_estimates
            .iter()
            .zip(similarity_far_estimates.iter()),
    ) {
        println!(
            "{},far,{},{},{}",
            path,
            sketch.as_nanos(),
            dedup.as_nanos(),
            verify.as_nanos()
        );
    }
}

fn main() {
    let path = std::env::args().nth(1).expect("missing dataset path");
    let samples = std::env::args()
        .nth(2)
        .expect("missing number of samples")
        .parse::<usize>()
        .expect("number of samples should be integer");
    let n_targets = std::env::args()
        .nth(3)
        .expect("missing number of queries")
        .parse::<usize>()
        .expect("number of targets should be integer");
    let threshold = 0.5;
    let epsilon = 0.01;

    let mut rng = XorShiftRng::seed_from_u64(4598729857);

    match content_type(&path) {
        ContentType::Vector => {
            let dim = Vector::peek_one(path.clone().into()).dim();
            bench(
                path,
                samples,
                n_targets,
                move |a, b| Vector::cosine_predicate(a, b, threshold),
                Hyperplane::builder(dim),
                Sketch512::from_cosine(dim, &mut rng),
                SketchPredicate::cosine(512, threshold, epsilon),
                &mut rng,
            )
        }
        ContentType::BagOfWords => bench(
            path,
            samples,
            n_targets,
            move |a, b| BagOfWords::jaccard_predicate(a, b, threshold),
            OneBitMinHash::builder(),
            Sketch512::from_jaccard(&mut rng),
            SketchPredicate::jaccard(512, threshold, epsilon),
            &mut rng,
        ),
    }
}
