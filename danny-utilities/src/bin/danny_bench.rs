// Benchmark to evaluate the performance of different
// components of the verification component

use danny::io::*;
use danny_base::sketch::*;
use danny_base::types::*;
use danny_base::{lsh::*, sketch::Sketcher512};
use rand::prelude::*;
use rand_xorshift::XorShiftRng;
use serde::Deserialize;
use std::iter::FromIterator;
use std::time::{Duration, Instant};

fn bench_similarity<T, P>(queries: &[(T, Vec<T>)], predicate: P) -> Vec<Duration>
where
    T: Clone,
    P: Fn(&T, &T) -> bool,
{
    let mut sink = 0;
    let mut estimates: Vec<Duration> = queries
        .iter()
        .map(|(q, pts)| {
            let timer = Instant::now();
            sink += pts.iter().filter(|x| predicate(q, x)).count();
            timer.elapsed() / pts.len() as u32
        })
        .collect();
    eprintln!("sink {}", sink);

    estimates.sort_unstable();
    estimates
}

fn bench_sketching<T, H>(queries: &[(T, Vec<T>)], sketcher: &Sketcher512<T, H>, predicate: &SketchPredicate<Sketch512>) -> Vec<Duration>
where
    T: Clone,
    H: LSHFunction<Input = T, Output = u32> + Clone,
{
    let mut sink = 0;
    let mut estimates: Vec<Duration> = queries
        .iter()
        .map(|(q, pts)| {
            let q_sketch = sketcher.sketch(q);
            let sketches: Vec<Sketch512> = pts.iter().map(|p| sketcher.sketch(p)).collect();
            let timer = Instant::now();
            sink += sketches.iter().filter(|x| predicate.eval(x, &q_sketch)).count();
            timer.elapsed() / pts.len() as u32
        })
        .collect();
    eprintln!("sink {}", sink);

    estimates.sort_unstable();
    estimates
}

fn bench_deduplicate<T, H>(queries: &[(T, Vec<T>)], hasher: &TensorCollection<H>) -> Vec<Duration>
where
    T: Clone,
    H: LSHFunction<Input = T, Output = u32> + Clone,
{
    let mut sink = 0;
    let mut estimates: Vec<Duration> = queries
        .iter()
        .map(|(q, pts)| {
            let q_pool = hasher.pool(q);
            let pools: Vec<TensorPool> = pts.iter().map(|v| hasher.pool(v)).collect();
            let repetitions = hasher.repetitions();
            let timer = Instant::now();
            for repetition in 0..repetitions {
                sink += pools.iter().filter(|p| hasher.already_seen(p, &q_pool, repetition)).count();
            }
            timer.elapsed() / (pts.len() * repetitions) as u32
        })
        .collect();
    eprintln!("sink {}", sink);

    estimates.sort_unstable();
    estimates
}


fn bench<T, H, P, B, R>(
    path: String,
    n_queries: usize,
    n_targets: usize,
    predicate: P,
    builder: B,
    sketcher: Sketcher512<T, H>,
    sketch_predicate: SketchPredicate<Sketch512>,
    rng: &mut R,
) where
    for<'de> T: ReadBinaryFile + Deserialize<'de> + SketchEstimate + Clone,
    H: LSHFunction<Input = T, Output = u32> + Clone,
    P: Fn(&T, &T) -> bool,
    R: Rng + SeedableRng + Send + Sync + Clone + 'static,
    B: Fn(usize, &mut R) -> H + Sized + Send + Sync + Clone + 'static,
{
    let hasher = TensorCollection::new(8, 0.5, 0.8, builder, rng);

    println!("reading data... ");
    let mut data = Vec::new();
    ReadBinaryFile::read_binary(path.into(), |_| true, |_, v: T| data.push(v));
    data.shuffle(rng);
    println!("loaded and shuffled data");

    println!("building close and far lists... ");
    let queries = &data[..n_queries];
    let close: Vec<(T, Vec<T>)> = queries
        .iter()
        .map(|q| {
            (
                q.clone(),
                Vec::from_iter(
                    data.iter()
                        .filter(|x| predicate(q, x))
                        .cycle()
                        .cloned()
                        .take(n_targets),
                ),
            )
        })
        .collect();
    let far: Vec<(T, Vec<T>)> = queries
        .iter()
        .map(|q| {
            (
                q.clone(),
                Vec::from_iter(
                    data.iter()
                        .filter(|x| !predicate(q, x))
                        .cycle()
                        .cloned()
                        .take(n_targets),
                ),
            )
        })
        .collect();
    println!("done");

    println!("Estimating cost of similarity predicate for close points");
    let similarity_close_estimates = bench_similarity(&close, &predicate);
    println!(
        "median time to verify similarity of close points {:?}",
        similarity_close_estimates[similarity_close_estimates.len() / 2]
    );
    println!("Estimating cost of similarity predicate for far points");
    let similarity_far_estimates = bench_similarity(&far, &predicate);
    println!(
        "average time to verify similarity of far points {:?}",
        similarity_far_estimates[similarity_far_estimates.len() / 2]
    );

    println!("Estimating cost of sketch predicate for close points");
    let sketch_close_estimates = bench_sketching(&close, &sketcher, &sketch_predicate);
    println!(
        "median time to verify sketch of close points {:?}",
        sketch_close_estimates[sketch_close_estimates.len() / 2]
    );
    println!("Estimating cost of sketch predicate for far points");
    let sketch_far_estimates = bench_sketching(&far, &sketcher, &sketch_predicate);
    println!(
        "average time to verify sketch of far points {:?}",
        sketch_far_estimates[sketch_far_estimates.len() / 2]
    );

    println!("Estimating cost of dedup predicate for close points");
    let dedup_close_estimates = bench_deduplicate(&close, &hasher);
    println!(
        "median time to dedup close points {:?}",
        dedup_close_estimates[dedup_close_estimates.len() / 2]
    );

    println!("Estimating cost of dedup predicate for far points");
    let dedup_far_estimates = bench_deduplicate(&far, &hasher);
    println!(
        "median time to dedup far points {:?}",
        dedup_far_estimates[dedup_far_estimates.len() / 2]
    );

}

fn main() {
    let path = std::env::args().nth(1).expect("missing dataset path");
    let n_queries = std::env::args()
        .nth(2)
        .expect("missing number of queries")
        .parse::<usize>()
        .expect("number of queries should be integer");
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
                n_queries,
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
            n_queries,
                n_targets,
            move |a, b| BagOfWords::jaccard_predicate(a, b, threshold),
            OneBitMinHash::builder(),
            Sketch512::from_jaccard(&mut rng),
            SketchPredicate::jaccard(512, threshold, epsilon),
            &mut rng,
        ),
    }
}
