#[macro_use]
extern crate log;
extern crate log_panics;
// extern crate syslog;
extern crate env_logger;
#[macro_use]
extern crate serde_derive;
extern crate envy;
extern crate serde;
#[macro_use]
extern crate clap;
#[macro_use]
extern crate abomonation;
extern crate core;
extern crate heapsize;
extern crate probabilistic_collections;
extern crate rand;
extern crate rand_xorshift;
extern crate smallbitvec;
extern crate timely;

mod baseline;
mod config;
/// Provides facilities to read and write files
mod io;
mod logging;
mod lsh;
mod measure;
mod operators;
/// This module collects algorithms to compute on some datasets,
/// which might be useful to understand their behaviour
mod stats;
mod types;

use crate::config::Config;
use crate::lsh::LSHFunction;
use crate::measure::{Cosine, Jaccard};
use crate::types::{BagOfWords, VectorWithNorm};

fn main() {
    let config = Config::get();
    crate::logging::init_logging(&config);

    let matches = clap_app!(danny =>
        (version: "0.1")
        (author: "Matteo Ceccarello <mcec@itu.dk>")
        (about: format!("Distributed Approximate Near Neighbours, Yo!\n\n{}", Config::help_str()).as_ref())
        (@arg ALGORITHM: -a --algorithm +takes_value "The algorithm to be used: (fixed-lsh, all-2-all)")
        (@arg MEASURE: -m --measure +required +takes_value "The similarity measure to be used")
        (@arg K: -k +takes_value "The number of concatenations of the hash function")
        (@arg THRESHOLD: -r --range +required +takes_value "The similarity threshold")
        (@arg DIMENSION: --dimension --dim +takes_value "The dimension of the space, required for Hyperplane LSH")
        (@arg LEFT: +required "Path to the left hand side of the join")
        (@arg RIGHT: +required "Path to the right hand side of the join")
    )
    .get_matches();

    let measure = matches
        .value_of("MEASURE")
        .expect("measure is a required argument");
    let threshold: f64 = matches
        .value_of("THRESHOLD")
        .expect("range is a required argument")
        .parse()
        .expect("Cannot convert the threshold into a f64");
    let left_path = matches
        .value_of("LEFT")
        .expect("left is a required argument")
        .to_owned();
    let right_path = matches
        .value_of("RIGHT")
        .expect("right is a required argument")
        .to_owned();
    let algorithm = matches
        .value_of("ALGORITHM")
        .unwrap_or("all-2-all")
        .to_owned();

    // Build timely context
    let timely_builder = config.get_timely_builder();
    println!("Starting...");
    let mut rng = config.get_random_generator(0);

    let count = match algorithm.as_ref() {
        "fixed-lsh" => match measure.as_ref() {
            "cosine" => {
                let k: usize = matches
                    .value_of("K")
                    .unwrap()
                    .parse()
                    .expect("k should be an unsigned integer");
                let repetitions = lsh::Hyperplane::repetitions_at_range(threshold, k);
                let dim: usize = matches
                    .value_of("DIMENSION")
                    .expect("Dimension is required for Hyperplane LSH.")
                    .parse()
                    .expect("The dimension must be an unsigned integer");
                lsh::fixed_param_lsh::<VectorWithNorm, _, _, _>(
                    &left_path,
                    &right_path,
                    lsh::Hyperplane::collection(k, repetitions, dim, &mut rng),
                    move |a, b| Cosine::cosine(a, b) >= threshold,
                    config,
                )
            }
            "jaccard" => {
                let k: usize = matches
                    .value_of("K")
                    .unwrap()
                    .parse()
                    .expect("k should be an unsigned integer");
                let repetitions = lsh::MinHash::repetitions_at_range(threshold, k);
                lsh::fixed_param_lsh::<BagOfWords, _, _, _>(
                    &left_path,
                    &right_path,
                    lsh::MinHash::collection(k, repetitions, &mut rng),
                    move |a, b| Jaccard::jaccard(a, b) >= threshold,
                    config,
                )
            }
            _ => unimplemented!("Unknown measure {}", measure),
        },
        "all-2-all" => match measure.as_ref() {
            "cosine" => baseline::all_pairs_parallel::<VectorWithNorm, _>(
                threshold,
                &left_path,
                &right_path,
                Cosine::cosine,
                timely_builder,
            ),
            "jaccard" => baseline::all_pairs_parallel::<BagOfWords, _>(
                threshold,
                &left_path,
                &right_path,
                Jaccard::jaccard,
                timely_builder,
            ),
            _ => unimplemented!(),
        },
        "seq-all-2-all" => match measure.as_ref() {
            "cosine" => baseline::sequential::<VectorWithNorm, _>(
                threshold,
                &left_path,
                &right_path,
                Cosine::cosine,
            ),
            "jaccard" => baseline::sequential::<BagOfWords, _>(
                threshold,
                &left_path,
                &right_path,
                Jaccard::jaccard,
            ),
            _ => unimplemented!(),
        },
        _ => unimplemented!("Unknown algorithm {}", algorithm),
    };
    println!("Pairs above similarity {} are {}", threshold, count);
}
