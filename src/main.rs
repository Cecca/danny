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
#[macro_use]
extern crate abomonation_derive;
extern crate chrono;
extern crate core;
extern crate heapsize;
extern crate probabilistic_collections;
extern crate rand;
extern crate rand_xorshift;
extern crate serde_json;
extern crate smallbitvec;
extern crate timely;

mod baseline;
mod config;
#[macro_use]
mod experiment;
/// Provides facilities to read and write files
mod io;
#[macro_use]
mod logging;
mod lsh;
mod measure;
mod operators;
/// This module collects algorithms to compute on some datasets,
/// which might be useful to understand their behaviour
mod stats;
mod types;
mod version {
    include!(concat!(env!("OUT_DIR"), "/version.rs"));
}

use crate::baseline::Baselines;
use crate::config::*;
use crate::experiment::Experiment;
use crate::lsh::LSHFunction;
use crate::measure::{Cosine, Jaccard};
use crate::types::{BagOfWords, VectorWithNorm};
use serde_json::Value;
use std::collections::HashMap;

fn main() {
    let config = Config::get();
    crate::logging::init_logging(&config);
    let args = CmdlineConfig::get();
    let mut experiment = Experiment::from_config(&config, &args);

    // Build timely context
    let timely_builder = config.get_timely_builder();
    info!("Starting...");
    let mut rng = config.get_random_generator(0);

    let start = std::time::Instant::now();
    let count = match args.algorithm.as_ref() {
        "fixed-lsh" => match args.measure.as_ref() {
            "cosine" => {
                let k = args.k.expect("K is needed on the command line");
                let repetitions = lsh::Hyperplane::repetitions_at_range(args.threshold, k);
                let dim: usize = args
                    .dimension
                    .expect("Dimension is needed on the command line");
                let threshold = args.threshold;
                lsh::fixed_param_lsh::<VectorWithNorm, _, _, _>(
                    &args.left_path,
                    &args.right_path,
                    lsh::Hyperplane::collection(k, repetitions, dim, &mut rng),
                    move |a, b| Cosine::cosine(a, b) >= threshold,
                    &config,
                    &mut experiment,
                )
            }
            "jaccard" => {
                let k = args.k.expect("K is needed on the command line");
                let repetitions = lsh::MinHash::repetitions_at_range(args.threshold, k);
                let threshold = args.threshold;
                lsh::fixed_param_lsh::<BagOfWords, _, _, _>(
                    &args.left_path,
                    &args.right_path,
                    lsh::MinHash::collection(k, repetitions, &mut rng),
                    move |a, b| Jaccard::jaccard(a, b) >= threshold,
                    &config,
                    &mut experiment,
                )
            }
            _ => unimplemented!("Unknown measure {}", args.measure),
        },
        "all-2-all" => match args.measure.as_ref() {
            "cosine" => baseline::all_pairs_parallel::<VectorWithNorm, _>(
                args.threshold,
                &args.left_path,
                &args.right_path,
                Cosine::cosine,
                &config,
            ),
            "jaccard" => baseline::all_pairs_parallel::<BagOfWords, _>(
                args.threshold,
                &args.left_path,
                &args.right_path,
                Jaccard::jaccard,
                &config,
            ),
            _ => unimplemented!(),
        },
        "seq-all-2-all" => match args.measure.as_ref() {
            "cosine" => baseline::sequential::<VectorWithNorm, _>(
                args.threshold,
                &args.left_path,
                &args.right_path,
                Cosine::cosine,
            ),
            "jaccard" => baseline::sequential::<BagOfWords, _>(
                args.threshold,
                &args.left_path,
                &args.right_path,
                Jaccard::jaccard,
            ),
            _ => unimplemented!(),
        },
        _ => unimplemented!("Unknown algorithm {}", args.algorithm),
    };
    let end = std::time::Instant::now();
    let total_time = end - start;
    let total_time = total_time.as_secs() * 1000 + total_time.subsec_millis() as u64;
    if config.is_master() {
        let recall = Baselines::new(&config)
            .recall(&args.left_path, &args.right_path, args.threshold, count)
            .expect("Could not compute the recall! Missing entry in the baseline file?");
        println!(
            "Pairs above similarity {} are {} (recall {})",
            args.threshold, count, recall
        );
        experiment.append(
            "result",
            row!("output_size" => count, "total_time_ms" => total_time, "recall" => recall),
        );
        experiment.save();
    }
}
