#[macro_use]
extern crate log;
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
extern crate rand;
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
use crate::measure::{Cosine, Jaccard};
use crate::types::{BagOfWords, VectorWithNorm};
use rand::rngs::StdRng;
use rand::SeedableRng;

fn main() {
    let config = Config::get();
    crate::logging::init_logging(&config);

    let matches = clap_app!(danny =>
        (version: "0.1")
        (author: "Matteo Ceccarello <mcec@itu.dk>")
        (about: format!("Distributed Approximate Near Neighbours, Yo!\n\n{}", Config::help_str()).as_ref())
        (@arg MEASURE: -m --measure +required +takes_value "The similarity measure to be used")
        (@arg THRESHOLD: -r --range +required +takes_value "The similarity threshold")
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

    // Build timely context
    let timely_builder = config.get_timely_builder();
    println!("Starting...");
    let mut rng = StdRng::seed_from_u64(123);

    let count = match measure.as_ref() {
        "cosine" => lsh::fixed_param_lsh::<VectorWithNorm, _, _, _>(
            &left_path,
            &right_path,
            lsh::Hyperplane::collection(2, 3, 300, &mut rng),
            move |a, b| Cosine::cosine(a, b) >= threshold,
            timely_builder,
        ),
        // "cosine" => baseline::all_pairs_parallel::<VectorWithNorm, _>(
        //     threshold,
        //     &left_path,
        //     &right_path,
        //     Cosine::cosine,
        //     timely_builder,
        // ),
        "jaccard" => baseline::all_pairs_parallel::<BagOfWords, _>(
            threshold,
            &left_path,
            &right_path,
            Jaccard::jaccard,
            timely_builder,
        ),
        _ => unimplemented!(),
    };
    println!("Pairs above similarity {} are {}", threshold, count);
}
