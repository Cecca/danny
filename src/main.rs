#[macro_use]
extern crate serde_derive;
extern crate envy;
extern crate serde;
#[macro_use]
extern crate clap;
#[macro_use]
extern crate abomonation;
extern crate core;
extern crate timely;

mod baseline;
mod config;
/// Provides facilities to read and write files
mod io;
mod measure;
mod operators;
/// This module collects algorithms to compute on some datasets,
/// which might be useful to understand their behaviour
mod stats;
mod types;

use config::Config;
use measure::{Cosine, Jaccard};
use types::{BagOfWords, VectorWithNorm};

fn main() {
    let config = Config::get();

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

    let mut args = std::env::args();
    args.next(); // Skip executable name

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

    let count = match measure.as_ref() {
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
    };
    println!("Pairs above similarity {} are {}", threshold, count);
}
