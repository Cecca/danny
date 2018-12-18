#[macro_use]
extern crate abomonation;
extern crate core;
extern crate timely;

mod baseline;
/// Provides facilities to read and write files
mod io;
mod measure;
mod operators;
/// This module collects algorithms to compute on some datasets,
/// which might be useful to understand their behaviour
mod stats;
mod types;

use io::ReadDataFile;
use measure::{Cosine, Jaccard};
use operators::*;
use std::iter::Sum;
use std::sync::{Arc, Mutex};
use timely::dataflow::operators::capture::{EventLink, Extract, Replay};
use timely::dataflow::operators::*;
use timely::dataflow::*;
use types::{BagOfWords, VectorWithNorm};

fn main() {
    let mut args = std::env::args();
    args.next(); // Skip executable name

    let workers: usize = args
        .next()
        .expect("number of workers required")
        .parse()
        .expect("unable to convert to integer number of workers");
    let measure = args.next().expect("measure is required");
    let thresh_str = args.next().expect("threshold is required");
    let threshold: f64 = thresh_str
        .parse()
        .expect("Cannot convert the threshold into a f64");
    let left_path = args.next().expect("left path is required");
    let right_path = args.next().expect("right path is required");

    // Build timely context
    let timely_builder = timely::Configuration::Process(workers).try_build().unwrap();

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
