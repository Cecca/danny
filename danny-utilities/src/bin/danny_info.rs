#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;
extern crate danny;
extern crate env_logger;
extern crate rand;
extern crate rand_xorshift;
extern crate serde;

use danny::io::*;
use danny_base::types::*;
use rand::{Rng, SeedableRng};
use serde::Serialize;
use std::io::Write;
use std::path::PathBuf;

fn main() {
    let matches = clap_app!(sampledata =>
        (version: "0.1")
        (author: "Matteo Ceccarello <mcec@itu.dk>")
        (about: "Sample the given dataset")
        (@arg MEASURE: -m --measure +takes_value +required "The measure")
        (@arg INPUT: +required "The input path")
    )
    .get_matches();

    let seed = 14598724;

    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .format(move |buf, record| writeln!(buf, "{}: {}", record.level(), record.args()))
        .init();

    let input: PathBuf = matches.value_of("INPUT").unwrap().into();
    let measure: String = matches.value_of("MEASURE").unwrap().to_owned();
    match measure.as_ref() {
        "jaccard" => {
            let first = BagOfWords::peek_one(input.clone());
            let n = <BagOfWords as ReadBinaryFile>::num_elements(input);
            let dim = first.universe;
            println!(
                "Jaccard similarity dataset: {} vectors, {} elements universe",
                n, dim
            );
        }
        "cosine" => {
            let first = Vector::peek_one(input.clone());
            let n = <Vector as ReadBinaryFile>::num_elements(input);
            let dim = first.dim();
            println!(
                "Cosine similarity dataset: {} vectors, {} elements universe",
                n, dim
            );
            unimplemented!()
        }
        e => panic!("Unsupported measure {}", e),
    };
}
