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

fn run<D>(path: &PathBuf, output: &PathBuf, sample_size: usize, seed: u64)
where
    D: ReadBinaryFile + ReadDataFile + WriteBinaryFile + Serialize + Send + Sync + 'static,
{
    let mut rng = rand_xorshift::XorShiftRng::seed_from_u64(seed);
    let mut data = Vec::new();
    let chunks = if path.ends_with(".txt") {
        let num_elems = <D as ReadDataFile>::num_elements(&path.to_path_buf());
        let p = sample_size as f64 / num_elems as f64;
        D::from_file_with_count(path, |c, v| {
            if rng.gen_bool(p) {
                data.push((c, v));
            }
        });
        40
    } else {
        let num_elems = <D as ReadBinaryFile>::num_elements(path.to_path_buf());
        let p = sample_size as f64 / num_elems as f64;
        D::read_binary(
            path.to_path_buf(),
            |_| true,
            |c, v| {
                if rng.gen_bool(p) {
                    data.push((c, v));
                }
            },
        );
        D::num_chunks(path.to_path_buf())
    };
    info!("Sampled dataset with {} elements", data.len());
    // Renumbering happens here!
    WriteBinaryFile::write_binary(output.to_path_buf(), chunks, data.into_iter().map(|p| p.1));
    info!("Written the output file (with renumbering of IDs)");
}

fn main() {
    let matches = clap_app!(sampledata =>
        (version: "0.1")
        (author: "Matteo Ceccarello <mcec@itu.dk>")
        (about: "Sample the given dataset")
        (@arg MEASURE: -m --measure +takes_value +required "The measure")
        (@arg SIZE: -s --size +takes_value +required "The sample size")
        (@arg INPUT: +required "The input path")
        (@arg OUTPUT: +required "The output path")
    )
    .get_matches();

    let seed = 14598724;

    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .format(move |buf, record| writeln!(buf, "{}: {}", record.level(), record.args()))
        .init();

    let input: PathBuf = matches.value_of("INPUT").unwrap().into();
    let output: PathBuf = matches.value_of("OUTPUT").unwrap().into();
    let measure: String = matches.value_of("MEASURE").unwrap().to_owned();
    let size: usize = matches.value_of("SIZE").unwrap().parse::<usize>().unwrap();
    match measure.as_ref() {
        "jaccard" => run::<BagOfWords>(&input, &output, size, seed),
        "cosine" => run::<UnitNormVector>(&input, &output, size, seed),
        e => panic!("Unsupported measure {}", e),
    };
}
