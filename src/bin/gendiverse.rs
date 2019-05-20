/// Generate a diverse dataset starting from the given one 
/// and the distribution of its LIDs
 
#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;
extern crate crossbeam_channel;
extern crate danny;
extern crate env_logger;
extern crate rayon;
extern crate serde;

use std::collections::HashMap;
use danny::io::*;
use danny::logging::ProgressLogger;
use danny::measure::*;
use danny::types::*;
use rayon::prelude::*;
use std::fs::File;
use std::io::BufWriter;
use std::io::BufReader;
use std::io::BufRead;
use std::io::Write;
use std::path::PathBuf;
use rand_xorshift::XorShiftRng;
use rand::SeedableRng;
use rand::Rng;
use rand::distributions::Uniform;

use crossbeam_channel::unbounded;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn add_lids<D>(path: &PathBuf, range: f64, vecs: &mut Vec<(f64, D)>) {
    let f = File::open(path).expect("Problem opening LID file");
    let input = BufReader::new(f);
    for line in input.lines() {
        let line = line.expect("Problem reading line");
        let mut tokens = line.split_whitespace();
        let idx = tokens.next().expect("Missing token").parse::<usize>().expect("Problem parsing idx");
        let r = tokens.next().expect("Missing token").parse::<f64>().expect("Problem parsing range");
        let lid = tokens.next().expect("Missing token").parse::<f64>().expect("Problem parsing lid");
        if r == range {
            vecs[idx].0 = lid;
        }
    }
}

fn run<D>(path: &PathBuf, outputpath: &PathBuf, lid_path: &PathBuf, range: f64, target: usize, buckets: usize, seed: u64)
where
    D: ReadBinaryFile + WriteBinaryFile + Send + Sync + Default + Clone + 'static,
{
    let n = D::num_elements(path.to_path_buf());
    let mut data: Vec<(f64, D)> = vec![(0.0f64, D::default()); n];
    D::read_binary(
        path.to_path_buf(),
        |_| true,
        |c, v| {
            data[c as usize].1 = v;
        },
    );
    info!("Loaded dataset with {} elements", data.len());
    add_lids(lid_path, range, &mut data);
    data.sort_by(|t1, t2| t1.0.partial_cmp(&t2.0).expect("Problem doing comparison"));
    info!("Sorted by local intrinsic dimensionality");

    let mut cnt = 0;
    let mut rng = XorShiftRng::seed_from_u64(seed);
    let bucket_distr = Uniform::new(0usize, buckets);
    info!("Extremal LIDs: {} and {}", data[0].0, data[n-1].0);

    info!("Start sampling");
    let output_iter = std::iter::from_fn(move || {
        if cnt >= target {
            return None;
        }
        cnt += 1;
        let idx = rng.sample(&bucket_distr);
        let w = n / buckets;
        let bucket_start = idx * w;
        let bucket_end = std::cmp::min(bucket_start + w, n);
        let d = Uniform::new(bucket_start, bucket_end);
        let idx = rng.sample(&d);
        Some(data[idx].1.clone())
    });

    WriteBinaryFile::write_binary(
        outputpath.to_path_buf(), 
        D::num_chunks(path.to_path_buf()), 
        output_iter);
}


fn main() {
    let matches = clap_app!(lid =>
        (version: "0.1")
        (author: "Matteo Ceccarello <mcec@itu.dk>")
        (about: "Build a dataset with a diverse distribution of local intrinsic dimensionalities")
        (@arg TYPE: -t +takes_value +required "The type of data, either bag-of-words or unit-norm-vector")
        (@arg RANGE: -r +takes_value +required "The range for the LID")
        (@arg BUCKETS: -b +takes_value "Number of buckets required for the random sampling, default 100")
        (@arg TARGET: -s +takes_value +required "Target size")
        (@arg SEED: --seed +takes_value "Seed for the random number generator")
        (@arg INPUT: +required "The input path")
        (@arg LID: +required "The LID path")
        (@arg OUTPUT: +required "The output path")
    )
    .get_matches();

    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .format(move |buf, record| writeln!(buf, "{}: {}", record.level(), record.args()))
        .init();

    let input: PathBuf = matches.value_of("INPUT").unwrap().into();
    let lid_path: PathBuf = matches.value_of("LID").unwrap().into();
    let output: PathBuf = matches.value_of("OUTPUT").unwrap().into();
    let buckets: usize = matches.value_of("BUCKETS").unwrap_or("100").parse::<usize>().expect("Problem parsing the number of buckets");
    let target: usize = matches.value_of("TARGET").unwrap().parse::<usize>().expect("Problem parsing the target");
    let seed: u64 = matches.value_of("SEED").unwrap_or("140981350987").parse::<u64>().expect("Problem parsing the seed");
    let range: f64 = matches.value_of("RANGE").unwrap().parse::<f64>().expect("Problem parsing the seed");
    let datatype: String = matches.value_of("TYPE").unwrap().to_owned();
    match datatype.as_ref() {
        "bag-of-words" => run::<BagOfWords>(&input, &output, &lid_path, range, target, buckets, seed),
        "unit-norm-vector" => run::<UnitNormVector>(&input, &output, &lid_path, range, target, buckets, seed),
        e => panic!("Unsupported measure {}", e),
    };
    info!("Done!");
}
