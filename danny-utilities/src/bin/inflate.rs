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
use rand::distributions::{Distribution, Normal};
use rand::{Rng, SeedableRng};
use serde::Serialize;
use std::io::Write;
use std::path::PathBuf;

fn rotation_matrix<R: Rng>(n: usize, rng: &mut R) -> Vec<Vec<f32>> {
    let mut out = Vec::new();
    let scale = 1.0 / (n as f64).sqrt();
    let normal = Normal::new(0.0, 0.0);
    for _ in 0..n {
        out.push(
            normal
                .sample_iter(rng)
                .take(n)
                .map(|x| (x * scale) as f32)
                .collect(),
        );
    }
    out
}

fn multiply(vec: &Vec<f32>, matrix: &Vec<Vec<f32>>) -> Vec<f32> {
    let n = vec.len();
    let mut out = vec![0.0_f32; n];
    for i in 0..n {
        for j in 0..n {
            out[i] += vec[i] * matrix[i][j];
        }
    }

    out
}

fn run_bow(path: &PathBuf, output: &PathBuf, factor: usize, seed: u64) {
    let universe_size = BagOfWords::peek_one(path.clone()).universe;
    let mut rng = rand_xorshift::XorShiftRng::seed_from_u64(seed);
    let mut data = Vec::new();
    let num_elems = <BagOfWords as ReadBinaryFile>::num_elements(path.to_path_buf());
    let mut cnt = 0;
    BagOfWords::read_binary(
        path.to_path_buf(),
        |_| true,
        |c, v| {
            for i in 0..factor {
                let mut new_vec = v.words().clone();
                for w in new_vec.iter_mut() {
                    *w += (i as u32 % universe_size);
                }
                let new_vec = BagOfWords::new(universe_size, new_vec);
                data.push((cnt, new_vec));
                cnt += 1;
            }
        },
    );
    let chunks = BagOfWords::num_chunks(path.to_path_buf());
    WriteBinaryFile::write_binary(output.to_path_buf(), chunks, data.into_iter().map(|p| p.1));
}

fn run_cosine(path: &PathBuf, output: &PathBuf, factor: usize, seed: u64) {
    let dimension = UnitNormVector::peek_one(path.clone()).dim();
    let mut rng = rand_xorshift::XorShiftRng::seed_from_u64(seed);
    let mut data = Vec::new();
    let mut cnt = 0;
    let mut rotations = Vec::new();
    for _ in 0..factor {
        rotations.push(rotation_matrix(dimension, &mut rng));
    }

    UnitNormVector::read_binary(
        path.to_path_buf(),
        |_| true,
        |_, v| {
            for rotation in rotations.iter() {
                let new_vec = UnitNormVector::new(multiply(v.data(), rotation));
                data.push((cnt, new_vec));
                cnt += 1;
            }
        },
    );
    let chunks = BagOfWords::num_chunks(path.to_path_buf());
    WriteBinaryFile::write_binary(output.to_path_buf(), chunks, data.into_iter().map(|p| p.1));
}

fn main() {
    let matches = clap_app!(sampledata =>
        (version: "0.1")
        (author: "Matteo Ceccarello <mcec@itu.dk>")
        (about: "Sample the given dataset")
        (@arg MEASURE: -m --measure +takes_value +required "The measure")
        (@arg FACTOR: -f --factor +takes_value +required "The factor of the inflation")
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
    let factor: usize = matches
        .value_of("FACTOR")
        .unwrap()
        .parse::<usize>()
        .unwrap();
    match measure.as_ref() {
        "jaccard" => run_bow(&input, &output, factor, seed),
        "cosine" => run_cosine(&input, &output, factor, seed),
        e => panic!("Unsupported measure {}", e),
    };
}
