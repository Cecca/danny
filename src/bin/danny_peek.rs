#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate danny;
extern crate serde;
extern crate crossbeam_channel;
extern crate rayon;

use danny::io::*;
use danny::measure::*;
use danny::logging::ProgressLogger;
use danny::types::*;
use rayon::prelude::*;
use std::path::PathBuf;
use std::fs::File;
use std::io::BufWriter;
use std::io::Write;
use std::sync::mpsc::channel;
use std::sync::{Mutex, Arc};
use crossbeam_channel::unbounded;
use std::thread;
use std::fmt::Debug;
use std::time::Duration;

fn run<D, F>(path: &PathBuf, ids: &Vec<u64>, similarity: F)
where
    D: ReadBinaryFile + ReadDataFile + Send + Sync + Debug + Clone + 'static,
    F: Fn(&D, &D) -> f64 +Send + Sync+ 'static,
{
    let mut queries = Vec::new();
    let mut data = Vec::new();
    if path.ends_with(".txt") {
        D::from_file_with_count(path, |c, v| {
            if ids.contains(&c) {
                queries.push((c, v.clone()));
            }
            data.push((c,v));
        });
    } else {
        D::read_binary(path.to_path_buf(), |_| true, |c, v| {
            if ids.contains(&c) {
                queries.push((c, v.clone()));
            }
            data.push((c,v));
        });
    }
    info!("Loaded {} queries from a dataset of {} elements", queries.len(), data.len());

    for (src, v) in queries.iter() {
        let mut lid = 0.0;
        let range = 0.6_f64;
        let denom = 1.0 - range;
        let mut count = 0;
        for (dst, u) in data.iter() {
            let sim = similarity(v, u);
            let sim = if sim > 1.0 {
                1.0
            } else {
                sim
            };
            println!("{} {} {}", src, dst, sim);
            let d = 1.0 - sim;
            if sim >= range && d >= 0.0 {
                info!("Vector above similarity {:?}", u);
                lid += (d / denom).ln();
                count += 1;
            }
        }
        let lid = lid / count as f64;
        let lid = -1.0 / lid;
        info!("LID (0.6) for {} is {:?} (count of within neighbours: {})", src, lid, count);
    }

}

fn main() {
    let matches = clap_app!(danny_peek =>
        (version: "0.1")
        (author: "Matteo Ceccarello <mcec@itu.dk>")
        (about: "Prints some vectors of the dataset")
        (@arg MEASURE: -m +takes_value +required "The measure")
        (@arg IDS: --ids +takes_value +required "Comma separated list of IDS")
        (@arg INPUT: +required "The input path")
    )
    .get_matches();

    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .format(move |buf, record| {
            writeln!(
                buf,
                "{}: {}",
                record.level(),
                record.args()
            )
        })
        .init();

    let input: PathBuf = matches.value_of("INPUT").unwrap().into();
    let ids: Vec<u64> = matches
        .value_of("IDS")
        .unwrap()
        .to_owned()
        .split(",")
        .map(|token| token.parse::<u64>().expect("Problem parsing id to u32"))
        .collect();
    let measure: String = matches.value_of("MEASURE").unwrap().to_owned();

    match measure.as_ref() {
        "jaccard" => run::<BagOfWords, _>(&input, &ids, Jaccard::jaccard),
        "cosine" => run::<UnitNormVector, _>(&input, &ids, InnerProduct::cosine),
        _ => panic!("Unsupported measure"),
    };
}
