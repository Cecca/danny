#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;
extern crate crossbeam_channel;
extern crate danny;
extern crate env_logger;
extern crate rayon;
extern crate serde;

use danny::io::*;
use danny::logging::ProgressLogger;
use danny::measure::*;
use danny::types::*;
use rayon::prelude::*;
use std::fs::File;
use std::io::BufWriter;
use std::io::Write;
use std::path::PathBuf;

use crossbeam_channel::unbounded;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn run<D, F>(path: &PathBuf, ranges: Vec<f64>, similarity: F)
where
    D: ReadBinaryFile + ReadDataFile + Send + Sync + 'static,
    F: Fn(&D, &D) -> f64 + Send + Sync + 'static,
{
    let ranges = Arc::new(ranges.clone());
    let mut data = Vec::new();
    if path.ends_with(".txt") {
        D::from_file_with_count(path, |c, v| {
            data.push((c, v));
        });
    } else {
        D::read_binary(
            path.to_path_buf(),
            |_| true,
            |c, v| {
                data.push((c, v));
            },
        );
    }
    info!("Loaded dataset with {} elements", data.len());

    let (send, recv) = unbounded();

    let mut output_path = path.clone();
    output_path.set_extension("lid");
    let output_file = File::create(output_path).expect("Error opening file");
    let mut output = BufWriter::new(output_file);
    let mut pl = ProgressLogger::new(
        Duration::from_secs(30),
        "points".to_owned(),
        Some((data.len() * ranges.len()) as u64),
    );

    let th = thread::spawn(move || {
        for (c, range, lid) in recv.iter() {
            pl.add(1);
            writeln!(output, "{} {} {}", c, range, lid).expect("Write failed");
        }
        pl.done();
    });

    data.par_iter().for_each(|(c, v)| {
        let mut lids = vec![0.0; ranges.len()];
        let mut counts = vec![0; ranges.len()];
        for (_, u) in data.iter() {
            let s = similarity(u, v);
            let s = if s > 1.0 { 1.0 } else { s };
            let d = 1.0 - s;
            for (i, range) in ranges.iter().enumerate() {
                if s >= *range && d > 0.0 {
                    let denom = 1.0 - *range;
                    lids[i] += (d / denom).ln();
                    counts[i] += 1;
                }
            }
        }
        for (i, range) in ranges.iter().enumerate() {
            let lid = lids[i] / f64::from(counts[i]);
            let lid = -1.0 / lid;
            if !(lid.is_nan() || lid.is_infinite()) {
                send.send((*c, *range, lid)).expect("Error in sending");
            }
        }
    });
    drop(send);

    th.join().expect("Problem joining threads");
}

fn main() {
    let matches = clap_app!(lid =>
        (version: "0.1")
        (author: "Matteo Ceccarello <mcec@itu.dk>")
        (about: "Compute the Local Intrinsic Dimensionality of a dataset")
        (@arg MEASURE: -m +takes_value +required "The measure")
        (@arg RANGE: -r +takes_value +required "Query ranges")
        (@arg INPUT: +required "The input path")
    )
    .get_matches();

    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .format(move |buf, record| writeln!(buf, "{}: {}", record.level(), record.args()))
        .init();

    let input: PathBuf = matches.value_of("INPUT").unwrap().into();
    let measure: String = matches.value_of("MEASURE").unwrap().to_owned();
    let ranges: Vec<f64> = matches
        .value_of("RANGE")
        .unwrap()
        .split(',')
        .map(|token| token.parse::<f64>().expect("Problem parsing range"))
        .collect();
    info!("Query ranges {:?}", ranges);
    match measure.as_ref() {
        "jaccard" => run::<BagOfWords, _>(&input, ranges, Jaccard::jaccard),
        "cosine" => run::<UnitNormVector, _>(&input, ranges, InnerProduct::cosine),
        e => panic!("Unsupported measure {}", e),
    };
}
