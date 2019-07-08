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

fn load<D>(path: &PathBuf) -> Vec<(u64, D)>
where
    D: ReadBinaryFile + ReadDataFile + Send + Sync + 'static,
{
    info!("Loading data from {:?}", path);
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
    data
}

fn run<D, F>(path: &PathBuf, base: &PathBuf, ranges: Vec<f64>, similarity: F)
where
    D: ReadBinaryFile + ReadDataFile + Send + Sync + 'static,
    F: Fn(&D, &D) -> f64 + Send + Sync + 'static,
{
    let ranges = Arc::new(ranges.clone());
    let data = load(path);
    let base = load::<D>(base);

    let (send, recv) = unbounded();

    let mut output_path = path.clone();
    output_path.set_extension("exp");
    let output_file = File::create(output_path).expect("Error opening file");
    let mut output = BufWriter::new(output_file);
    let mut pl = ProgressLogger::new(
        Duration::from_secs(30),
        "points".to_owned(),
        Some((data.len() * ranges.len()) as u64),
    );

    let th = thread::spawn(move || {
        for (c, range, expansion) in recv.iter() {
            pl.add(1);
            writeln!(output, "{} {} {}", c, range, expansion).expect("Write failed");
        }
        pl.done();
    });

    data.par_iter().for_each(|(c, v)| {
        let mut expansions = vec![0.0; ranges.len()];
        let mut count_in = vec![0; ranges.len()];
        let mut count_out = vec![0; ranges.len()];
        let mut similarities: Vec<f64> = Vec::new();
        for (_, u) in base.iter() {
            let s = similarity(u, v);
            similarities.push(s);
        }
        similarities.sort_unstable_by(|x, y| x.partial_cmp(y).unwrap().reverse());
        for s in similarities {
            for (i, &range) in ranges.iter().enumerate() {
                if s >= range {
                    count_in[i] += 1;
                } else if count_out[i] <= 2 * count_in[i] {
                    count_out[i] += 1;
                    expansions[i] = range / s;
                }
            }
        }
        for (i, range) in ranges.iter().enumerate() {
            let exp = expansions[i];
            send.send((*c, *range, exp)).expect("Error in sending");
        }
    });
    drop(send);

    th.join().expect("Problem joining threads");
}

fn main() {
    let matches = clap_app!(danny_expansion =>
        (version: "0.1")
        (author: "Matteo Ceccarello <mcec@itu.dk>")
        (about: "Compute the expansion of the points of a dataset")
        (@arg MEASURE: -m +takes_value +required "The measure")
        (@arg RANGE: -r +takes_value +required "Query ranges")
        (@arg INPUT: +required "The input path")
        (@arg BASE: "The path to compare with")
    )
    .get_matches();

    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .format(move |buf, record| writeln!(buf, "{}: {}", record.level(), record.args()))
        .init();

    let input: PathBuf = matches.value_of("INPUT").unwrap().into();
    let base: PathBuf = matches
        .value_of("BASE")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            info!("Using the input as the base");
            input.clone()
        });
    let measure: String = matches.value_of("MEASURE").unwrap().to_owned();
    let ranges: Vec<f64> = matches
        .value_of("RANGE")
        .unwrap()
        .split(',')
        .map(|token| token.parse::<f64>().expect("Problem parsing range"))
        .collect();
    info!("Query ranges {:?}", ranges);
    match measure.as_ref() {
        "jaccard" => run::<BagOfWords, _>(&input, &base, ranges, Jaccard::jaccard),
        "cosine" => run::<UnitNormVector, _>(&input, &base, ranges, InnerProduct::cosine),
        e => panic!("Unsupported measure {}", e),
    };
}
