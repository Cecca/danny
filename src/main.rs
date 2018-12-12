mod baseline;
/// Provides facilities to read and write files
mod io;
mod measure;
/// This module collects algorithms to compute on some datasets,
/// which might be useful to understand their behaviour
mod stats;
mod types;

use io::ReadDataFile;
use measure::Cosine;
use std::env;
use types::VectorWithNorm;

fn main() {
    let mut args = env::args();
    args.next(); // Skip executable name
    let thresh_str = args.next().expect("threshold is required");
    let thresh = thresh_str
        .parse::<f64>()
        .expect(&format!("Error while parsing threshold `{}`", thresh_str));
    let left_path = args.next().expect("left path is required");
    let right_path = args.next().expect("right path is required");

    let mut left: Vec<VectorWithNorm> = Vec::new();
    let mut right: Vec<VectorWithNorm> = Vec::new();
    VectorWithNorm::from_file(&left_path.into(), |v| left.push(v));
    VectorWithNorm::from_file(&right_path.into(), |v| right.push(v));

    baseline::sequential(thresh, &left, &right, Cosine::cosine);
}
