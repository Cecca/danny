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
use operators::{cartesian, echo};
use std::env;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::*;
use timely::dataflow::*;
use types::{BagOfWords, VectorWithNorm};

fn main() {
    let mut args = env::args();
    args.next(); // Skip executable name
    let measure = args.next().expect("measure is required");
    let thresh_str = args.next().expect("threshold is required");
    let thresh = thresh_str
        .parse::<f64>()
        .expect(&format!("Error while parsing threshold `{}`", thresh_str));
    let left_path = args.next().expect("left path is required");
    let right_path = args.next().expect("right path is required");

    let timely_args = "".split_whitespace();

    timely::execute_from_args(std::env::args(), move |worker| {
        let index = worker.index();
        println!("Greetings from worker {}", index);

        let (mut left, mut right, mut probe) = worker.dataflow(|scope| {
            let (left_in, left_stream) = scope.new_input();
            let (right_in, right_stream) = scope.new_input();
            let probe = cartesian(&left_stream, &right_stream)
                // .inspect_batch(|t, xs| println!("{}: {:?}", t, xs))
                .inspect(|p| println!("{:?}", p))
                .probe();
            (left_in, right_in, probe)
        });

        // Push data into the dataflow graph
        if index == 0 {
            // let left_p = &left_path;
            // let right_p = &right_path;
            // VectorWithNorm::from_file(&left_p.into(), |v| left.send(v));
            // VectorWithNorm::from_file(&right_p.into(), |v| right.send(v));
            for i in 0..3 {
                left.send(i);
                right.send(i);
            }
            left.advance_to(1);
            right.advance_to(1);
        }
        worker.step_while(|| probe.less_than(left.time()));
    })
    .expect("Something went wrong with the dataflow");

    // match measure.as_ref() {
    //     "cosine" => {
    //         let mut left: Vec<VectorWithNorm> = Vec::new();
    //         let mut right: Vec<VectorWithNorm> = Vec::new();
    //         VectorWithNorm::from_file(&left_path.into(), |v| left.push(v));
    //         VectorWithNorm::from_file(&right_path.into(), |v| right.push(v));

    //         baseline::sequential(thresh, &left, &right, Cosine::cosine);
    //     }
    //     "jaccard" => {
    //         let mut left: Vec<BagOfWords> = Vec::new();
    //         let mut right: Vec<BagOfWords> = Vec::new();
    //         BagOfWords::from_file(&left_path.into(), |v| left.push(v));
    //         BagOfWords::from_file(&right_path.into(), |v| right.push(v));

    //         baseline::sequential(thresh, &left, &right, Jaccard::jaccard);
    //     }
    //     _ => unimplemented!(),
    // };
}
