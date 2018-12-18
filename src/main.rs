#[macro_use]
extern crate abomonation;
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

    let (send, recv) = ::std::sync::mpsc::channel();
    let send = Arc::new(Mutex::new(send));

    // Build timely context
    let (builders, others) = timely::Configuration::Process(workers).try_build().unwrap();

    timely::execute::execute_from(builders, others, move |worker| {
        let index = worker.index();
        let peers = worker.peers() as u64;
        println!("Greetings from worker {} (over {})", index, peers);

        let (mut left, mut right, probe) = worker.dataflow(|scope| {
            let threshold = threshold.clone();
            let send = send.lock().unwrap().clone();
            let (left_in, left_stream) = scope.new_input::<(u64, VectorWithNorm)>();
            let (right_in, right_stream) = scope.new_input::<(u64, VectorWithNorm)>();
            // let (left_in, left_stream) = scope.new_input::<u32>();
            // let (right_in, right_stream) = scope.new_input::<u32>();
            // let mut count = Rc::new(EventLink::new());
            // let mut count = ();
            let mut probe = ProbeHandle::new();
            left_stream
                .cartesian_filter(
                    &right_stream,
                    move |ref x, ref y| Cosine::cosine(&x.1, &y.1) >= threshold,
                    |ref x| x.route(),
                    |ref x| x.route(),
                    peers,
                )
                .count()
                .probe_with(&mut probe)
                .capture_into(send);
            (left_in, right_in, probe)
        });

        // Push data into the dataflow graph
        if index == 0 {
            let left_p = &left_path;
            let right_p = &right_path;
            VectorWithNorm::from_file_with_count(&left_p.into(), |c, v| left.send((c, v)));
            VectorWithNorm::from_file_with_count(&right_p.into(), |c, v| right.send((c, v)));
            // for i in 0..(2u32.pow(12u32)) {
            //     right.send(i);
            //     left.send(i);
            // }
            left.advance_to(1);
            right.advance_to(1);
        }
        worker.step_while(|| probe.less_than(left.time()));
    })
    .expect("Something went wrong with the dataflow");

    println!(
        "Content is {:?}",
        recv.extract()
            .iter()
            .map(|pair| pair.1.clone().iter().sum::<usize>())
            .collect::<Vec<usize>>()
    );

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
