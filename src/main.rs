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
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use timely::dataflow::operators::capture::{EventLink, Extract, Replay};
use timely::dataflow::operators::*;
use timely::dataflow::*;
use types::{BagOfWords, VectorWithNorm};

fn main() {
    // let mut args = env::args();
    // args.next(); // Skip executable name

    // let measure = args.next().expect("measure is required");
    // let thresh_str = args.next().expect("threshold is required");
    // let thresh = thresh_str
    // let left_path = args.next().expect("left path is required");
    // let right_path = args.next().expect("right path is required");

    // let timely_args = "".split_whitespace();

    let (send, recv) = ::std::sync::mpsc::channel();
    let send = Arc::new(Mutex::new(send));

    timely::execute_from_args(std::env::args(), move |worker| {
        let index = worker.index();
        let peers = worker.peers() as u64;
        println!("Greetings from worker {} (over {})", index, peers);

        let (mut left, mut right, probe, count) = worker.dataflow(|scope| {
            let send = send.lock().unwrap().clone();
            let (left_in, left_stream) = scope.new_input();
            let (right_in, right_stream) = scope.new_input();
            // let mut count = Rc::new(EventLink::new());
            // let mut count = ();
            let mut probe = ProbeHandle::new();
            let count = left_stream
                .cartesian_filter(
                    &right_stream,
                    |&x, &y| x <= y,
                    |&x| x as u64,
                    |&x| x as u64,
                    peers,
                )
                .count()
                .probe_with(&mut probe)
                .capture_into(send);
            (left_in, right_in, probe, count)
        });

        // Push data into the dataflow graph
        if index == 0 {
            // let left_p = &left_path;
            // let right_p = &right_path;
            // VectorWithNorm::from_file(&left_p.into(), |v| left.send(v));
            // VectorWithNorm::from_file(&right_p.into(), |v| right.send(v));
            for i in 0..(2u32.pow(12u32)) {
                let i = i as i32;
                right.send(i);
                left.send(i);
            }
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
