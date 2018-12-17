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
use operators::cartesian;
use operators::BinaryOperator;
use std::env;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::*;
use timely::dataflow::*;
use types::{BagOfWords, VectorWithNorm};

fn main() {
    // let mut args = env::args();
    // args.next(); // Skip executable name

    // let measure = args.next().expect("measure is required");
    // let thresh_str = args.next().expect("threshold is required");
    // let thresh = thresh_str
    //     .parse::<f64>()
    //     .expect(&format!("Error while parsing threshold `{}`", thresh_str));
    // let left_path = args.next().expect("left path is required");
    // let right_path = args.next().expect("right path is required");

    // let timely_args = "".split_whitespace();

    timely::execute_from_args(std::env::args(), move |worker| {
        let index = worker.index();
        let peers = worker.peers() as u64;
        println!("Greetings from worker {} (over {})", index, peers);

        let (mut left, mut right, probe) = worker.dataflow(|scope| {
            let (left_in, left_stream) = scope.new_input();
            let (right_in, right_stream) = scope.new_input();
            let probe = cartesian(&left_stream, &right_stream, |&x| x as u64, peers)
                .inspect_batch(|t, xs| println!("{}: {:?}", t, xs))
                // .inspect(|p| println!("{:?}", p))
                .probe();
            // let (out1, out2) = left_stream
            //     .inspect(|x| println!("in1 {}", x))
            //     .binary_in_out_frontier(
            //         &right_stream.inspect(|x| println!("in2 {}", x)),
            //         Pipeline,
            //         Pipeline,
            //         "cross",
            //         |_, _| {
            //             |in1, in2, out1, out2| {
            //                 in1.for_each(|t, d| {
            //                     out2.session(&t).give_vec(&mut d.replace(Vec::new()));
            //                 });
            //                 in2.for_each(|t, d| {
            //                     out1.session(&t).give_vec(&mut d.replace(Vec::new()));
            //                 });
            //             }
            //         },
            //     );
            // let mut probe = ProbeHandle::new();
            // out1.inspect(|x| println!("out1 {}", x))
            //     .probe_with(&mut probe);
            // out2.inspect(|x| println!("out2 {}", x))
            //     .probe_with(&mut probe);
            (left_in, right_in, probe)
        });

        // Push data into the dataflow graph
        if index == 0 {
            // let left_p = &left_path;
            // let right_p = &right_path;
            // VectorWithNorm::from_file(&left_p.into(), |v| left.send(v));
            // VectorWithNorm::from_file(&right_p.into(), |v| right.send(v));
            for i in 1..4 {
                let i = i as i32;
                right.send(-i);
                left.send(i);
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
