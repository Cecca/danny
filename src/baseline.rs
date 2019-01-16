use crate::io::ReadDataFile;
use crate::operators::*;
use abomonation::Abomonation;
use core::any::Any;
use heapsize::HeapSizeOf;
use std::clone::Clone;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use timely::communication::allocator::generic::GenericBuilder;
use timely::dataflow::operators::capture::Extract;
use timely::dataflow::operators::*;
use timely::dataflow::*;
use timely::Data;

// TODO: Implement a Baseline data type

pub fn sequential<T, F>(thresh: f64, left_path: &String, right_path: &String, sim_fn: F) -> usize
where
    T: ReadDataFile + HeapSizeOf,
    F: Fn(&T, &T) -> f64,
{
    let mut left = Vec::new();
    let mut right = Vec::new();
    ReadDataFile::from_file(&left_path.into(), |v| left.push(v));
    ReadDataFile::from_file(&right_path.into(), |v| right.push(v));
    println!(
        "Loaded data:\n  left: {} ({} bytes)\n  right: {} ({} bytes)",
        left.len(),
        left.heap_size_of_children(),
        right.len(),
        right.heap_size_of_children()
    );

    let mut sim_cnt = 0;
    for l in left.iter() {
        for r in right.iter() {
            let sim = sim_fn(l, r);
            if sim >= thresh {
                sim_cnt += 1;
            }
        }
    }
    sim_cnt
}

#[allow(dead_code)]
pub fn all_pairs_parallel<T, F>(
    threshold: f64,
    left_path: &String,
    right_path: &String,
    sim_fn: F,
    timely_builder: (Vec<GenericBuilder>, Box<dyn Any + 'static>),
) -> usize
where
    T: ReadDataFile + Data + Sync + Send + Clone + Abomonation + Debug,
    F: Fn(&T, &T) -> f64 + Send + Clone + Sync + 'static,
{
    // This channel is used to get the results
    let (output_send_ch, recv) = ::std::sync::mpsc::channel();
    let output_send_ch = Arc::new(Mutex::new(output_send_ch));

    let left_path = left_path.clone();
    let right_path = right_path.clone();

    timely::execute::execute_from(timely_builder.0, timely_builder.1, move |worker| {
        let index = worker.index();
        let peers = worker.peers() as u64;
        info!("Started worker {}/{}", index, peers);
        let sim_fn = sim_fn.clone();

        let (mut left, mut right, probe) = worker.dataflow(|scope| {
            let output_send_ch = output_send_ch.lock().unwrap().clone();

            let (left_in, left_stream) = scope.new_input::<(u64, T)>();
            let (right_in, right_stream) = scope.new_input::<(u64, T)>();
            let mut probe = ProbeHandle::new();
            // left_stream
            //     .cartesian_filter(
            //         &right_stream,
            //         move |ref x, ref y| sim_fn(&x.1, &y.1) >= threshold,
            //         |ref x| x.route(),
            //         |ref x| x.route(),
            //         peers,
            //     )
            left_stream
                .two_way_predicate_join(
                    &right_stream,
                    move |ref x, ref y| sim_fn(x, y) >= threshold,
                    peers,
                )
                .count()
                .inspect_time(|t, d| println!("Output count at {:?} :: {:?} ", t, d))
                .exchange(|_| 0)
                .probe_with(&mut probe)
                .capture_into(output_send_ch);
            (left_in, right_in, probe)
        });

        // Push data into the dataflow graph
        let start = Instant::now();
        let left_path = left_path.clone();
        let right_path = right_path.clone();
        ReadDataFile::from_file_partially(
            &left_path.into(),
            |l| l % peers == index as u64,
            |c, v| left.send((c, v)),
        );
        ReadDataFile::from_file_partially(
            &right_path.into(),
            |l| l % peers == index as u64,
            |c, v| right.send((c, v)),
        );
        left.advance_to(1);
        right.advance_to(1);
        let end = Instant::now();
        let elapsed = end - start;
        println!(
            "Time to feed the input to the dataflow graph: {:?}",
            elapsed
        );
        worker.step_while(|| probe.less_than(left.time()));
    })
    .expect("Something went wrong with the timely dataflow execution");

    let count: usize = recv
        .extract()
        .iter()
        .map(|pair| pair.1.clone().iter().sum::<usize>())
        .next() // The iterator has one item for each timestamp. We have just one timestamp, 0
        .expect("Failed to get the result out of the channel");
    count
}
