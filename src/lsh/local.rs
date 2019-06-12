use crate::bloom::*;
use crate::config::*;
use crate::dataset::*;
use crate::experiment::*;
use crate::io::*;
use crate::logging::*;
use crate::lsh::functions::*;

use crate::operators::*;
use rand::{Rng, SeedableRng};
use serde::de::Deserialize;

use std::fmt::Debug;
use std::hash::Hash;
use std::sync::{Arc, Mutex};
use timely::dataflow::operators::capture::Extract;
use timely::dataflow::operators::generic::operator::source;
use timely::dataflow::operators::*;
use timely::ExchangeData;

#[allow(clippy::too_many_arguments)]
pub fn local_lsh<D, F, H, O, B, R>(
    left_path: &str,
    right_path: &str,
    k: ParamK,
    hash_collection_builder: B,
    sim_pred: F,
    config: &Config,
    rng: &mut R,
    _experiment: &mut Experiment,
) -> usize
where
    for<'de> D: ReadBinaryFile + Deserialize<'de> + ExchangeData + Debug,
    F: Fn(&D, &D) -> bool + Send + Clone + Sync + 'static,
    H: LSHFunction<Input = D, Output = O> + Sync + Send + Clone + 'static,
    O: ExchangeData + Eq + Hash + Ord + Clone,
    R: Rng + SeedableRng + Send + Sync + Clone + 'static,
    B: Fn(usize, &mut R) -> LSHCollection<H> + Sized + Send + Sync + Clone + 'static,
{
    let timely_builder = config.get_timely_builder();
    // This channel is used to get the results
    let (output_send_ch, recv) = ::std::sync::mpsc::channel();
    let output_send_ch = Arc::new(Mutex::new(output_send_ch));

    let (global_left, global_right) = load_vectors(left_path, right_path, &config);

    let bloom_filter = Arc::new(AtomicBloomFilter::<u32>::new(
        4usize.gb_to_bits(),
        5,
        rng.clone(),
    ));

    let hash_fns = match k {
        ParamK::Fixed(k) => hash_collection_builder(k, rng),
        ParamK::Adaptive(_, _) => panic!("You should not be here!!"),
    };

    timely::execute::execute_from(timely_builder.0, timely_builder.1, move |worker| {
        let index = worker.index();
        let peers = worker.peers() as u64;
        let matrix = MatrixDescription::for_workers(peers as usize);
        let (row, col) = matrix.row_major_to_pair(index as u64);
        info!("Started worker {}/{}", index, peers);
        let sim_pred = sim_pred.clone();
        let hash_fns = hash_fns.clone();

        worker.dataflow::<u32, _, _>(|scope| {
            let output_send_ch = output_send_ch.lock().unwrap().clone();
            let global_left = Arc::clone(&global_left);
            let global_right = Arc::clone(&global_right);
            let bloom = Arc::clone(&bloom_filter);
            let sim_pred = sim_pred.clone();
            let hash_fns = hash_fns.clone();

            source(scope, "Source", move |capability| {
                let sim_pred = sim_pred.clone();
                let hash_fns = hash_fns.clone();
                let mut cap = Some(capability);
                let left = Arc::clone(&global_left);
                let right = Arc::clone(&global_right);
                move |output| {
                    let sim_pred = sim_pred.clone();
                    let hash_fns = hash_fns.clone();
                    if let Some(cap) = cap.take() {
                        info!("Starting to count pairs (memory {})", proc_mem!());
                        let count = run_local(
                            Arc::clone(&left),
                            Arc::clone(&right),
                            col as usize,
                            row as usize,
                            hash_fns,
                            sim_pred,
                            Arc::clone(&bloom),
                        );
                        info!(
                            "Worker {} outputting count {} (memory {})",
                            index,
                            count,
                            proc_mem!()
                        );
                        output.session(&cap).give(count);
                    }
                }
            })
            .exchange(|_| 0)
            .capture_into(output_send_ch);
        });
    })
    .expect("Something went wrong with the timely dataflow execution");

    if config.is_master() {
        let count: usize = recv
            .extract()
            .iter()
            .map(|pair| pair.1.clone().iter().sum::<usize>())
            .next() // The iterator has one item for each timestamp. We have just one timestamp, 0
            .expect("Failed to get the result out of the channel");
        count
    } else {
        0
    }
}

fn run_local<K, D, F, H, O>(
    left: Arc<ChunkedDataset<K, D>>,
    right: Arc<ChunkedDataset<K, D>>,
    col: usize,
    row: usize,
    hash_fns: LSHCollection<H>,
    _sim_pred: F,
    _bloom: Arc<AtomicBloomFilter<K>>,
) -> usize
where
    K: Route + Eq + Hash + Copy + Into<u64>,
    F: Fn(&D, &D) -> bool + Send + Clone + Sync + 'static,
    H: LSHFunction<Input = D, Output = O> + Sync + Send + Clone + 'static,
    O: Eq + Hash + Ord + Clone,
{
    let count = 0;
    for repetition in 0..hash_fns.repetitions() {
        info!(
            "Start repetition {}/{} (memory {})",
            repetition,
            hash_fns.repetitions(),
            proc_mem!()
        );
        let mut buckets = std::collections::BTreeMap::new(); // TODO: Find way to move this outside of the loop
        for (lk, lv) in left.iter_chunk(row) {
            let h = hash_fns.hash(lv, repetition);
            let bucket = buckets.entry(h).or_insert_with(|| (Vec::new(), Vec::new()));
            bucket.0.push(lk);
        }
        for (rk, rv) in right.iter_chunk(col) {
            let h = hash_fns.hash(rv, repetition);
            let bucket = buckets.entry(h).or_insert_with(|| (Vec::new(), Vec::new()));
            bucket.1.push(rk);
        }
        unimplemented!("Replace the pairgenerator with the new buckets");
        // let generator = PairGenerator::new(buckets);
        // for (lk, rk) in generator {
        //     if !bloom.test_and_insert(&(*lk, *rk)) && sim_pred(&left[lk], &right[rk]) {
        //         count += 1;
        //     }
        // }
    }

    count
}
