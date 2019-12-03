use crate::config::*;
use crate::experiment::Experiment;
use crate::io::*;
use crate::join::Join;
use crate::logging::init_event_logging;
use crate::logging::*;
use crate::lsh::operators::*;
use crate::operators::*;
use danny_base::lsh::*;
use danny_base::sketch::*;
use rand::{Rng, SeedableRng};
use serde::de::Deserialize;
use std::clone::Clone;
use std::fmt::Debug;
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex};
use timely::dataflow::operators::capture::{Event as TimelyEvent, Extract};
use timely::dataflow::operators::*;
use timely::dataflow::*;
use timely::ExchangeData;

#[allow(clippy::too_many_arguments)]
pub fn hu_baseline<D, F, H, B, R>(
    left_path: &str,
    right_path: &str,
    range: f64,
    k: usize,
    hash_function_builder: B,
    sim_pred: F,
    rng: &mut R,
    config: &Config,
    experiment: &mut Experiment,
) -> usize
where
    for<'de> D: ReadBinaryFile + Deserialize<'de> + ExchangeData + Debug + SketchEstimate,
    F: Fn(&D, &D) -> bool + Send + Clone + Sync + 'static,
    H: LSHFunction<Input = D, Output = u32> + Sync + Send + Clone + 'static,
    R: Rng + SeedableRng + Send + Sync + Clone + 'static,
    B: Fn(usize, &mut R) -> H + Sized + Send + Sync + Clone + 'static,
{
    let network = NetworkGauge::start();

    let timely_builder = config.get_timely_builder();
    // This channel is used to get the results
    let (output_send_ch, recv) = channel();
    let output_send_ch = Arc::new(Mutex::new(output_send_ch));

    let (send_exec_summary, recv_exec_summary) = channel();
    let send_exec_summary = Arc::new(Mutex::new(send_exec_summary));

    let hasher = TensorCollection::new(k, range, hash_function_builder, rng);
    let hasher = Arc::new(hasher);

    debug!(
        "Left dataset has {} points, right has {}",
        D::num_elements(left_path.into()),
        D::num_elements(right_path.into())
    );
    let (global_left, global_right) = load_vectors(left_path, right_path, &config);

    timely::execute::execute_from(timely_builder.0, timely_builder.1, move |mut worker| {
        let global_left = Arc::clone(&global_left);
        let global_right = Arc::clone(&global_right);
        let hasher = Arc::clone(&hasher);
        let execution_summary = init_event_logging(&worker);
        let output_send_ch = output_send_ch
            .lock()
            .expect("Cannot get lock on output channel")
            .clone();
        let sim_pred = sim_pred.clone();

        let probe = worker.dataflow::<u32, _, _>(move |scope| {
            let mut probe = ProbeHandle::new();
            let matrix = MatrixDescription::for_workers(scope.peers() as usize);

            let left_hashes = source_hashed_one_round(
                scope,
                Arc::clone(&global_left),
                Arc::clone(&hasher),
                matrix,
                MatrixDirection::Rows,
            );
            let right_hashes = source_hashed_one_round(
                scope,
                Arc::clone(&global_right),
                Arc::clone(&hasher),
                matrix,
                MatrixDirection::Columns,
            );
            left_hashes
                .join_map_slice(
                    &right_hashes,
                    move |(repetition, _hash), left_vals, right_vals| {
                        let mut cnt = 0usize;
                        for (_, (_, l_pool, l)) in left_vals {
                            for (_, (_, r_pool, r)) in right_vals {
                                if !hasher.already_seen(l_pool, r_pool, *repetition)
                                    && sim_pred(l, r)
                                {
                                    cnt += 1;
                                }
                            }
                        }
                        vec![cnt]
                    },
                )
                .exchange(|_| 0) // Bring all the counts to the first worker
                .probe_with(&mut probe)
                .capture_into(output_send_ch);

            probe
        });

        // Do the stepping even though it's not strictly needed: we use it to wait for the dataflow
        // to finish
        // worker.step_while(|| probe.less_than(&(repetitions as u32)));
        worker.step_while(|| probe.with_frontier(|f| !f.is_empty()));

        info!("Finished stepping");

        collect_execution_summaries(execution_summary, send_exec_summary.clone(), &mut worker);
    })
    .expect("Problems with the dataflow");

    info!("Collecting summaries");

    let network_summaries = network.map(|n| n.measure().collect_from_workers(&config));

    if config.is_master() {
        let mut exec_summaries = Vec::new();
        for summary in recv_exec_summary.iter() {
            if let TimelyEvent::Messages(_, msgs) = summary {
                exec_summaries.extend(msgs);
            }
        }
        for summary in exec_summaries.iter() {
            summary.add_to_experiment(experiment);
        }
        if network_summaries.is_some() {
            network_summaries
                .unwrap()
                .iter()
                .for_each(|n| n.report(experiment));
        }
        // From `recv` we get an entry for each timestamp, containing a one-element vector with the
        // count of output pairs for a given timestamp. We sum across all the timestamps, so we need to
        // remove the duplicates
        let count: u64 = recv
            .extract()
            .iter()
            .map(|pair| pair.1.clone().iter().sum::<usize>())
            .sum::<usize>() as u64;

        count as usize
    } else {
        0
    }
}
