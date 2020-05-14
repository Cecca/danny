use crate::config::*;
use crate::experiment::Experiment;
use crate::io::*;
use crate::join::Join;
use crate::logging::init_event_logging;
use crate::logging::*;
use crate::lsh::repetition_stopwatch::*;
use crate::operators::*;
use danny_base::lsh::*;
use danny_base::sketch::*;
use danny_base::types::ElementId;
use rand::{Rng, SeedableRng};
use serde::de::Deserialize;
use std::clone::Clone;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use timely::communication::Allocator;
use timely::dataflow::operators::capture::{Event as TimelyEvent, Extract};
use timely::dataflow::operators::generic::source;
use timely::dataflow::operators::*;
use timely::dataflow::*;
use timely::progress::Timestamp;
use timely::worker::Worker;
use timely::ExchangeData;

pub fn source_hashed_one_round<G, T, D, S, F>(
    scope: &G,
    global_vecs: Arc<Vec<(ElementId, D)>>,
    hash_fns: Arc<TensorCollection<F>>,
    sketcher: Arc<S>,
    matrix: MatrixDescription,
    direction: MatrixDirection,
) -> Stream<G, ((usize, u32), (ElementId, TensorPool, S::Output, D))>
where
    G: Scope<Timestamp = T>,
    T: Timestamp + Succ,
    D: ExchangeData + Debug,
    S: Sketcher<Input = D> + Clone + 'static,
    S::Output: SketchData + Debug,
    F: LSHFunction<Input = D, Output = u32> + Sync + Send + Clone + 'static,
{
    let worker: u64 = scope.index() as u64;
    let logger = scope.danny_logger();
    let repetitions = hash_fns.repetitions();
    let vecs = Arc::clone(&global_vecs);
    let mut stopwatch = RepetitionStopWatch::new("repetition", worker == 0, logger);
    let mut bit_pools: HashMap<ElementId, TensorPool> = HashMap::new();
    let mut sketches = HashMap::new();
    info!("Computing the bit pools");
    let start = Instant::now();
    for (k, v) in vecs.iter() {
        bit_pools.insert(*k, hash_fns.pool(v));
    }
    let end = Instant::now();
    info!(
        "Computed the bit pools ({:?}, {})",
        end - start,
        proc_mem!()
    );

    info!("Computing sketches");
    let start = Instant::now();
    for (k, v) in vecs.iter() {
        let s = sketcher.sketch(v);
        sketches.insert(*k, s);
    }
    let end = Instant::now();
    info!(
        "Computed the bit pools ({:?}, {})",
        end - start,
        proc_mem!()
    );

    source(scope, "hashed source one round", move |capability| {
        let mut cap = Some(capability);
        move |output| {
            let mut done = false;
            if let Some(cap) = cap.as_mut() {
                for current_repetition in 0..repetitions {
                    stopwatch.maybe_stop();
                    stopwatch.start();
                    if worker == 0 {
                        debug!("Repetition {} (Hu et al. baseline)", current_repetition,);
                    }
                    let mut session = output.session(&cap);
                    for (k, v) in vecs.iter() {
                        let h = hash_fns.hash(&bit_pools[k], current_repetition as usize);
                        session.give((
                            (current_repetition, h),
                            (
                                k.clone(),
                                bit_pools[k].clone(),
                                sketches[k].clone(),
                                v.clone(),
                            ),
                        ));
                    }
                }
                done = true;
            }

            if done {
                // Drop the capability to signal that we will send no more data
                cap = None;
            }
        }
    })
}

#[allow(clippy::too_many_arguments)]
pub fn hu_baseline<D, F, H, S, V, B, R>(
    worker: &mut Worker<Allocator>,
    left_path: &str,
    right_path: &str,
    range: f64,
    k: usize,
    hash_function_builder: B,
    sketcher: S,
    sketch_predicate: SketchPredicate<V>,
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
    S: Sketcher<Input = D, Output = V> + Send + Sync + Clone + 'static,
    V: SketchData + Debug,
    B: Fn(usize, &mut R) -> H + Sized + Send + Sync + Clone + 'static,
{
    use std::cell::RefCell;
    use std::rc::Rc;
    let result = Rc::new(RefCell::new(0usize));
    let result_read = Rc::clone(&result);

    let network = NetworkGauge::start();
    let no_dedup = config.no_dedup;
    let no_verify = config.no_verify;

    let left_vectors = Arc::new(load_for_worker::<D, _>(
        worker.index(),
        worker.peers(),
        left_path,
    ));
    let right_vectors = Arc::new(load_for_worker::<D, _>(
        worker.index(),
        worker.peers(),
        left_path,
    ));

    // let (send_exec_summary, recv_exec_summary) = channel();
    // let send_exec_summary = Arc::new(Mutex::new(send_exec_summary));

    let hasher = TensorCollection::new(k, range, config.recall, hash_function_builder, rng);
    let hasher = Arc::new(hasher);

    let hasher = Arc::clone(&hasher);
    let execution_summary = init_event_logging(&worker);
    let sim_pred = sim_pred.clone();
    let sketch_predicate = sketch_predicate.clone();
    let sketcher = sketcher.clone();
    let sketcher = Arc::new(sketcher);

    let probe = worker.dataflow::<u32, _, _>(move |scope| {
            let mut probe = ProbeHandle::new();
            let matrix = MatrixDescription::for_workers(scope.peers() as usize);
            let logger = scope.danny_logger();

            let left_hashes = source_hashed_one_round(
                scope,
                Arc::clone(&left_vectors),
                Arc::clone(&hasher),
                Arc::clone(&sketcher),
                matrix,
                MatrixDirection::Rows,
            );
            let right_hashes = source_hashed_one_round(
                scope,
                Arc::clone(&right_vectors),
                Arc::clone(&hasher),
                Arc::clone(&sketcher),
                matrix,
                MatrixDirection::Columns,
            );
            left_hashes
                .join_map_slice(
                    &right_hashes,
                    move |(repetition, _hash), left_vals, right_vals| {
                        let mut cnt = 0usize;
                        let mut total = 0usize;
                        let mut sketch_discarded = 0;
                        let mut duplicate_cnt = 0usize;
                        let start = Instant::now();
                        // for (_, (_, _, v)) in left_vals.iter() {
                        //     info!("{:?}", v);
                        // }
                        for (_, (_, l_pool, l_sketch, l)) in left_vals {
                            for (_, (_, r_pool, r_sketch, r)) in right_vals {
                                total += 1;
                                if sketch_predicate.eval(l_sketch, r_sketch) {
                                    if no_verify || sim_pred(l, r) {
                                        if no_dedup
                                            || !hasher.already_seen(l_pool, r_pool, *repetition)
                                        {
                                            cnt += 1;
                                        } else {
                                            duplicate_cnt += 1;
                                        }
                                    }
                                } else {
                                    sketch_discarded += 1;
                                }
                            }
                        }
                        info!(
                            "Candidates {}: Emitted {} / Sketch discarded {} / Duplicates {} in {:?} ({})",
                            total,
                            cnt,
                            sketch_discarded,
                            duplicate_cnt,
                            Instant::now() - start,
                            proc_mem!(),
                        );
                        log_event!(
                            logger,
                            LogEvent::SketchDiscarded(*repetition, sketch_discarded)
                        );
                        log_event!(logger, LogEvent::GeneratedPairs(*repetition, cnt));
                        log_event!(
                            logger,
                            LogEvent::DuplicatesDiscarded(*repetition, duplicate_cnt)
                        );
                        vec![cnt]
                    },
                )
                .exchange(|_| 0) // Bring all the counts to the first worker
                .unary(timely::dataflow::channels::pact::Pipeline, "count collection", move |_, _| {
                    move |input, output| {
                        input.for_each(|t, data| {
                            let data = data.replace(Vec::new());
                            for c in data.into_iter() {
                                *result.borrow_mut() += c;
                            }
                            output.session(&t).give(());
                        });
                    }
                })
                .probe_with(&mut probe);

            probe
        });

    // Do the stepping even though it's not strictly needed: we use it to wait for the dataflow
    // to finish
    // worker.step_while(|| probe.less_than(&(repetitions as u32)));
    worker.step_while(|| probe.with_frontier(|f| !f.is_empty()));

    info!("Finished stepping");
    // collect_execution_summaries(execution_summary, send_exec_summary.clone(), worker);
    // info!("Collecting summaries");
    // let network_summaries = network.map(|n| n.measure().collect_from_workers(worker, &config));

    if worker.index() == 0 {
        result_read.replace(0)
    } else {
        0
    }
}
