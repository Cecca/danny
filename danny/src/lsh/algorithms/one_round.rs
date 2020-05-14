use crate::config::*;
use crate::experiment::Experiment;
use crate::io::*;
use crate::join::Joiner;
use crate::logging::init_event_logging;
use crate::logging::*;
use crate::operators::*;
use danny_base::lsh::*;
use danny_base::sketch::*;
use danny_base::types::*;
use rand::{Rng, SeedableRng};
use serde::de::Deserialize;
use std::clone::Clone;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use timely::communication::Allocator;
use timely::dataflow::channels::pact::Pipeline as PipelinePact;
use timely::dataflow::operators::capture::{Event as TimelyEvent, Extract};
use timely::dataflow::operators::generic::source;
use timely::dataflow::operators::*;
use timely::dataflow::*;
use timely::worker::Worker;
use timely::ExchangeData;

fn simple_source<G, F, D, S>(
    scope: &G,
    vecs: Arc<Vec<(ElementId, D)>>,
    sketcher: Arc<S>,
    hash_fns: Arc<TensorCollection<F>>,
    worker: u64,
    matrix: MatrixDescription,
    direction: MatrixDirection,
) -> Stream<G, (ElementId, (D, TensorPool, S::Output))>
where
    G: Scope,
    G::Timestamp: Succ,
    D: ExchangeData + SketchEstimate + Debug,
    S: Sketcher<Input = D> + Clone + 'static,
    S::Output: SketchData + Debug,
    F: LSHFunction<Input = D, Output = u32> + Sync + Send + Clone + 'static,
{
    let logger = scope.danny_logger();
    source(scope, "hashed source", move |capability| {
        let mut cap = Some(capability);
        move |output| {
            if let Some(cap) = cap.take() {
                let start = Instant::now();
                let mut session = output.session(&cap);
                for (k, v) in vecs.iter() {
                    let pool = hash_fns.pool(v);
                    let s = sketcher.sketch(v);
                    let output_element = (k.clone(), (v.clone(), pool.clone(), s.clone()));
                    match direction {
                        MatrixDirection::Columns => {
                            let col = (k.route() % u64::from(matrix.columns)) as u8;
                            for row in 0..matrix.rows {
                                session.give(((row, col), output_element.clone()));
                            }
                        }
                        MatrixDirection::Rows => {
                            let row = (k.route() % u64::from(matrix.rows)) as u8;
                            for col in 0..matrix.columns {
                                session.give(((row, col), output_element.clone()));
                            }
                        }
                    };
                }
                let end = Instant::now();
                info!("Distributed sketches and pools in {:?}", end - start);
            }
        }
    })
    .exchange(move |tuple| matrix.row_major((tuple.0).0, (tuple.0).1))
    .map(|pair| pair.1)
}

pub fn one_round_lsh<D, F, H, S, V, B, R>(
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
    S: Sketcher<Input = D, Output = V> + Send + Sync + Clone + 'static,
    V: SketchData + Debug,
    R: Rng + SeedableRng + Send + Sync + Clone + 'static,
    B: Fn(usize, &mut R) -> H + Sized + Send + Sync + Clone + 'static,
{
    use std::cell::RefCell;
    use std::rc::Rc;

    let no_dedup = config.no_dedup;
    let no_verify = config.no_verify;

    let result = Rc::new(RefCell::new(0usize));
    let result_read = Rc::clone(&result);

    let hasher = Arc::new(TensorCollection::new(
        k,
        range,
        config.recall,
        hash_function_builder,
        rng,
    ));

    let left_vectors = Arc::new(load_for_worker(worker.index(), worker.peers(), left_path));
    let right_vectors = Arc::new(load_for_worker(worker.index(), worker.peers(), left_path));

    let hasher = Arc::clone(&hasher);

    let sim_pred = sim_pred.clone();
    let sketch_predicate = sketch_predicate.clone();
    let sketcher = sketcher.clone();
    let sketcher = Arc::new(sketcher);
    let worker_index = worker.index() as u64;
    let matrix = MatrixDescription::for_workers(worker.peers());

    let probe = worker.dataflow::<u32, _, _>(move |scope| {
        let mut probe = ProbeHandle::<u32>::new();
        let logger = scope.danny_logger();
        let mut pl = progress_logger::ProgressLogger::builder()
            .with_items_name("repetitions")
            .with_expected_updates(hasher.repetitions() as u64)
            .start();

        let left = simple_source(
            scope,
            Arc::clone(&left_vectors),
            Arc::clone(&sketcher),
            Arc::clone(&hasher),
            worker_index,
            matrix,
            MatrixDirection::Rows,
        );
        let right = simple_source(
            scope,
            Arc::clone(&right_vectors),
            Arc::clone(&sketcher),
            Arc::clone(&hasher),
            worker_index,
            matrix,
            MatrixDirection::Columns,
        );

        left.binary_frontier(&right, PipelinePact, PipelinePact, "bucket", move |_, _| {
            let mut notificator = FrontierNotificator::new();
            let mut left_data = HashMap::new();
            let mut right_data = HashMap::new();
            let mut left_vectors = HashMap::new();
            let mut right_vectors = HashMap::new();
            move |left_in, right_in, output| {
                left_in.for_each(|t, data| {
                    log_event!(logger, (LogEvent::Load(t.time().to_step_id()), data.len()));
                    let local_data = left_data
                        .entry(t.time().clone())
                        .or_insert_with(|| Vec::new());
                    let local_vectors = left_vectors
                        .entry(t.time().clone())
                        .or_insert_with(HashMap::new);
                    for (k, (v, p, s)) in data.replace(Vec::new()).drain(..) {
                        local_data.push((k, p, s));
                        local_vectors.entry(k).or_insert(v);
                    }
                    notificator.notify_at(t.retain());
                });
                right_in.for_each(|t, data| {
                    log_event!(logger, (LogEvent::Load(t.time().to_step_id()), data.len()));
                    let local_data = right_data
                        .entry(t.time().clone())
                        .or_insert_with(|| Vec::new());
                    let local_vectors = right_vectors
                        .entry(t.time().clone())
                        .or_insert_with(HashMap::<ElementId, D>::new);
                    for (k, (v, p, s)) in data.replace(Vec::new()).drain(..) {
                        local_data.push((k, p, s));
                        local_vectors.entry(k).or_insert(v);
                    }
                    notificator.notify_at(t.retain());
                });
                notificator.for_each(&[left_in.frontier(), &right_in.frontier()], |t, _| {
                    if let Some(left_data) = left_data.remove(&t) {
                        let right_data = right_data.remove(&t).expect("missing right data");
                        let left_vectors = left_vectors.remove(&t).expect("missing left vectors");
                        let right_vectors =
                            right_vectors.remove(&t).expect("missing right vectors");
                        let repetitions = hasher.repetitions();
                        let mut cnt = 0;
                        info!("Starting {} repetitions", repetitions);
                        for rep in 0..repetitions {
                            let start = Instant::now();
                            let mut joiner = Joiner::default();
                            for (k, pool, sketch) in left_data.iter() {
                                joiner.push_left(hasher.hash(pool, rep), (*k, *sketch, pool));
                            }
                            for (k, pool, sketch) in right_data.iter() {
                                joiner.push_right(hasher.hash(pool, rep), (*k, *sketch, pool));
                            }
                            let mut sketch_discarded = 0;
                            let mut duplicates_discarded = 0;
                            let mut examined_pairs = 0;
                            joiner.join_map(
                                |_hash, (lk, l_sketch, l_pool), (rk, r_sketch, r_pool)| {
                                    examined_pairs += 1;
                                    if sketch_predicate.eval(l_sketch, r_sketch) {
                                        if no_verify
                                            || sim_pred(&left_vectors[lk], &right_vectors[rk])
                                        {
                                            if no_dedup || !hasher.already_seen(l_pool, r_pool, rep)
                                            {
                                                cnt += 1;
                                            } else {
                                                duplicates_discarded += 1;
                                            }
                                        }
                                    } else {
                                        sketch_discarded += 1;
                                    }
                                },
                            );
                            let end = Instant::now();
                            info!("Repetition {} ended in {:?}", rep, end - start);
                            pl.update(1u64);
                            log_event!(logger, (LogEvent::GeneratedPairs(rep), examined_pairs));
                            log_event!(logger, (LogEvent::SketchDiscarded(rep), sketch_discarded));
                            log_event!(
                                logger,
                                (LogEvent::DuplicatesDiscarded(rep), duplicates_discarded)
                            );
                        }
                        output.session(&t).give(cnt);
                    }
                });
            }
        })
        .stream_sum()
        .exchange(|_| 0) // Bring all the counts to the first worker
        .inspect_time(|t, cnt| info!("count at {}: {}", t, cnt))
        .unary(PipelinePact, "count collection", move |_, _| {
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

    worker.step_while(|| !probe.done());

    // info!("Collect network summaries");
    // let network_summaries = network.map(|n| n.measure().collect_from_workers(worker, &config));

    if worker.index() == 0 {
        result_read.replace(0)
    } else {
        0
    }
}
