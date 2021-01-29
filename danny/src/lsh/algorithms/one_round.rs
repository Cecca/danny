use crate::cartesian::*;
use crate::config::*;
use crate::experiment::Experiment;
use crate::io::*;
use crate::join::{Joiner, SelfJoiner};
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
use timely::logging::Logger;

use std::sync::Arc;
use std::time::Instant;
use timely::communication::Allocator;
use timely::dataflow::channels::pact::Pipeline as PipelinePact;

use timely::dataflow::operators::generic::source;
use timely::dataflow::operators::*;
use timely::dataflow::*;
use timely::worker::Worker;
use timely::ExchangeData;

pub const ONE_ROUND_VERSION: u8 = 2;

fn distribute<G, D>(
    stream: &Stream<G, (ElementId, D)>,
) -> Stream<G, (CartesianKey, Marker, (ElementId, D))>
where
    G: Scope,
    G::Timestamp: Succ,
    D: ExchangeData,
{
    let cartesian = SelfCartesian::for_peers(stream.scope().peers());
    stream
        .unary(PipelinePact, "distribute", move |_, _| {
            move |input, output| {
                input.for_each(|t, data| {
                    let data = data.replace(Vec::new());
                    let mut session = output.session(&t);
                    for (k, d) in data {
                        let output_element = (k, d);
                        let iter = cartesian
                            .keys_for(k)
                            .map(|(key, marker)| (key, marker, output_element.clone()));
                        session.give_iterator(iter);
                    }
                })
            }
        })
        .exchange(move |tuple| tuple.0.route())
}

fn simple_source<G, F, D, S>(
    scope: &G,
    vecs: Arc<Vec<(ElementId, D)>>,
    sketcher: Arc<S>,
    hash_fns: Arc<TensorCollection<F>>,
) -> Stream<G, (ElementId, (D, TensorPool, S::Output))>
where
    G: Scope,
    G::Timestamp: Succ,
    D: ExchangeData + SketchEstimate + Debug,
    S: Sketcher<Input = D> + Clone + 'static,
    S::Output: SketchData + Debug,
    F: LSHFunction<Input = D, Output = u32> + Sync + Send + Clone + 'static,
{
    let _logger = scope.danny_logger();
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
                    session.give(output_element);
                }
                let end = Instant::now();
                info!("Distributed sketches and pools in {:?}", end - start);
            }
        }
    })
}

fn solve_subproblem<D, F, V, H>(
    subproblem_key: CartesianKey,
    subproblem: Vec<(ElementId, TensorPool, V, Marker)>,
    vectors: &HashMap<ElementId, D>,
    hasher: &TensorCollection<H>,
    sketch_predicate: &SketchPredicate<V>,
    sim_pred: F,
    no_verify: bool,
    no_dedup: bool,
    logger: Option<Logger<(LogEvent, usize)>>,
) -> usize
where
    H: LSHFunction<Input = D, Output = u32> + Sync + Send + Clone + 'static,
    V: SketchData,
    F: Fn(&D, &D) -> bool + Send + Clone + Copy + Sync + 'static,
{
    let repetitions = hasher.repetitions();
    // info!(
    //     "Starting {} repetitions for subproblem {:?}",
    //     repetitions, subproblem_key
    // );
    let mut pl = progress_logger::ProgressLogger::builder()
        .with_expected_updates(repetitions as u64)
        .with_items_name("repetitions")
        .start();

    let mut cnt = 0;

    for rep in 0..repetitions {
        let mut sketch_discarded = 0;
        let mut duplicates_discarded = 0;
        let mut examined_pairs = 0;

        let start = Instant::now();

        if subproblem_key.on_diagonal() {
            let mut joiner = SelfJoiner::default();
            for (k, pool, sketch, _marker) in subproblem.iter() {
                joiner.push(hasher.hash(pool, rep), (*k, *sketch, pool));
            }

            joiner.join_map_slice(|_hash, bucket| {
                for (i, (_, (lk, l_sketch, l_pool))) in bucket.iter().enumerate() {
                    for (_, (rk, r_sketch, r_pool)) in bucket[i..].iter() {
                        examined_pairs += 1;
                        if sketch_predicate.eval(l_sketch, r_sketch) {
                            if no_verify || sim_pred(&vectors[lk], &vectors[rk]) {
                                if no_dedup || !hasher.already_seen(l_pool, r_pool, rep) {
                                    cnt += 1;
                                } else {
                                    duplicates_discarded += 1;
                                }
                            }
                        } else {
                            sketch_discarded += 1;
                        }
                    }
                }
            });
        } else {
            let mut joiner = Joiner::default();
            for (k, pool, sketch, _marker) in subproblem.iter().filter(|t| t.3.keep_left()) {
                joiner.push_left(hasher.hash(pool, rep), (*k, *sketch, pool));
            }
            for (k, pool, sketch, _marker) in subproblem.iter().filter(|t| t.3.keep_right()) {
                joiner.push_right(hasher.hash(pool, rep), (*k, *sketch, pool));
            }

            joiner.join_map(|_hash, (lk, l_sketch, l_pool), (rk, r_sketch, r_pool)| {
                examined_pairs += 1;
                if sketch_predicate.eval(l_sketch, r_sketch) {
                    if no_verify || sim_pred(&vectors[lk], &vectors[rk]) {
                        if no_dedup || !hasher.already_seen(l_pool, r_pool, rep) {
                            cnt += 1;
                        } else {
                            duplicates_discarded += 1;
                        }
                    }
                } else {
                    sketch_discarded += 1;
                }
            });
        }
        let end = Instant::now();
        pl.update(1u64);
        debug!("Repetition {} ended in {:?}", rep, end - start);
        log_event!(logger, (LogEvent::GeneratedPairs(rep), examined_pairs));
        log_event!(logger, (LogEvent::SketchDiscarded(rep), sketch_discarded));
        log_event!(
            logger,
            (LogEvent::DuplicatesDiscarded(rep), duplicates_discarded)
        );
    }
    pl.stop();

    cnt
}

pub fn one_round_lsh<D, F, H, S, V, B, R>(
    worker: &mut Worker<Allocator>,
    path: &str,
    range: f64,
    k: usize,
    hash_function_builder: B,
    sketcher: S,
    sketch_predicate: SketchPredicate<V>,
    sim_pred: F,
    rng: &mut R,
    config: &Config,
    _experiment: &mut Experiment,
) -> usize
where
    for<'de> D: ReadBinaryFile + Deserialize<'de> + ExchangeData + Debug + SketchEstimate,
    F: Fn(&D, &D) -> bool + Send + Clone + Copy + Sync + 'static,
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

    let worker_index = worker.index();

    let result = Rc::new(RefCell::new(0usize));
    let result_read = Rc::clone(&result);

    let hasher = Arc::new(TensorCollection::new(
        k,
        range,
        config.recall,
        hash_function_builder,
        rng,
    ));

    let vectors = Arc::new(load_for_worker(worker.index(), worker.peers(), path));

    let hasher = Arc::clone(&hasher);

    let sim_pred = sim_pred.clone();
    let sketch_predicate = sketch_predicate.clone();
    let sketcher = sketcher.clone();
    let sketcher = Arc::new(sketcher);

    let probe = worker.dataflow::<u32, _, _>(move |scope| {
        let mut probe = ProbeHandle::<u32>::new();
        let logger = scope.danny_logger();

        let hashes = simple_source(
            scope,
            Arc::clone(&vectors),
            Arc::clone(&sketcher),
            Arc::clone(&hasher),
        );
        let hashes = distribute(&hashes);

        hashes
            .unary_frontier(PipelinePact, "bucket", move |_, _| {
                let mut notificator = FrontierNotificator::new();
                let mut subproblems = HashMap::new();
                let mut vectors = HashMap::new();
                move |input, output| {
                    input.for_each(|t, data| {
                        log_event!(logger, (LogEvent::Load(t.time().to_step_id()), data.len()));
                        let subproblems = subproblems
                            .entry(t.time().clone())
                            .or_insert_with(HashMap::new);
                        let local_vectors =
                            vectors.entry(t.time().clone()).or_insert_with(HashMap::new);
                        for (subproblem_key, marker, (k, (v, p, s))) in
                            data.replace(Vec::new()).drain(..)
                        {
                            subproblems
                                .entry(subproblem_key)
                                .or_insert_with(Vec::new)
                                .push((k, p, s, marker));
                            local_vectors.entry(k).or_insert(v);
                        }
                        notificator.notify_at(t.retain());
                    });

                    notificator.for_each(&[input.frontier()], |t, _| {
                        if let Some(subproblems) = subproblems.remove(&t) {
                            info!(
                                "Worker {} has {} subproblems",
                                worker_index,
                                subproblems.len()
                            );
                            let vectors = vectors.remove(&t).expect("missing left vectors");

                            for (subproblem_key, subproblem) in subproblems {
                                let cnt = solve_subproblem(
                                    subproblem_key,
                                    subproblem,
                                    &vectors,
                                    &hasher,
                                    &sketch_predicate,
                                    sim_pred,
                                    no_verify,
                                    no_dedup,
                                    logger.clone(),
                                );
                                output.session(&t).give(cnt);
                            }
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
