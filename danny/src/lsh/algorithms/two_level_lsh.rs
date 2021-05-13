use crate::cartesian::*;
use crate::config::*;
use crate::experiment::Experiment;
use crate::io::*;
use crate::join::*;
use crate::logging::*;
use crate::lsh::repetition_stopwatch::RepetitionStopWatch;
use crate::operators::*;
use danny_base::lsh::*;
use danny_base::sketch::*;
use danny_base::types::ElementId;
use rand::{Rng, SeedableRng};
use serde::de::Deserialize;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Instant;
use timely::communication::Allocator;
use timely::dataflow::operators::generic::source;
use timely::dataflow::operators::*;
use timely::dataflow::*;
use timely::progress::Timestamp;
use timely::worker::Worker;
use timely::ExchangeData;

pub const TWO_LEVEL_LSH_VERSION: u8 = 10;

#[allow(clippy::too_many_arguments)]
pub fn two_level_lsh<D, F, H, B, R, S, V>(
    worker: &mut Worker<Allocator>,
    path: &str,
    range: f64,
    k: usize,
    k2: usize,
    hash_function_builder: B,
    sketcher: S,
    sketch_pred: SketchPredicate<V>,
    sim_pred: F,
    rng: &mut R,
    config: &Config,
    _experiment: &mut Experiment,
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
    let result = Rc::new(RefCell::new(0usize));
    let result_read = Rc::clone(&result);

    let vectors = Arc::new(load_for_worker::<D, _>(
        worker.index(),
        worker.peers(),
        path,
    ));

    let r = config.recall;
    let mut eps = 0.01;
    let epsilons = std::iter::from_fn(|| {
        if eps > 0.99 - r {
            None
        } else {
            let e = eps;
            eps += 0.001;
            Some(e)
        }
    });
    let mut rngb = rng.clone();
    let (hasher, hasher_intern) = epsilons
        .map(|eps| {
            (
                Arc::new(TensorCollection::new(
                    k,
                    range,
                    r + eps,
                    hash_function_builder.clone(),
                    &mut rngb,
                )),
                Arc::new(TensorCollection::new(
                    k2,
                    range,
                    r / (r + eps),
                    hash_function_builder.clone(),
                    &mut rngb,
                )),
            )
        })
        .chain(Some((
            Arc::new(TensorCollection::new(
                k,
                range,
                r.sqrt(),
                hash_function_builder.clone(),
                rng,
            )),
            Arc::new(TensorCollection::new(
                k2,
                range,
                r.sqrt(),
                hash_function_builder.clone(),
                rng,
            )),
        )))
        .min_by_key(|(outer, inner)| outer.repetitions() * inner.repetitions())
        .unwrap();

    info!(
        "outer repetitions: {} inner repetitions: {}, total {}",
        hasher.repetitions(),
        hasher_intern.repetitions(),
        hasher.repetitions() * hasher_intern.repetitions()
    );

    let hasher = Arc::clone(&hasher);
    let hasher_intern = Arc::clone(&hasher_intern);
    let sim_pred = sim_pred.clone();
    let sketch_pred = sketch_pred.clone();

    let sketcher = sketcher.clone();
    let sketcher = Arc::new(sketcher);

    let probe = worker.dataflow::<u32, _, _>(move |scope| {
        let mut probe = ProbeHandle::new();
        let logger = scope.danny_logger();

        let hashes = source_hashed_two_round(
            scope,
            Arc::clone(&vectors),
            Arc::clone(&sketcher),
            Arc::clone(&hasher),
            Arc::clone(&hasher_intern),
        );

        info!(
            "Starting {} internal repetitions",
            hasher_intern.repetitions()
        );
        hashes
            .self_join_map(
                Balance::SubproblemSize,
                move |((outer_repetition, _h), subproblem_key), subproblem, payloads| {
                    let mut self_joiner = SelfJoiner::default();
                    let mut joiner = Joiner::default();
                    let repetitions = hasher_intern.repetitions();
                    let mut total_matching_cnt = 0;
                    for rep in 0..repetitions {
                        let mut matching_cnt = 0;
                        let mut candidate_pairs = 0;
                        let mut self_pairs_discarded = 0;
                        let mut similarity_discarded = 0;
                        let mut sketch_cnt = 0;
                        let mut duplicate_cnt = 0;
                        if subproblem_key.on_diagonal() {
                            self_joiner.clear();
                            for (_marker, element_id) in subproblem.iter() {
                                let (v, s, outer_pool, inner_pool) =
                                    payloads.get(element_id).expect("missing payload");
                                self_joiner.push(
                                    hasher_intern.hash(inner_pool, rep),
                                    (s, (element_id, v), outer_pool, inner_pool),
                                );
                            }
                            self_joiner.join_map(|_h, l, r| {
                                candidate_pairs += 1;
                                if (l.1).0 != (r.1).0 {
                                    if sketch_pred.eval(l.0, r.0) {
                                        if !hasher_intern.already_seen(&l.3, &r.3, rep)
                                            && !hasher.already_seen(&l.2, &r.2, outer_repetition)
                                        {
                                            if sim_pred(&(l.1).1, &(r.1).1) {
                                                matching_cnt += 1;
                                            } else {
                                                similarity_discarded += 1;
                                            }
                                        } else {
                                            duplicate_cnt += 1;
                                        }
                                    } else {
                                        sketch_cnt += 1;
                                    }
                                } else {
                                    self_pairs_discarded += 1;
                                }
                            })
                        } else {
                            joiner.clear();
                            for (marker, element_id) in subproblem.iter() {
                                let (v, s, outer_pool, inner_pool) =
                                    payloads.get(element_id).expect("missing payload");
                                match marker {
                                    Marker::Left => joiner.push_left(
                                        hasher_intern.hash(inner_pool, rep),
                                        (s, (element_id, v), outer_pool, inner_pool),
                                    ),
                                    Marker::Right => joiner.push_right(
                                        hasher_intern.hash(inner_pool, rep),
                                        (s, (element_id, v), outer_pool, inner_pool),
                                    ),
                                    Marker::Both => panic!("cannot get a both here"),
                                }
                            }
                            joiner.join_map(|_h, l, r| {
                                candidate_pairs += 1;
                                if (l.1).0 != (r.1).0 {
                                    if sketch_pred.eval(l.0, r.0) {
                                        if !hasher_intern.already_seen(&l.3, &r.3, rep)
                                            && !hasher.already_seen(&l.2, &r.2, outer_repetition)
                                        {
                                            if sim_pred(&(l.1).1, &(r.1).1) {
                                                matching_cnt += 1;
                                            } else {
                                                similarity_discarded += 1;
                                            }
                                        } else {
                                            duplicate_cnt += 1;
                                        }
                                    } else {
                                        sketch_cnt += 1;
                                    }
                                } else {
                                    self_pairs_discarded += 1;
                                }
                            })
                        }
                        log_event!(
                            logger,
                            (LogEvent::CandidatePairs(outer_repetition), candidate_pairs)
                        );
                        log_event!(
                            logger,
                            (
                                LogEvent::SelfPairsDiscarded(outer_repetition),
                                self_pairs_discarded
                            )
                        );
                        log_event!(
                            logger,
                            (LogEvent::OutputPairs(outer_repetition), matching_cnt)
                        );
                        log_event!(
                            logger,
                            (
                                LogEvent::SimilarityDiscarded(outer_repetition),
                                similarity_discarded
                            )
                        );
                        log_event!(
                            logger,
                            (LogEvent::SketchDiscarded(outer_repetition), sketch_cnt)
                        );
                        log_event!(
                            logger,
                            (
                                LogEvent::DuplicatesDiscarded(outer_repetition),
                                duplicate_cnt
                            )
                        );
                        total_matching_cnt += matching_cnt;
                    }

                    info!(
                        "Partial count {} for repetition {}",
                        total_matching_cnt, outer_repetition
                    );
                    Some(total_matching_cnt)
                },
            )
            .exchange(|_| 0) // Bring all the counts to the first worker
            .unary(
                timely::dataflow::channels::pact::Pipeline,
                "count collection",
                move |_, _| {
                    move |input, output| {
                        input.for_each(|t, data| {
                            let data = data.replace(Vec::new());
                            for c in data.into_iter() {
                                *result.borrow_mut() += c;
                            }
                            output.session(&t).give(());
                        });
                    }
                },
            )
            .probe_with(&mut probe);
        probe
    });

    // Do the stepping even though it's not strictly needed: we use it to wait for the dataflow
    // to finish
    worker.step_while(|| probe.with_frontier(|f| !f.is_empty()));

    if worker.index() == 0 {
        result_read.replace(0)
    } else {
        0
    }
}

impl<S: SketchData, D: ExchangeData> KeyPayload for (TensorPool, TensorPool, S, (ElementId, D)) {
    type Key = ElementId;
    type Value = (D, S, TensorPool, TensorPool);

    fn split_payload(self) -> (Self::Key, Self::Value) {
        ((self.3).0, ((self.3 .1), self.2, self.0, self.1))
    }
}

fn source_hashed_two_round<G, T, D, F, S>(
    scope: &G,
    vecs: Arc<Vec<(ElementId, D)>>,
    sketcher: Arc<S>,
    hash_fns: Arc<TensorCollection<F>>,
    hash_fns2: Arc<TensorCollection<F>>,
) -> Stream<
    G,
    (
        // (repetition, hash, subproblem split)
        (usize, u32),
        (TensorPool, TensorPool, S::Output, (ElementId, D)),
    ),
>
where
    G: Scope<Timestamp = T>,
    T: Timestamp + Succ,
    D: ExchangeData,
    F: LSHFunction<Input = D, Output = u32> + Sync + Send + Clone + 'static,
    S: Sketcher<Input = D> + Clone + 'static,
    S::Output: SketchData,
{
    let worker: u64 = scope.index() as u64;
    let logger = scope.danny_logger();
    let repetitions = hash_fns.repetitions();
    info!("Doing {} external repetitions", repetitions);
    let mut stopwatch = RepetitionStopWatch::new("repetition", worker == 0, logger);
    let mut bit_pools: HashMap<ElementId, TensorPool> = HashMap::new();
    let mut bit_pools_intern: HashMap<ElementId, TensorPool> = HashMap::new();
    info!("Computing the bit pools");
    let start = Instant::now();
    for (k, v) in vecs.iter() {
        bit_pools.insert(*k, hash_fns.pool(v));
        bit_pools_intern.insert(*k, hash_fns2.pool(v));
    }
    let end = Instant::now();
    info!(
        "Computed the bit pools ({:?}, {})",
        end - start,
        proc_mem!()
    );

    source(scope, "hashed source two round", move |capability| {
        let mut cap = Some(capability);
        let mut done = false;
        let mut current_repetition = 0;

        move |output| {
            if let Some(cap) = cap.as_mut() {
                stopwatch.maybe_stop();
                stopwatch.start();
                if worker == 0 {
                    debug!("Repetition {} (two level LSH)", current_repetition);
                }
                let mut session = output.session(&cap);
                for (k, v) in vecs.iter() {
                    let h = hash_fns.hash(&bit_pools[k], current_repetition as usize);
                    let s = sketcher.sketch(v);
                    session.give((
                        (current_repetition, h),
                        (
                            bit_pools[k].clone(),
                            hash_fns2.pool(v),
                            s.clone(),
                            (k.clone(), v.clone()),
                        ),
                    ));
                }
                current_repetition += 1;
                // cap.downgrade(&cap.time().succ());
                done = current_repetition == repetitions;
                if done {
                    info!("Completed generation of hashes and sketches");
                }
            }

            if done {
                // Drop the capability to signal that we will send no more data
                cap = None;
            }
        }
    })
}
