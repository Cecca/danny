use crate::config::*;
use crate::experiment::Experiment;
use crate::io::*;
use crate::join::*;
use crate::logging::*;
use crate::lsh::repetition_stopwatch::RepetitionStopWatch;
use crate::operators::*;
use channels::pact;
use danny_base::lsh::*;
use danny_base::sketch::*;
use danny_base::types::ElementId;
use pact::Pipeline;
use rand::{Rng, SeedableRng};
use serde::de::Deserialize;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Instant;
use timely::communication::Allocator;
use timely::dataflow::operators::aggregation::Aggregate;
use timely::dataflow::operators::generic::source;
use timely::dataflow::operators::*;
use timely::dataflow::*;
use timely::progress::Timestamp;
use timely::worker::Worker;
use timely::ExchangeData;

pub const TWO_ROUND_VERSION: u8 = 2;

#[allow(clippy::too_many_arguments)]
pub fn two_round_lsh<D, F, H, B, R, S, V>(
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

    let no_dedup = config.no_dedup;
    let no_verify = config.no_verify;

    let vectors = Arc::new(load_for_worker::<D, _>(
        worker.index(),
        worker.peers(),
        path,
    ));

    let individual_recall = config.recall.sqrt();

    let hasher = TensorCollection::new(
        k,
        range,
        individual_recall,
        hash_function_builder.clone(),
        rng,
    );
    let hasher = Arc::new(hasher);

    let hasher_intern =
        TensorCollection::new(k2, range, individual_recall, hash_function_builder, rng);
    let hasher_intern = Arc::new(hasher_intern);

    let repetition_batch = config.repetition_batch;

    info!("configured recall {}", config.recall);

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
            probe.clone(),
            repetition_batch,
        );

        info!(
            "Starting {} internal repetitions",
            hasher_intern.repetitions()
        );
        hashes
            .self_join_map_slice(move |(outer_repetition, _hash, _subproblem), vals| {
                let mut cnt = 0;
                let mut total = 0;
                let mut sketch_cnt = 0;
                let mut duplicate_cnt = 0;
                let repetitions = hasher_intern.repetitions();
                let mut joiner = SelfJoiner::default();
                let all_to_all = vals.len() * vals.len();
                let start = Instant::now();
                for rep in 0..repetitions {
                    joiner.clear();
                    for (_, (marker, outer_pool, inner_pool, s, v)) in vals.iter() {
                        joiner.push(
                            hasher_intern.hash(inner_pool, rep),
                            (s, v, outer_pool, inner_pool, marker),
                        );
                    }

                    joiner.join_map_slice(|_hash, values| {
                        for (_, l) in values.iter().filter(|t| (t.1).4.keep_left()) {
                            for (_, r) in values.iter().filter(|t| (t.1).4.keep_right()) {
                                total += 1;
                                if sketch_pred.eval(l.0, r.0) {
                                    if no_verify || sim_pred(&(l.1).1, &(r.1).1) {
                                        if no_dedup
                                            || (!hasher_intern.already_seen(&l.3, &r.3, rep)
                                                && !hasher.already_seen(
                                                    &l.2,
                                                    &r.2,
                                                    *outer_repetition,
                                                ))
                                        {
                                            cnt += 1;
                                        } else {
                                            duplicate_cnt += 1;
                                        }
                                    }
                                } else {
                                    sketch_cnt += 1;
                                }
                            }
                        }
                    });
                }
                debug!(
                    "Candidates {} ({}): Emitted {} / Discarded {} / Duplicates {} in {:?} ({})",
                    total,
                    all_to_all,
                    cnt,
                    sketch_cnt,
                    duplicate_cnt,
                    Instant::now() - start,
                    proc_mem!(),
                );
                log_event!(logger, (LogEvent::GeneratedPairs(*outer_repetition), cnt));
                log_event!(
                    logger,
                    (LogEvent::SketchDiscarded(*outer_repetition), sketch_cnt)
                );
                log_event!(
                    logger,
                    (
                        LogEvent::DuplicatesDiscarded(*outer_repetition),
                        duplicate_cnt
                    )
                );
                vec![cnt]
            })
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

fn source_hashed_two_round<G, T, D, F, S>(
    scope: &G,
    vecs: Arc<Vec<(ElementId, D)>>,
    sketcher: Arc<S>,
    hash_fns: Arc<TensorCollection<F>>,
    hash_fns2: Arc<TensorCollection<F>>,
    throttle: ProbeHandle<T>,
    repetition_batch: usize,
) -> Stream<
    G,
    (
        // (repetition, hash, subproblem split)
        (usize, u32, u8),
        (Marker, TensorPool, TensorPool, S::Output, (ElementId, D)),
    ),
>
where
    G: Scope<Timestamp = T>,
    T: Timestamp + Succ,
    D: ExchangeData + Debug,
    F: LSHFunction<Input = D, Output = u32> + Sync + Send + Clone + 'static,
    S: Sketcher<Input = D> + Clone + 'static,
    S::Output: SketchData + Debug,
{
    use std::cell::RefCell;
    use std::rc::Rc;

    let worker: u64 = scope.index() as u64;
    let logger = scope.danny_logger();
    let repetitions = hash_fns.repetitions();
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
    let hashes = Rc::new(RefCell::new(HashMap::new()));
    let hashes1 = Rc::clone(&hashes);
    let mut subproblem_sizes = HashMap::new();

    source(scope, "hashed source two round", move |capability| {
        let mut cap = Some(capability);
        let mut current_repetition = 0;

        move |output| {
            let mut done = false;
            if let Some(cap) = cap.as_mut() {
                if !throttle.less_than(&cap) {
                    stopwatch.maybe_stop();
                    stopwatch.start();
                    if worker == 0 {
                        debug!("Repetition {} (two round LSH)", current_repetition);
                    }
                    let mut hashes = hashes.borrow_mut();
                    let stash = hashes.entry(cap.time().clone()).or_insert_with(Vec::new);
                    let mut session = output.session(&cap);
                    for (k, v) in vecs.iter() {
                        let h = hash_fns.hash(&bit_pools[k], current_repetition as usize);
                        let s = sketcher.sketch(v);
                        stash.push((
                            (current_repetition, h),
                            (
                                bit_pools[k].clone(),
                                hash_fns2.pool(v),
                                s.clone(),
                                (k.clone(), v.clone()),
                            ),
                        ));
                        session.give(((current_repetition, h), 1));
                    }
                    if current_repetition % repetition_batch == 0 {
                        cap.downgrade(&cap.time().succ());
                    }
                    current_repetition += 1;
                    done = current_repetition >= repetitions;
                }
            }

            if done {
                // Drop the capability to signal that we will send no more data
                cap = None;
            }
        }
    })
    // Now accumulate the counters into the first worker to see the subproblem size
    .aggregate(
        |_key, val, agg| {
            *agg += val;
        },
        |key, agg: u32| (key, agg),
        |k| k.route() as u64,
    )
    .exchange(|_k| 0)
    .aggregate(
        |_key, val, agg| {
            *agg += val;
        },
        |key, agg: u32| (key, agg),
        |k| k.route() as u64,
    )
    .broadcast()
    // Determine the split points, and replicate vector appropriately
    .unary_notify(
        Pipeline,
        "split point determination",
        None,
        move |input, output, notificator| {
            input.for_each(|t, data| {
                subproblem_sizes
                    .entry(t.time().clone())
                    .or_insert_with(Vec::new)
                    .extend(data.replace(Vec::new()));
                notificator.notify_at(t.retain());
            });
            notificator.for_each(|t, _, _| {
                let mut sizes = subproblem_sizes
                    .remove(t.time())
                    .expect("missing subproblem sizes!");
                // Sort by increasing count
                sizes.sort_unstable_by_key(|x| x.1);
                // Define a threshold for heavy hitters
                // let threshold = (total as f64 / peers as f64).ceil() as u32;
                let threshold = sizes[sizes.len() / 2].1;
                info!("subproblem sizes {:?}", sizes);
                info!("heavy hitters threshold: {}", threshold);
                let heavy_hitters: HashMap<(usize, u32), u32> =
                    sizes.into_iter().filter(|x| x.1 >= threshold).collect();
                info!("heavy hitters: {:?}", heavy_hitters);

                // Get the hashes and output them, splitting the subproblem if needed
                if let Some(hashes) = hashes1.borrow_mut().remove(&t.time()) {
                    let mut session = output.session(&t);
                    for (key, payload) in hashes {
                        if let Some(count) = heavy_hitters.get(&key) {
                            // Deal with the heavy hitter by replicating it into
                            // rows and columns
                            let id = ((payload.3).0).0;
                            let groups = (count / threshold) as u8;
                            assert!(groups > 0);
                            let col = (id % groups as u32) as u8;
                            for row in col..groups {
                                let i = row * groups + col; // row major
                                session.give((
                                    (key.0, key.1, i),
                                    (
                                        Marker::HeavyLeft,
                                        payload.0.clone(),
                                        payload.1.clone(),
                                        payload.2.clone(),
                                        payload.3.clone(),
                                    ),
                                ));
                            }
                            let row = (id % groups as u32) as u8;
                            for col in row..groups {
                                let i = row * groups + col; // row major
                                session.give((
                                    (key.0, key.1, i),
                                    (
                                        Marker::HeavyRight,
                                        payload.0.clone(),
                                        payload.1.clone(),
                                        payload.2.clone(),
                                        payload.3.clone(),
                                    ),
                                ));
                            }
                        } else {
                            // This point is light, no need of treating it specially
                            session.give((
                                (key.0, key.1, 0u8),
                                (Marker::Light, payload.0, payload.1, payload.2, payload.3),
                            ));
                        }
                    }
                }
            });
        },
    )
}

#[derive(Clone, Copy, Hash, Ord, PartialOrd, Eq, PartialEq, Abomonation, Debug)]
enum Marker {
    Light,
    HeavyLeft,
    HeavyRight,
}

impl Marker {
    fn keep_left(&self) -> bool {
        match self {
            Self::Light | Self::HeavyLeft => true,
            Self::HeavyRight => false,
        }
    }
    fn keep_right(&self) -> bool {
        match self {
            Self::Light | Self::HeavyRight => true,
            Self::HeavyLeft => false,
        }
    }
}

impl Route for (usize, u32, u8) {
    #[inline(always)]
    fn route(&self) -> u64 {
        (self.0 as u64)
            .wrapping_mul(31u64)
            .wrapping_add(u64::from(self.1.route()))
            .wrapping_mul(31u64)
            .wrapping_add(u64::from(self.2.route()))
    }
}
