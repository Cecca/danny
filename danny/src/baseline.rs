use crate::cartesian::*;
use crate::io::*;
use crate::operators::*;
use abomonation::Abomonation;
use danny_base::sketch::{SketchPredicate, Sketcher};
use danny_base::types::*;
use serde::de::Deserialize;
use std::clone::Clone;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use timely::communication::Allocator;
use timely::dataflow::channels::pact::Pipeline as PipelinePact;
use timely::dataflow::operators::generic::operator::source;
use timely::dataflow::operators::*;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::worker::Worker;
use timely::Data;
use timely::ExchangeData;

pub const ALL_2_ALL_VERSION: u8 = 3;

#[cfg(feature = "seq-all-2-all")]
pub fn sequential<T, F>(thresh: f64, path: &str, sim_fn: F) -> usize
where
    for<'de> T: ReadDataFile + Deserialize<'de>,
    F: Fn(&T, &T) -> f64,
{
    let mut vecs = Vec::new();
    ReadBinaryFile::read_binary(path.into(), |_| true, |_, v| vecs.push(v));
    println!("Loaded data: {}", vecs.len(),);
    let mut pl = progress_logger::ProgressLogger::builder()
        .with_frequency(Duration::from_secs(10))
        .with_items_name("pairs")
        .with_expected_updates((vecs.len() * vecs.len()) as u64)
        .start();

    let mut sim_cnt = 0;
    for (i, l) in vecs.iter().enumerate() {
        for r in vecs[i..].iter() {
            let sim = sim_fn(l, r);
            if sim >= thresh {
                sim_cnt += 1;
            }
        }
        pl.update_light(vecs.len() as u64);
    }
    pl.stop();
    sim_cnt
}

#[cfg(feature = "all-2-all")]
pub fn all_pairs_parallel<T, F, S>(
    worker: &mut Worker<Allocator>,
    path: &str,
    sim_pred: F,
    sketcher: S,
    sketch_predicate: SketchPredicate<S::Output>,
) -> usize
where
    for<'de> T: Deserialize<'de> + ReadDataFile + Data + Sync + Send + Clone + Abomonation + Debug,
    F: Fn(&T, &T) -> bool + Send + Clone + Sync + 'static,
    S: Sketcher<Input = T> + Clone + 'static,
    S::Output: SketchData,
{
    use std::cell::RefCell;
    use std::rc::Rc;
    let result = Rc::new(RefCell::new(0usize));
    let result_read = Rc::clone(&result);

    let _start_time = Instant::now();

    let worker_vectors = Arc::new(load_for_worker::<T, _>(
        worker.index(),
        worker.peers(),
        path,
    ));
    info!("Worker has {} vectors", worker_vectors.len());

    let index = worker.index();
    let peers = worker.peers() as u64;
    info!("Started worker {}/{}", index, peers);
    let sim_pred = sim_pred.clone();

    let probe = worker.dataflow::<u32, _, _>(|scope| {
        let matrix = MatrixDescription::for_workers(peers as usize);
        let (_row, _col) = matrix.row_major_to_pair(index as u64);

        let vectors = distribute(&simple_source(scope, Arc::clone(&worker_vectors), sketcher));

        vectors
            .unary_frontier(PipelinePact, "bucket", move |_, _| {
                let mut notificator = FrontierNotificator::new();
                let mut vectors = HashMap::new();
                move |input, output| {
                    input.for_each(|t, data| {
                        let local_vectors =
                            vectors.entry(t.time().clone()).or_insert_with(HashMap::new);
                        for (k, marker, v) in data.replace(Vec::new()).drain(..) {
                            local_vectors
                                .entry(k)
                                .or_insert_with(Vec::new)
                                .push((marker, v));
                        }
                        notificator.notify_at(t.retain());
                    });

                    notificator.for_each(&[input.frontier()], |t, _| {
                        if let Some(subproblems) = vectors.remove(&t) {
                            info!("Worker {} has {} subproblems", index, subproblems.len());
                            for (subproblem_key, subproblem) in subproblems {
                                let mut cnt = 0;

                                let mut pl = progress_logger::ProgressLogger::builder()
                                    .with_frequency(Duration::from_secs(60))
                                    .with_items_name("pairs")
                                    .with_expected_updates(
                                        (subproblem.len() * subproblem.len()) as u64,
                                    )
                                    .start();

                                // we deal differently with subproblems on the diagonal. For those, we look at all pairs
                                // without duplicates by appropriately indexing into the subproblem.
                                // As for the other subproblems, we filter items marked as to be considered
                                // `Left` or `Right` in order to avoid duplicates
                                if subproblem_key.on_diagonal() {
                                    for (i, (_, (_lk, lv, l_sketch))) in
                                        subproblem.iter().enumerate()
                                    {
                                        let mut pairs_looked = 0_u64;
                                        for (_, (_rk, rv, r_sketch)) in subproblem[i..].iter() {
                                            if sketch_predicate.eval(l_sketch, r_sketch)
                                                && sim_pred(lv, rv)
                                            {
                                                cnt += 1;
                                            }
                                            pairs_looked += 1;
                                        }
                                        pl.update_light(pairs_looked);
                                    }
                                } else {
                                    for (_, (_lk, lv, l_sketch)) in
                                        subproblem.iter().filter(|t| t.0.keep_left())
                                    {
                                        let mut pairs_looked = 0_u64;
                                        for (_, (_rk, rv, r_sketch)) in
                                            subproblem.iter().filter(|t| t.0.keep_right())
                                        {
                                            if sketch_predicate.eval(l_sketch, r_sketch)
                                                && sim_pred(lv, rv)
                                            {
                                                cnt += 1;
                                            }
                                            pairs_looked += 1;
                                        }
                                        pl.update_light(pairs_looked);
                                    }
                                }
                                pl.stop();

                                info!(
                                    "{:?} matching pairs: {} (subproblem size {})",
                                    subproblem_key,
                                    cnt,
                                    subproblem.len()
                                );
                                output.session(&t).give(cnt);
                            }
                        }
                    });
                }
            })
            .exchange(|_| 0)
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
            .probe()
    });

    worker.step_while(|| !probe.done());

    if worker.index() == 0 {
        result_read.replace(0)
    } else {
        0
    }
}

fn simple_source<G, D, S>(
    scope: &G,
    vecs: Arc<Vec<(ElementId, D)>>,
    sketcher: S,
) -> Stream<G, (ElementId, D, S::Output)>
where
    G: Scope,
    G::Timestamp: Succ,
    D: ExchangeData + Debug,
    S: Sketcher<Input = D> + Clone + 'static,
    S::Output: SketchData,
{
    source(scope, "hashed source", move |capability| {
        let mut cap = Some(capability);
        move |output| {
            if let Some(cap) = cap.take() {
                let start = Instant::now();
                let mut session = output.session(&cap);
                for (k, v) in vecs.iter() {
                    let s = sketcher.sketch(v);
                    let output_element = (k.clone(), v.clone(), s);
                    session.give(output_element);
                }
                let end = Instant::now();
                info!("Distributed vectors in {:?}", end - start);
            }
        }
    })
}

fn distribute<G, D, V>(
    stream: &Stream<G, (ElementId, D, V)>,
) -> Stream<G, (CartesianKey, Marker, (ElementId, D, V))>
where
    G: Scope,
    G::Timestamp: Succ,
    D: ExchangeData,
    V: ExchangeData,
{
    let cartesian = SelfCartesian::for_peers(stream.scope().peers());
    stream
        .unary(PipelinePact, "distribute", move |_, _| {
            move |input, output| {
                input.for_each(|t, data| {
                    let data = data.replace(Vec::new());
                    let mut session = output.session(&t);
                    for (k, d, sketch) in data {
                        let output_element = (k, d, sketch);
                        let iter = cartesian
                            .keys_for(k)
                            .map(|key| (key.0, key.1, output_element.clone()));
                        session.give_iterator(iter);
                    }
                })
            }
        })
        .exchange(move |tuple| cartesian.diagonal_major(tuple.0))
}
