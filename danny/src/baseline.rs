use crate::config::Config;
use crate::io::*;
use crate::operators::*;
use abomonation::Abomonation;
use danny_base::types::*;
use serde::de::Deserialize;
use std::clone::Clone;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
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

#[cfg(feature = "seq-all-2-all")]
pub fn sequential<T, F>(thresh: f64, left_path: &str, right_path: &str, sim_fn: F) -> usize
where
    for<'de> T: ReadDataFile + Deserialize<'de>,
    F: Fn(&T, &T) -> f64,
{
    let mut left = Vec::new();
    let mut right = Vec::new();
    ReadBinaryFile::read_binary(left_path.into(), |_| true, |_, v| left.push(v));
    ReadBinaryFile::read_binary(right_path.into(), |_| true, |_, v| right.push(v));
    println!(
        "Loaded data:\n  left: {}\n  right: {}",
        left.len(),
        right.len(),
    );
    let mut pl = progress_logger::ProgressLogger::builder()
        .with_frequency(Duration::from_secs(10))
        .with_items_name("pairs")
        .with_expected_updates((left.len() * right.len()) as u64)
        .start();

    let mut sim_cnt = 0;
    for l in left.iter() {
        for r in right.iter() {
            let sim = sim_fn(l, r);
            if sim >= thresh {
                sim_cnt += 1;
            }
        }
        pl.update_light(right.len() as u64);
    }
    pl.stop();
    sim_cnt
}

#[cfg(feature = "all-2-all")]
pub fn all_pairs_parallel<T, F>(
    worker: &mut Worker<Allocator>,
    threshold: f64,
    left_path: &str,
    right_path: &str,
    sim_pred: F,
    config: &Config,
) -> usize
where
    for<'de> T: Deserialize<'de> + ReadDataFile + Data + Sync + Send + Clone + Abomonation + Debug,
    F: Fn(&T, &T) -> bool + Send + Clone + Sync + 'static,
{
    use std::cell::RefCell;
    use std::rc::Rc;
    let result = Rc::new(RefCell::new(0usize));
    let result_read = Rc::clone(&result);

    let start_time = Instant::now();

    let worker_vectors = Arc::new(load_for_worker::<T, _>(
        worker.index(),
        worker.peers(),
        left_path,
    ));
    info!("Worker has {} vectors", worker_vectors.len());

    let index = worker.index();
    let peers = worker.peers() as u64;
    info!("Started worker {}/{}", index, peers);
    let sim_pred = sim_pred.clone();

    let probe = worker.dataflow::<u32, _, _>(|scope| {
        let matrix = MatrixDescription::for_workers(peers as usize);
        let (row, col) = matrix.row_major_to_pair(index as u64);

        let left = simple_source(
            scope,
            Arc::clone(&worker_vectors),
            matrix,
            MatrixDirection::Rows,
        );
        let right = simple_source(
            scope,
            Arc::clone(&worker_vectors),
            matrix,
            MatrixDirection::Columns,
        );

        left.binary_frontier(&right, PipelinePact, PipelinePact, "bucket", move |_, _| {
            let mut notificator = FrontierNotificator::new();
            let mut left_vectors = HashMap::new();
            let mut right_vectors = HashMap::new();
            move |left_in, right_in, output| {
                left_in.for_each(|t, data| {
                    let local_vectors = left_vectors
                        .entry(t.time().clone())
                        .or_insert_with(HashMap::new);
                    for (k, v) in data.replace(Vec::new()).drain(..) {
                        local_vectors.entry(k).or_insert(v);
                    }
                    notificator.notify_at(t.retain());
                });
                right_in.for_each(|t, data| {
                    let local_vectors = right_vectors
                        .entry(t.time().clone())
                        .or_insert_with(HashMap::new);
                    for (k, v) in data.replace(Vec::new()).drain(..) {
                        local_vectors.entry(k).or_insert(v);
                    }
                    notificator.notify_at(t.retain());
                });
                notificator.for_each(&[left_in.frontier(), &right_in.frontier()], |t, _| {
                    if let Some(left_vectors) = left_vectors.remove(&t) {
                        let right_vectors =
                            right_vectors.remove(&t).expect("missing right vectors");
                        let mut pl = progress_logger::ProgressLogger::builder()
                            .with_frequency(Duration::from_secs(60))
                            .with_items_name("pairs")
                            .with_expected_updates(
                                (left_vectors.len() * right_vectors.len()) as u64,
                            )
                            .start();

                        let mut cnt = 0;
                        for (_lk, lv) in left_vectors.iter() {
                            let mut pairs_looked = 0_u64;
                            for (_rk, rv) in right_vectors.iter() {
                                if sim_pred(lv, rv) {
                                    cnt += 1;
                                }
                                pairs_looked += 1;
                            }
                            pl.update_light(pairs_looked);
                        }
                        pl.stop();
                        info!("Matching pairs: {}", cnt);
                        output.session(&t).give(cnt);
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

fn simple_source<G, D>(
    scope: &G,
    vecs: Arc<Vec<(ElementId, D)>>,
    matrix: MatrixDescription,
    direction: MatrixDirection,
) -> Stream<G, (ElementId, D)>
where
    G: Scope,
    G::Timestamp: Succ,
    D: ExchangeData + Debug,
{
    source(scope, "hashed source", move |capability| {
        let mut cap = Some(capability);
        move |output| {
            if let Some(cap) = cap.take() {
                let start = Instant::now();
                let mut session = output.session(&cap);
                for (k, v) in vecs.iter() {
                    let output_element = (k.clone(), v.clone());
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
                info!("Distributed vectors in {:?}", end - start);
            }
        }
    })
    .exchange(move |tuple| matrix.row_major((tuple.0).0, (tuple.0).1))
    .map(|pair| pair.1)
}
