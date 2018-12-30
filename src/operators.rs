use abomonation::Abomonation;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;
use timely::dataflow::channels::pact::ParallelizationContract;
use timely::dataflow::channels::pact::{Exchange as ExchangePact, Pipeline as PipelinePact};
use timely::dataflow::channels::pushers::tee::Tee;
use timely::dataflow::operators::capture::{EventLink, Extract, Replay};
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::generic::OperatorInfo;
use timely::dataflow::operators::generic::{FrontieredInputHandle, InputHandle, OutputHandle};
use timely::dataflow::operators::Capability;
use timely::dataflow::operators::{
    Concat, ConnectLoop, Enter, Input, Inspect, Leave, LoopVariable, Map, Operator,
};
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::Data;

/// Trait for types that can tell where they should be redirected when data is exchanged among
/// workers
pub trait Route {
    fn route(&self) -> u64;
}

impl Route for i32 {
    #[inline(always)]
    fn route(&self) -> u64 {
        *self as u64
    }
}

impl Route for u32 {
    #[inline(always)]
    fn route(&self) -> u64 {
        *self as u64
    }
}

impl Route for u64 {
    #[inline(always)]
    fn route(&self) -> u64 {
        *self
    }
}

impl Route for Vec<bool> {
    #[inline(always)]
    fn route(&self) -> u64 {
        // TODO check how this function distributes values
        let mut h = 0u64;
        let mut tmp = 0u64;
        let mut cnt = 0;
        for b in self.iter() {
            if cnt == 64 {
                h = h ^ tmp;
                tmp = 0u64;
            }
            if *b {
                tmp = (tmp << 1) | 1u64;
            } else {
                tmp = tmp << 1;
            }
            cnt += 1;
        }
        h
    }
}

impl<D> Route for (u64, D) {
    #[inline(always)]
    fn route(&self) -> u64 {
        self.0
    }
}

impl<D> Route for (u32, D) {
    #[inline(always)]
    fn route(&self) -> u64 {
        self.0 as u64
    }
}

pub trait Duplicate<G, D>
where
    G: Scope,
    D: Data,
{
    fn duplicate(&self) -> (Stream<G, D>, Stream<G, D>);
}

impl<G, D> Duplicate<G, D> for Stream<G, D>
where
    G: Scope,
    D: Data,
{
    fn duplicate(&self) -> (Stream<G, D>, Stream<G, D>) {
        let mut builder = OperatorBuilder::new("duplicate".to_owned(), self.scope());

        let mut input = builder.new_input(self, PipelinePact);
        let (mut output1, stream1) = builder.new_output();
        let (mut output2, stream2) = builder.new_output();

        builder.build(move |_| {
            move |_| {
                // Maybe move the activation outside?
                let mut output1_handle = output1.activate();
                let mut output2_handle = output2.activate();
                input.for_each(|t, d| {
                    let cap1 = t.delayed_for_output(t.time(), 0);
                    let cap2 = t.delayed_for_output(t.time(), 1);
                    let mut session1 = output1_handle.session(&cap1);
                    let mut session2 = output2_handle.session(&cap2);
                    session1.give_iterator(d.iter().map(|x| x.clone()));
                    session2.give_vec(&mut d.replace(Vec::new()));
                });
            }
        });

        (stream1, stream2)
    }
}

pub trait BinaryOperator<G: Scope, D1: Data> {
    /// Like Operator::binary_frontier, but with one additional output stream
    fn binary_in_out_frontier<D2, D3, D4, B, L, P1, P2>(
        &self,
        right: &Stream<G, D2>,
        pact1: P1,
        pact2: P2,
        name: &str,
        constructor: B,
    ) -> (Stream<G, D3>, Stream<G, D4>)
    where
        D2: Data,
        D3: Data,
        D4: Data,
        P1: ParallelizationContract<G::Timestamp, D1>,
        P2: ParallelizationContract<G::Timestamp, D2>,
        B: FnOnce(Capability<G::Timestamp>, Capability<G::Timestamp>, OperatorInfo) -> L,
        L: FnMut(
                &mut FrontieredInputHandle<G::Timestamp, D1, P1::Puller>,
                &mut FrontieredInputHandle<G::Timestamp, D2, P2::Puller>,
                &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>,
                &mut OutputHandle<G::Timestamp, D4, Tee<G::Timestamp, D4>>,
            ) + 'static;
}

impl<G: Scope, D1: Data> BinaryOperator<G, D1> for Stream<G, D1> {
    fn binary_in_out_frontier<D2, D3, D4, B, L, P1, P2>(
        &self,
        right: &Stream<G, D2>,
        pact1: P1,
        pact2: P2,
        name: &str,
        constructor: B,
    ) -> (Stream<G, D3>, Stream<G, D4>)
    where
        D2: Data,
        D3: Data,
        D4: Data,
        P1: ParallelizationContract<G::Timestamp, D1>,
        P2: ParallelizationContract<G::Timestamp, D2>,
        B: FnOnce(Capability<G::Timestamp>, Capability<G::Timestamp>, OperatorInfo) -> L,
        L: FnMut(
                &mut FrontieredInputHandle<G::Timestamp, D1, P1::Puller>,
                &mut FrontieredInputHandle<G::Timestamp, D2, P2::Puller>,
                &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>,
                &mut OutputHandle<G::Timestamp, D4, Tee<G::Timestamp, D4>>,
            ) + 'static,
    {
        let mut builder = OperatorBuilder::new(name.to_owned(), self.scope());
        let index = builder.index();
        let global = builder.global();

        // builder.set_notify(false);

        let mut input1 = builder.new_input(self, pact1);
        let mut input2 = builder.new_input(right, pact2);
        let (mut output1, stream1) = builder.new_output();
        let (mut output2, stream2) = builder.new_output();

        builder.build(move |mut capabilities| {
            // `capabilities` should be a two-element vector.
            let capability2 = capabilities.pop().unwrap();
            let capability1 = capabilities.pop().unwrap();
            let operator_info = OperatorInfo::new(index, global);
            let mut logic = constructor(capability1, capability2, operator_info);
            move |frontiers| {
                let mut input1_handle = FrontieredInputHandle::new(&mut input1, &frontiers[0]);
                let mut input2_handle = FrontieredInputHandle::new(&mut input2, &frontiers[1]);
                // Maybe move the activation outside?
                let mut output1_handle = output1.activate();
                let mut output2_handle = output2.activate();
                logic(
                    &mut input1_handle,
                    &mut input2_handle,
                    &mut output1_handle,
                    &mut output2_handle,
                );
            }
        });

        (stream1, stream2)
    }
}

pub trait Cartesian<G, D1>
where
    G: Scope,
    D1: Data + Debug + Send + Sync + Abomonation + Clone,
{
    fn cartesian<D2, H1, H2>(
        &self,
        right: &Stream<G, D2>,
        router1: H1,
        router2: H2,
        workers: u64,
    ) -> Stream<G, (D1, D2)>
    where
        D2: Data + Debug + Send + Sync + Abomonation + Clone,
        H1: Fn(&D1) -> u64 + 'static,
        H2: Fn(&D2) -> u64 + 'static,
    {
        self.cartesian_filter(&right, |_, _| true, router1, router2, workers)
    }

    fn cartesian_filter<D2, F, H1, H2>(
        &self,
        right: &Stream<G, D2>,
        filter: F,
        router1: H1,
        router2: H2,
        workers: u64,
    ) -> Stream<G, (D1, D2)>
    where
        D2: Data + Debug + Send + Sync + Abomonation + Clone,
        F: Fn(&D1, &D2) -> bool + 'static,
        H1: Fn(&D1) -> u64 + 'static,
        H2: Fn(&D2) -> u64 + 'static;
}

fn pair_router<D>(pair: &(u64, D)) -> u64
where
    D: Data,
{
    pair.0
}

impl<G, D1> Cartesian<G, D1> for Stream<G, D1>
where
    G: Scope,
    D1: Data + Debug + Send + Sync + Abomonation + Clone,
{
    fn cartesian_filter<D2, F, H1, H2>(
        &self,
        right: &Stream<G, D2>,
        filter: F,
        router1: H1,
        router2: H2,
        workers: u64,
    ) -> Stream<G, (D1, D2)>
    where
        D2: Data + Debug + Send + Sync + Abomonation + Clone,
        F: Fn(&D1, &D2) -> bool + 'static,
        H1: Fn(&D1) -> u64 + 'static,
        H2: Fn(&D2) -> u64 + 'static,
    {
        let result_stream = self.scope().iterative::<u32, _, _>(|inner_scope| {
            let (handle, right_cycle) = inner_scope.loop_variable(1);
            // let (handle, right_cycle) = inner_scope.feedback(1);
            let right_cycle = right
                .enter(inner_scope)
                .map(move |x| (router2(&x) % workers, x))
                .concat(&right_cycle); // concat with the loop variable

            // Operator state
            let mut left_vectors = Vec::new();
            let mut right_stash = HashMap::new();

            let iterations = workers as u32;

            let (result_stream, loop_stream) = self.enter(inner_scope).binary_in_out_frontier(
                &right_cycle,
                ExchangePact::new(router1),
                ExchangePact::new(pair_router),
                "cartesian filter loop",
                move |_, _, _| {
                    move |left_in, right_in, results, into_loop| {
                        left_in.for_each(|_, data| {
                            left_vectors.extend(data.replace(Vec::new()));
                        });
                        right_in.for_each(|time, data| {
                            // Here we have to specify that we will use the time capability with output
                            // 2, otherwise we will get a panic for using the wrong buffer.
                            let result_time = time.delayed_for_output(time.time(), 0);
                            right_stash
                                .entry(time.retain_for_output(1))
                                .or_insert((result_time, Vec::new()))
                                .1
                                .append(&mut data.replace(Vec::new()));
                        });

                        let frontiers = &[left_in.frontier(), right_in.frontier()];
                        for (time, (results_time, elems)) in right_stash.iter_mut() {
                            if frontiers.iter().all(|f| !f.less_than(time)) {
                                // At this point we have none of the two inputs can produce elements at
                                // a time before the stashed one.
                                if time.inner < iterations {
                                    // Produce the output pairs for this iteration
                                    debug!(
                                        "Filtering block of pairs {}x{}",
                                        left_vectors.len(),
                                        elems.len()
                                    );
                                    let mut result_session = results.session(&results_time);
                                    for (_, rv) in elems.iter() {
                                        for lv in left_vectors.iter() {
                                            if filter(lv, rv) {
                                                result_session.give((lv.clone(), rv.clone()));
                                            }
                                        }
                                    }
                                    // Produce right pairs for the next iteration, if there is a next
                                    // iteration.
                                    let mut loop_session = into_loop.session(&time);
                                    for (p, rv) in elems.drain(..) {
                                        loop_session.give((p + 1, rv.clone()));
                                    }
                                } else {
                                    // Not emitting elements, just draining them
                                    elems.drain(..);
                                }
                            }
                        }
                        // VERY IMPORTANT. Clean up empty stash times.
                        // This will drop the time,signalling the system that we are done with it.
                        // Failing to do it will make the system hang indefinitely.
                        right_stash.retain(|_time, pair| pair.1.len() > 0);
                    }
                },
            );

            loop_stream.connect_loop(handle);

            result_stream.leave()
        });

        result_stream
    }
}

enum JoinMessage<K, D>
where
    K: Data,
    D: Data,
{
    Pair(K, K),
    Data(D),
}

pub trait ThreeWayJoin<G, K, D1>
where
    G: Scope,
    K: Data + Route + Abomonation + Sync + Send + Clone + Eq + Hash + Debug,
    D1: Data + Abomonation + Sync + Send + Clone + Debug,
{
    /// Return a stream of pairs which is a subset of the center dataset, such that the left key
    /// comes from the calling stream, and the right key comes from the right stream. Membership of
    /// pairs to the returned substream is determined by evaluating the given predicate filter on
    /// pairs of values associated with the keys from the left and the right streams, respectively
    fn three_way_join<D2, F>(
        &self,
        center: &Stream<G, (K, K)>,
        right: &Stream<G, (K, D2)>,
        filter: F,
        workers: u64,
    ) -> Stream<G, (K, K)>
    where
        D2: Data + Abomonation + Sync + Send + Clone + Debug,
        F: Fn(&D1, &D2) -> bool + 'static;
}

impl<G, K, D1> ThreeWayJoin<G, K, D1> for Stream<G, (K, D1)>
where
    G: Scope,
    K: Data + Route + Abomonation + Sync + Send + Clone + Eq + Hash + Debug,
    D1: Data + Abomonation + Sync + Send + Clone + Debug,
{
    fn three_way_join<D2, F>(
        &self,
        center: &Stream<G, (K, K)>,
        right: &Stream<G, (K, D2)>,
        filter: F,
        workers: u64,
    ) -> Stream<G, (K, K)>
    where
        D2: Data + Abomonation + Sync + Send + Clone + Debug,
        F: Fn(&D1, &D2) -> bool + 'static,
    {
        let route_left_vector = move |r: &K| r.route() % workers;

        // State for the three way join
        let mut left_complete = false;
        let mut left_timestamp = None;
        let mut right_complete = false;
        let mut right_timestamp = None;
        let mut left_vectors = HashMap::new();
        // A map between workers and the set of vectors sent to them.
        let mut left_vectors_sent: Vec<HashSet<K>> = vec![HashSet::new(); workers as usize];
        let mut right_vectors = HashMap::new();
        // If the capability is None, then the pair has already been sent. We keep it into the
        // hashmap because we use the set of keys as a deduplication device.
        let mut center_pairs: HashMap<(K, K), Option<Capability<G::Timestamp>>> = HashMap::new();
        let mut candidate_pairs = HashMap::new();
        // The vectors coming from the left to be joined with values from the right
        let mut left_vectors_for_right_side: HashMap<K, D1> = HashMap::new();

        let candidates = self.binary_frontier(
            &center,
            ExchangePact::new(|pair: &(K, D1)| pair.0.route()),
            ExchangePact::new(|pair: &(K, K)| pair.0.route()),
            "three-way-join-left",
            move |_, _| {
                move |left_in, center_in, output| {
                    // Accumulate left vectors
                    // TODO: is there a way to leverage the partitioning from the previous steps?
                    // Maybe with replaying a stream?
                    left_in.for_each(|t, data| {
                        left_timestamp.get_or_insert(t.time().clone());
                        let mut data = data.replace(Vec::new());
                        for (k, v) in data.drain(..) {
                            left_vectors.insert(k, v);
                        }
                    });
                    // println!("Left Vectors: {:#?}", left_vectors);
                    center_in.for_each(|t, data| {
                        let mut data = data.replace(Vec::new());
                        let t = t.retain();
                        for pair in data.drain(..) {
                            center_pairs.entry(pair).or_insert(Some(t.clone()));
                        }
                    });

                    // Check if the left input is complete
                    if !left_complete {
                        left_complete = left_timestamp
                            .clone()
                            .map(|t| !left_in.frontier().less_than(&t))
                            .unwrap_or(false);
                    }

                    if left_complete {
                        // We have seen all the values from the left stream
                        for pair in center_pairs.iter_mut() {
                            if pair.1.is_some() {
                                // Get the capability out of the pair, leaving a None in its
                                // place. This has the effect of marking the pair as being sent
                                let time = pair.1.take().unwrap();
                                let mut session = output.session(&time);
                                let l = (pair.0).0.clone();
                                let r = (pair.0).1.clone();
                                let dest = route_left_vector(&r);
                                let opt_vec = if !left_vectors_sent[dest as usize].contains(&l) {
                                    left_vectors_sent[dest as usize].insert(l.clone());
                                    Some(left_vectors[&l].clone())
                                } else {
                                    None
                                };
                                session.give((r.clone(), (l.clone(), opt_vec)));
                            }
                        }
                    }
                    // TODO: Do cleanup of left vectors?
                }
            },
        );

        let output = candidates.binary_frontier(
            &right,
            ExchangePact::new(|pair: &(K, (K, Option<D1>))| pair.0.route()),
            ExchangePact::new(|pair: &(K, D2)| pair.0.route()),
            "three-way-join-right",
            move |_, _| {
                move |candidates_in, right_in, output| {
                    right_in.for_each(|t, data| {
                        right_timestamp.get_or_insert(t.time().clone());
                        let mut data = data.replace(Vec::new());
                        // println!("3w right: {:?}", data);
                        for (k, v) in data.drain(..) {
                            right_vectors.insert(k, v);
                        }
                    });

                    candidates_in.for_each(|t, data| {
                        // TODO: Handle the different types of messages
                        let mut entry = candidate_pairs.entry(t.retain()).or_insert(HashSet::new());
                        let mut data = &mut data.replace(Vec::new());
                        for (r, (l, opt_vec)) in data.drain(..) {
                            entry.insert((l.clone(), r.clone()));
                            if opt_vec.is_some() && !left_vectors_for_right_side.contains_key(&l) {
                                left_vectors_for_right_side
                                    .insert(l, opt_vec.expect("The vector should be here!"));
                            }
                        }
                    });

                    // Check if the left input is complete
                    if !right_complete {
                        right_complete = right_timestamp
                            .clone()
                            .map(|t| !right_in.frontier().less_than(&t))
                            .unwrap_or(false);
                    }

                    if right_complete {
                        // We have seen all the values from the right stream
                        for (time, candidate_pairs) in candidate_pairs.drain() {
                            let mut session = output.session(&time);
                            for (l, r) in candidate_pairs.iter() {
                                let out_pair = (l.clone(), r.clone());
                                // Deduplication happend on the left hand side of the join
                                if filter(&left_vectors_for_right_side[&l], &right_vectors[&r]) {
                                    session.give(out_pair.clone());
                                }
                            }
                        }
                    }
                    // TODO: Cleanup right?
                }
            },
        );

        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use timely::dataflow::operators::{Capture, Inspect, Probe};
    use timely::dataflow::ProbeHandle;

    #[test]
    fn test_three_way_join() {
        let (send, recv) = mpsc::channel();
        let send = Arc::new(Mutex::new(send));
        // let conf = timely::Configuration::Process(2).try_build().unwrap();
        let conf = timely::Configuration::Thread.try_build().unwrap();
        timely::execute::execute_from(conf.0, conf.1, move |worker| {
            let peers = worker.peers();
            let send = send.lock().unwrap().clone();
            let (mut in1, mut in_center, mut in2, probe) = worker.dataflow(|scope| {
                let mut probe = ProbeHandle::new();
                let (in1, s1) = scope.new_input();
                let (in_center, stream_center) = scope.new_input();
                let (in2, s2) = scope.new_input();
                s1.three_way_join(
                    &stream_center,
                    &s2,
                    |w1: &String, w2: &String| w2.starts_with(w1),
                    peers as u64,
                )
                .probe_with(&mut probe)
                .capture_into(send);
                (in1, in_center, in2, probe)
            });

            if worker.index() == 0 {
                in1.send((0, "a".to_owned()));
                in1.send((1, "b".to_owned()));
                in1.send((2, "p".to_owned()));
                in1.send((10, "p".to_owned()));
                in_center.send_batch(&mut vec![(0, 1), (2, 10)]);
                in2.send((1, "ciao".to_owned()));
                in2.send_batch(&mut vec![
                    (1, "aiuola".to_owned()),
                    (3, "coriandoli".to_owned()),
                    (10, "pizza".to_owned()),
                ]);
                in1.advance_to(1);
                in_center.advance_to(1);
                in2.advance_to(1);
            }
            worker.step_while(|| probe.less_than(in1.time()));
        })
        .unwrap();

        assert_eq!(vec![(0, vec![(0, 1), (2, 10)])], recv.extract());
    }

}
