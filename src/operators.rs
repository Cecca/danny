use abomonation::Abomonation;
use std::collections::HashMap;
use std::fmt::Debug;
use timely::dataflow::channels::pact::Exchange as ExchangePact;
use timely::dataflow::channels::pact::ParallelizationContract;
use timely::dataflow::channels::pushers::tee::Tee;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::generic::OperatorInfo;
use timely::dataflow::operators::generic::{FrontieredInputHandle, OutputHandle};
use timely::dataflow::operators::Capability;
use timely::dataflow::operators::{Concat, ConnectLoop, Enter, Leave, LoopVariable, Map};
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::Data;

/// Trait for types that can tell where they should be redirected when data is exchanged among
/// workers
pub trait Route {
    fn route(&self) -> u64;
}

impl Route for u32 {
    #[inline(always)]
    fn route(&self) -> u64 {
        *self as u64
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
                                    let mut result_session = results.session(&results_time);
                                    println!(
                                        "Computing cross product of {} and {} elements",
                                        left_vectors.len(),
                                        elems.len()
                                    );
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
