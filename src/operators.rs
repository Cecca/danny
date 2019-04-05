use crate::bloom::*;
use crate::logging::*;
use abomonation::Abomonation;
use rand::SeedableRng;
use rand_xorshift::XorShiftRng;
use std::cell::Ref;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::ops::Add;
use std::ops::Deref;
use std::rc::Rc;
use std::sync::Arc;
use timely::dataflow::channels::pact::Pipeline as PipelinePact;
use timely::dataflow::operators::capture::event::{Event, EventPusher};
use timely::dataflow::operators::*;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::order::Product;
use timely::progress::Timestamp;
use timely::Data;

pub trait Succ
where
    Self: Clone,
{
    fn succ(&self) -> Self;

    // TODO: specialize for optimization (e.g. direct sum for integers)
    fn succs(&self, n: usize) -> Self {
        let mut x = self.clone();
        for _i in 0..n {
            x = x.succ();
        }
        x
    }
}

impl Succ for i32 {
    fn succ(&self) -> i32 {
        self + 1
    }
}

impl Succ for u8 {
    fn succ(&self) -> u8 {
        self + 1
    }
}

impl Succ for u32 {
    fn succ(&self) -> u32 {
        self + 1
    }
}

impl Succ for u64 {
    fn succ(&self) -> u64 {
        self + 1
    }
}

impl<O, I> Succ for Product<O, I>
where
    O: Clone,
    I: Succ + Clone,
{
    fn succ(&self) -> Self {
        let mut new = self.clone();
        new.inner = new.inner.succ().clone();
        new
    }
}

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
        assert!(
            self.len() < 64,
            "Vectors longer than 64 elements cannot be routed yet."
        );
        let mut h = 0u64;
        for b in self.iter() {
            h = h << 1;
            if *b {
                h += 1;
            }
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

pub trait PairRoute<G, K>
where
    K: Data + Abomonation + Sync + Send + Clone + Route,
    G: Scope,
{
    fn pair_route(&self, matrix_description: MatrixDescription) -> Stream<G, ((u8, u8), (K, K))>;
}

impl<G, K> PairRoute<G, K> for Stream<G, (K, K)>
where
    K: Data + Abomonation + Sync + Send + Clone + Route,
    G: Scope,
{
    fn pair_route(&self, matrix_description: MatrixDescription) -> Stream<G, ((u8, u8), (K, K))> {
        self.map(move |pair| {
            let row = pair.0.route() % matrix_description.rows as u64;
            let col = pair.1.route() % matrix_description.columns as u64;
            ((row as u8, col as u8), pair)
        })
        .exchange(move |tuple| matrix_description.row_major((tuple.0).0, (tuple.0).1))
    }
}

#[derive(Clone, Copy, Debug)]
pub enum MatrixDirection {
    Columns,
    Rows,
}

#[derive(Clone, Copy, Debug)]
pub struct MatrixDescription {
    pub rows: u8,
    pub columns: u8,
}

impl MatrixDescription {
    pub fn for_workers(num_workers: usize) -> MatrixDescription {
        let mut r: usize = (num_workers as f64).sqrt().floor() as usize;
        loop {
            if num_workers % r == 0 {
                let rows = r as u8;
                let columns = (num_workers / rows as usize) as u8;
                return MatrixDescription { rows, columns };
            }
            r -= 1;
        }
    }

    pub fn row_major(&self, i: u8, j: u8) -> u64 {
        i as u64 * self.columns as u64 + j as u64
    }

    pub fn row_major_to_pair(&self, idx: u64) -> (u8, u8) {
        let i = idx / self.columns as u64;
        let j = idx % self.columns as u64;
        (i as u8, j as u8)
    }

    pub fn worker_for<R: Route>(&self, l: R, r: R) -> u64 {
        let row = l.route() % self.rows as u64;
        let col = r.route() % self.columns as u64;
        self.row_major(row as u8, col as u8)
    }

    pub fn strip_partitioner(
        &self,
        num_workers: u64,
        direction: MatrixDirection,
    ) -> StripMatrixPartitioner {
        StripMatrixPartitioner {
            matrix: self.clone(),
            num_workers,
            direction,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct StripMatrixPartitioner {
    matrix: MatrixDescription,
    num_workers: u64,
    direction: MatrixDirection,
}

impl StripMatrixPartitioner {
    pub fn belongs_to_worker<K>(&self, k: K, w: u64) -> bool
    where
        K: Route,
    {
        let strip = k.route() % self.num_workers;
        match self.direction {
            MatrixDirection::Columns => {
                strip
                    == self.matrix.rows as u64 * (w % self.matrix.columns as u64)
                        + (w / self.matrix.columns as u64)
            }
            MatrixDirection::Rows => {
                strip
                    == self.matrix.columns as u64 * (w % self.matrix.rows as u64)
                        + (w / self.matrix.rows as u64)
            }
        }
    }
}

pub trait MatrixDistribute<G, T, K, D>
where
    G: Scope<Timestamp = T>,
    T: Timestamp,
    K: Data + Route + Clone + Abomonation + Sync + Send + Debug,
    D: Data + Clone + Abomonation + Sync + Send + Debug,
{
    fn matrix_distribute(
        &self,
        direction: MatrixDirection,
        matrix_description: MatrixDescription,
    ) -> Stream<G, ((u8, u8), K, D)>;
}

impl<G, T, K, D> MatrixDistribute<G, T, K, D> for Stream<G, (K, D)>
where
    G: Scope<Timestamp = T>,
    T: Timestamp,
    K: Data + Route + Clone + Abomonation + Sync + Send + Debug,
    D: Data + Clone + Abomonation + Sync + Send + Debug,
{
    fn matrix_distribute(
        &self,
        direction: MatrixDirection,
        matrix_description: MatrixDescription,
    ) -> Stream<G, ((u8, u8), K, D)> {
        self.unary(PipelinePact, "matrix distribute", move |_, _| {
            move |input, output| {
                input.for_each(|t, data| {
                    let t = t.retain();
                    let mut session = output.session(&t);
                    let mut data = data.replace(Vec::new());
                    for (k, v) in data.drain(..) {
                        match direction {
                            MatrixDirection::Rows => {
                                let col = (k.route() % matrix_description.columns as u64) as u8;
                                for row in 0..matrix_description.rows {
                                    session.give(((row, col), k.clone(), v.clone()));
                                }
                            }
                            MatrixDirection::Columns => {
                                let row = (k.route() % matrix_description.rows as u64) as u8;
                                for col in 0..matrix_description.columns {
                                    session.give(((row, col), k.clone(), v.clone()));
                                }
                            }
                        };
                    }
                });
            }
        })
        .exchange(move |tuple| matrix_description.row_major((tuple.0).0, (tuple.0).1))
    }
}

pub trait PredicateJoin<G, T, K, D1>
where
    G: Scope<Timestamp = T>,
    T: Timestamp + Succ,
    K: Data + Route + Clone + Abomonation + Sync + Send + Debug,
    D1: Data + Clone + Abomonation + Sync + Send + Debug,
{
    fn two_way_predicate_join<D2, P>(
        &self,
        right: &Stream<G, (K, D2)>,
        predicate: P,
        workers: u64,
    ) -> Stream<G, (K, K)>
    where
        D2: Data + Clone + Abomonation + Sync + Send + Debug,
        P: Fn(&D1, &D2) -> bool + 'static;
}

impl<G, T, K, D1> PredicateJoin<G, T, K, D1> for Stream<G, (K, D1)>
where
    G: Scope<Timestamp = T>,
    T: Timestamp + Succ,
    K: Data + Route + Clone + Abomonation + Sync + Send + Debug,
    D1: Data + Clone + Abomonation + Sync + Send + Debug,
{
    fn two_way_predicate_join<D2, P>(
        &self,
        right: &Stream<G, (K, D2)>,
        predicate: P,
        workers: u64,
    ) -> Stream<G, (K, K)>
    where
        D2: Data + Clone + Abomonation + Sync + Send + Debug,
        P: Fn(&D1, &D2) -> bool + 'static,
    {
        // Round to the next power of two
        let matrix = MatrixDescription::for_workers(workers as usize);
        info!(
            "Each vector will be replicated on a {} x {} matrix",
            matrix.rows, matrix.columns
        );
        let left_replicas = self.matrix_distribute(MatrixDirection::Rows, matrix);
        let right_replicas = right.matrix_distribute(MatrixDirection::Columns, matrix);

        let mut left_stash = HashMap::new();
        let mut right_stash = HashMap::new();

        left_replicas.binary_frontier(
            &right_replicas,
            PipelinePact, // Communication happened in matrix_distribute
            PipelinePact, // Same as above
            "two way predicate join",
            move |_, _| {
                move |left_in, right_in, output| {
                    left_in.for_each(|t, data| {
                        let mut data = data.replace(Vec::new());
                        let inner = left_stash.entry(t.retain()).or_insert(HashMap::new());
                        for (p, k, v) in data.drain(..) {
                            inner.entry(p).or_insert(Vec::new()).push((k, v));
                        }
                    });
                    right_in.for_each(|t, data| {
                        let mut data = data.replace(Vec::new());
                        let inner = right_stash.entry(t.retain()).or_insert(HashMap::new());
                        for (p, k, v) in data.drain(..) {
                            inner.entry(p).or_insert(Vec::new()).push((k, v));
                        }
                    });
                    let frontiers = &[left_in.frontier(), right_in.frontier()];

                    for (time, left_stash) in left_stash.iter_mut() {
                        if let Some(right_stash) = right_stash.get_mut(&time) {
                            if frontiers.iter().all(|f| !f.less_equal(time)) {
                                info!(
                                    "Time {:?}: still {} blocks remaining",
                                    time.time(),
                                    left_stash.len()
                                );
                                let mut session = output.session(&time);
                                for (left_matrix_key, left_stash) in left_stash.drain() {
                                    if let Some(right_stash) = right_stash.get(&left_matrix_key) {
                                        info!(
                                            "Time {:?} :: {:?} :: {}x{}",
                                            time.time(),
                                            left_matrix_key,
                                            left_stash.len(),
                                            right_stash.len()
                                        );
                                        for (lk, lv) in left_stash.iter() {
                                            for (rk, rv) in right_stash.iter() {
                                                if predicate(&lv, &rv) {
                                                    session.give((lk.clone(), rk.clone()));
                                                }
                                            }
                                        }
                                        info!("Completed block {:?}.", left_matrix_key);
                                    }
                                }
                                right_stash.clear();
                            }
                        }
                    }

                    left_stash.retain(|_, data| data.len() > 0);
                    right_stash.retain(|_, data| data.len() > 0);
                }
            },
        )
    }
}

pub trait ApproximateDistinct<G, D>
where
    G: Scope,
    D: Data + Hash,
{
    fn approximate_distinct(&self, expected_elements: usize, fpp: f64, seed: u64) -> Stream<G, D>;
    fn approximate_distinct_atomic(&self, filter: Arc<AtomicBloomFilter<D>>) -> Stream<G, D>;
}

impl<G, T, D> ApproximateDistinct<G, D> for Stream<G, D>
where
    G: Scope<Timestamp = T>,
    T: Timestamp + ToStepId,
    D: Data + Hash,
{
    fn approximate_distinct_atomic(&self, filter: Arc<AtomicBloomFilter<D>>) -> Stream<G, D> {
        let logger = self.scope().danny_logger();
        let mut pl = ProgressLogger::new(
            std::time::Duration::from_secs(60),
            "candidates".to_owned(),
            None,
        );
        self.unary(PipelinePact, "approximate-distinct-atomic", move |_, _| {
            move |input, output| {
                input.for_each(|t, d| {
                    let _pg = ProfileGuard::new(
                        logger.clone(),
                        t.time().to_step_id(),
                        1,
                        "distinct_atomic",
                    );
                    let mut data = d.replace(Vec::new());
                    let mut cnt = 0;
                    let mut received = 0;
                    for v in data.drain(..) {
                        received += 1;
                        if !filter.test_and_insert(&v) {
                            output.session(&t).give(v);
                            cnt += 1;
                        }
                    }
                    pl.add(received as u64);
                    log_event!(logger, LogEvent::DistinctPairs(t.time().to_step_id(), cnt));
                    log_event!(
                        logger,
                        LogEvent::DuplicatesDiscarded(t.time().to_step_id(), received - cnt)
                    );
                    debug!(
                        "Filtered {} elements out of {} received",
                        received - cnt,
                        received
                    );
                });
            }
        })
    }
    fn approximate_distinct(&self, expected_elements: usize, fpp: f64, seed: u64) -> Stream<G, D> {
        let mut rng = XorShiftRng::seed_from_u64(seed);
        info!("Memory before creating bloom filter data {}", proc_mem!());
        let mut filter = BloomFilter::<D>::new(expected_elements, fpp, &mut rng);
        info!(
            "Initialized {:?} (overall memory used {})",
            filter,
            proc_mem!()
        );
        let logger = self.scope().danny_logger();
        self.unary(PipelinePact, "approximate-distinct", move |_, _| {
            move |input, output| {
                input.for_each(|t, d| {
                    let mut data = d.replace(Vec::new());
                    let mut cnt = 0;
                    let mut received = 0;
                    for v in data.drain(..) {
                        received += 1;
                        if !filter.contains(&v) {
                            filter.insert(&v);
                            output.session(&t).give(v);
                            cnt += 1;
                        }
                    }
                    log_event!(logger, LogEvent::DistinctPairs(t.time().to_step_id(), cnt));
                    log_event!(
                        logger,
                        LogEvent::DuplicatesDiscarded(t.time().to_step_id(), received - cnt)
                    );
                    debug!(
                        "Filtered {} elements out of {} received",
                        received - cnt,
                        received
                    );
                    filter.assert_size();
                });
            }
        })
    }
}

pub trait BroadcastedMin<G>
where
    G: Scope,
{
    fn broadcasted_min(&self) -> Stream<G, usize>;
}

impl<G, K> BroadcastedMin<G> for Stream<G, (K, usize)>
where
    G: Scope,
    K: Data + Debug + Send + Sync + Abomonation,
{
    fn broadcasted_min(&self) -> Stream<G, usize> {
        self.map(|p| p.1)
            // Find the minimum in each worker
            .accumulate(std::usize::MAX, |min_val, data| {
                for &x in data.iter() {
                    *min_val = std::cmp::min(*min_val, x);
                }
            })
            // Find the minimum of the minimum
            .exchange(|_| 0)
            .accumulate(std::usize::MAX, |min_val, data| {
                for &x in data.iter() {
                    *min_val = std::cmp::min(*min_val, x);
                }
            })
            // Send the overall minimum to everybody
            .broadcast()
    }
}

pub trait StreamCount<G, D>
where
    G: Scope,
    D: Data,
{
    fn stream_count(&self) -> Stream<G, u64>;
}

impl<G, D> StreamCount<G, D> for Stream<G, D>
where
    G: Scope,
    D: Data,
{
    fn stream_count(&self) -> Stream<G, u64> {
        let mut counts: HashMap<Capability<G::Timestamp>, Option<u64>> = HashMap::new();
        self.unary_frontier(PipelinePact, "stream-count", move |_, _| {
            move |input, output| {
                input.for_each(|t, d| {
                    counts
                        .entry(t.retain())
                        .and_modify(|e| *e = e.map(|c| c + d.len() as u64))
                        .or_insert(Some(d.len() as u64));
                });

                for (time, cnt) in counts.iter_mut() {
                    if !input.frontier().less_equal(time) {
                        output.session(time).give(cnt.take().unwrap());
                    }
                }

                counts.retain(|_, c| c.is_some());
            }
        })
    }
}

pub trait StreamSum<G, D>
where
    G: Scope,
    D: Add<Output = D> + std::iter::Sum + Data,
{
    fn stream_sum(&self) -> Stream<G, D>;
}

impl<G, T, D> StreamSum<G, D> for Stream<G, D>
where
    G: Scope<Timestamp = T>,
    T: Timestamp + ToStepId,
    D: Add<Output = D> + std::iter::Sum + Data + Copy,
{
    fn stream_sum(&self) -> Stream<G, D> {
        let mut sums: HashMap<Capability<G::Timestamp>, Option<D>> = HashMap::new();
        let logger = self.scope().danny_logger();
        self.unary_frontier(PipelinePact, "stream-count", move |_, _| {
            move |input, output| {
                input.for_each(|t, d| {
                    let _pg =
                        ProfileGuard::new(logger.clone(), t.time().to_step_id(), 1, "stream_sum");
                    let mut data = d.replace(Vec::new());
                    let local_sum: D = data.drain(..).sum();
                    sums.entry(t.retain())
                        .and_modify(|e| *e = e.map(|c| c + local_sum))
                        .or_insert_with(|| Some(local_sum));
                });

                for (time, cnt) in sums.iter_mut() {
                    if !input.frontier().less_equal(time) {
                        output.session(time).give(cnt.take().unwrap());
                    }
                }

                sums.retain(|_, c| c.is_some());
            }
        })
    }
}

pub struct LocalData<D> {
    local_data: Rc<RefCell<Vec<D>>>,
}

impl<D> Clone for LocalData<D> {
    fn clone(&self) -> Self {
        Self {
            local_data: Rc::clone(&self.local_data),
        }
    }
}

impl<D> LocalData<D> {
    pub fn new() -> Self {
        Self {
            local_data: Rc::new(RefCell::new(Vec::new())),
        }
    }

    pub fn data(&self) -> Ref<Vec<D>> {
        self.local_data.borrow()
    }
}

impl<T, D> EventPusher<T, D> for LocalData<D> {
    fn push(&mut self, event: Event<T, D>) {
        if let Event::Messages(_, mut data) = event {
            for d in data.drain(..) {
                self.local_data.borrow_mut().push(d);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use timely::dataflow::operators::capture::event::Event;
    use timely::dataflow::operators::{Capture, Probe};
    use timely::dataflow::ProbeHandle;

    #[test]
    fn test_two_way_join_2() {
        let n_left = 5;
        let n_right = 5;
        let conf = timely::Configuration::Process(4).try_build().unwrap();
        let (send, recv) = mpsc::channel();
        let send = Arc::new(Mutex::new(send));
        timely::execute::execute_from(conf.0, conf.1, move |worker| {
            let peers = worker.peers();
            let send = send.lock().unwrap().clone();
            worker.dataflow::<u32, _, _>(|scope| {
                let left = (0..n_left).to_stream(scope).map(|i| (i, i));
                let right = (0..n_right).to_stream(scope).map(|i| (i, i));
                left.two_way_predicate_join(&right, |a, b| a < b, peers as u64)
                    // .inspect(|x| println!("Binary join check {:?} ", x))
                    .capture_into(send);
            });
        })
        .unwrap();

        let mut check = HashSet::new();
        for output in recv.iter() {
            println!("{:?} ", output);
            match output {
                Event::Messages(_t, data) => {
                    for pair in data.iter() {
                        check.insert(pair.clone());
                    }
                }
                _ => (),
            }
        }
        let mut expected = HashSet::new();
        for i in 0..n_left {
            for j in 0..n_right {
                if i < j {
                    expected.insert((i, j));
                }
            }
        }
        assert_eq!(check, expected);
    }

    fn test_matrix_distribute(threads: usize, num_workers: u8, num_elements: u32) {
        // Check that elemens are distributed equally
        let matrix_description = MatrixDescription::for_workers(num_workers as usize);
        let conf = timely::Configuration::Process(threads).try_build().unwrap();
        let (send, recv) = mpsc::channel();
        let send = Arc::new(Mutex::new(send));
        timely::execute::execute_from(conf.0, conf.1, move |worker| {
            let send = send.lock().unwrap().clone();
            let (mut left, mut right, probe) = worker.dataflow::<u32, _, _>(|scope| {
                let mut probe = ProbeHandle::new();
                let (lin, left) = scope.new_input();
                let (rin, right) = scope.new_input();
                let left_distribute =
                    left.matrix_distribute(MatrixDirection::Rows, matrix_description);
                let right_distribute =
                    right.matrix_distribute(MatrixDirection::Columns, matrix_description);

                left_distribute
                    .concat(&right_distribute)
                    .probe_with(&mut probe)
                    .capture_into(send);
                (lin, rin, probe)
            });

            if worker.index() == 0 {
                for i in 0..num_elements {
                    left.send((i, ()));
                    right.send((i, ()));
                }
                left.advance_to(1);
                right.advance_to(1);
            }
            worker.step_while(|| probe.less_than(left.time()));
        })
        .unwrap();

        let mut check = HashMap::new();
        for output in recv.iter() {
            match output {
                Event::Messages(_t, data) => {
                    for (idx, _, _) in data.iter() {
                        let cnt = check.entry(idx.clone()).or_insert(0);
                        *cnt += 1;
                    }
                }
                _ => (),
            }
        }
        for i in 0..matrix_description.rows {
            for j in 0..matrix_description.columns {
                print!(
                    " {:4} ",
                    check
                        .get(&(i, j))
                        .map(|x| format!("{}", x))
                        .unwrap_or("!".to_owned())
                );
            }
            println!();
        }
        println!();

        let first = check.get(&(0, 0)).unwrap();
        let mut count = 0;
        for i in 0..matrix_description.rows {
            for j in 0..matrix_description.columns {
                let opt_cnt = check.get(&(i, j));
                assert!(opt_cnt.is_some());
                let cnt = opt_cnt.unwrap();
                assert_eq!(cnt, first);
                count += cnt;
            }
        }
        assert_eq!(
            count,
            num_elements
                * (matrix_description.rows as u32 + matrix_description.columns as u32) as u32
        );
    }

    #[test]
    fn run_test_matrix_distribute() {
        test_matrix_distribute(1, 2, 1000);
        test_matrix_distribute(4, 2, 1000);
        test_matrix_distribute(1, 4, 4);
        test_matrix_distribute(1, 4, 1000);
        test_matrix_distribute(4, 4, 1000);
        test_matrix_distribute(4, 40, 4000);
    }

    #[test]
    fn test_matrix_description_builder() {
        let m = MatrixDescription::for_workers(16);
        assert_eq!(m.rows, 4);
        assert_eq!(m.columns, 4);
        let m = MatrixDescription::for_workers(32);
        assert_eq!(m.rows, 4);
        assert_eq!(m.columns, 8);
        let m = MatrixDescription::for_workers(40);
        assert_eq!(m.rows, 5);
        assert_eq!(m.columns, 8);
    }

    #[test]
    fn test_index_conversions() {
        let matrix = MatrixDescription::for_workers(40);
        println!("Initialized matrix {:?}", matrix);
        for i in 0..matrix.rows {
            println!("i={}", i);
            for j in 0..matrix.columns {
                assert_eq!((i, j), matrix.row_major_to_pair(matrix.row_major(i, j)));
            }
        }
    }

}
