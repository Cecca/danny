use crate::logging::*;
use abomonation::Abomonation;
use danny_base::bloom::*;
use danny_base::sketch::*;
use danny_base::types::*;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::hash::Hasher;
use std::ops::Add;
use std::sync::Arc;
use timely::dataflow::channels::pact::ParallelizationContract;
use timely::dataflow::channels::pact::Pipeline as PipelinePact;

use timely::dataflow::operators::*;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::order::Product;
use timely::progress::Timestamp;
use timely::Data;
use timely::ExchangeData;

/// Composite trait for keys. Basically everything that behaves like an integer
pub trait KeyData: ExchangeData + Hash + Eq + Ord + Copy + Route {}
impl<T: ExchangeData + Hash + Eq + Ord + Copy + Route> KeyData for T {}

/// Composite trait for hash values
pub trait HashData: ExchangeData + Hash + Eq + Copy + Ord + Route {}
impl<T: ExchangeData + Hash + Eq + Copy + Ord + Route> HashData for T {}

/// Composite trait for sketch data.
pub trait SketchData: ExchangeData + Copy + Hash + Eq + BitBasedSketch {}
impl<T: ExchangeData + Copy + Hash + Eq + BitBasedSketch> SketchData for T {}

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

impl Succ for usize {
    fn succ(&self) -> usize {
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
        let mut h = std::collections::hash_map::DefaultHasher::new();
        self.hash(&mut h);
        h.finish()
    }
}

impl Route for ElementId {
    #[inline(always)]
    fn route(&self) -> u64 {
        self.0 as u64
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
    #[allow(clippy::cast_lossless)]
    fn route(&self) -> u64 {
        assert!(
            self.len() < 64,
            "Vectors longer than 64 elements cannot be routed yet."
        );
        let mut h = 0u64;
        for b in self.iter() {
            h <<= 1;
            if *b {
                h += 1;
            }
        }
        h
    }
}

impl Route for Vec<u32> {
    #[inline(always)]
    fn route(&self) -> u64 {
        let mut h = 0u64;
        for &x in self.iter() {
            h = h.wrapping_mul(31).wrapping_add(u64::from(x));
        }
        h
    }
}

impl Route for &[u32] {
    #[inline(always)]
    fn route(&self) -> u64 {
        let mut h = 0u64;
        for &x in self.iter() {
            h = h.wrapping_mul(31).wrapping_add(u64::from(x));
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
        u64::from(self.0)
    }
}

// impl Route for (usize, u32) {
//     #[inline(always)]
//     fn route(&self) -> u64 {
//         (self.0 as u64)
//             .wrapping_mul(31u64)
//             .wrapping_add(u64::from(self.1))
//     }
// }

impl<R: Route> Route for (usize, R) {
    #[inline(always)]
    fn route(&self) -> u64 {
        (self.0 as u64)
            .wrapping_mul(31u64)
            .wrapping_add(u64::from(self.1.route()))
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

    pub fn row_major(self, i: u8, j: u8) -> u64 {
        u64::from(i) * u64::from(self.columns) + u64::from(j)
    }

    pub fn row_major_to_pair(self, idx: u64) -> (u8, u8) {
        let i = idx / u64::from(self.columns);
        let j = idx % u64::from(self.columns);
        (i as u8, j as u8)
    }

    pub fn worker_for<R: Route>(self, l: R, r: R) -> u64 {
        let row = l.route() % u64::from(self.rows);
        let col = r.route() % u64::from(self.columns);
        self.row_major(row as u8, col as u8)
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
                            MatrixDirection::Columns => {
                                let col = (k.route() % u64::from(matrix_description.columns)) as u8;
                                for row in 0..matrix_description.rows {
                                    session.give(((row, col), k.clone(), v.clone()));
                                }
                            }
                            MatrixDirection::Rows => {
                                let row = (k.route() % u64::from(matrix_description.rows)) as u8;
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

pub trait ApproximateDistinct<G, K>
where
    G: Scope,
    K: ExchangeData + Hash + Into<u64> + Copy,
{
    fn approximate_distinct_atomic<P>(
        &self,
        pact: P,
        filter: Arc<AtomicBloomFilter<K>>,
    ) -> Stream<G, (K, K)>
    where
        P: ParallelizationContract<G::Timestamp, (K, K)>;
}

impl<G, T, K> ApproximateDistinct<G, K> for Stream<G, (K, K)>
where
    G: Scope<Timestamp = T>,
    T: Timestamp + ToStepId,
    K: ExchangeData + Hash + Into<u64> + Copy,
{
    fn approximate_distinct_atomic<P>(
        &self,
        pact: P,
        filter: Arc<AtomicBloomFilter<K>>,
    ) -> Stream<G, (K, K)>
    where
        P: ParallelizationContract<G::Timestamp, (K, K)>,
    {
        let logger = self.scope().danny_logger();
        self.unary(pact, "approximate-distinct-atomic", move |_, _| {
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
                        info!("sending sum for time");
                        output
                            .session(time)
                            .give(cnt.take().expect("the count is None!"));
                        info!("sent sum for time");
                    }
                }

                sums.retain(|_, c| c.is_some());
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use danny_base::lsh::*;
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use timely::dataflow::operators::capture::event::Event;
    use timely::dataflow::operators::{Capture, Probe};
    use timely::dataflow::ProbeHandle;

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

    #[test]
    fn test_minhash_route() {
        let mut rng = StdRng::seed_from_u64(1232);
        let k = 18;
        let n = 10000;
        let mut distrib = vec![0; 40];
        let hasher = OneBitMinHash::new(k, &mut rng);
        for _ in 0..n {
            let v = BagOfWords::random(3000, 0.01, &mut rng);
            if !v.is_empty() {
                let h = hasher.hash(&v);
                let r = h.route() as usize % distrib.len();
                distrib[r] += 1;
            }
        }
        println!("{:?}", distrib);
    }
}
