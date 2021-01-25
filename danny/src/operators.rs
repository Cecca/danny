use crate::logging::*;

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
                    log_event!(
                        logger,
                        (LogEvent::DistinctPairs(t.time().to_step_id()), cnt)
                    );
                    log_event!(
                        logger,
                        (
                            LogEvent::DuplicatesDiscarded(t.time().to_step_id()),
                            received - cnt
                        )
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
        let _logger = self.scope().danny_logger();
        self.unary_frontier(PipelinePact, "stream-count", move |_, _| {
            move |input, output| {
                input.for_each(|t, d| {
                    let mut data = d.replace(Vec::new());
                    let local_sum: D = data.drain(..).sum();
                    sums.entry(t.retain())
                        .and_modify(|e| *e = e.map(|c| c + local_sum))
                        .or_insert_with(|| Some(local_sum));
                });

                for (time, cnt) in sums.iter_mut() {
                    if !input.frontier().less_equal(time) {
                        output
                            .session(time)
                            .give(cnt.take().expect("the count is None!"));
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
