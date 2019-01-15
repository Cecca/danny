use crate::io::ReadDataFile;
use crate::operators::*;
use abomonation::Abomonation;
use core::any::Any;
use heapsize::HeapSizeOf;
use measure::InnerProduct;
use operators::{Duplicate, Route};
use probabilistic_collections::cuckoo::CuckooFilter;
use rand::distributions::{Distribution, Normal, Uniform};
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;
use smallbitvec::SmallBitVec;
use std::clone::Clone;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;
use std::num::Wrapping as w;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use timely::communication::allocator::generic::GenericBuilder;
use timely::dataflow::channels::pact::{Exchange as ExchangePact, Pipeline};
use timely::dataflow::operators::capture::{EventLink, Extract, Replay};
use timely::dataflow::operators::*;
use timely::dataflow::*;
use timely::order::Product;
use timely::progress::Timestamp;
use timely::Data;
use types::{BagOfWords, VectorWithNorm};

pub trait LSHFunction {
    type Input;
    type Output;
    fn hash(&self, v: &Self::Input) -> Self::Output;
    fn probability_at_range(range: f64) -> f64;

    fn repetitions_at_range(range: f64, k: usize) -> usize {
        let p = Self::probability_at_range(range);
        (1_f64 / p).powi(k as i32).ceil() as usize
    }
}

#[derive(Clone)]
pub struct LSHCollection<F, H>
where
    F: LSHFunction<Output = H> + Clone,
    H: Clone,
{
    functions: Vec<F>,
}

impl<F, H> LSHCollection<F, H>
where
    F: LSHFunction<Output = H> + Clone,
    H: Clone,
{
    pub fn for_each_hash<L>(&self, v: &F::Input, mut logic: L) -> ()
    where
        L: FnMut(usize, F::Output),
    {
        let mut repetition = 0;
        self.functions.iter().for_each(|f| {
            let h = f.hash(v);
            logic(repetition, h);
            repetition += 1;
        });
    }

    pub fn hash(&self, v: &F::Input, repetition: usize) -> F::Output {
        self.functions[repetition].hash(v)
    }

    pub fn repetitions(&self) -> usize {
        self.functions.len()
    }
}

#[derive(Clone)]
pub struct Hyperplane {
    k: usize,
    planes: Vec<VectorWithNorm>,
}

impl Hyperplane {
    pub fn new<R>(k: usize, dim: usize, rng: &mut R) -> Hyperplane
    where
        R: Rng + ?Sized,
    {
        let mut planes = Vec::with_capacity(k);
        let gaussian = Normal::new(0.0, 1.0);
        for _ in 0..k {
            let mut plane = Vec::with_capacity(dim);
            for _ in 0..dim {
                plane.push(gaussian.sample(rng));
            }
            let plane = VectorWithNorm::new(plane);
            planes.push(plane);
        }
        Hyperplane { k, planes }
    }

    pub fn collection<R>(
        k: usize,
        repetitions: usize,
        dim: usize,
        rng: &mut R,
    ) -> LSHCollection<Hyperplane, Vec<bool>>
    where
        R: Rng + ?Sized,
    {
        let mut functions = Vec::with_capacity(repetitions);
        for _ in 0..repetitions {
            functions.push(Hyperplane::new(k, dim, rng));
        }
        LSHCollection { functions }
    }
}

impl LSHFunction for Hyperplane {
    type Input = VectorWithNorm;
    // TODO:use SmallBitVec
    type Output = Vec<bool>;

    fn hash(&self, v: &VectorWithNorm) -> Vec<bool> {
        let mut h = Vec::with_capacity(self.k);
        for plane in self.planes.iter() {
            if InnerProduct::inner_product(plane, v) >= 0_f64 {
                h.push(true);
            } else {
                h.push(false);
            }
        }
        h
    }

    fn probability_at_range(range: f64) -> f64 {
        1_f64 - range.acos() / std::f64::consts::PI
    }
}

/// Produces 64 bit hashes of 32 bits values
struct TabulatedHasher {
    table0: [u64; 256],
    table1: [u64; 256],
    table2: [u64; 256],
    table3: [u64; 256],
}

impl TabulatedHasher {
    fn new<R>(rng: &mut R) -> TabulatedHasher
    where
        R: Rng + ?Sized,
    {
        let uniform = Uniform::new(0u64, std::u64::MAX);
        let mut table0 = [0_u64; 256];
        let mut table1 = [0_u64; 256];
        let mut table2 = [0_u64; 256];
        let mut table3 = [0_u64; 256];
        for i in 0..256 {
            table0[i] = uniform.sample(rng);
        }
        for i in 0..256 {
            table1[i] = uniform.sample(rng);
        }
        for i in 0..256 {
            table2[i] = uniform.sample(rng);
        }
        for i in 0..256 {
            table3[i] = uniform.sample(rng);
        }
        TabulatedHasher {
            table0,
            table1,
            table2,
            table3,
        }
    }

    fn hash(&self, x: u32) -> u64 {
        let mut h = self.table0[(x & 0xFF) as usize];
        h ^= self.table1[((x >> 8) & 0xFF) as usize];
        h ^= self.table2[((x >> 16) & 0xFF) as usize];
        h ^= self.table3[((x >> 24) & 0xFF) as usize];
        h
    }
}

struct MinHash {
    k: usize,
    hashers: Vec<TabulatedHasher>,
    coeffs: Vec<u64>,
}

impl MinHash {
    fn new<R>(k: usize, rng: &mut R) -> MinHash
    where
        R: Rng + ?Sized,
    {
        let mut hashers = Vec::with_capacity(k);
        let uniform = Uniform::new(0u64, std::u64::MAX);
        let mut coeffs = Vec::new();
        for _ in 0..k {
            hashers.push(TabulatedHasher::new(rng));
            coeffs.push(uniform.sample(rng));
        }
        MinHash { k, hashers, coeffs }
    }
}

impl LSHFunction for MinHash {
    type Input = BagOfWords;
    type Output = u32;

    fn hash(&self, v: &BagOfWords) -> u32 {
        let n = v.num_words();
        let mut h = 0u64;

        for hi in 0..self.k {
            let mut min_v = std::u64::MAX;
            let mut min_idx = 0;
            for vi in 0..n {
                let x = self.hashers[hi].hash(v.word_at(vi));
                if x < min_v {
                    min_idx = vi;
                    min_v = x;
                }
            }
            h = h.wrapping_add(self.coeffs[hi].wrapping_mul(min_idx as u64));
        }

        (h >> 32) as u32
    }

    fn probability_at_range(range: f64) -> f64 {
        range
    }
}

pub trait HashStream<G, K, D>
where
    G: Scope,
    K: Data + Debug,
    D: Data + Debug,
{
    fn hash_buffered<F, H>(&self, hash_coll: &LSHCollection<F, H>) -> Stream<G, (H, K)>
    where
        F: LSHFunction<Input = D, Output = H> + Clone + 'static,
        H: Data + Clone;

    fn hash<F, H>(&self, hash_coll: &LSHCollection<F, H>) -> Stream<G, (H, K)>
    where
        F: LSHFunction<Input = D, Output = H> + Clone + 'static,
        H: Data + Clone;
}

impl<G, T, K, D> HashStream<G, K, D> for Stream<G, (K, D)>
where
    G: Scope<Timestamp = T>,
    T: Succ + Clone + Timestamp,
    K: Data + Debug,
    D: Data + Debug,
{
    fn hash_buffered<F, H>(&self, hash_coll: &LSHCollection<F, H>) -> Stream<G, (H, K)>
    where
        F: LSHFunction<Input = D, Output = H> + Clone + 'static,
        H: Data + Clone,
    {
        let repetitions = hash_coll.repetitions();
        let mut probe1 = ProbeHandle::new();
        let probe2 = probe1.clone();
        let hash_coll = hash_coll.clone();
        let mut stash: Vec<Option<(Capability<G::Timestamp>, Vec<(K, D)>)>> =
            vec![None; repetitions + 1]; // The last cell is always None: it is just to simplify the code below
        info!("Initial stash size {:?} ", stash.len());
        self.unary(Pipeline, "hash", move |_, _| {
            move |input, output| {
                let hash_coll = hash_coll.clone();
                // Accumulate the vectors in the stash area. This stash vector has an entry per
                // repetition, containing the vectors that need to be hashed for that repetition.
                // A given vector starts at index 0 and then moves forward to the next cells,
                // dropping out at the end of the vector. When a cell has no more associated
                // vectors, its associated capability is dropped.
                input.for_each(|t, data| {
                    let mut data = data.replace(Vec::new());
                    stash[0]
                        .get_or_insert((t.retain(), Vec::new()))
                        .1
                        .append(&mut data)
                });

                for i in 0..repetitions {
                    let mut to_move: Vec<(K, D)> =
                        if let Some((time, data)) = stash[i].iter_mut().next() {
                            if !probe2.less_than(time.time()) {
                                let mut session = output.session(&time);
                                for (k, v) in data.iter() {
                                    let h = hash_coll.hash(&v, i);
                                    session.give((h, k.clone()));
                                }
                                data.drain(..).collect()
                            } else {
                                Vec::new()
                            }
                        } else {
                            Vec::new()
                        };
                    if to_move.len() > 0 && i + 1 < repetitions {
                        let time = stash[i]
                            .as_ref()
                            .expect(
                                "At this point we should be sure that there are vectors to move",
                            )
                            .0
                            .clone();
                        let time = time.delayed(&(time.time().succ()));
                        stash[i + 1]
                            .get_or_insert((time, Vec::new()))
                            .1
                            .append(&mut to_move);
                    }
                }

                // Remove items whose time is past the last repetition
                for entry in stash.iter_mut() {
                    *entry = entry.take().filter(|pair| pair.1.len() > 0);
                }
            }
        })
        .probe_with(&mut probe1)
    }

    fn hash<F, H>(&self, hash_coll: &LSHCollection<F, H>) -> Stream<G, (H, K)>
    where
        F: LSHFunction<Input = D, Output = H> + Clone + 'static,
        H: Data + Clone,
    {
        let hash_coll = hash_coll.clone();
        self.unary(Pipeline, "hash", move |_, _| {
            move |input, output| {
                let hash_coll = hash_coll.clone();
                input.for_each(move |t, data| {
                    let t = t.retain();
                    let mut data = data.replace(Vec::new());
                    for (id, v) in data.drain(..) {
                        hash_coll.for_each_hash(&v, |rep, h| {
                            let t_out = t.delayed(&(t.time().succs(rep)));
                            debug!("Outputting hashes at time {:?} ", t_out.time());
                            output.session(&t_out).give((h, id.clone()));
                        });
                    }
                });
            }
        })
    }
}

trait BucketStream<G, H, K>
where
    G: Scope,
    H: Data + Route + Debug + Send + Sync + Abomonation + Clone + Eq + Hash,
    K: Data + Debug + Send + Sync + Abomonation + Clone,
{
    fn bucket(&self, right: &Stream<G, (H, K)>) -> Stream<G, (K, K)>;
}

impl<G, H, K> BucketStream<G, H, K> for Stream<G, (H, K)>
where
    G: Scope,
    H: Data + Route + Debug + Send + Sync + Abomonation + Clone + Eq + Hash,
    K: Data + Debug + Send + Sync + Abomonation + Clone + Eq + Hash,
{
    fn bucket(&self, right: &Stream<G, (H, K)>) -> Stream<G, (K, K)> {
        let mut left_buckets = HashMap::new();
        let mut right_buckets = HashMap::new();

        let item_count = 1 << 28;
        let fpp = 0.01;
        let fingerprint = 8;
        let mut filter =
            CuckooFilter::<(K, K)>::from_fingerprint_bit_count(item_count, fpp, fingerprint);
        info!(
            "Initialized Cockoo filter of {} bytes in bucketing function",
            std::mem::size_of_val(&filter)
        );

        self.binary_frontier(
            &right,
            ExchangePact::new(|pair: &(H, K)| pair.0.route()),
            ExchangePact::new(|pair: &(H, K)| pair.0.route()),
            "bucket",
            move |_, _| {
                move |left_in, right_in, output| {
                    left_in.for_each(|t, d| {
                        debug!(
                            "Received batch of left messages for time {:?}:\n\t{:?}",
                            t.time(),
                            d.iter()
                        );
                        let rep_entry = left_buckets.entry(t.retain()).or_insert(HashMap::new());
                        let mut data = d.replace(Vec::new());
                        for (h, k) in data.drain(..) {
                            rep_entry.entry(h).or_insert(Vec::new()).push(k);
                        }
                    });
                    right_in.for_each(|t, d| {
                        debug!(
                            "Received batch of right messages for time {:?}:\n\t{:?}",
                            t.time(),
                            d.iter()
                        );
                        let rep_entry = right_buckets.entry(t.retain()).or_insert(HashMap::new());
                        let mut data = d.replace(Vec::new());
                        for (h, k) in data.drain(..) {
                            rep_entry.entry(h).or_insert(Vec::new()).push(k);
                        }
                    });
                    let frontiers = &[left_in.frontier(), right_in.frontier()];
                    for (time, left_buckets) in left_buckets.iter_mut() {
                        if frontiers.iter().all(|f| !f.less_equal(time)) {
                            let mut session = output.session(&time);
                            // We got all data for the repetition at `time`
                            if let Some(right_buckets) = right_buckets.get_mut(time) {
                                debug!("Start to generate candidates from bucket");
                                let mut cnt = 0;
                                let mut potential = 0;
                                for (h, left_keys) in left_buckets.drain() {
                                    if let Some(right_keys) = right_buckets.get(&h) {
                                        for kl in left_keys.iter() {
                                            for kr in right_keys.iter() {
                                                //  do output
                                                potential += 1;
                                                let out_pair = (kl.clone(), kr.clone());
                                                if !filter.contains(&out_pair) {
                                                    filter.insert(&out_pair);
                                                    if filter.is_nearly_full() {
                                                        warn!("Cockoo filter for bucketing is nearly full!");
                                                    }
                                                    session.give(out_pair);
                                                    cnt += 1;
                                                }
                                            }
                                        }
                                    }
                                }
                                debug!(
                                    "[{:?}] Output {} candidates (out of {} potential ones, discarded {})",
                                    time.time(),
                                    cnt,
                                    potential,
                                    potential - cnt
                                );
                            }
                            // Remove the repetition from the right buckets
                            right_buckets.remove(time);
                        }
                    }
                    // Clean up the entries with empty buckets from the left (from the right we
                    // already did it)
                    left_buckets.retain(|t, buckets| {
                        let to_keep = buckets.len() > 0;
                        to_keep
                    });
                    right_buckets.retain(|t, buckets| {
                        let to_keep = buckets.len() > 0;
                        to_keep
                    });
                    // TODO: cleanup the duplicates filter when we are done with bucketing. We need
                    // this to free memory.
                }
            },
        )
    }
}

trait CollidingPairs<G, T, K, D, F, H>
where
    G: Scope<Timestamp = T>,
    T: Timestamp + Succ,
    K: Data + Sync + Send + Clone + Abomonation + Debug + Route,
    D: Data + Sync + Send + Clone + Abomonation + Debug,
    F: LSHFunction<Input = D, Output = H> + Sync + Send + Clone + 'static,
    H: Data + Sync + Send + Clone + Abomonation + Debug + Route + Eq + Hash,
{
    fn colliding_pairs(
        &self,
        right: &Stream<G, (K, D)>,
        hash_coll: &LSHCollection<F, H>,
    ) -> Stream<G, (K, K)>;
}

impl<G, T, K, D, F, H> CollidingPairs<G, T, K, D, F, H> for Stream<G, (K, D)>
where
    G: Scope<Timestamp = T>,
    T: Timestamp + Succ,
    D: Data + Sync + Send + Clone + Abomonation + Debug,
    F: LSHFunction<Input = D, Output = H> + Sync + Send + Clone + 'static,
    H: Data + Route + Debug + Send + Sync + Abomonation + Clone + Eq + Hash,
    K: Data + Debug + Send + Sync + Abomonation + Clone + Eq + Hash + Route,
{
    fn colliding_pairs(
        &self,
        right: &Stream<G, (K, D)>,
        hash_coll: &LSHCollection<F, H>,
    ) -> Stream<G, (K, K)> {
        self.scope()
            .scoped::<Product<_, u32>, _, _>("candidate generation", |inner| {
                let left_hashes = self.enter(inner).hash_buffered(&hash_coll);
                let right_hashes = right.enter(inner).hash_buffered(&hash_coll);
                let candidate_pairs = left_hashes.bucket(&right_hashes);
                candidate_pairs.leave()
            })
    }
}

#[allow(dead_code)]
pub fn fixed_param_lsh<D, F, H, O>(
    left_path: &String,
    right_path: &String,
    hash_fn: LSHCollection<H, O>,
    sim_pred: F,
    timely_builder: (Vec<GenericBuilder>, Box<dyn Any + 'static>),
) -> usize
where
    D: ReadDataFile + Data + Sync + Send + Clone + Abomonation + Debug + HeapSizeOf,
    F: Fn(&D, &D) -> bool + Send + Clone + Sync + 'static,
    H: LSHFunction<Input = D, Output = O> + Sync + Send + Clone + 'static,
    O: Data + Sync + Send + Clone + Abomonation + Debug + Route + Eq + Hash,
{
    // This channel is used to get the results
    let (output_send_ch, recv) = ::std::sync::mpsc::channel();
    let output_send_ch = Arc::new(Mutex::new(output_send_ch));

    let left_path = left_path.clone();
    let right_path = right_path.clone();
    let repetitions = hash_fn.repetitions();

    timely::execute::execute_from(timely_builder.0, timely_builder.1, move |worker| {
        let output_send_ch = output_send_ch.lock().unwrap().clone();
        let sim_pred = sim_pred.clone();
        let index = worker.index();
        let peers = worker.peers() as u64;
        let hash_fn = hash_fn.clone();
        info!("Started worker {}/{}", index, peers);
        let (mut left, mut right, probe) = worker.dataflow(move |scope| {
            let (left_in, left_stream) = scope.new_input::<(u64, D)>();
            let (right_in, right_stream) = scope.new_input::<(u64, D)>();
            let mut probe = ProbeHandle::new();
            let hash_fn = hash_fn;

            let candidate_pairs = left_stream.colliding_pairs(&right_stream, &hash_fn);

            left_stream
                .three_way_join(&candidate_pairs, &right_stream, sim_pred, peers)
                .count()
                .exchange(|_| 0)
                .probe_with(&mut probe)
                .capture_into(output_send_ch);

            (left_in, right_in, probe)
        });

        // Push data into the dataflow graph. Each worker will read some of the lines of the input
        info!("Reading data files:\n\t{:?}\n\t{:?}", left_path, right_path);
        let start = Instant::now();
        let left_path = left_path.clone();
        let right_path = right_path.clone();
        ReadDataFile::from_file_partially(
            &left_path.into(),
            |l| l % peers == index as u64,
            |c, v| left.send((c, v)),
        );
        ReadDataFile::from_file_partially(
            &right_path.into(),
            |l| l % peers == index as u64,
            |c, v| right.send((c, v)),
        );
        // Explicitly state that the input will not feed times smaller than the number of
        // repetitions. This is needed to make the program terminate
        left.advance_to(repetitions as u32);
        right.advance_to(repetitions as u32);
        let end = Instant::now();
        let elapsed = end - start;
        println!(
            "Time to feed the input to the dataflow graph: {:?}",
            elapsed
        );
        worker.step_while(|| probe.less_than(&(repetitions as u32)));
    })
    .unwrap();

    // From `recv` we get an entry for each timestamp, containing a one-element vector with the
    // count of output pairs for a given timestamp. We sum across all the timestamps, so we need to
    // remove the duplicates
    let count: usize = recv
        .extract()
        .iter()
        .map(|pair| pair.1.clone().iter().sum::<usize>())
        .sum();
    count
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hyperplane() {
        let mut rng = StdRng::seed_from_u64(123);
        let k = 20;
        let hasher = Hyperplane::new(20, 3, &mut rng);
        let a = VectorWithNorm::new(vec![0.0, 1.0, 3.0]);
        let ha = hasher.hash(&a);
        let b = VectorWithNorm::new(vec![1.0, 1.0, 3.0]);
        let hb = hasher.hash(&b);

        println!("{:?}", ha);
        println!("{:?}", hb);

        assert!(ha.len() == k);
        assert!(hb.len() == k);
        assert!(ha != hb);
    }

    #[test]
    fn test_minhash() {
        let mut rng = StdRng::seed_from_u64(123);
        let k = 2;
        let hasher = MinHash::new(20, &mut rng);
        let a = BagOfWords::new(10, vec![1, 3, 4]);
        let ha = hasher.hash(&a);
        let b = BagOfWords::new(10, vec![0, 1]);
        let hb = hasher.hash(&b);
        let c = BagOfWords::new(10, vec![0, 1]);
        let hc = hasher.hash(&c);

        println!("{:?}", ha);
        println!("{:?}", hb);
        println!("{:?}", hc);

        assert!(ha != hb);
        assert!(hc == hb);
    }
}
