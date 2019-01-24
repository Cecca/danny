use crate::config::Config;
use crate::io::ReadDataFile;
use crate::logging::init_event_logging;
use crate::logging::ToSpaceString;
use crate::logging::*;
use crate::operators::*;
use abomonation::Abomonation;
use heapsize::HeapSizeOf;
use measure::InnerProduct;
use operators::Route;
use probabilistic_collections::cuckoo::CuckooFilter;
use rand::distributions::{Distribution, Normal, Uniform};
use rand::Rng;
use std::clone::Clone;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::Instant;
use timely::dataflow::channels::pact::{Exchange as ExchangePact, Pipeline};
use timely::dataflow::operators::capture::{Event as TimelyEvent, Extract};
use timely::dataflow::operators::*;
use timely::dataflow::*;
use timely::order::Product;
use timely::progress::Timestamp;
use timely::worker::AsWorker;
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
#[derive(Clone)]
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

#[derive(Clone)]
pub struct MinHash {
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

    pub fn collection<R>(k: usize, repetitions: usize, rng: &mut R) -> LSHCollection<MinHash, u32>
    where
        R: Rng + ?Sized,
    {
        let mut functions = Vec::with_capacity(repetitions);
        for _ in 0..repetitions {
            functions.push(MinHash::new(k, rng));
        }
        LSHCollection { functions }
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
                            // TODO: maybe here there is room for optimization
                            if !probe2.less_than(time.time()) {
                                let mut session = output.session(&time);
                                let mut cnt = 0;
                                for (k, v) in data.iter() {
                                    let h = hash_coll.hash(&v, i);
                                    session.give((h, k.clone()));
                                }
                                debug!("Emitted {} hashes for repetition {}", cnt, i);
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

struct PairGenerator<H, K>
where
    H: Hash + Eq,
{
    left: HashMap<H, Vec<K>>,
    right: HashMap<H, Vec<K>>,
    cur_left: Vec<K>,
    cur_right: Vec<K>,
    cur_left_idx: usize,
    cur_right_idx: usize,
}

impl<H: Hash + Eq + Clone, K: Clone> PairGenerator<H, K> {
    fn new(left: HashMap<H, Vec<K>>, right: HashMap<H, Vec<K>>) -> Self {
        PairGenerator {
            left,
            right,
            cur_left: Vec::new(),
            cur_right: Vec::new(),
            cur_left_idx: 0,
            cur_right_idx: 0,
        }
    }

    fn done(&self) -> bool {
        self.left.is_empty()
            && self.right.is_empty()
            && self.cur_left.is_empty()
            && self.cur_right.is_empty()
    }
}

impl<H: Hash + Eq + Clone, K: Clone> Iterator for PairGenerator<H, K> {
    type Item = (K, K);

    fn next(&mut self) -> Option<(K, K)> {
        if self.done() {
            return None;
        }
        if self.cur_left.is_empty() {
            assert!(
                self.cur_right.is_empty(),
                "left vector is empty, but right one is not"
            );
            let k = self
                .left
                .keys()
                .cloned()
                .filter(|k| self.right.contains_key(k))
                .next();
            if k.is_none() {
                // No more common keys between left and right, cleanup and return
                self.left.clear();
                self.right.clear();
                return None;
            }
            let key = k.unwrap();
            self.cur_left = self
                .left
                .remove(&key)
                .expect("This left key should be present");
            self.cur_right = self
                .right
                .remove(&key)
                .expect("This right key should be present");
            self.cur_left_idx = 0;
            self.cur_right_idx = 0;
        }
        let left_elem = self.cur_left[self.cur_left_idx].clone();
        let right_elem = self.cur_right[self.cur_right_idx].clone();
        let pair = (left_elem, right_elem);
        // Move the index
        if self.cur_right_idx + 1 >= self.cur_right.len() {
            self.cur_right_idx = 0;
            if self.cur_left_idx + 1 >= self.cur_left.len() {
                // We are done for this key
                self.cur_left.clear();
                self.cur_right.clear();
            } else {
                self.cur_left_idx += 1;
            }
        } else {
            self.cur_right_idx += 1;
        }

        Some(pair)
    }
}

trait BucketStream<G, H, K>
where
    G: Scope,
    H: Data + Route + Debug + Send + Sync + Abomonation + Clone + Eq + Hash,
    K: Data + Debug + Send + Sync + Abomonation + Clone,
{
    fn bucket(&self, right: &Stream<G, (H, K)>) -> Stream<G, (K, K)>;
    fn bucket_batched(&self, right: &Stream<G, (H, K)>) -> Stream<G, (K, K)>;
}

impl<G, H, K> BucketStream<G, H, K> for Stream<G, (H, K)>
where
    G: Scope,
    H: Data + Route + Debug + Send + Sync + Abomonation + Clone + Eq + Hash,
    K: Data + Debug + Send + Sync + Abomonation + Clone + Eq + Hash,
{
    fn bucket_batched(&self, right: &Stream<G, (H, K)>) -> Stream<G, (K, K)> {
        let mut left_buckets = HashMap::new();
        let mut right_buckets = HashMap::new();
        let mut generators = HashMap::new();

        let item_count = 1 << 28;
        let fpp = 0.01;
        let fingerprint = 8;
        // let mut filter =
        //     CuckooFilter::<(K, K)>::from_fingerprint_bit_count(item_count, fpp, fingerprint);
        let logger = self.scope().danny_logger();

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
                    let time = left_buckets
                        .keys()
                        .cloned()
                        .filter(|t| {
                            right_buckets.contains_key(t)
                                && frontiers.iter().all(|f| !f.less_equal(t))
                        })
                        .next();
                    match time {
                        Some(time) => {
                            // We got all data for the repetition at `time`
                            // Enqueue the pairs generator
                            let left_buckets = left_buckets.remove(&time).unwrap();
                            let right_buckets = right_buckets.remove(&time).unwrap();
                            let generator = PairGenerator::new(left_buckets, right_buckets);
                            generators.insert(time.clone(), generator);
                        }
                        None => (),
                    }
                    // Emit some output pairs
                    for (time, mut generator) in generators.iter_mut() {
                        let mut session = output.session(time);
                        let mut cnt = 0;
                        for pair in generator.take(10000) {
                            // if !filter.contains(&pair) {
                            //     filter.insert(&pair);
                            session.give(pair);
                            cnt += 1;
                            // }
                        }
                        debug!(
                            "Emitted batch of {} pairs (is done: {})",
                            cnt,
                            generator.done()
                        );
                        log_event!(logger, LogEvent::GeneratedPairs(cnt));
                    }

                    // Cleanup exhausted generators
                    generators.retain(|_, gen| !gen.done());
                }
            },
        )
    }

    fn bucket(&self, right: &Stream<G, (H, K)>) -> Stream<G, (K, K)> {
        let mut left_buckets = HashMap::new();
        let mut right_buckets = HashMap::new();

        let item_count = 1 << 30;
        let fpp = 0.01;
        let fingerprint = 8;
        let mut filter =
            CuckooFilter::<(K, K)>::from_fingerprint_bit_count(item_count, fpp, fingerprint);

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
                                                    assert!(!filter.is_nearly_full(), "Cockoo filter for bucketing is nearly full!");
                                                    session.give(out_pair);
                                                    cnt += 1;
                                                }
                                            }
                                        }
                                    }
                                }
                                info!(
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
                    left_buckets.retain(|_t, buckets| {
                        let to_keep = buckets.len() > 0;
                        to_keep
                    });
                    right_buckets.retain(|_t, buckets| {
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
                // TODO: Reconsider if you have to buffer the repetitions: maybe batching the pairs
                // is sufficient.
                let left_hashes = self.enter(inner).hash_buffered(&hash_coll);
                let right_hashes = right.enter(inner).hash_buffered(&hash_coll);
                let candidate_pairs = left_hashes.bucket_batched(&right_hashes);
                candidate_pairs.leave()
            })
    }
}

pub fn fixed_param_lsh<D, F, H, O>(
    left_path: &String,
    right_path: &String,
    hash_fn: LSHCollection<H, O>,
    sim_pred: F,
    config: &Config,
) -> usize
where
    D: ReadDataFile + Data + Sync + Send + Clone + Abomonation + Debug + HeapSizeOf,
    F: Fn(&D, &D) -> bool + Send + Clone + Sync + 'static,
    H: LSHFunction<Input = D, Output = O> + Sync + Send + Clone + 'static,
    O: Data + Sync + Send + Clone + Abomonation + Debug + Route + Eq + Hash,
{
    let timely_builder = config.get_timely_builder();
    // This channel is used to get the results
    let (output_send_ch, recv) = channel();
    let output_send_ch = Arc::new(Mutex::new(output_send_ch));

    let (send_exec_summary, recv_exec_summary) = channel();
    let send_exec_summary = Arc::new(Mutex::new(send_exec_summary));

    let left_path = left_path.clone();
    let right_path = right_path.clone();
    let left_path_main = left_path.clone();
    let right_path_main = right_path.clone();
    let repetitions = hash_fn.repetitions();

    // These two maps hold the vectors that need to be accessed by all threads in this machine.
    let global_left_write: Arc<RwLock<HashMap<u64, D>>> = Arc::new(RwLock::new(HashMap::new()));
    let global_right_write: Arc<RwLock<HashMap<u64, D>>> = Arc::new(RwLock::new(HashMap::new()));
    let global_left_read = global_left_write.clone();
    let global_right_read = global_right_write.clone();

    let (send_coords, recv_coords) = channel();
    let send_coords = Arc::new(Mutex::new(send_coords));

    let total_workers = config.get_total_workers();
    let worker_threads = config.get_threads();
    let waiting_threads = worker_threads + 1;
    let io_barrier = Arc::new(std::sync::Barrier::new(waiting_threads));
    let io_barrier_reader = io_barrier.clone();
    debug!(
        "Cloned the barrier, which waits on {} threads",
        waiting_threads
    );

    // Read the coordinates, to load in memory the relevant sections of the files
    let reader_handle = thread::spawn(move || {
        let start = Instant::now();
        let matrix_desc = MatrixDescription::for_workers(total_workers);
        info!(
            "Data partitioned according to {} x {}",
            matrix_desc.rows, matrix_desc.columns
        );
        let mut row_set = HashSet::new();
        let mut column_set = HashSet::new();
        let mut global_left = global_left_write.write().unwrap();
        let mut global_right = global_right_write.write().unwrap();
        debug!("Getting the pairs on the main thread");
        for _ in 0..worker_threads {
            // We know we will receive exactly that many messages
            let (i, j) = recv_coords.recv().expect("Problem receiving coordinate");
            row_set.insert(i);
            column_set.insert(j);
        }
        info!("This machine is responsible for rows: {:?}", row_set);
        info!("This machine is responsible for columns: {:?}", column_set);
        ReadDataFile::from_file_partially(
            &left_path_main.into(),
            |l| row_set.contains(&((l % matrix_desc.rows as u64) as u8)),
            |c, v| {
                global_left.insert(c, v);
            },
        );
        ReadDataFile::from_file_partially(
            &right_path_main.into(),
            |l| column_set.contains(&((l % matrix_desc.columns as u64) as u8)),
            |c, v| {
                global_right.insert(c, v);
            },
        );
        let end = Instant::now();
        let elapsed = end - start;
        info!(
            "Loaded {} left vectors ({} bytes) and {} right vectors ({} bytes) (in {:?})",
            global_left.len(),
            global_left.heap_size_of_children().to_space_string(),
            global_right.len(),
            global_right.heap_size_of_children().to_space_string(),
            elapsed
        );

        debug!("Reader is calling wait on the main barrier");
        io_barrier_reader.wait();
        debug!("After reader barrier!");
    });

    timely::execute::execute_from(timely_builder.0, timely_builder.1, move |mut worker| {
        let execution_summary = init_event_logging(&worker);
        let output_send_ch = output_send_ch.lock().unwrap().clone();
        let sim_pred = sim_pred.clone();
        let index = worker.index();
        let peers = worker.peers() as u64;

        let send_coords = send_coords.lock().unwrap().clone();
        let matrix_coords =
            MatrixDescription::for_workers(peers as usize).row_major_to_pair(index as u64);
        debug!("Sending coordinates {:?}", matrix_coords);
        send_coords
            .send(matrix_coords)
            .expect("Error while pushing into coordinates channel");
        debug!("Waiting for input to be loaded");
        io_barrier.wait();
        debug!("After worker barrier!");

        let global_left_read = global_left_read.clone();
        let global_right_read = global_right_read.clone();

        let hash_fn = hash_fn.clone();
        debug!("Started worker {}/{}", index, peers);
        let (mut left, mut right, probe) = worker.dataflow(move |scope| {
            // TODO: check if these two clones are harmful
            let global_left = global_left_read.read().unwrap().clone();
            let global_right = global_right_read.read().unwrap().clone();

            let (left_in, left_stream) = scope.new_input::<(u64, D)>();
            let (right_in, right_stream) = scope.new_input::<(u64, D)>();
            let mut probe = ProbeHandle::new();
            let hash_fn = hash_fn;

            let matrix = MatrixDescription::for_workers(peers as usize);
            let candidate_pairs = left_stream
                .colliding_pairs(&right_stream, &hash_fn)
                .pair_route(matrix);
            candidate_pairs
                .map(|pair| pair.1)
                .filter(move |(lk, rk)| {
                    let lv = global_left.get(lk).unwrap();
                    let rv = global_right.get(rk).unwrap();
                    sim_pred(lv, rv)
                })
                .approximate_distinct()
                .count()
                .exchange(|_| 0)
                .probe_with(&mut probe)
                .capture_into(output_send_ch);

            (left_in, right_in, probe)
        });

        // Push data into the dataflow graph. Each worker will read some of the lines of the input
        debug!("Reading data files:\n\t{:?}\n\t{:?}", left_path, right_path);
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
        info!(
            "Time to feed the input to the dataflow graph: {:?}",
            elapsed
        );
        worker.step_while(|| probe.less_than(&(repetitions as u32)));

        info!(
            "Execution summary for worker {}: {:?}",
            index, execution_summary
        );
        collect_execution_summaries(execution_summary, send_exec_summary.clone(), &mut worker);
    })
    .unwrap();

    reader_handle
        .join()
        .expect("Problem joining the reader thread");

    if config.is_master() {
        let mut exec_summaries = Vec::new();
        for summary in recv_exec_summary.iter() {
            match summary {
                TimelyEvent::Messages(_, msgs) => exec_summaries.extend(msgs),
                _ => (),
            }
        }
        let global_summary = exec_summaries
            .iter()
            .fold(FrozenExecutionSummary::zero(), |a, b| a.sum(b));
        println!("Global summary {:?}", global_summary);
        // From `recv` we get an entry for each timestamp, containing a one-element vector with the
        // count of output pairs for a given timestamp. We sum across all the timestamps, so we need to
        // remove the duplicates
        let count: usize = recv
            .extract()
            .iter()
            .map(|pair| pair.1.clone().iter().sum::<usize>())
            .sum();
        count
    } else {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

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

    #[test]
    fn test_pair_iterator() {
        let mut left = HashMap::new();
        left.insert(0, vec![1, 2, 3]);
        left.insert(2, vec![1, 2, 3, 4]);
        left.insert(3, vec![3, 4]);
        let mut right = HashMap::new();
        right.insert(0, vec![10, 11, 12]);
        right.insert(1, vec![19]);
        right.insert(3, vec![30]);

        let mut expected = HashSet::new();
        for l in left.get(&0).unwrap() {
            for r in right.get(&0).unwrap() {
                expected.insert((l.clone(), r.clone()));
            }
        }
        for l in left.get(&3).unwrap() {
            for r in right.get(&3).unwrap() {
                expected.insert((l.clone(), r.clone()));
            }
        }

        let mut iterator = PairGenerator::new(left, right);
        let mut actual = HashSet::new();
        assert!(!iterator.done());
        while let Some(pair) = iterator.next() {
            actual.insert(pair);
        }

        assert_eq!(expected, actual);
        assert!(iterator.done());
    }
}
