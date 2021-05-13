use crate::cartesian::*;
use crate::logging::*;
use crate::operators::*;
use channels::pact::Pipeline;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use timely::dataflow::channels::pact::Exchange as ExchangePact;
use timely::dataflow::operators::aggregation::Aggregate;
use timely::dataflow::operators::*;
use timely::dataflow::*;
use timely::ExchangeData;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Balance {
    Load,
    SubproblemSize,
}

impl std::str::FromStr for Balance {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Load" | "load" => Ok(Self::Load),
            "SubproblemSize" | "subproblemsize" | "size" => Ok(Self::SubproblemSize),
            _ => Err(format!("unknown balancing {}", s)),
        }
    }
}

impl Balance {
    fn size(&self, x: u64) -> u64 {
        match self {
            Self::Load => x,
            Self::SubproblemSize => x * (x + 1) / 2,
        }
    }
}

/// Trait for join payloads which can be further split into key-value pairs
pub trait KeyPayload {
    type Key: std::hash::Hash + Eq + Clone + ExchangeData;
    type Value: ExchangeData;
    fn split_payload(self) -> (Self::Key, Self::Value);
}

pub trait Join<G, K, V>
where
    G: Scope,
    K: KeyData + Ord + std::fmt::Debug,
    V: ExchangeData + KeyPayload,
{
    fn self_join_map_slice<F, I, O>(&self, f: F) -> Stream<G, O>
    where
        I: IntoIterator<Item = O>,
        O: ExchangeData,
        F: FnMut(&K, &[(K, V)]) -> I + 'static;

    fn self_join_map<F, I, O>(&self, balance: Balance, f: F) -> Stream<G, O>
    where
        I: IntoIterator<Item = O>,
        O: ExchangeData,
        F: FnMut((K, CartesianKey), &[(Marker, V::Key)], &HashMap<V::Key, V::Value>) -> I + 'static;
}

impl<G, K, V> Join<G, K, V> for Stream<G, (K, V)>
where
    G: Scope,
    G::Timestamp: ToStepId,
    K: KeyData + Ord + std::fmt::Debug,
    V: ExchangeData + KeyPayload,
{
    fn self_join_map<F, I, O>(&self, balance: Balance, mut f: F) -> Stream<G, O>
    where
        I: IntoIterator<Item = O>,
        O: ExchangeData,
        F: FnMut((K, CartesianKey), &[(Marker, V::Key)], &HashMap<V::Key, V::Value>) -> I + 'static,
    {
        let peers = self.scope().peers();
        let worker_index = self.scope().index();
        let logger = self.scope().danny_logger();
        let stash = Rc::new(RefCell::new(HashMap::new()));
        let stash1 = Rc::clone(&stash);
        let payloads_stash = Rc::new(RefCell::new(HashMap::new()));
        let payloads_stash1 = Rc::clone(&payloads_stash);
        let mut bucket_stash: HashMap<
            G::Timestamp,
            HashMap<(K, CartesianKey), Vec<(Marker, V::Key)>>,
        > = HashMap::new();
        let mut histograms: HashMap<G::Timestamp, HashMap<K, u32>> = HashMap::new();
        let mut subproblem_sizes: HashMap<G::Timestamp, Vec<(K, u32)>> = HashMap::new();
        let payloads: Rc<RefCell<HashMap<V::Key, V::Value>>> =
            Rc::new(RefCell::new(HashMap::new()));

        self.unary_notify(
            Pipeline,
            "count subproblem size",
            None,
            move |input, output, notificator| {
                notificator.for_each(|t, _, _| {
                    if let Some(histogram) = histograms.remove(&t) {
                        let mut session = output.session(&t);
                        session.give_iterator(histogram.into_iter());
                    }
                });

                input.for_each(|t, data| {
                    let data = data.replace(Vec::new());
                    for (k, v) in data {
                        let (payload_k, payload_v) = v.split_payload();
                        stash
                            .borrow_mut()
                            .entry(t.time().clone())
                            .or_insert_with(Vec::new)
                            .push((k, payload_k.clone()));
                        payloads_stash
                            .borrow_mut()
                            .entry(payload_k)
                            .or_insert(payload_v);
                        *histograms
                            .entry(t.time().clone())
                            .or_insert_with(HashMap::new)
                            .entry(k)
                            .or_insert(0u32) += 1;
                    }
                    notificator.notify_at(t.retain());
                });
            },
        )
        // Now accumulate the counters into the first worker to compute the subproblem size
        .exchange(|_k| 0)
        .aggregate(
            |_key, val, agg| {
                *agg += val;
            },
            |key, agg: u32| (key, agg),
            |k| k.route() as u64,
        )
        .broadcast()
        .unary_notify(
            Pipeline,
            "split subproblems",
            None,
            move |input, output, notificator| {
                notificator.for_each(|t, _, _| {
                    let mut sizes = subproblem_sizes.remove(&t).expect("missing histograms!");

                    // Sort by decreasing count
                    sizes.sort_unstable_by_key(|x| std::cmp::Reverse(x.1));
                    let total_pairs = sizes.iter().map(|p| balance.size(p.1 as u64)).sum::<u64>();

                    // Split subproblems that are too big
                    let threshold = (total_pairs as f64 / peers as f64).ceil() as u64;
                    let mut subproblems: Vec<(K, CartesianKey, Marker, u64)> = sizes
                        .into_iter()
                        .flat_map(|(key, size)| {
                            let groups = if size as u64 > threshold {
                                (size as f64 / threshold as f64).ceil() as u8
                            } else {
                                1
                            };
                            let subproblem_size = balance.size(size as u64 / groups as u64);
                            let cartesian = SelfCartesian::with_groups(groups);
                            cartesian
                                .keys_for(key)
                                .map(move |(sk, m)| (key, sk, m, subproblem_size))
                        })
                        .collect();
                    subproblems.sort_unstable_by_key(|x| std::cmp::Reverse(x.3));
                    let n_subproblems = subproblems.len();

                    // Allocate subproblems to processors
                    let mut processor_allocations = vec![Vec::new(); peers];
                    info!("threshold {}", threshold);
                    let mut i = 0;
                    let mut cur = 0u64;
                    for (sub_idx, (key, subproblem_key, marker, subproblem_size)) in
                        subproblems.into_iter().enumerate()
                    {
                        if cur > threshold {
                            cur = 0;
                            i += 1;
                        }
                        if i >= processor_allocations.len() {
                            panic!(
                                "number or processors ({}) exceeded: still {} to go",
                                peers,
                                n_subproblems - sub_idx
                            );
                        }
                        processor_allocations[i].push((key, subproblem_key, marker));
                        cur += subproblem_size;
                    }

                    let mut subproblem_allocations = HashMap::new();
                    for (p, allocs) in processor_allocations.into_iter().enumerate() {
                        for (key, subproblem_key, marker) in allocs {
                            subproblem_allocations
                                .entry(key)
                                .or_insert_with(Vec::new)
                                .push((p as u64, subproblem_key, marker));
                        }
                    }
                    info!(
                        "Subproblem allocations hashmap has {} entries",
                        subproblem_allocations.len()
                    );

                    // Get the keys and output them to the appropriate processor
                    if let Some(mut pairs) = stash1.borrow_mut().remove(&t.time()) {
                        let payloads = payloads_stash1.borrow();
                        info!(
                            "Redistributing items to workers, the stash has {} items, payloads are {}",
                            pairs.len(),
                            payloads.len()
                        );
                        let mut session = output.session(&t);
                        for (key, payload_key) in pairs.drain(..) {
                            let payload =
                                payloads.get(&payload_key).expect("missing payload for key");
                            for &(p, subproblem_key, marker) in subproblem_allocations
                                .get(&key)
                                .expect("missing key!")
                                .iter()
                            {
                                session.give((
                                    (p, key, subproblem_key),
                                    (marker, payload_key.clone(), payload.clone()),
                                ));
                            }
                        }
                        info!("Done sending");
                        drop(pairs);
                    }
                });

                input.for_each(|t, data| {
                    let data = data.replace(Vec::new());
                    subproblem_sizes
                        .entry(t.time().clone())
                        .or_insert_with(Vec::new)
                        .extend(data.into_iter());
                    notificator.notify_at(t.retain());
                })
            },
        )
        .unary_notify(
            ExchangePact::new(
                |(key, _payload): &((u64, K, CartesianKey), (Marker, V::Key, V::Value))| key.0,
            ),
            "bucket",
            None,
            move |input, output, notificator| {
                notificator.for_each(|t, _, _| {
                    if let Some(subproblems) = bucket_stash.remove(&t) {
                        info!(
                            "Worker {} has {} subproblems, with {} payloads",
                            worker_index,
                            subproblems.len(),
                            payloads.borrow().len()
                        );
                        let mut session = output.session(&t);
                        for (key, subproblem) in subproblems {
                            let res = f(key, &subproblem, &payloads.borrow());
                            session.give_iterator(res.into_iter());
                        }
                    }
                });

                input.for_each(|t, data| {
                    log_event!(logger, (LogEvent::Load(t.time().to_step_id()), data.len()));
                    let data = data.replace(Vec::new());
                    let stash = bucket_stash
                        .entry(t.time().clone())
                        .or_insert_with(HashMap::new);
                    for ((_processor, k, subproblem), (marker, payload_k, payload_v)) in data {
                        stash
                            .entry((k, subproblem))
                            .or_insert_with(Vec::new)
                            .push((marker, payload_k.clone()));
                        payloads.borrow_mut().entry(payload_k).or_insert(payload_v);
                    }
                    notificator.notify_at(t.retain());
                });
            },
        )
    }

    fn self_join_map_slice<F, I, O>(&self, mut f: F) -> Stream<G, O>
    where
        G: Scope,
        I: IntoIterator<Item = O>,
        O: ExchangeData,
        F: FnMut(&K, &[(K, V)]) -> I + 'static,
    {
        let mut joiners = HashMap::new();
        let mut notificator = FrontierNotificator::new();
        let logger = self.scope().danny_logger();

        self.unary_frontier(
            ExchangePact::new(|pair: &(K, V)| pair.0.route()),
            "bucket",
            move |_, _| {
                move |input, output| {
                    input.for_each(|t, d| {
                        log_event!(logger, (LogEvent::Load(t.time().to_step_id()), d.len()));
                        let mut data = d.replace(Vec::new());
                        let rep_entry = joiners
                            .entry(t.time().clone())
                            .or_insert_with(SelfJoiner::default);
                        for (k, v) in data.drain(..) {
                            rep_entry.push(k, v);
                        }
                        notificator.notify_at(t.retain());
                    });

                    let frontiers = &[input.frontier()];
                    notificator.for_each(frontiers, |time, _| {
                        if let Some(mut joiner) = joiners.remove(&time) {
                            if joiner.has_work() {
                                let mut session = output.session(&time);
                                joiner.join_map_slice(|k, vals| {
                                    let res = f(k, vals);
                                    for r in res {
                                        session.give(r);
                                    }
                                });
                            }
                        }
                    });
                }
            },
        )
    }
}

pub struct SelfJoiner<K, V>
where
    K: Ord,
{
    vecs: Vec<(K, V)>,
}

impl<K: Ord, V> Default for SelfJoiner<K, V> {
    fn default() -> Self {
        Self { vecs: Vec::new() }
    }
}

pub struct Joiner<K, V>
where
    K: Ord,
{
    left: Vec<(K, V)>,
    right: Vec<(K, V)>,
}

impl<K: Ord, V> Default for Joiner<K, V> {
    fn default() -> Self {
        Self {
            left: Vec::new(),
            right: Vec::new(),
        }
    }
}

impl<K: Ord, V> SelfJoiner<K, V> {
    pub fn has_work(&self) -> bool {
        !self.vecs.is_empty()
    }

    pub fn push(&mut self, k: K, v: V) {
        self.vecs.push((k, v));
    }

    fn sort_inner(&mut self) {
        self.vecs.sort_unstable_by(|p1, p2| p1.0.cmp(&p2.0));
    }

    pub fn join_map_slice<F>(&mut self, mut f: F)
    where
        F: FnMut(&K, &[(K, V)]),
    {
        self.sort_inner();
        let iter = SelfJoinIter::new(&self.vecs);
        for slice in iter {
            f(&slice[0].0, slice);
        }
    }

    pub fn join_map<F>(&mut self, mut f: F)
    where
        F: FnMut(&K, &V, &V),
    {
        self.join_map_slice(|k, vals| {
            for (i, l) in vals.iter().enumerate() {
                for r in vals[i..].iter() {
                    f(k, &l.1, &r.1);
                }
            }
        });
    }

    pub fn clear(&mut self) {
        self.vecs.clear();
    }
}

impl<K: Ord, V> Joiner<K, V> {
    pub fn has_work(&self) -> bool {
        !(self.left.is_empty() || self.right.is_empty())
    }

    pub fn push_left(&mut self, k: K, v: V) {
        self.left.push((k, v));
    }

    pub fn push_right(&mut self, k: K, v: V) {
        self.right.push((k, v));
    }

    fn sort_inner(&mut self) {
        self.left.sort_unstable_by(|p1, p2| p1.0.cmp(&p2.0));
        self.right.sort_unstable_by(|p1, p2| p1.0.cmp(&p2.0));
    }

    pub fn join_map_slice<F>(&mut self, mut f: F)
    where
        F: FnMut(&K, &[(K, V)], &[(K, V)]),
    {
        self.sort_inner();
        let iter = JoinIter::new(&self.left, &self.right);
        for (l_slice, r_slice) in iter {
            f(&l_slice[0].0, l_slice, r_slice);
        }
    }

    pub fn join_map<F>(&mut self, mut f: F)
    where
        F: FnMut(&K, &V, &V),
    {
        self.join_map_slice(|k, l_vals, r_vals| {
            for l in l_vals {
                for r in r_vals {
                    f(k, &l.1, &r.1);
                }
            }
        });
    }

    pub fn clear(&mut self) {
        self.left.clear();
        self.right.clear();
    }
}

struct SelfJoinIter<'a, K, V>
where
    K: Ord,
{
    vecs: &'a [(K, V)],
    cur_idx: usize,
}

impl<'a, K, V> SelfJoinIter<'a, K, V>
where
    K: Ord,
{
    fn new(vecs: &'a [(K, V)]) -> Self {
        Self { vecs, cur_idx: 0 }
    }

    fn find_bucket_end(&self, items: &'a [(K, V)], start: usize) -> (&'a K, usize) {
        let start_hash = &items[start].0;
        let end = start
            + items[start..]
                .iter()
                .take_while(|p| &p.0 == start_hash)
                .count();
        (start_hash, end)
    }
}

impl<'a, K, V> Iterator for SelfJoinIter<'a, K, V>
where
    K: Ord,
{
    type Item = &'a [(K, V)];

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.cur_idx >= self.vecs.len() {
                return None;
            }
            let end = self.find_bucket_end(self.vecs, self.cur_idx);

            let start = self.cur_idx;
            self.cur_idx = end.1;
            return Some(&self.vecs[start..self.cur_idx]);
        }
    }
}

struct JoinIter<'a, K, V>
where
    K: Ord,
{
    left: &'a [(K, V)],
    right: &'a [(K, V)],
    cur_left: usize,
    cur_right: usize,
}

impl<'a, K, V> JoinIter<'a, K, V>
where
    K: Ord,
{
    fn new(left: &'a [(K, V)], right: &'a [(K, V)]) -> Self {
        Self {
            left,
            right,
            cur_left: 0,
            cur_right: 0,
        }
    }

    fn find_bucket_end(&self, items: &'a [(K, V)], start: usize) -> (&'a K, usize) {
        let start_hash = &items[start].0;
        let end = start
            + items[start..]
                .iter()
                .take_while(|p| &p.0 == start_hash)
                .count();
        (start_hash, end)
    }
}

impl<'a, K, V> Iterator for JoinIter<'a, K, V>
where
    K: Ord,
{
    type Item = (&'a [(K, V)], &'a [(K, V)]);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.cur_left >= self.left.len() || self.cur_right >= self.right.len() {
                return None;
            }
            let lend = self.find_bucket_end(self.left, self.cur_left);
            let rend = self.find_bucket_end(self.right, self.cur_right);
            if lend.0 < rend.0 {
                self.cur_left = lend.1;
            } else if lend.0 > rend.0 {
                self.cur_right = rend.1;
            } else {
                // We are in a non empty bucket!
                let lstart = self.cur_left;
                let rstart = self.cur_right;
                self.cur_left = lend.1;
                self.cur_right = rend.1;
                return Some((
                    &self.left[lstart..self.cur_left],
                    &self.right[rstart..self.cur_right],
                ));
            }
        }
    }
}
