use crate::operators::*;
use std::collections::HashMap;
use timely::dataflow::channels::pact::Exchange as ExchangePact;
use timely::dataflow::operators::*;
use timely::dataflow::*;
use timely::ExchangeData;

pub trait Join<G, K, V>
where
    G: Scope,
    K: KeyData + Ord,
    V: ExchangeData,
{
    fn join_map_slice<F, I, O>(&self, other: &Stream<G, (K, V)>, f: F) -> Stream<G, O>
    where
        I: IntoIterator<Item = O>,
        O: ExchangeData,
        F: FnMut(&K, &[(K, V)], &[(K, V)]) -> I + 'static;
}

impl<G, K, V> Join<G, K, V> for Stream<G, (K, V)>
where
    G: Scope,
    K: KeyData + Ord,
    V: ExchangeData,
{
    fn join_map_slice<F, I, O>(&self, other: &Stream<G, (K, V)>, mut f: F) -> Stream<G, O>
    where
        G: Scope,
        I: IntoIterator<Item = O>,
        O: ExchangeData,
        F: FnMut(&K, &[(K, V)], &[(K, V)]) -> I + 'static,
    {
        let mut joiners = HashMap::new();
        let mut notificator = FrontierNotificator::new();

        self.binary_frontier(
            &other,
            ExchangePact::new(|pair: &(K, V)| pair.0.route()),
            ExchangePact::new(|pair: &(K, V)| pair.0.route()),
            "bucket",
            move |_, _| {
                move |left_in, right_in, output| {
                    left_in.for_each(|t, d| {
                        let mut data = d.replace(Vec::new());
                        let rep_entry = joiners
                            .entry(t.time().clone())
                            .or_insert_with(Joiner::default);
                        for (k, v) in data.drain(..) {
                            rep_entry.push_left(k, v);
                        }
                        notificator.notify_at(t.retain());
                        info!("There are {} joiners", joiners.len());
                    });
                    right_in.for_each(|t, d| {
                        let mut data = d.replace(Vec::new());
                        let rep_entry = joiners
                            .entry(t.time().clone())
                            .or_insert_with(Joiner::default);
                        for (k, v) in data.drain(..) {
                            rep_entry.push_right(k, v);
                        }
                        notificator.notify_at(t.retain());
                    });

                    let frontiers = &[left_in.frontier(), right_in.frontier()];
                    notificator.for_each(frontiers, |time, _| {
                        info!("Joiners saved at time {:?}: {}", time, joiners.len());
                        if let Some(mut joiner) = joiners.remove(&time) {
                            if joiner.has_work() {
                                let mut session = output.session(&time);
                                joiner.join_map_slice(|k, l_vals, r_vals| {
                                    let res = f(k, l_vals, r_vals);
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