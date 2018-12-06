use std::collections::BTreeSet;

#[allow(dead_code)]
pub fn pairwise_similarity_distribution<P, F>(left: &[P], right: &[P], similarity: F) -> Vec<f64>
where
    F: Fn(&P, &P) -> f64,
{
    let mut res = vec![0.0_f64; left.len() * right.len()];
    let mut i = 0;
    for l in left.iter() {
        for r in right.iter() {
            let d = similarity(l, r);
            res[i] = d;
            i += 1;
        }
    }
    res
}

#[allow(dead_code)]
pub fn nearest_neighbour_similarity_distribution<P, F>(
    left: &[P],
    right: &[P],
    similarity: F,
) -> Vec<f64>
where
    F: Fn(&P, &P) -> f64,
{
    let mut res = vec![0.0_f64; left.len()];
    let mut i = 0;
    for l in left.iter() {
        let mut m = std::f64::INFINITY;
        for r in right.iter() {
            let d = similarity(l, r);
            if d < m {
                m = d;
            }
        }
        res[i] = m;
        i += 1;
    }
    res
}

#[derive(Debug)]
struct TopK<T> {
    k: usize,
    elems: BTreeSet<T>,
}

#[derive(PartialEq, PartialOrd, Clone)]
struct WrappedFloat {
    f: f64,
}

impl WrappedFloat {
    fn new(f: f64) -> WrappedFloat {
        WrappedFloat { f }
    }
}

impl Eq for WrappedFloat {}

impl Ord for WrappedFloat {
    fn cmp(&self, b: &Self) -> std::cmp::Ordering {
        self.f.partial_cmp(&b.f).expect("NaN sneaked in!")
    }
}

impl<T> TopK<T>
where
    T: Ord + Clone,
{
    fn new(k: usize) -> TopK<T> {
        TopK {
            k: k,
            elems: BTreeSet::new(),
        }
    }

    fn update(&mut self, x: T) {
        if self.elems.len() < self.k {
            self.elems.insert(x);
        } else {
            let smallest: T = self
                .elems
                .iter()
                .next()
                .expect("Got an empty iterator from the TopK data structure")
                .clone();
            self.elems.remove(&smallest);
            self.elems.insert(x);
        }
    }
}

#[allow(dead_code)]
pub fn first_to_k_ratio_distribution<P, F>(
    k: usize,
    left: &[P],
    right: &[P],
    similarity: F,
) -> Vec<f64>
where
    F: Fn(&P, &P) -> f64,
{
    let mut res = vec![0.0_f64; left.len()];
    let mut i = 0;
    for l in left.iter() {
        let mut topk = TopK::new(k);
        for r in right.iter() {
            let d = WrappedFloat::new(similarity(l, r));
            topk.update(d);
        }
        let mut it = topk.elems.iter();
        let ratio = it.next().expect("problems").f / it.last().unwrap().f;
        res[i] = ratio;
        i += 1;
    }
    res
}

#[allow(dead_code)]
pub fn first_to_percentile_ratio_distribution<P, F>(
    perc: f64,
    left: &[P],
    right: &[P],
    similarity: F,
) -> Vec<f64>
where
    F: Fn(&P, &P) -> f64,
{
    let mut res = vec![0.0_f64; left.len()];
    let mut i = 0;
    for l in left.iter() {
        let mut sims = Vec::with_capacity(right.len());
        for r in right.iter() {
            let d = similarity(l, r);
            sims.push(d);
        }
        sims.sort_unstable_by(|a, b| a.partial_cmp(b).expect("A NaN sneaked in"));
        let first = sims[0];
        let elem_at_perc = sims[(perc * sims.len() as f64) as usize];
        let ratio = first / elem_at_perc;
        res[i] = ratio;
        i += 1;
    }
    res
}
