use crate::measure::{InnerProduct, Jaccard};

use rand::distributions::{Distribution, Exp, Normal, Uniform};
use rand::Rng;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;

#[derive(Clone, Default, Eq, Ord, Hash, PartialEq, PartialOrd, Abomonation, Copy, Debug)]
pub struct ElementId(pub u32);

impl Into<u64> for ElementId {
    fn into(self) -> u64 {
        self.0.into()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Abomonation, Default)]
pub struct Vector {
    data: Vec<f32>,
}

impl Vector {
    pub fn normalized(data: Vec<f32>) -> Self {
        let norm = InnerProduct::norm_2(&data) as f32;
        let data = data.iter().map(|x| x / norm).collect();
        Vector { data }
    }

    pub fn new(data: Vec<f32>) -> Self {
        Vector { data }
    }

    pub fn normalize(&self) -> Self {
        Self::normalized(self.data.clone())
    }

    #[allow(dead_code)]
    pub fn random_normal<R: Rng>(dim: usize, rng: &mut R) -> Self {
        let dist = Normal::new(0.0, 1.0);
        let data = dist.sample_iter(rng).take(dim).map(|x| x as f32).collect();
        Self::new(data)
    }

    pub fn data(&self) -> &Vec<f32> {
        &self.data
    }

    pub fn dim(&self) -> usize {
        self.data.len()
    }
}

#[derive(Abomonation, Serialize, Deserialize, Clone, Debug, Default)]
pub struct BagOfWords {
    pub universe: u32,
    words: Vec<u32>,
}

impl BagOfWords {
    pub fn new(universe: u32, mut words: Vec<u32>) -> BagOfWords {
        words.sort();
        BagOfWords { universe, words }
    }

    #[allow(dead_code)]
    pub fn random<R: Rng>(universe: u32, lambda: f64, rng: &mut R) -> BagOfWords {
        let dist = Exp::new(lambda);
        let length = Uniform::new(0, universe).sample(rng) as usize;
        let mut words = BTreeSet::new();
        for w in dist.sample_iter(rng).take(length) {
            let w = w.floor() as u32;
            words.insert(w);
        }
        let words = words.iter().cloned().collect();
        BagOfWords { universe, words }
    }

    pub fn words(&self) -> &Vec<u32> {
        &self.words
    }

    pub fn remap_tokens(&mut self, m: &HashMap<u32, u32>) {
        for token in &mut self.words {
            *token = m[token];
        }
        self.words.sort();
    }

    pub fn len(&self) -> usize {
        self.words.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn jaccard_predicate(r: &Self, s: &Self, sim: f64) -> bool {
        let t_unrounded = sim * (r.len() + s.len()) as f64 / (1.0 + sim);
        let t_rounded = t_unrounded.round();
        // The rounding below with the comparison with EPS is needed to counter the
        // floating point errors introduced by the division
        let t = if (t_rounded - t_unrounded).abs() < 0.000_000_000_000_01 {
            t_rounded
        } else {
            t_unrounded
        };
        let mut olap = 0;
        let mut pr = 0;
        let mut ps = 0;
        let mut maxr = r.len();
        let mut maxs = s.len();

        while maxr as f64 >= t && maxs as f64 >= t && f64::from(olap) < t {
            if r.words[pr] == s.words[ps] {
                pr += 1;
                ps += 1;
                olap += 1;
            } else if r.words[pr] < s.words[ps] {
                pr += 1;
                maxr -= 1;
            } else {
                ps += 1;
                maxs -= 1;
            }
        }
        f64::from(olap) >= t
    }
}

impl Jaccard for BagOfWords {
    fn jaccard(a: &BagOfWords, b: &BagOfWords) -> f64 {
        let mut aws = a.words.iter();
        let mut bws = b.words.iter();

        let mut intersection = 0;

        let mut aw = aws.next();
        let mut bw = bws.next();
        loop {
            if aw.is_none() || bw.is_none() {
                break;
            }
            let a = aw.unwrap();
            let b = bw.unwrap();
            if a < b {
                aw = aws.next();
            } else if a > b {
                bw = bws.next();
            } else {
                intersection += 1;
                aw = aws.next();
                bw = bws.next();
            }
        }

        intersection as f64 / (a.words.len() + b.words.len() - intersection) as f64
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use rand::SeedableRng;
    use rand_xorshift::XorShiftRng;
    use std::collections::BTreeSet;

    fn simple_jaccard(a: &BagOfWords, b: &BagOfWords) -> f64 {
        let mut a_set: BTreeSet<u32> = BTreeSet::new();
        a_set.extend(a.words().iter());
        let mut b_set: BTreeSet<u32> = BTreeSet::new();
        b_set.extend(b.words().iter());
        a_set.intersection(&b_set).count() as f64 / a_set.union(&b_set).count() as f64
    }

    #[test]
    fn test_jaccard() {
        let mut rng = XorShiftRng::seed_from_u64(1412);
        let a = BagOfWords::random(3000, 1.5, &mut rng);
        let b = BagOfWords::random(3000, 1.5, &mut rng);
        let actual = Jaccard::jaccard(&a, &b);
        let expected = simple_jaccard(&a, &b);
        assert_eq!(actual, expected);
    }
}
