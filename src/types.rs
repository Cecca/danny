use crate::measure::{InnerProduct, Jaccard};
use abomonation::Abomonation;
use std::fmt;
use std::fmt::Debug;

#[derive(Clone)]
pub struct VectorWithNorm {
    data: Vec<f32>,
    norm: f64,
}

unsafe_abomonate!(VectorWithNorm: data, norm);

impl Debug for VectorWithNorm {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "VectorWithNorm({})", self.norm)
    }
}

impl VectorWithNorm {
    #[allow(dead_code)]
    pub fn dim(&self) -> usize {
        self.data.len()
    }

    pub fn new(data: Vec<f32>) -> VectorWithNorm {
        let norm = InnerProduct::norm_2(&data);
        VectorWithNorm { data, norm }
    }

    pub fn data(&self) -> &Vec<f32> {
        &self.data
    }

    pub fn norm(&self) -> f64 {
        self.norm
    }
}

#[derive(Debug, Clone, Abomonation)]
pub struct UnitNormVector {
    data: Vec<f32>,
}

impl UnitNormVector {
    pub fn new(data: Vec<f32>) -> Self {
        let norm = InnerProduct::norm_2(&data) as f32;
        let data = data.iter().map(|x| x / norm).collect();
        UnitNormVector { data }
    }

    pub fn data(&self) -> &Vec<f32> {
        &self.data
    }

    pub fn dim(&self) -> usize {
        self.data.len()
    }
}

impl From<VectorWithNorm> for UnitNormVector {
    fn from(v: VectorWithNorm) -> Self {
        let norm = v.norm() as f32;
        let data = v.data.iter().map(|x| x / norm).collect();
        UnitNormVector { data }
    }
}

#[derive(Clone, Debug)]
pub struct BagOfWords {
    universe: u32,
    words: Vec<u32>,
}

unsafe_abomonate!(BagOfWords: universe, words);

impl BagOfWords {
    pub fn new(universe: u32, mut words: Vec<u32>) -> BagOfWords {
        words.sort();
        BagOfWords { universe, words }
    }

    pub fn num_words(&self) -> usize {
        self.words.len()
    }

    pub fn word_at(&self, i: usize) -> u32 {
        self.words[i]
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
