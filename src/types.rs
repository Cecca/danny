use crate::measure::{InnerProduct, Jaccard};
use abomonation::Abomonation;
use heapsize::HeapSizeOf;
use std::fmt;
use std::fmt::Debug;

#[derive(Clone)]
pub struct VectorWithNorm {
    data: Vec<f64>,
    norm: f64,
}

unsafe_abomonate!(VectorWithNorm: data, norm);

impl Debug for VectorWithNorm {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "VectorWithNorm({})", self.norm)
    }
}

impl VectorWithNorm {
    pub fn dim(&self) -> usize {
        self.data.len()
    }

    pub fn new(data: Vec<f64>) -> VectorWithNorm {
        let norm = InnerProduct::norm_2(&data);
        VectorWithNorm { data, norm }
    }

    pub fn data(&self) -> &Vec<f64> {
        &self.data
    }

    pub fn norm(&self) -> f64 {
        self.norm
    }
}

impl HeapSizeOf for VectorWithNorm {
    fn heap_size_of_children(&self) -> usize {
        self.data.heap_size_of_children()
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

impl HeapSizeOf for BagOfWords {
    fn heap_size_of_children(&self) -> usize {
        self.words.heap_size_of_children()
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
