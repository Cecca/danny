use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::path::PathBuf;
use types::{BagOfWords, VectorWithNorm};

pub trait ReadDataFile {
    type Item;
    fn from_file<F>(path: &PathBuf, mut fun: F) -> ()
    where
        F: FnMut(Self::Item) -> ();
}

impl ReadDataFile for VectorWithNorm {
    type Item = VectorWithNorm;
    fn from_file<F>(path: &PathBuf, mut fun: F) -> ()
    where
        F: FnMut(VectorWithNorm) -> (),
    {
        let file = File::open(path).expect("Error opening file");
        let buf_reader = BufReader::new(file);
        for line in buf_reader.lines() {
            let data: Vec<f64> = line
                .expect("Error getting line")
                .split_whitespace()
                .skip(1)
                .map(|s| {
                    s.parse::<f64>()
                        .expect(&format!("Error parsing floating point number `{}`", s))
                }).collect();
            let vec = VectorWithNorm::new(data);
            fun(vec);
        }
    }
}

impl ReadDataFile for BagOfWords {
    type Item = BagOfWords;
    fn from_file<F>(path: &PathBuf, mut fun: F) -> ()
    where
        F: FnMut(BagOfWords) -> (),
    {
        let file = File::open(path).expect("Error opening file");
        let buf_reader = BufReader::new(file);
        for line in buf_reader.lines() {
            line.and_then(|l| {
                let mut tokens = l.split_whitespace().skip(1);
                let universe = tokens
                    .next()
                    .expect("Error getting the universe size")
                    .parse::<u32>()
                    .expect("Error parsing the universe size");
                let data: Vec<u32> = tokens
                    .map(|s| {
                        s.parse::<u32>()
                            .expect(&format!("Error parsing floating point number `{}`", s))
                    }).collect();
                let bow = BagOfWords::new(universe, data);
                fun(bow);
                Ok(())
            }).expect("Error processing line");
        }
    }
}