use crate::types::{BagOfWords, VectorWithNorm};
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::path::PathBuf;

pub trait ReadDataFile
where
    Self: std::marker::Sized,
{
    fn from_file<F>(path: &PathBuf, mut fun: F) -> ()
    where
        F: FnMut(Self) -> (),
    {
        Self::from_file_with_count(path, |_, d| fun(d));
    }

    fn from_file_with_count<F>(path: &PathBuf, fun: F) -> ()
    where
        F: FnMut(u64, Self) -> (),
    {
        Self::from_file_partially(path, |_| true, fun);
    }

    fn from_file_partially<P, F>(path: &PathBuf, pred: P, mut fun: F) -> ()
    where
        P: Fn(u64) -> bool,
        F: FnMut(u64, Self) -> (),
    {
        let file = File::open(path).expect("Error opening file");
        let buf_reader = BufReader::new(file);
        let mut cnt = 0;
        for line in buf_reader.lines() {
            line.and_then(|l| {
                if pred(cnt) {
                    let elem = Self::from_line(&l);
                    fun(cnt, elem);
                }
                cnt += 1;
                Ok(())
            })
            .expect("Error processing line");
        }
    }

    fn peek_first(path: &PathBuf) -> Self {
        let file = File::open(path).expect("Error opening file");
        let buf_reader = BufReader::new(file);
        let first_line = buf_reader
            .lines()
            .next()
            .expect("Problem reading the first line")
            .expect("Error getting the line");
        Self::from_line(&first_line)
    }

    fn from_line(line: &str) -> Self;
}

impl ReadDataFile for VectorWithNorm {
    fn from_line(line: &str) -> Self {
        let data: Vec<f64> = line
            .split_whitespace()
            .skip(1)
            .map(|s| {
                s.parse::<f64>()
                    .expect(&format!("Error parsing floating point number `{}`", s))
            })
            .collect();
        VectorWithNorm::new(data)
    }
}

impl ReadDataFile for BagOfWords {
    fn from_line(line: &str) -> Self {
        let mut tokens = line.split_whitespace().skip(1);
        let universe = tokens
            .next()
            .expect("Error getting the universe size")
            .parse::<u32>()
            .expect("Error parsing the universe size");
        let data: Vec<u32> = tokens
            .map(|s| {
                s.parse::<u32>()
                    .expect(&format!("Error parsing floating point number `{}`", s))
            })
            .collect();
        BagOfWords::new(universe, data)
    }
}
