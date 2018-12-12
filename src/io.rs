use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::path::PathBuf;
use types::VectorWithNorm;

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
