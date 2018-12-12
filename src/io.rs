use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::path::PathBuf;
use types::VectorWithNorm;

trait ReadDataFile {
    type Item;
    fn foreach<F>(path: &PathBuf, fun: F) -> ()
    where
        F: Fn(Self::Item) -> ();
}

impl ReadDataFile for VectorWithNorm {
    type Item = VectorWithNorm;
    fn foreach<F>(path: &PathBuf, fun: F) -> ()
    where
        F: Fn(VectorWithNorm) -> (),
    {
        let file = File::open(path).expect("Error opening file");
        let buf_reader = BufReader::new(file);
        for line in buf_reader.lines() {
            let data: Vec<f64> = line
                .expect("Error getting line")
                .split_whitespace()
                .map(|s| {
                    s.parse::<f64>()
                        .expect("Error parsing floating point number")
                }).collect();
            let vec = VectorWithNorm::new(data);
            fun(vec);
        }
    }
}
