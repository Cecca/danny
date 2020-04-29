#[macro_use]
extern crate clap;
extern crate danny;
extern crate serde;

use danny::io::*;
use danny_base::types::*;
use serde::ser::Serialize;
use std::path::PathBuf;

fn read_write<T>(input: PathBuf, output: PathBuf, chunks: usize)
where
    T: Serialize + ReadDataFile,
{
    let mut elements = Vec::new();
    T::from_file(&input, |e| elements.push(e));
    WriteBinaryFile::write_binary(output, chunks, elements.iter());
}

fn main() {
    let matches = clap_app!(danny_convert =>
        (version: "0.1")
        (author: "Matteo Ceccarello <mcec@itu.dk>")
        (about: "Convert text files to binary data format")
        (@arg TYPE: -t +takes_value +required "The type of data (unit-norm-vector, bag-of-words)")
        (@arg CHUNKS: -n +takes_value +required "The number of chunks")
        (@arg INPUT: -i +takes_value +required "The input path")
        (@arg OUTPUT: -o +takes_value +required "The output path")
    )
    .get_matches();

    let input: PathBuf = matches.value_of("INPUT").unwrap().into();
    let output: PathBuf = matches.value_of("OUTPUT").unwrap().into();
    let chunks: usize = matches
        .value_of("CHUNKS")
        .unwrap()
        .parse()
        .expect("Chunks should be an integer");

    match matches.value_of("TYPE").unwrap() {
        "vector" => {
        println!("Converting to vectors");
            let mut elements = Vec::new();
            Vector::from_file(&input, |e| elements.push(e));
            Vector::write_binary(output, chunks, elements.into_iter());
        }
        "vector-normalized" => {
            println!("Converting to normalized vectors");
            let mut elements = Vec::new();
            Vector::from_file(&input, |e| elements.push(e.normalize()));
            Vector::write_binary(output, chunks, elements.into_iter());
        }
        "bag-of-words" => {
            println!("Converting to bag of words");
            let mut elements = Vec::new();
            BagOfWords::from_file(&input, |e| elements.push(e));
            BagOfWords::write_binary(output, chunks, elements.into_iter());
        }
        x => panic!("Unsupported type {}", x),
    }
}
