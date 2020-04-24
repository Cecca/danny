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
        "unit-norm-vector" => {
            read_write::<UnitNormVector>(input, output.clone(), chunks);
        }
        "vector" => {
            read_write::<VectorWithNorm>(input, output.clone(), chunks);
        }
        "bag-of-words" => {
            read_write::<BagOfWords>(input, output, chunks);
        }
        x => panic!("Unsupported measure {}", x),
    }
}
