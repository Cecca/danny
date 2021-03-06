extern crate bincode;

use danny_base::types::*;
use serde::de::Deserialize;
use serde::ser::Serialize;

use std::fs::create_dir;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::path::Path;
use std::path::PathBuf;

pub enum ContentType {
    Vector,
    BagOfWords,
}

pub trait ReadBinaryFile
where
    Self: std::marker::Sized,
{
    fn peek_one(path: PathBuf) -> Self;
    fn read_binary<P, F>(path: PathBuf, predicate: P, fun: F)
    where
        P: Fn(usize) -> bool,
        F: FnMut(u64, Self) -> ();
    fn num_elements(path: PathBuf) -> usize;
    fn num_chunks(path: PathBuf) -> usize {
        path.read_dir()
            .expect("Problems reading the directory")
            .map(|entry| entry.expect("Problem reading entry").path())
            .count()
    }
}

pub fn content_type<P: AsRef<Path>>(path: P) -> ContentType {
    let path = path.as_ref().to_path_buf();
    assert!(path.is_dir());
    let file = path
        .read_dir()
        .expect("Problem reading directory")
        .next()
        .expect("Problem reading next entry")
        .expect("Problem reading entry")
        .path();

    if has_content_type::<Vector>(&file) {
        ContentType::Vector
    } else if has_content_type::<BagOfWords>(&file) {
        ContentType::BagOfWords
    } else {
        panic!("could not determine the content type")
    }
}

fn has_content_type<T>(f: &PathBuf) -> bool
where
    for<'de> T: Deserialize<'de> + std::marker::Sized,
{
    let file = File::open(f).expect("Problem opening file");
    let mut r = BufReader::new(file);
    bincode::deserialize_from::<_, (u32, T)>(&mut r).is_ok()
}

impl<T> ReadBinaryFile for T
where
    for<'de> T: Deserialize<'de> + std::marker::Sized,
{
    fn peek_one(path: PathBuf) -> T {
        assert!(path.is_dir());
        let file = path
            .read_dir()
            .expect("Problem reading directory")
            .next()
            .expect("Problem reading next entry")
            .expect("Problem reading entry")
            .path();
        let file = File::open(file).expect("Problem opening file");
        let mut buf_reader = BufReader::new(file);
        let res: bincode::Result<(u32, T)> = bincode::deserialize_from(&mut buf_reader);
        res.expect("Error deserializing").1
    }

    fn read_binary<P, F>(path: PathBuf, predicate: P, mut fun: F)
    where
        P: Fn(usize) -> bool,
        F: FnMut(u64, T) -> (),
    {
        assert!(path.is_dir());
        let files: Vec<PathBuf> = path
            .read_dir()
            .expect("Problems reading the directory")
            .map(|entry| entry.expect("Problem reading entry").path())
            .filter(|path| {
                let file_id = path
                    .file_name()
                    .expect("error getting the file name")
                    .to_string_lossy()
                    .to_string()
                    .parse::<usize>()
                    .expect("Error parsing the file name into an integer");
                predicate(file_id)
            })
            .collect();
        for path in files.iter() {
            let file = File::open(path).expect("Error opening file");
            let mut buf_reader = BufReader::new(file);
            loop {
                let res: bincode::Result<(u32, T)> = bincode::deserialize_from(&mut buf_reader);
                match res {
                    Ok((i, element)) => fun(u64::from(i), element),
                    Err(_) => {
                        break;
                    }
                }
            }
        }
    }

    fn num_elements(path: PathBuf) -> usize {
        assert!(path.is_dir());
        let files: Vec<PathBuf> = path
            .read_dir()
            .expect("Problems reading the directory")
            .map(|entry| entry.expect("Problem reading entry").path())
            .collect();
        let mut cnt = 0;
        for path in files.iter() {
            let file = File::open(path).expect("Error opening file");
            let mut buf_reader = BufReader::new(file);
            loop {
                let res: bincode::Result<(u32, T)> = bincode::deserialize_from(&mut buf_reader);
                match res {
                    Ok((_i, _element)) => cnt += 1,
                    Err(_) => {
                        break;
                    }
                }
            }
        }
        cnt
    }
}

pub trait WriteBinaryFile
where
    Self: Sized,
{
    fn write_binary<I>(path: PathBuf, num_chunks: usize, elements: I)
    where
        I: Iterator<Item = Self>;
}

impl<T> WriteBinaryFile for T
where
    T: std::marker::Sized + Serialize,
{
    fn write_binary<I>(directory: PathBuf, num_chunks: usize, elements: I)
    where
        I: Iterator<Item = Self>,
    {
        create_dir(&directory).expect("Error creating directory");
        let mut files = Vec::with_capacity(num_chunks);
        for chunk in 0..num_chunks {
            let mut path = directory.clone();
            path.push(format!("{}", chunk));
            let writer = File::create(path).expect("Error creating file");
            files.push(writer);
        }

        let mut cnt = 0;
        for (i, element) in elements.enumerate() {
            let writer = files.get_mut(i % num_chunks).expect("Out of bounds index");
            let pair = (i as u32, element);
            bincode::serialize_into(writer, &pair).expect("Error while serializing");
            cnt += 1;
        }
        info!("Actually written {} elements", cnt);
    }
}

pub struct BinaryDataset {
    directory: PathBuf,
}

impl BinaryDataset {
    pub fn new(path: PathBuf) -> Self {
        BinaryDataset { directory: path }
    }

    pub fn read<T, P, F>(&self, predicate: P, mut fun: F)
    where
        for<'de> T: Deserialize<'de>,
        P: Fn(usize) -> bool,
        F: FnMut(T) -> (),
    {
        assert!(self.directory.is_dir());
        let files: Vec<PathBuf> = self
            .directory
            .read_dir()
            .expect("Problems reading the directory")
            .map(|entry| entry.expect("Problem reading entry").path())
            .filter(|path| {
                let file_id = path
                    .file_name()
                    .expect("error getting the file name")
                    .to_string_lossy()
                    .to_string()
                    .parse::<usize>()
                    .expect("Error parsing the file name into an integer");
                predicate(file_id)
            })
            .collect();
        for path in files.iter() {
            let file = File::open(path).expect("Error opening file");
            let mut buf_reader = BufReader::new(file);
            loop {
                let res: bincode::Result<T> = bincode::deserialize_from(&mut buf_reader);
                match res {
                    Ok(element) => fun(element),
                    Err(_) => break,
                }
            }
        }
    }

    pub fn write<T, I>(&self, num_chunks: usize, elements: I)
    where
        T: Serialize,
        I: Iterator<Item = T>,
    {
        create_dir(&self.directory).expect("Error creating directory");
        let mut files = Vec::with_capacity(num_chunks);
        for chunk in 0..num_chunks {
            let mut path = self.directory.clone();
            path.push(format!("{}", chunk));
            let writer = File::create(path).expect("Error creating file");
            files.push(writer);
        }

        for (i, element) in elements.enumerate() {
            let writer = files.get_mut(i % num_chunks).expect("Out of bounds index");
            bincode::serialize_into(writer, &element).expect("Error while serializing");
        }
    }
}

pub trait ReadDataFile
where
    Self: std::marker::Sized,
{
    fn from_file<F>(path: &PathBuf, mut fun: F)
    where
        F: FnMut(Self) -> (),
    {
        Self::from_file_with_count(path, |_, d| fun(d));
    }

    fn from_file_with_count<F>(path: &PathBuf, fun: F)
    where
        F: FnMut(u64, Self) -> (),
    {
        Self::from_file_partially(path, |_| true, fun);
    }

    fn from_file_partially<P, F>(path: &PathBuf, pred: P, mut fun: F)
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
                    if let Some(elem) = Self::from_line(&l) {
                        fun(cnt, elem);
                    }
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
        Self::from_line(&first_line).expect("Cannot decode the first line")
    }

    fn num_elements(path: &PathBuf) -> usize {
        let file = File::open(path).expect("Error opening file");
        let buf_reader = BufReader::new(file);
        buf_reader.lines().count()
    }

    fn from_line(line: &str) -> Option<Self>;
}

impl ReadDataFile for Vector {
    fn from_line(line: &str) -> Option<Self> {
        let data: Vec<f32> = line
            .split_whitespace()
            .skip(1)
            .map(|s| {
                s.parse::<f32>()
                    .unwrap_or_else(|_| panic!("Error parsing floating point number `{}`", s))
            })
            .collect();
        Some(Vector::new(data))
    }
}

impl ReadDataFile for BagOfWords {
    fn from_line(line: &str) -> Option<Self> {
        let mut tokens = line.split_whitespace().skip(1);
        let universe = tokens
            .next()
            .expect("Error getting the universe size")
            .parse::<u32>()
            .expect("Error parsing the universe size");
        let data: Vec<u32> = tokens
            .map(|s| {
                s.parse::<u32>()
                    .unwrap_or_else(|_| panic!("Error parsing floating point number `{}`", s))
            })
            .collect();
        if data.is_empty() {
            None
        } else {
            Some(BagOfWords::new(universe, data))
        }
    }
}

pub fn load_for_worker<D, P: AsRef<Path>>(
    index: usize,
    peers: usize,
    path: P,
) -> Vec<(ElementId, D)>
where
    for<'de> D: Deserialize<'de> + ReadBinaryFile + Sync + Send + Clone + 'static,
{
    let mut data = Vec::new();
    ReadBinaryFile::read_binary(
        path.as_ref().into(),
        |file_id| file_id % peers == index,
        |c, v| data.push((ElementId(c as u32), v)),
    );
    data
}
