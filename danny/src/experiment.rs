extern crate bzip2;

use crate::config::*;
use crate::version;
use bzip2::read::BzDecoder;
use bzip2::write::BzEncoder;
use bzip2::Compression;
use chrono::prelude::*;
use serde_json::Value;
use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Write;
use std::path::PathBuf;

#[derive(Serialize)]
pub struct Experiment {
    tags: HashMap<String, Value>,
    tables: HashMap<String, Vec<HashMap<String, Value>>>,
}

impl Default for Experiment {
    fn default() -> Experiment {
        Experiment::new()
    }
}

impl Experiment {
    pub fn new() -> Experiment {
        let date = Utc::now().to_rfc3339();
        let mut tags = HashMap::new();
        tags.insert("date".to_owned(), serde_json::Value::String(date));
        let tables = HashMap::new();
        Experiment { tags, tables }
    }

    pub fn from_config(config: &Config, cmdline: &CmdlineConfig) -> Experiment {
        let experiment = Experiment::new()
            .tag("threads_per_worker", config.get_threads())
            .tag("num_hosts", config.get_num_hosts())
            .tag("total_threads", config.get_total_workers())
            .tag("seed", config.get_seed())
            .tag("sketch_epsilon", config.get_sketch_epsilon())
            .tag("required_recall", config.get_recall())
            .tag("no_dedup", config.no_dedup)
            .tag("no_verify", config.no_verify)
            // .tag("measure", cmdline.measure.clone())
            .tag("threshold", cmdline.threshold)
            .tag("left_path", cmdline.left_path.clone())
            .tag("right_path", cmdline.right_path.clone())
            .tag("algorithm", cmdline.algorithm.clone())
            .tag("git_revision", version::short_sha())
            .tag("git_commit_date", version::commit_date());
        let experiment = if cmdline.k.is_some() {
            let k_str = cmdline.k.unwrap().to_string().clone();
            let exp = experiment.tag("k", k_str);
            if cmdline.k2.is_some() {
                let k2_str = cmdline.k2.unwrap().to_string().clone();
                exp.tag("k2", k2_str)
            } else {
                exp
            }
        } else {
            experiment
        };
        if cmdline.sketch_bits.is_some() {
            experiment.tag("sketch_bits", cmdline.get_sketch_bits())
        } else {
            experiment
        }
    }

    pub fn from_env(config: &Config) -> Experiment {
        Experiment::new()
            .tag("threads_per_worker", config.get_threads())
            .tag("hosts", config.get_hosts().clone())
            .tag("num_hosts", config.get_num_hosts())
            .tag("total_threads", config.get_total_workers())
            .tag("seed", config.get_seed())
            .tag("sketch_epsilon", config.get_sketch_epsilon())
            .tag("git_revision", version::short_sha())
            .tag("git_commit_date", version::commit_date())
    }

    pub fn tag<T>(mut self, name: &str, value: T) -> Self
    where
        T: Into<Value>,
    {
        self.tags.insert(name.to_owned(), value.into());
        self
    }

    pub fn add_tag<T>(&mut self, name: &str, value: T)
    where
        T: Into<Value>,
    {
        self.tags.insert(name.to_owned(), value.into());
    }

    pub fn append(&mut self, table: &str, row: HashMap<String, Value>) {
        self.tables
            .entry(table.to_owned())
            .or_insert_with(Vec::new)
            .push(row);
    }

    pub fn save(self) {
        let json_str =
            serde_json::to_string(&self).expect("Error converting the experiment to string");
        info!("Writing result file");
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open("results.json")
            .expect("Error opening file");
        file.write_all(json_str.as_bytes())
            .expect("Error writing data");
        file.write_all(b"\n").expect("Error writing final newline");
        info!("Results file written");
    }

    fn get_header_and_writer<'a, I>(
        table_name: &str,
        header_names: Option<I>,
    ) -> (Vec<String>, BzEncoder<File>)
    where
        I: Iterator<Item = &'a String>,
    {
        // Test if the csv file exists
        let mut path = PathBuf::new();
        path.set_file_name(table_name);
        path.set_extension("csv.bz2");
        if path.exists() {
            // read the header
            let header: Vec<String> = {
                let f = File::open(&path).expect("Error opening csv file");
                let reader = BzDecoder::new(f);
                let reader = BufReader::new(reader);
                reader
                    .lines()
                    .next()
                    .expect("empty file")
                    .expect("error reading first line")
                    .split(",")
                    .map(|s| s.trim().to_owned())
                    .collect()
            };
            let file = OpenOptions::new()
                .append(true)
                .open(path)
                .expect("Error opening file");
            let writer = BzEncoder::new(file, Compression::Best);
            // let writer = BufWriter::new(file);
            (header, writer)
        } else {
            // write the header
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .open(path)
                .expect("Error opening file to write header");
            let tmp: Vec<String> = header_names
                .expect("Header names should be provided!")
                .cloned()
                .collect();
            let header = tmp.join(",");
            let mut writer = BzEncoder::new(file, Compression::Best);
            writeln!(writer, "{}", header).expect("error writing header");
            writer.flush().expect("error flushing file");
            drop(writer);
            Self::get_header_and_writer::<I>(table_name, None)
        }
    }

    pub fn save_csv(self) {
        // serialize tables one at a time
        for (name, table) in self.tables.iter() {
            let (header, mut writer) = Self::get_header_and_writer(
                name,
                Some(self.tags.keys().chain(table.iter().next().unwrap().keys())),
            );
            for row in table {
                let mut names = header.iter();
                let mut opt_col_name = names.next();
                while let Some(col_name) = opt_col_name {
                    let value = row
                        .get(col_name)
                        .or_else(|| self.tags.get(col_name))
                        .unwrap_or_else(|| panic!("Cannot find value for key {}", col_name));
                    let str_value = serde_json::to_string(value).expect("Error converting value");
                    write!(writer, "{}", str_value).expect("error writing value");
                    opt_col_name = names.next();
                    if opt_col_name.is_some() {
                        //write the comma
                        write!(writer, ",").expect("error writing comma");
                    }
                }
                writeln!(writer, "").expect("error writing newline");
            }
            writer.flush().expect("error flushing file");
            drop(writer);
        }
    }
}

#[macro_export]
macro_rules! row(
    { $($key:expr => $value:expr),+ } => {
        {
            let mut m: HashMap<String, Value> = ::std::collections::HashMap::new();
                $(
                    m.insert($key.to_owned(), $value.into());
                )+
            m
        }
    };
);
