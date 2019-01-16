use crate::config::*;
use chrono::prelude::*;
use serde_json::Value;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;

#[derive(Serialize)]
pub struct Experiment {
    date: String,
    tags: HashMap<String, Value>,
    tables: HashMap<String, Vec<HashMap<String, Value>>>,
}

impl Experiment {
    pub fn new() -> Experiment {
        let date = Utc::now().to_rfc3339();
        let tags = HashMap::new();
        let tables = HashMap::new();
        Experiment { date, tags, tables }
    }

    pub fn from_config(config: &Config, cmdline: &CmdlineConfig) -> Experiment {
        let experiment = Experiment::new()
            .tag("threads_per_worker", config.get_threads())
            .tag("hosts", config.get_hosts().clone())
            .tag("num_hosts", config.get_num_hosts())
            .tag("total_threads", config.get_total_workers())
            .tag("seed", config.get_seed())
            .tag("measure", cmdline.measure.clone())
            .tag("threshold", cmdline.threshold)
            .tag("left_path", cmdline.left_path.clone())
            .tag("right_path", cmdline.right_path.clone())
            .tag("algorithm", cmdline.algorithm.clone());
        let experiment = if cmdline.k.is_some() {
            experiment.tag("k", cmdline.k.unwrap())
        } else {
            experiment
        };
        let experiment = if cmdline.dimension.is_some() {
            experiment.tag("dimension", cmdline.dimension.unwrap())
        } else {
            experiment
        };

        experiment
    }

    pub fn tag<T>(mut self, name: &str, value: T) -> Self
    where
        T: Into<Value>,
    {
        self.tags.insert(name.to_owned(), value.into());
        self
    }

    pub fn append(&mut self, table: &str, row: HashMap<String, Value>) {
        self.tables
            .entry(table.to_owned())
            .or_insert(Vec::new())
            .push(row);
    }

    pub fn save(self) {
        let json_str =
            serde_json::to_string(&self).expect("Error converting the experiment to string");
        println!("Experiment json is {}", json_str);
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open("results.json")
            .expect("Error opening file");
        file.write_all(json_str.as_bytes())
            .expect("Error writing data");
        file.write(b"\n").expect("Error writing final newline");
    }
}

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
