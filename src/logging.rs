use crate::config::*;
use crate::experiment::Experiment;
use env_logger::Builder;
use serde_json::Value;
use std::collections::HashMap;
use std::fs::File;
use std::hash::Hash;
use std::io::{Read, Write};
use std::ops::Drop;
use std::ops::{Add, AddAssign};
use std::process;

use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use timely::dataflow::operators::capture::event::Event as TimelyEvent;
use timely::dataflow::operators::*;
use timely::dataflow::ProbeHandle;
use timely::logging::Logger;
use timely::worker::{AsWorker, Worker};

// This is the place to hook into if we want to use syslog
pub fn init_logging(_conf: &Config) {
    let hostname = get_hostname();
    let start = Instant::now();
    Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .format(move |buf, record| {
            writeln!(
                buf,
                "[{}, {:?}] {:.2?} - {}: {}",
                hostname,
                std::thread::current().id(),
                Instant::now() - start,
                record.level(),
                record.args()
            )
        })
        .init();
    log_panics::init();
}

pub trait ToSpaceString {
    fn to_space_string(self) -> String;
}

impl ToSpaceString for usize {
    fn to_space_string(self) -> String {
        let bytes = self;
        if bytes >= 1024 * 1024 * 1024 {
            format!("{:.2} Gb", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
        } else if bytes >= 1024 * 1024 {
            format!("{:.2} Mb", bytes as f64 / (1024.0 * 1024.0))
        } else if bytes >= 1024 {
            format!("{:.2} Kb", bytes as f64 / 1024.0)
        } else {
            format!("{} bytes", bytes)
        }
    }
}

/// Gets the total process memory from /proc/<pid>/statm
/// Returns None if there are problems parsing this file, or on platforms where this file does not
/// exist.
/// Assumes the usual page size of 4Kb
pub fn get_total_process_memory() -> Option<usize> {
    File::open(format!("/proc/{}/statm", process::id()))
        .and_then(|mut statm| {
            let mut buf = String::new();
            statm.read_to_string(&mut buf)?;
            let mem = buf
                .split_whitespace()
                .next()
                .expect("Missing first token")
                .parse::<usize>()
                .expect("Cannot parse first token as usize");
            Ok(mem * 4096)
        })
        .ok()
}

#[macro_export]
macro_rules! proc_mem {
    () => {
        get_total_process_memory()
            .map(ToSpaceString::to_space_string)
            .unwrap_or("--".to_owned());
    };
}

pub fn init_event_logging<A>(worker: &Worker<A>) -> Arc<Mutex<ExecutionSummary>>
where
    A: timely::communication::Allocate,
{
    let summary = Arc::new(Mutex::new(ExecutionSummary::default()));
    let summary_thread = Arc::clone(&summary);
    worker
        .log_register()
        .insert::<LogEvent, _>("danny", move |_time, data| {
            let mut summary = summary_thread.lock().unwrap();
            for event in data.drain(..) {
                summary.add(event.2);
            }
        });
    summary
}

pub fn collect_execution_summaries<A>(
    execution_summary: Arc<Mutex<ExecutionSummary>>,
    send: Arc<Mutex<Sender<TimelyEvent<u32, FrozenExecutionSummary>>>>,
    worker: &mut Worker<A>,
) where
    A: timely::communication::Allocate,
{
    let (mut input, probe) = worker.dataflow::<u32, _, _>(move |scope| {
        let send = send.lock().unwrap().clone();
        let (input, stream) = scope.new_input::<FrozenExecutionSummary>();
        let mut probe = ProbeHandle::new();
        stream
            .exchange(|_| 0)
            .probe_with(&mut probe)
            .capture_into(send);

        (input, probe)
    });
    let execution_summary = execution_summary.lock().unwrap().freeze();
    input.send(execution_summary);
    input.advance_to(1);
    worker.step_while(|| probe.less_than(&1));
}

#[macro_export]
macro_rules! log_event {
    ( $logger:expr, $event:expr ) => {
        $logger.clone().map(|l| l.log($event));
    };
}

/// To be used to map from a timestamp to a step of the algorithm
pub trait ToStepId {
    fn to_step_id(&self) -> usize;
}

impl ToStepId for u32 {
    fn to_step_id(&self) -> usize {
        *self as usize
    }
}

impl<O, I> ToStepId for timely::order::Product<O, I>
where
    O: ToStepId,
{
    fn to_step_id(&self) -> usize {
        self.outer.to_step_id()
    }
}

pub struct ProfileGuard {
    logger: Logger<LogEvent>,
    step: usize,
    depth: u8,
    name: String,
    start: Instant,
}

impl ProfileGuard {
    pub fn new(logger: Option<Logger<LogEvent>>, step: usize, depth: u8, name: &str) -> Self {
        Self {
            logger: logger.unwrap(),
            step,
            depth,
            name: name.to_owned(),
            start: Instant::now(),
        }
    }
}

impl Drop for ProfileGuard {
    fn drop(&mut self) {
        let end = Instant::now();
        self.logger.log(LogEvent::Profile(
            self.step,
            self.depth,
            self.name.clone(),
            end - self.start,
        ));
    }
}

#[derive(Debug, Clone, Abomonation)]
pub enum LogEvent {
    SketchDiscarded(usize, usize),
    DistinctPairs(usize, usize),
    DuplicatesDiscarded(usize, usize),
    GeneratedPairs(usize, usize),
    /// The number of received hashes during bucketing. This is a proxy for the load measure
    ReceivedHashes(usize, usize),
    /// Hash values generated for best level by the adaptive algorithm at level, across all repetitions
    AdaptiveBestGenerated(usize, usize),
    /// Hash values generated for current levels by the adaptive algorithm at level, across all repetitions
    AdaptiveCurrentGenerated(usize, usize),
    /// Profiling event, with (step, depth, name, duration)
    Profile(usize, u8, String, Duration),
}

pub trait AsDannyLogger {
    fn danny_logger(&self) -> Option<Logger<LogEvent>>;
}

impl<T> AsDannyLogger for T
where
    T: AsWorker,
{
    fn danny_logger(&self) -> Option<Logger<LogEvent>> {
        self.log_register().get("danny")
    }
}

#[derive(Debug, Default)]
pub struct ExecutionSummary {
    sketch_discarded: HashMap<usize, usize>,
    distinct_pairs: HashMap<usize, usize>,
    duplicates_discarded: HashMap<usize, usize>,
    generated_pairs: HashMap<usize, usize>,
    received_hashes: HashMap<usize, usize>,
    adaptive_best: HashMap<usize, usize>,
    adaptive_current: HashMap<usize, usize>,
    profile: HashMap<(usize, u8, String), Duration>,
}

impl ExecutionSummary {
    fn map_to_vec<K: Clone + Eq + Hash, V: Clone>(m: &HashMap<K, V>) -> Vec<(K, V)> {
        m.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }

    pub fn freeze(&self) -> FrozenExecutionSummary {
        FrozenExecutionSummary {
            sketch_discarded: Self::map_to_vec(&self.sketch_discarded),
            distinct_pairs: Self::map_to_vec(&self.distinct_pairs),
            duplicates_discarded: Self::map_to_vec(&self.duplicates_discarded),
            generated_pairs: Self::map_to_vec(&self.generated_pairs),
            received_hashes: Self::map_to_vec(&self.received_hashes),
            adaptive_best: Self::map_to_vec(&self.adaptive_best),
            adaptive_current: Self::map_to_vec(&self.adaptive_current),
            profile: Self::map_to_vec(&self.profile),
        }
    }

    pub fn add(&mut self, event: LogEvent) {
        match event {
            LogEvent::SketchDiscarded(step, count) => {
                *self.sketch_discarded.entry(step).or_insert(0usize) += count;
            }
            LogEvent::DistinctPairs(step, count) => {
                *self.distinct_pairs.entry(step).or_insert(0usize) += count;
            }
            LogEvent::DuplicatesDiscarded(step, count) => {
                *self.duplicates_discarded.entry(step).or_insert(0usize) += count;
            }
            LogEvent::GeneratedPairs(step, count) => {
                *self.generated_pairs.entry(step).or_insert(0usize) += count;
            }
            LogEvent::ReceivedHashes(step, count) => {
                *self.received_hashes.entry(step).or_insert(0usize) += count;
            }
            LogEvent::AdaptiveBestGenerated(level, count) => {
                *self.adaptive_best.entry(level).or_insert(0usize) += count;
            }
            LogEvent::AdaptiveCurrentGenerated(level, count) => {
                *self.adaptive_current.entry(level).or_insert(0usize) += count;
            }
            LogEvent::Profile(step, depth, name, duration) => {
                *self
                    .profile
                    .entry((step, depth, name))
                    .or_insert_with(Duration::default) += duration;
            }
        }
    }
}

#[derive(Debug, Abomonation, Clone, Default)]
pub struct FrozenExecutionSummary {
    pub sketch_discarded: Vec<(usize, usize)>,
    pub distinct_pairs: Vec<(usize, usize)>,
    pub duplicates_discarded: Vec<(usize, usize)>,
    pub generated_pairs: Vec<(usize, usize)>,
    pub received_hashes: Vec<(usize, usize)>,
    pub adaptive_best: Vec<(usize, usize)>,
    pub adaptive_current: Vec<(usize, usize)>,
    pub profile: Vec<((usize, u8, String), Duration)>,
}

/// Abstracts boilerplate code in dumping tables to the experiment
macro_rules! append_step_counter {
    ( $self: ident, $experiment: ident, $step:expr, $name:ident ) => {
        $self.$name.iter().find(|(s, _)| s == $step).map(|(_, count)| {
            $experiment.append(
                "step_counters",
                row!("step" => *$step, "kind" => stringify!($name), "count" => *count),
            );
        });
    };
}

impl FrozenExecutionSummary {
    fn sum_vecs<K, V>(a: &[(K, V)], b: &[(K, V)]) -> Vec<(K, V)>
    where
        K: Ord + Clone + Hash + Eq,
        V: Add + AddAssign + Clone + Default,
    {
        let mut data: HashMap<K, V> = a.iter().cloned().collect();
        for (k, v) in b.iter() {
            *data.entry(k.clone()).or_insert_with(V::default) += v.clone();
        }
        data.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }

    pub fn sum(&self, other: &Self) -> Self {
        let mut sketch_discarded = Self::sum_vecs(&self.sketch_discarded, &other.sketch_discarded);
        let mut distinct_pairs = Self::sum_vecs(&self.distinct_pairs, &other.distinct_pairs);
        let mut duplicates_discarded =
            Self::sum_vecs(&self.duplicates_discarded, &other.duplicates_discarded);
        let mut generated_pairs = Self::sum_vecs(&self.generated_pairs, &other.generated_pairs);
        let mut received_hashes = Self::sum_vecs(&self.received_hashes, &other.received_hashes);
        let mut adaptive_best = Self::sum_vecs(&self.adaptive_best, &other.adaptive_best);
        let mut adaptive_current = Self::sum_vecs(&self.adaptive_current, &other.adaptive_current);
        let mut profile = Self::sum_vecs(&self.profile, &other.profile);
        sketch_discarded.sort();
        distinct_pairs.sort();
        duplicates_discarded.sort();
        generated_pairs.sort();
        received_hashes.sort();
        adaptive_best.sort();
        adaptive_current.sort();
        profile.sort();
        FrozenExecutionSummary {
            sketch_discarded,
            distinct_pairs,
            duplicates_discarded,
            generated_pairs,
            received_hashes,
            adaptive_best,
            adaptive_current,
            profile,
        }
    }

    pub fn add_to_experiment(&self, experiment: &mut Experiment) {
        // experiment.append(
        //     "aggregated_counters",
        //     row!(
        //         "sketch_discarded" => self.sketch_discarded,
        //         "distinct_pairs" => self.distinct_pairs,
        //         "duplicates_discarded" => self.duplicates_discarded,
        //         "generated_pairs" => self.generated_pairs
        //     ),
        // );
        for (step, _rec_hashes) in self.received_hashes.iter() {
            // experiment.append(
            //     "step_counters",
            //     row!("step" => *step, "kind" => "received_hashes", "count" => *rec_hashes),
            // );
            append_step_counter!(self, experiment, step, received_hashes);
            append_step_counter!(self, experiment, step, sketch_discarded);
            append_step_counter!(self, experiment, step, distinct_pairs);
            append_step_counter!(self, experiment, step, duplicates_discarded);
            append_step_counter!(self, experiment, step, generated_pairs);
        }
        for (level, best_c) in self.adaptive_best.iter() {
            let current_c = self
                .adaptive_current
                .iter()
                .find(|p| p.0 == *level)
                .expect("Level not found during experiment dumping")
                .1;
            experiment.append(
                "adaptive_counters",
                row!("level" => *level, "kind" => "best", "count" => *best_c),
            );
            experiment.append(
                "adaptive_counters",
                row!("level" => *level, "kind" => "current", "count" => current_c),
            );
        }
        for ((step, depth, name), duration) in self.profile.iter() {
            experiment.append(
                "profile",
                row!(
                    "step" => *step,
                    "depth" => *depth,
                    "name" => name.clone(),
                    "duration" => duration.as_millis() as u64
                ),
            );
        }
    }
}

pub struct ProgressLogger {
    start: Instant,
    last: Instant,
    interval: Duration,
    count: u64,
    items: String,
    expected: Option<u64>,
}

fn f64_to_strtime(seconds: f64) -> String {
    if seconds >= 60.0 * 60.0 {
        format!("{:.2} hours", seconds / (60.0 * 60.0))
    } else if seconds >= 60.0 {
        format!("{:.2} minutes", seconds / 60.0)
    } else {
        format!("{:.2} seconds", seconds)
    }
}

impl ProgressLogger {
    pub fn new(interval: Duration, items: String, expected: Option<u64>) -> Self {
        ProgressLogger {
            start: Instant::now(),
            last: Instant::now(),
            interval,
            count: 0,
            items,
            expected,
        }
    }

    pub fn add(&mut self, cnt: u64) {
        self.count += cnt;
        let now = Instant::now();
        if now - self.last > self.interval {
            let elapsed = now - self.start;
            let elapsed = elapsed.as_secs() as f64 + f64::from(elapsed.subsec_millis()) / 1000.0;
            let throughput = self.count as f64 / elapsed;
            match self.expected {
                Some(expected) => {
                    let estimated = (expected - self.count) as f64 / throughput;
                    info!(
                        "{:?} :: {} {} :: {} {}/sec :: estimated {} ({})",
                        elapsed,
                        self.count,
                        self.items,
                        throughput,
                        self.items,
                        f64_to_strtime(estimated),
                        proc_mem!()
                    )
                }
                None => info!(
                    "{:?} :: {} {} :: {} {}/sec ({})",
                    elapsed,
                    self.count,
                    self.items,
                    throughput,
                    self.items,
                    proc_mem!()
                ),
            }
            self.last = now;
        }
    }

    pub fn done(self) {
        let now = Instant::now();
        let elapsed = now - self.start;
        let elapsed = elapsed.as_secs() as f64 + f64::from(elapsed.subsec_millis()) / 1000.0;
        let throughput = self.count as f64 / elapsed;
        info!(
            "Completed {:?} :: {} {} :: {} {}/sec",
            elapsed, self.count, self.items, throughput, self.items
        );
    }
}
