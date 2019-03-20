use crate::config::*;
use crate::experiment::Experiment;
use env_logger::Builder;
use serde_json::Value;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Write};
use std::process;
use std::sync::atomic::{AtomicUsize, Ordering};
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
pub fn init_logging(_conf: &Config) -> () {
    let hostname = get_hostname();
    Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .format(move |buf, record| {
            writeln!(
                buf,
                "[{}, {:?}] {}: {}",
                hostname,
                std::thread::current().id(),
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
            .map(|m| m.to_space_string())
            .unwrap_or("--".to_owned());
    };
}

pub fn init_event_logging<A>(worker: &Worker<A>) -> Arc<ExecutionSummary>
where
    A: timely::communication::Allocate,
{
    let summary = Arc::new(ExecutionSummary::new());
    let summary_thread = summary.clone();
    worker
        .log_register()
        .insert::<LogEvent, _>("danny", move |_time, data| {
            for event in data.drain(..) {
                summary_thread.add(event.2);
            }
        });
    summary
}

pub fn collect_execution_summaries<A>(
    execution_summary: Arc<ExecutionSummary>,
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
    let execution_summary = execution_summary.clone().freeze();
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

#[derive(Debug, Clone, Abomonation)]
pub enum LogEvent {
    SketchDiscarded(usize),
    DistinctPairs(usize),
    DuplicatesDiscarded(usize),
    GeneratedPairs(usize),
    ReceivedHashes(usize),
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

#[derive(Debug)]
pub struct ExecutionSummary {
    sketch_discarded: AtomicUsize,
    distinct_pairs: AtomicUsize,
    duplicates_discarded: AtomicUsize,
    generated_pairs: AtomicUsize,
    received_hashes: AtomicUsize,
}

impl ExecutionSummary {
    pub fn new() -> Self {
        ExecutionSummary {
            sketch_discarded: 0.into(),
            distinct_pairs: 0.into(),
            duplicates_discarded: 0.into(),
            generated_pairs: 0.into(),
            received_hashes: 0.into(),
        }
    }

    pub fn freeze(&self) -> FrozenExecutionSummary {
        FrozenExecutionSummary {
            sketch_discarded: self.sketch_discarded.load(Ordering::SeqCst),
            distinct_pairs: self.distinct_pairs.load(Ordering::SeqCst),
            duplicates_discarded: self.duplicates_discarded.load(Ordering::SeqCst),
            generated_pairs: self.generated_pairs.load(Ordering::SeqCst),
            received_hashes: self.received_hashes.load(Ordering::SeqCst),
        }
    }

    pub fn add(&self, event: LogEvent) {
        match event {
            LogEvent::SketchDiscarded(count) => {
                self.sketch_discarded.fetch_add(count, Ordering::SeqCst);
            }
            LogEvent::DistinctPairs(count) => {
                self.distinct_pairs.fetch_add(count, Ordering::SeqCst);
            }
            LogEvent::DuplicatesDiscarded(count) => {
                self.duplicates_discarded.fetch_add(count, Ordering::SeqCst);
            }
            LogEvent::GeneratedPairs(count) => {
                self.generated_pairs.fetch_add(count, Ordering::SeqCst);
            }
            LogEvent::ReceivedHashes(count) => {
                self.received_hashes.fetch_add(count, Ordering::SeqCst);
            }
        }
    }
}

#[derive(Debug, Abomonation, Clone)]
pub struct FrozenExecutionSummary {
    pub sketch_discarded: usize,
    pub distinct_pairs: usize,
    pub duplicates_discarded: usize,
    pub generated_pairs: usize,
    pub received_hashes: usize,
}

impl FrozenExecutionSummary {
    pub fn zero() -> Self {
        FrozenExecutionSummary {
            sketch_discarded: 0,
            distinct_pairs: 0,
            duplicates_discarded: 0,
            generated_pairs: 0,
            received_hashes: 0,
        }
    }
    pub fn sum(&self, other: &Self) -> Self {
        FrozenExecutionSummary {
            sketch_discarded: self.sketch_discarded + other.sketch_discarded,
            distinct_pairs: self.distinct_pairs + other.distinct_pairs,
            duplicates_discarded: self.duplicates_discarded + other.duplicates_discarded,
            generated_pairs: self.generated_pairs + other.generated_pairs,
            received_hashes: self.received_hashes + other.received_hashes,
        }
    }

    pub fn add_to_experiment(&self, table: &str, experiment: &mut Experiment) {
        experiment.append(
            table,
            row!(
                "sketch_discarded" => self.sketch_discarded,
                "distinct_pairs" => self.distinct_pairs,
                "duplicates_discarded" => self.duplicates_discarded,
                "generated_pairs" => self.generated_pairs,
                "received_hashes" => self.received_hashes
            ),
        )
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
            let elapsed = elapsed.as_secs() as f64 + elapsed.subsec_millis() as f64 / 1000.0;
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
        let elapsed = elapsed.as_secs() as f64 + elapsed.subsec_millis() as f64 / 1000.0;
        let throughput = self.count as f64 / elapsed;
        info!(
            "Completed {:?} :: {} {} :: {} {}/sec",
            elapsed, self.count, self.items, throughput, self.items
        );
    }
}
