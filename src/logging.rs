use crate::config::*;
use crate::experiment::Experiment;
use env_logger::Builder;
use serde_json::Value;
use std::collections::HashMap;
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::sync::Mutex;
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
            writeln!(buf, "[{}] {}: {}", hostname, record.level(), record.args())
        })
        .init();
    log_panics::init();
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

#[derive(Debug, Clone, Abomonation)]
pub enum LogEvent {
    DistinctPairs(usize),
    DuplicatesDiscarded(usize),
    GeneratedPairs(usize),
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
    distinct_pairs: AtomicUsize,
    duplicates_discarded: AtomicUsize,
    generated_pairs: AtomicUsize,
}

impl ExecutionSummary {
    pub fn new() -> Self {
        ExecutionSummary {
            distinct_pairs: 0.into(),
            duplicates_discarded: 0.into(),
            generated_pairs: 0.into(),
        }
    }

    pub fn freeze(&self) -> FrozenExecutionSummary {
        FrozenExecutionSummary {
            distinct_pairs: self.distinct_pairs.load(Ordering::Acquire),
            duplicates_discarded: self.duplicates_discarded.load(Ordering::Acquire),
            generated_pairs: self.generated_pairs.load(Ordering::Acquire),
        }
    }

    pub fn add(&self, event: LogEvent) {
        match event {
            LogEvent::DistinctPairs(count) => {
                self.distinct_pairs.fetch_add(count, Ordering::Relaxed);
            }
            LogEvent::DuplicatesDiscarded(count) => {
                self.duplicates_discarded
                    .fetch_add(count, Ordering::Relaxed);
            }
            LogEvent::GeneratedPairs(count) => {
                self.generated_pairs.fetch_add(count, Ordering::Relaxed);
            }
        }
    }
}

#[derive(Debug, Abomonation, Clone)]
pub struct FrozenExecutionSummary {
    pub distinct_pairs: usize,
    pub duplicates_discarded: usize,
    pub generated_pairs: usize,
}

impl FrozenExecutionSummary {
    pub fn zero() -> Self {
        FrozenExecutionSummary {
            distinct_pairs: 0,
            duplicates_discarded: 0,
            generated_pairs: 0,
        }
    }
    pub fn sum(&self, other: &Self) -> Self {
        FrozenExecutionSummary {
            distinct_pairs: self.distinct_pairs + other.distinct_pairs,
            duplicates_discarded: self.duplicates_discarded + other.duplicates_discarded,
            generated_pairs: self.generated_pairs + other.generated_pairs,
        }
    }

    pub fn add_to_experiment(&self, table: &str, experiment: &mut Experiment) {
        experiment.append(
            table,
            row!(
                "distinct_pairs" => self.distinct_pairs,
                "generated_pairs" => self.generated_pairs
            ),
        )
    }
}
