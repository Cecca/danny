use crate::config::*;
use crate::experiment::Experiment;
use env_logger::Builder;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::File;
use std::hash::Hash;
use std::io::BufRead;
use std::io::BufReader;
use std::io::{Read, Write};
use std::ops::Drop;
use std::process;
use std::rc::Rc;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use timely::communication::Allocator;
use timely::dataflow::operators::capture::event::Event as TimelyEvent;
use timely::dataflow::operators::*;
use timely::dataflow::InputHandle;
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

pub struct NetworkInfo {
    pub received: usize,
    pub transmitted: usize,
}

#[derive(Abomonation, Clone)]
pub struct NetworkDiff {
    pub received: usize,
    pub transmitted: usize,
}

impl std::fmt::Display for NetworkDiff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "tx: {} rx: {}",
            self.transmitted.to_space_string(),
            self.received.to_space_string()
        )
    }
}

#[derive(Abomonation, Clone)]
pub struct NetworkSummary {
    pub hostname: String,
    pub interfaces: Vec<(String, NetworkDiff)>,
}

impl std::fmt::Display for NetworkSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: ", self.hostname)?;
        for (iface, ndiff) in self.interfaces.iter() {
            write!(f, "[{} {}] ", iface, ndiff)?;
        }
        Ok(())
    }
}

impl NetworkSummary {
    pub fn report(&self, experiment: &mut Experiment) {
        for (iface, diff) in self.interfaces.iter() {
            experiment.append_network_info(
                self.hostname.clone(),
                iface.clone(),
                diff.transmitted as i64,
                diff.received as i64,
            );
        }
    }

    /// sets up a small dataflow to exchange information about the network exchanges
    pub fn collect_from_workers(
        worker: &mut Worker<Allocator>,
        summary: Option<NetworkSummary>,
    ) -> Vec<NetworkSummary> {
        use std::cell::RefCell;
        use std::rc::Rc;
        use timely::dataflow::channels::pact::Pipeline;

        let result = Rc::new(RefCell::new(Vec::new()));
        let result_read = Rc::clone(&result);

        let (mut input, probe) = worker.dataflow::<u32, _, _>(move |scope| {
            let (input, stream) = scope.new_input::<NetworkSummary>();
            let mut probe = ProbeHandle::new();
            stream
                .exchange(|_| 0)
                .unary(Pipeline, "collector", move |_, _| {
                    move |input, output| {
                        input.for_each(|t, data| {
                            let data = data.replace(Vec::new());
                            result.borrow_mut().extend(data.into_iter());
                            output.session(&t).give(());
                        });
                    }
                })
                .probe_with(&mut probe);

            (input, probe)
        });

        if let Some(machine_summary) = summary {
            input.send(machine_summary);
        }

        input.advance_to(1);
        worker.step_while(|| probe.less_than(&1));

        result_read.replace(Vec::new())
    }
}

pub struct NetworkGauge {
    initial: HashMap<String, NetworkInfo>,
}

impl NetworkGauge {
    fn current() -> Option<HashMap<String, NetworkInfo>> {
        File::open("/proc/net/dev")
            .and_then(|dev| {
                let dev = BufReader::new(dev);
                let mut res = HashMap::new();
                for line in dev.lines().skip(2) {
                    let line = line.expect("Error getting line");
                    let tokens: Vec<&str> = line.split_whitespace().collect();
                    let iface = tokens[0].to_owned();
                    let info = NetworkInfo {
                        received: tokens[1].parse::<usize>().expect("Cannot parse as usize"),
                        transmitted: tokens[9].parse::<usize>().expect("Cannot parse as usize"),
                    };
                    res.insert(iface, info);
                }
                Ok(res)
            })
            .ok()
    }

    pub fn start() -> Option<Self> {
        Self::current().map(|initial| Self { initial })
    }

    pub fn measure(&self) -> NetworkSummary {
        let hostname = get_hostname();
        let current = Self::current().expect("Cannot get network information");
        let mut interfaces = Vec::new();
        for iface in self.initial.keys() {
            interfaces.push((
                iface.clone(),
                NetworkDiff {
                    received: current.get(iface).expect("Interface not present").received
                        - self.initial[iface].received,
                    transmitted: current
                        .get(iface)
                        .expect("Interface not present")
                        .transmitted
                        - self.initial[iface].transmitted,
                },
            ));
        }
        NetworkSummary {
            hostname,
            interfaces,
        }
    }
}

pub fn init_event_logging<A>(
    worker: &mut Worker<A>,
) -> (
    ProbeHandle<()>,
    Rc<RefCell<Option<InputHandle<(), (LogEvent, usize)>>>>,
    Rc<RefCell<HashMap<LogEvent, usize>>>,
)
where
    A: timely::communication::Allocate,
{
    use timely::dataflow::channels::pact::Pipeline;
    use timely::dataflow::operators::aggregation::Aggregate;
    use timely::dataflow::operators::*;

    let events = Rc::new(RefCell::new(HashMap::new()));
    let events_read = Rc::clone(&events);

    let (input, probe) = worker.dataflow::<(), _, _>(move |scope| {
        let (input, stream) = scope.new_input::<(LogEvent, usize)>();
        let mut probe = ProbeHandle::new();

        stream
            .aggregate(
                |_key, val, agg| {
                    *agg += val;
                },
                |key, agg: usize| (key, agg),
                |_key| 0,
            )
            .unary_notify(
                Pipeline,
                "report",
                None,
                move |input, output, notificator| {
                    input.for_each(|t, data| {
                        let data = data.replace(Vec::new());
                        events.borrow_mut().extend(data.into_iter());
                        notificator.notify_at(t.retain());
                    });
                    notificator.for_each(|t, _, _| {
                        output.session(&t).give(());
                    });
                },
            )
            .probe_with(&mut probe);

        (input, probe)
    });

    let input = Rc::new(RefCell::new(Some(input)));
    let input_2 = input.clone();

    worker
        .log_register()
        .insert::<(LogEvent, usize), _>("danny", move |_time, data| {
            for (_time_bound, worker_id, (key, value)) in data.drain(..) {
                input
                    .borrow_mut()
                    .as_mut()
                    .map(|input| input.send((key, value)));
            }
        });

    (probe, input_2, events_read)
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
#[derive(Debug, Clone, Abomonation, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub enum LogEvent {
    Load(usize),
    SketchDiscarded(usize),
    DistinctPairs(usize),
    DuplicatesDiscarded(usize),
    GeneratedPairs(usize),
    /// The number of received hashes during bucketing. This is a proxy for the load measure
    ReceivedHashes(usize),
    // The hashes generated in each iteration
    GeneratedHashes(usize),
}

impl LogEvent {
    pub fn kind(&self) -> String {
        use LogEvent::*;
        match self {
            Load(_) => String::from("Load"),
            SketchDiscarded(_) => String::from("SketchDiscarded"),
            DistinctPairs(_) => String::from("DistinctPairs"),
            DuplicatesDiscarded(_) => String::from("DuplicatesDiscarded"),
            GeneratedPairs(_) => String::from("GeneratedPairs"),
            ReceivedHashes(_) => String::from("ReceivedHashes"),
            GeneratedHashes(_) => String::from("GeneratedHashes"),
        }
    }

    pub fn step(&self) -> u32 {
        use LogEvent::*;
        match self {
            Load(step) => *step as u32,
            SketchDiscarded(step) => *step as u32,
            DistinctPairs(step) => *step as u32,
            DuplicatesDiscarded(step) => *step as u32,
            GeneratedPairs(step) => *step as u32,
            ReceivedHashes(step) => *step as u32,
            GeneratedHashes(step) => *step as u32,
        }
    }
}

pub trait AsDannyLogger {
    fn danny_logger(&self) -> Option<Logger<(LogEvent, usize)>>;
}

impl<T> AsDannyLogger for T
where
    T: AsWorker,
{
    fn danny_logger(&self) -> Option<Logger<(LogEvent, usize)>> {
        self.log_register().get("danny")
    }
}
