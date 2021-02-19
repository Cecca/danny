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

use std::process;
use std::rc::Rc;

use std::time::Instant;
use timely::communication::Allocator;

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

        input.close();
        worker.step_while(|| !probe.done());

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
    Rc<RefCell<Option<InputHandle<(), ((LogEvent, usize), usize)>>>>,
    Rc<RefCell<HashMap<(LogEvent, usize), usize>>>,
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
        let (input, stream) = scope.new_input::<((LogEvent, usize), usize)>();
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
                    .map(|input| input.send(((key, worker_id), value)));
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
    CandidatePairs(usize),
    SelfPairsDiscarded(usize),
    SketchDiscarded(usize),
    DuplicatesDiscarded(usize),
    SimilarityDiscarded(usize),
    OutputPairs(usize),
}

impl LogEvent {
    pub fn kind(&self) -> String {
        use LogEvent::*;
        match self {
            Load(_) => String::from("Load"),
            CandidatePairs(_) => String::from("CandidatePairs"),
            SelfPairsDiscarded(_) => String::from("SelfPairsDiscarded"),
            SketchDiscarded(_) => String::from("SketchDiscarded"),
            SimilarityDiscarded(_) => String::from("SimilarityDiscarded"),
            DuplicatesDiscarded(_) => String::from("DuplicatesDiscarded"),
            OutputPairs(_) => String::from("OutputPairs"),
        }
    }

    pub fn step(&self) -> u32 {
        use LogEvent::*;
        match self {
            Load(step) => *step as u32,
            CandidatePairs(step) => *step as u32,
            SelfPairsDiscarded(step) => *step as u32,
            SketchDiscarded(step) => *step as u32,
            SimilarityDiscarded(step) => *step as u32,
            DuplicatesDiscarded(step) => *step as u32,
            OutputPairs(step) => *step as u32,
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

#[derive(Abomonation, Clone, Debug)]
pub struct ProfileFunction {
    pub hostname: String,
    pub name: String,
    pub thread: String,
    pub count: i32,
}

pub fn collect_profiling_info<'a>(
    worker: &mut Worker<Allocator>,
    guard: Option<pprof::ProfilerGuard<'a>>,
    // a barrier to sync all the threads local to a host
    barrier: std::sync::Arc<std::sync::Barrier>
) -> Vec<ProfileFunction> {
    use timely::dataflow::channels::pact::Pipeline;

    // resolve the profile and drop it as soon as possible, 
    // otherwise we are going to capture the setup of the exchange dataflow.
    // While unintuitive, this can skew the results a lot.
    // The other threads wait on a barrier
    let report = guard.as_ref().map(|g| {
        info!("Building the profile");
        let report = g.report().build().unwrap();
        info!("done building the profile");
        report
    });
    drop(guard); // this stops the profiling
    barrier.wait();

    let hostname = get_hostname();

    let result = Rc::new(RefCell::new(Vec::new()));
    let result_read = Rc::clone(&result);

    let (mut input, probe) = worker.dataflow::<(), _, _>(move |scope| {
        let (input, stream) = scope.new_input::<ProfileFunction>();
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

    if let Some(report) = report {
        use pprof::protos::Message;
        let mut file = File::create("profile.pb").unwrap();
        let profile = report.pprof().unwrap();

        let mut content = Vec::new();
        profile.encode(&mut content).unwrap();
        file.write_all(&content).unwrap();


        let mut counts_table = HashMap::new();
        for (frames, frame_count) in report.data.iter() {
            let frame_count = *frame_count as i32;
            let frame_str = format!("{:?}", frames);
            counts_table
                .entry(frames.thread_name.clone())
                .or_insert_with(|| HashMap::new())
                .entry(frame_str)
                .and_modify(|c| *c += frame_count)
                .or_insert(frame_count);
        }

        for (thread, values) in counts_table {
            for (name, count) in values {
                input.send(ProfileFunction {
                    hostname: hostname.clone(),
                    thread: thread.clone(),
                    name,
                    count,
                });
            }
        }
    }
    input.close();
    info!("Collecting the profiling information");
    worker.step_while(|| !probe.done());
    info!("Done");

    result_read.replace(Vec::new())
}
