#[macro_use]
extern crate log;
extern crate env_logger;
extern crate log_panics;
#[macro_use]
extern crate serde_derive;
extern crate envy;
extern crate serde;
#[macro_use]
extern crate clap;
#[macro_use]
extern crate abomonation;
#[macro_use]
extern crate abomonation_derive;
extern crate chrono;
extern crate core;
extern crate rand;
extern crate rand_xorshift;
extern crate serde_json;
extern crate siphasher;
extern crate smallbitvec;
extern crate timely;

#[macro_use]
pub mod experiment;
#[macro_use]
pub mod logging;
pub mod baseline;
pub mod bloom;
pub mod config;
pub mod dataset;
pub mod io;
pub mod lsh;
pub mod measure;
pub mod operators;
pub mod sketch;
/// This module collects algorithms to compute on some datasets,
/// which might be useful to understand their behaviour
pub mod stats;
pub mod types;
pub mod version {
    include!(concat!(env!("OUT_DIR"), "/version.rs"));
}

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;
