#![feature(is_sorted)]
#![feature(fn_traits)]
#![feature(unboxed_closures)]
#![feature(range_contains)]

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
#[macro_use]
extern crate lazy_static;
extern crate bitvec;
extern crate chrono;
extern crate core;
extern crate probabilistic_collections;
extern crate rand;
extern crate rand_xorshift;
extern crate serde_json;
extern crate siphasher;
extern crate smallbitvec;
extern crate statrs;
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
pub mod types;
pub mod version {
    include!(concat!(env!("OUT_DIR"), "/version.rs"));
}

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;
