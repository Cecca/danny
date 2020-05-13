#![feature(is_sorted)]
#![feature(fn_traits)]
#![feature(unboxed_closures)]

extern crate danny_base;
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate log_panics;
#[macro_use]
extern crate serde_derive;
extern crate serde;
#[macro_use]
extern crate abomonation_derive;
extern crate chrono;
extern crate core;
extern crate rand;
extern crate rand_xorshift;
extern crate siphasher;
extern crate timely;

#[macro_use]
pub mod experiment;
#[macro_use]
pub mod logging;
pub mod baseline;
pub mod config;
pub mod dataset;
pub mod io;
pub mod join;
pub mod lsh;
pub mod operators;
pub mod version {
    include!(concat!(env!("OUT_DIR"), "/version.rs"));
}

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;
