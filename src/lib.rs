//! coop — fire remote jobs down a channel nothing else can take.
//!
//! The library half exists so integration tests can drive exactly the code the
//! binary does, including the `Fake` transport that makes test layer 1 possible
//! without a network or an ssh master.

pub mod cli;
pub mod config;
pub mod errors;
pub mod jobs;
pub mod lock;
pub mod probe;
pub mod run;
pub mod tail;
pub mod transport;
pub mod wrapper;
