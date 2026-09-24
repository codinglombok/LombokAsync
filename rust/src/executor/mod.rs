//! Single-threaded async executor.
//!
//! Drives futures to completion by polling them when woken.

mod task;
mod runtime;
mod waker;

pub use task::JoinHandle;
pub use runtime::{block_on, spawn};

pub(crate) use runtime::RUNTIME;
