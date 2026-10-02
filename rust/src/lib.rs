//! LombokAsync: a small single-threaded async runtime.
//!
//! - [`block_on`] drives a future to completion on the current thread, running
//!   tasks created with [`spawn`] and firing timers while it waits.
//! - [`sleep`], [`timeout`] and [`interval`] use the runtime's timer heap; the
//!   thread parks between events instead of spinning.
//! - [`mpsc`] and [`oneshot`] channels are `Send`, so values can also be sent
//!   from other threads.
//! - [`join`], [`join3`], [`join_all`], [`try_join_all`], [`select`] and
//!   [`select_all`] compose futures.
//!
//! Behaviour that every LombokAsync port shares is specified in
//! `docs/SPEC_LombokAsync_v0.2.0.md` and checked against the shared vectors.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod channel;
mod combinators;
mod executor;
mod timer;

pub use channel::{mpsc, oneshot};
pub use combinators::{
    join, join3, join_all, select, select_all, try_join_all, Either, Join, JoinAll, Select,
    SelectAll, TryJoinAll,
};
pub use executor::{block_on, spawn, yield_now, JoinHandle, YieldNow};
pub use timer::{interval, sleep, timeout, Elapsed, Interval, Sleep, Timeout};

/// Compatibility path for 0.1 code that imported combinators from `sync`.
pub mod sync {
    pub use crate::combinators::{join, join3, join_all, select, Either};
}
