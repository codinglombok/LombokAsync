//! LombokAsync — lightweight async runtime.
//!
//! Provides a single-threaded executor, timers, channels, and task combinators.
//! Designed for RAG pipeline orchestration: concurrent embedding generation,
//! parallel LLM calls, streaming responses.

pub mod executor;
pub mod timer;
pub mod channel;
pub mod sync;

pub use executor::{block_on, spawn, JoinHandle};
pub use timer::{sleep, timeout, interval, Sleep, Timeout, Interval};
pub use channel::mpsc;
pub use channel::oneshot;
