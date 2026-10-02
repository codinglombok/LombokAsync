//! Channels: [`mpsc`] (many senders, one receiver, bounded or unbounded) and
//! [`oneshot`] (a single value). Both are `Send` when `T: Send`.

pub mod mpsc;
pub mod oneshot;

use std::sync::{Mutex, MutexGuard};

pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}
