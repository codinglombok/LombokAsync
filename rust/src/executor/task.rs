//! Task and JoinHandle types.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::waker::WakerData;

/// An opaque handle to a spawned task. Await it to get the task's result.
pub struct JoinHandle<T> {
    pub(crate) result: Arc<std::sync::Mutex<Option<T>>>,
    pub(crate) task_id: usize,
}

impl<T> Future for JoinHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        let mut guard = self.result.lock().unwrap();
        if let Some(val) = guard.take() {
            Poll::Ready(val)
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

/// Internal task that the executor polls.
pub(crate) struct Task {
    pub future: Pin<Box<dyn Future<Output = ()> + 'static>>,
    pub waker_data: Arc<WakerData>,
    pub completed: bool,
}
