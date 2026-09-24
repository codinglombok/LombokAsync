//! The single-threaded runtime — event loop that polls tasks.

use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::task::{JoinHandle, Task};
use super::waker::{WakerData, create_waker};

thread_local! {
    pub(crate) static RUNTIME: RefCell<Runtime> = RefCell::new(Runtime::new());
}

pub(crate) struct Runtime {
    tasks: Vec<Task>,
    next_id: usize,
}

impl Runtime {
    fn new() -> Self {
        Runtime {
            tasks: Vec::new(),
            next_id: 0,
        }
    }

    fn spawn_task(&mut self, future: Pin<Box<dyn Future<Output = ()> + 'static>>) -> usize {
        let id = self.next_id;
        self.next_id += 1;
        let waker_data = WakerData::new(id);
        self.tasks.push(Task {
            future,
            waker_data,
            completed: false,
        });
        id
    }

    fn poll_all(&mut self) -> bool {
        let mut any_pending = false;
        for task in self.tasks.iter_mut() {
            if task.completed {
                continue;
            }
            if !task.waker_data.wake_flag.load(std::sync::atomic::Ordering::Acquire) {
                any_pending = true;
                continue;
            }
            task.waker_data.wake_flag.store(false, std::sync::atomic::Ordering::Release);

            let waker = create_waker(task.waker_data.clone());
            let mut cx = Context::from_waker(&waker);
            match task.future.as_mut().poll(&mut cx) {
                Poll::Ready(()) => {
                    task.completed = true;
                }
                Poll::Pending => {
                    any_pending = true;
                }
            }
        }
        any_pending
    }
}

/// Run a future to completion on the current thread.
///
/// This is the entry point for the async runtime.
///
/// # Examples
/// ```
/// use lombokasync::block_on;
///
/// let result = block_on(async { 1 + 1 });
/// assert_eq!(result, 2);
/// ```
pub fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = Box::pin(future);
    let waker_data = WakerData::new(usize::MAX);
    let waker = create_waker(waker_data.clone());
    let mut cx = Context::from_waker(&waker);

    loop {
        // Poll the main future
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(val) => return val,
            Poll::Pending => {}
        }

        // Poll spawned tasks
        let any_pending = RUNTIME.with(|rt| rt.borrow_mut().poll_all());

        // If nothing is pending and main future is pending, we'd deadlock
        // Give one more round in case wakers fired
        if !any_pending {
            // Try main future one more time
            match future.as_mut().poll(&mut cx) {
                Poll::Ready(val) => return val,
                Poll::Pending => {
                    // Check if spawned tasks might have new work
                    let still_pending = RUNTIME.with(|rt| rt.borrow_mut().poll_all());
                    if !still_pending {
                        // Yield to avoid busy-spin; timers use thread::sleep internally
                        std::thread::sleep(std::time::Duration::from_millis(1));
                    }
                }
            }
        }
    }
}

/// Spawn a future onto the executor. Returns a JoinHandle to await its result.
///
/// # Examples
/// ```
/// use lombokasync::{block_on, spawn};
///
/// let result = block_on(async {
///     let handle = spawn(async { 42 });
///     handle.await
/// });
/// assert_eq!(result, 42);
/// ```
pub fn spawn<F, T>(future: F) -> JoinHandle<T>
where
    F: Future<Output = T> + 'static,
    T: 'static,
{
    let result = Arc::new(std::sync::Mutex::new(None));
    let result_clone = result.clone();

    let wrapper = async move {
        let val = future.await;
        *result_clone.lock().unwrap() = Some(val);
    };

    let task_id = RUNTIME.with(|rt| {
        rt.borrow_mut().spawn_task(Box::pin(wrapper))
    });

    JoinHandle { result, task_id }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_on_immediate() {
        let result = block_on(async { 42 });
        assert_eq!(result, 42);
    }

    #[test]
    fn test_spawn_and_await() {
        let result = block_on(async {
            let h = spawn(async { 10 + 20 });
            h.await
        });
        assert_eq!(result, 30);
    }

    #[test]
    fn test_multiple_spawns() {
        let result = block_on(async {
            let h1 = spawn(async { 1 });
            let h2 = spawn(async { 2 });
            let h3 = spawn(async { 3 });
            h1.await + h2.await + h3.await
        });
        assert_eq!(result, 6);
    }
}
