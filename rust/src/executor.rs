//! Single-threaded executor: ready queue, timer heap, and thread parking.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};
use std::time::Instant;

use crate::timer;

type LocalTask = Pin<Box<dyn Future<Output = ()>>>;

/// Shared between the thread and every waker (wakers may be used on other threads).
struct Shared {
    ready: Mutex<VecDeque<usize>>,
    thread: Thread,
}

thread_local! {
    static SHARED: Arc<Shared> = Arc::new(Shared { ready: Mutex::new(VecDeque::new()), thread: thread::current() });
    static TASKS: RefCell<HashMap<usize, (LocalTask, Arc<TaskWaker>)>> = RefCell::new(HashMap::new());
    static NEXT_ID: Cell<usize> = const { Cell::new(0) };
    static RUNNING: Cell<bool> = const { Cell::new(false) };
}

struct TaskWaker {
    id: usize,
    queued: AtomicBool,
    shared: Arc<Shared>,
}

impl Wake for TaskWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        if !self.queued.swap(true, Ordering::AcqRel) {
            self.shared
                .ready
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push_back(self.id);
            self.shared.thread.unpark();
        }
    }
}

struct MainWaker {
    woken: AtomicBool,
    thread: Thread,
}

impl Wake for MainWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.woken.store(true, Ordering::Release);
        self.thread.unpark();
    }
}

struct RunningGuard;

impl Drop for RunningGuard {
    fn drop(&mut self) {
        RUNNING.with(|r| r.set(false));
    }
}

/// Runs `future` to completion on the current thread.
///
/// While the future is pending, tasks created with [`spawn`] run and timers
/// fire. When nothing is ready the thread parks until the next timer deadline
/// or until a waker is called (also from another thread).
///
/// # Panics
///
/// Panics when called from inside another `block_on` on the same thread. A
/// panic inside a spawned task propagates out of `block_on`.
///
/// # Examples
/// ```
/// let v = lombokasync::block_on(async { 1 + 1 });
/// assert_eq!(v, 2);
/// ```
pub fn block_on<F: Future>(future: F) -> F::Output {
    assert!(
        !RUNNING.with(|r| r.replace(true)),
        "lombokasync::block_on cannot be nested"
    );
    let _guard = RunningGuard;
    let mut future = Box::pin(future);
    let main = Arc::new(MainWaker {
        woken: AtomicBool::new(true),
        thread: thread::current(),
    });
    let main_waker = Waker::from(main.clone());
    let shared = SHARED.with(Arc::clone);
    loop {
        if main.woken.swap(false, Ordering::AcqRel) {
            if let Poll::Ready(v) = future.as_mut().poll(&mut Context::from_waker(&main_waker)) {
                return v;
            }
        }
        // Run the tasks that are ready now; tasks woken while running go to the next round.
        let batch: Vec<usize> = shared
            .ready
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain(..)
            .collect();
        for id in batch {
            run_task(id);
        }
        let next_deadline = timer::fire_due(Instant::now());
        if main.woken.load(Ordering::Acquire)
            || !shared
                .ready
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .is_empty()
        {
            continue;
        }
        match next_deadline {
            Some(deadline) => {
                let now = Instant::now();
                if deadline > now {
                    thread::park_timeout(deadline - now);
                }
            }
            None => thread::park(),
        }
    }
}

fn run_task(id: usize) {
    let Some((mut task, waker)) = TASKS.with(|t| t.borrow_mut().remove(&id)) else {
        return;
    };
    waker.queued.store(false, Ordering::Release);
    let w = Waker::from(waker.clone());
    if task
        .as_mut()
        .poll(&mut Context::from_waker(&w))
        .is_pending()
    {
        TASKS.with(|t| t.borrow_mut().insert(id, (task, waker)));
    }
}

struct JoinState<T> {
    result: Option<T>,
    waker: Option<Waker>,
}

/// Handle to a task created with [`spawn`]. Await it to get the task's output.
///
/// Dropping the handle does not cancel the task.
pub struct JoinHandle<T> {
    state: Rc<RefCell<JoinState<T>>>,
}

impl<T> JoinHandle<T> {
    /// Returns true when the task has finished and its output is waiting.
    pub fn is_finished(&self) -> bool {
        self.state.borrow().result.is_some()
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        let mut st = self.state.borrow_mut();
        match st.result.take() {
            Some(v) => Poll::Ready(v),
            None => {
                st.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

/// Creates a task on the current thread's executor and returns its handle.
///
/// The task starts running at the next scheduling step of [`block_on`]. Tasks
/// created outside `block_on` wait until the thread next calls it.
///
/// # Examples
/// ```
/// use lombokasync::{block_on, spawn};
///
/// let v = block_on(async {
///     let a = spawn(async { 20 });
///     let b = spawn(async { 22 });
///     a.await + b.await
/// });
/// assert_eq!(v, 42);
/// ```
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + 'static,
    F::Output: 'static,
{
    let state = Rc::new(RefCell::new(JoinState {
        result: None,
        waker: None,
    }));
    let st = state.clone();
    let task: LocalTask = Box::pin(async move {
        let v = future.await;
        let waker = {
            let mut s = st.borrow_mut();
            s.result = Some(v);
            s.waker.take()
        };
        if let Some(w) = waker {
            w.wake();
        }
    });
    let id = NEXT_ID.with(|n| {
        let id = n.get();
        n.set(id.wrapping_add(1));
        id
    });
    let shared = SHARED.with(Arc::clone);
    let waker = Arc::new(TaskWaker {
        id,
        queued: AtomicBool::new(false),
        shared,
    });
    TASKS.with(|t| t.borrow_mut().insert(id, (task, waker.clone())));
    waker.wake_by_ref();
    JoinHandle { state }
}

/// Future returned by [`yield_now`].
#[must_use = "futures do nothing unless awaited"]
pub struct YieldNow {
    yielded: bool,
}

impl Future for YieldNow {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.yielded {
            return Poll::Ready(());
        }
        self.yielded = true;
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

/// Gives other ready tasks a chance to run before continuing.
pub fn yield_now() -> YieldNow {
    YieldNow { yielded: false }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_on_ready() {
        assert_eq!(block_on(async { 42 }), 42);
    }

    #[test]
    fn spawn_and_await() {
        assert_eq!(block_on(async { spawn(async { 10 + 20 }).await }), 30);
    }

    #[test]
    fn spawned_tasks_interleave() {
        let log = Rc::new(RefCell::new(Vec::new()));
        let (a, b) = (log.clone(), log.clone());
        block_on(async move {
            let h1 = spawn(async move {
                for i in 0..3 {
                    a.borrow_mut().push(("a", i));
                    yield_now().await;
                }
            });
            let h2 = spawn(async move {
                for i in 0..3 {
                    b.borrow_mut().push(("b", i));
                    yield_now().await;
                }
            });
            h1.await;
            h2.await;
        });
        assert_eq!(
            *log.borrow(),
            vec![("a", 0), ("b", 0), ("a", 1), ("b", 1), ("a", 2), ("b", 2)]
        );
    }

    #[test]
    fn nested_spawn() {
        let v = block_on(async { spawn(async { spawn(async { 7 }).await * 6 }).await });
        assert_eq!(v, 42);
    }

    #[test]
    fn is_finished() {
        block_on(async {
            let h = spawn(async { 1 });
            assert!(!h.is_finished());
            yield_now().await;
            yield_now().await;
            assert!(h.is_finished());
            assert_eq!(h.await, 1);
        });
    }

    #[test]
    fn wake_from_other_thread() {
        let (tx, rx) = crate::oneshot::channel::<u32>();
        let t = thread::spawn(move || {
            thread::sleep(std::time::Duration::from_millis(20));
            tx.send(5).unwrap();
        });
        assert_eq!(block_on(rx), Ok(5));
        t.join().unwrap();
    }

    #[test]
    #[should_panic(expected = "cannot be nested")]
    fn nested_block_on_panics() {
        block_on(async { block_on(async {}) });
    }

    #[test]
    fn running_flag_reset_after_panic() {
        let r = std::panic::catch_unwind(|| block_on(async { panic!("x") }));
        assert!(r.is_err());
        assert_eq!(block_on(async { 1 }), 1);
    }
}
