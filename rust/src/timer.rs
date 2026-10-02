//! Timers: sleep, timeout, interval. Deadlines live in a per-thread heap that
//! [`block_on`](crate::block_on) fires; the thread parks until the earliest one.

use std::cell::{Cell, RefCell};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

struct Entry {
    deadline: Instant,
    seq: u64,
    waker: Waker,
}

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        (self.deadline, self.seq) == (other.deadline, other.seq)
    }
}
impl Eq for Entry {}
impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Entry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.deadline, self.seq).cmp(&(other.deadline, other.seq))
    }
}

thread_local! {
    static HEAP: RefCell<BinaryHeap<Reverse<Entry>>> = RefCell::new(BinaryHeap::new());
    static SEQ: Cell<u64> = const { Cell::new(0) };
}

fn register(deadline: Instant, waker: &Waker) {
    let seq = SEQ.with(|s| {
        let v = s.get();
        s.set(v.wrapping_add(1));
        v
    });
    HEAP.with(|h| {
        h.borrow_mut().push(Reverse(Entry {
            deadline,
            seq,
            waker: waker.clone(),
        }))
    });
}

/// Wakes every timer due at `now`; returns the earliest remaining deadline.
pub(crate) fn fire_due(now: Instant) -> Option<Instant> {
    let mut due = Vec::new();
    let next = HEAP.with(|h| {
        let mut h = h.borrow_mut();
        while let Some(Reverse(e)) = h.peek() {
            if e.deadline > now {
                break;
            }
            if let Some(Reverse(e)) = h.pop() {
                due.push(e.waker);
            }
        }
        h.peek().map(|Reverse(e)| e.deadline)
    });
    for w in due {
        w.wake();
    }
    next
}

/// Future returned by [`sleep`].
#[must_use = "futures do nothing unless awaited"]
#[derive(Debug)]
pub struct Sleep {
    deadline: Instant,
    registered: Option<Waker>,
}

impl Sleep {
    /// The instant at which this sleep completes.
    pub fn deadline(&self) -> Instant {
        self.deadline
    }
}

impl Future for Sleep {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if Instant::now() >= self.deadline {
            return Poll::Ready(());
        }
        let fresh = match &self.registered {
            Some(w) => !w.will_wake(cx.waker()),
            None => true,
        };
        if fresh {
            register(self.deadline, cx.waker());
            self.registered = Some(cx.waker().clone());
        }
        Poll::Pending
    }
}

/// Completes after `duration`.
///
/// # Examples
/// ```
/// use std::time::{Duration, Instant};
/// let start = Instant::now();
/// lombokasync::block_on(lombokasync::sleep(Duration::from_millis(10)));
/// assert!(start.elapsed() >= Duration::from_millis(10));
/// ```
pub fn sleep(duration: Duration) -> Sleep {
    sleep_until(Instant::now() + duration)
}

fn sleep_until(deadline: Instant) -> Sleep {
    Sleep {
        deadline,
        registered: None,
    }
}

/// Error returned by [`timeout`] when the deadline passes first (code `TIMEOUT`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Elapsed;

impl fmt::Display for Elapsed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("TIMEOUT: deadline elapsed")
    }
}

impl std::error::Error for Elapsed {}

/// Future returned by [`timeout`].
#[must_use = "futures do nothing unless awaited"]
pub struct Timeout<F> {
    future: Pin<Box<F>>,
    sleep: Sleep,
}

impl<F: Future> Future for Timeout<F> {
    type Output = Result<F::Output, Elapsed>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Poll::Ready(v) = self.future.as_mut().poll(cx) {
            return Poll::Ready(Ok(v));
        }
        match Pin::new(&mut self.sleep).poll(cx) {
            Poll::Ready(()) => Poll::Ready(Err(Elapsed)),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Runs `future` with a deadline. The future is polled before the deadline is
/// checked, so a value that is ready at the deadline still wins. On timeout
/// the future is dropped.
///
/// # Examples
/// ```
/// use std::time::Duration;
/// use lombokasync::{block_on, sleep, timeout, Elapsed};
///
/// assert_eq!(block_on(timeout(Duration::from_millis(50), async { 1 })), Ok(1));
/// let slow = timeout(Duration::from_millis(5), sleep(Duration::from_secs(5)));
/// assert_eq!(block_on(slow), Err(Elapsed));
/// ```
pub fn timeout<F: Future>(duration: Duration, future: F) -> Timeout<F> {
    Timeout {
        future: Box::pin(future),
        sleep: sleep(duration),
    }
}

/// Periodic timer created by [`interval`].
#[derive(Debug)]
pub struct Interval {
    period: Duration,
    next: Instant,
    count: u64,
}

impl Interval {
    /// Waits for the next tick and returns its index (0, 1, 2, ...). Ticks are
    /// scheduled at fixed multiples of the period, so delays do not accumulate.
    pub async fn tick(&mut self) -> u64 {
        sleep_until(self.next).await;
        self.next += self.period;
        let n = self.count;
        self.count += 1;
        n
    }
}

/// Creates a timer whose first tick is one `period` from now.
///
/// # Panics
///
/// Panics when `period` is zero.
pub fn interval(period: Duration) -> Interval {
    assert!(
        !period.is_zero(),
        "interval period must be greater than zero"
    );
    Interval {
        period,
        next: Instant::now() + period,
        count: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{block_on, spawn};

    #[test]
    fn sleep_waits() {
        let start = Instant::now();
        block_on(sleep(Duration::from_millis(30)));
        assert!(start.elapsed() >= Duration::from_millis(30));
    }

    #[test]
    fn zero_sleep_is_ready() {
        block_on(sleep(Duration::ZERO));
        assert!(sleep(Duration::ZERO).deadline() <= Instant::now());
    }

    #[test]
    fn sleeps_finish_in_deadline_order() {
        let order = std::rc::Rc::new(RefCell::new(Vec::new()));
        block_on({
            let order = order.clone();
            async move {
                let mut hs = Vec::new();
                for ms in [30u64, 10, 20] {
                    let o = order.clone();
                    hs.push(spawn(async move {
                        sleep(Duration::from_millis(ms)).await;
                        o.borrow_mut().push(ms);
                    }));
                }
                for h in hs {
                    h.await;
                }
            }
        });
        assert_eq!(*order.borrow(), vec![10, 20, 30]);
    }

    #[test]
    fn timeout_ok_and_elapsed() {
        assert_eq!(
            block_on(timeout(Duration::from_millis(100), async { 42 })),
            Ok(42)
        );
        assert_eq!(
            block_on(timeout(
                Duration::from_millis(10),
                sleep(Duration::from_secs(10))
            )),
            Err(Elapsed)
        );
        assert_eq!(Elapsed.to_string(), "TIMEOUT: deadline elapsed");
    }

    #[test]
    fn interval_ticks() {
        let start = Instant::now();
        let ticks = block_on(async {
            let mut ivl = interval(Duration::from_millis(15));
            [ivl.tick().await, ivl.tick().await, ivl.tick().await]
        });
        assert_eq!(ticks, [0, 1, 2]);
        assert!(start.elapsed() >= Duration::from_millis(45));
    }

    #[test]
    #[should_panic(expected = "greater than zero")]
    fn interval_zero_panics() {
        let _ = interval(Duration::ZERO);
    }

    #[test]
    fn entry_ordering() {
        struct Nop;
        impl std::task::Wake for Nop {
            fn wake(self: std::sync::Arc<Self>) {}
        }
        let w = Waker::from(std::sync::Arc::new(Nop));
        let now = Instant::now();
        let a = Entry {
            deadline: now,
            seq: 0,
            waker: w.clone(),
        };
        let b = Entry {
            deadline: now,
            seq: 1,
            waker: w,
        };
        assert!(a < b);
        assert!(a != b);
    }
}
