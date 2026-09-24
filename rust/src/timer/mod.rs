//! Timer utilities — sleep, timeout, interval.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

/// A future that completes after a given duration.
pub struct Sleep {
    deadline: Instant,
}

impl Future for Sleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if Instant::now() >= self.deadline {
            Poll::Ready(())
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

/// Returns a future that completes after the given duration.
///
/// # Examples
/// ```
/// use lombokasync::{block_on, sleep};
/// use std::time::Duration;
///
/// block_on(async {
///     sleep(Duration::from_millis(10)).await;
/// });
/// ```
pub fn sleep(duration: Duration) -> Sleep {
    Sleep {
        deadline: Instant::now() + duration,
    }
}

/// A future that wraps another future with a deadline.
/// If the inner future doesn't complete before the deadline, returns None.
pub struct Timeout<F> {
    future: Pin<Box<F>>,
    deadline: Instant,
}

impl<F: Future> Future for Timeout<F> {
    type Output = Option<F::Output>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<F::Output>> {
        // Check timeout first
        if Instant::now() >= self.deadline {
            return Poll::Ready(None);
        }

        // Try the inner future
        match self.future.as_mut().poll(cx) {
            Poll::Ready(val) => Poll::Ready(Some(val)),
            Poll::Pending => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

/// Wraps a future with a timeout. Returns `None` if the deadline elapses.
///
/// # Examples
/// ```
/// use lombokasync::{block_on, timeout, sleep};
/// use std::time::Duration;
///
/// let result = block_on(async {
///     timeout(Duration::from_millis(100), async { 42 }).await
/// });
/// assert_eq!(result, Some(42));
/// ```
pub fn timeout<F: Future>(duration: Duration, future: F) -> Timeout<F> {
    Timeout {
        future: Box::pin(future),
        deadline: Instant::now() + duration,
    }
}

/// An async interval timer that yields at regular intervals.
pub struct Interval {
    period: Duration,
    next_tick: Instant,
}

impl Interval {
    /// Wait for the next tick.
    pub async fn tick(&mut self) {
        sleep_until(self.next_tick).await;
        self.next_tick += self.period;
    }
}

/// Create an interval timer that yields every `period`.
pub fn interval(period: Duration) -> Interval {
    Interval {
        period,
        next_tick: Instant::now() + period,
    }
}

/// Sleep until a specific instant.
fn sleep_until(deadline: Instant) -> Sleep {
    Sleep { deadline }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_on;

    #[test]
    fn test_sleep() {
        let start = Instant::now();
        block_on(async {
            sleep(Duration::from_millis(50)).await;
        });
        assert!(start.elapsed() >= Duration::from_millis(40));
    }

    #[test]
    fn test_timeout_ok() {
        let result = block_on(async {
            timeout(Duration::from_millis(100), async { 42 }).await
        });
        assert_eq!(result, Some(42));
    }

    #[test]
    fn test_timeout_expired() {
        let result = block_on(async {
            timeout(Duration::from_millis(10), sleep(Duration::from_millis(200))).await
        });
        assert_eq!(result, None);
    }

    #[test]
    fn test_interval() {
        let start = Instant::now();
        block_on(async {
            let mut ivl = interval(Duration::from_millis(20));
            ivl.tick().await;
            ivl.tick().await;
        });
        assert!(start.elapsed() >= Duration::from_millis(35));
    }
}
