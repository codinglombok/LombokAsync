//! Multi-producer, single-consumer async channel.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

struct Inner<T> {
    queue: VecDeque<T>,
    closed: bool,
    recv_waker: Option<Waker>,
}

/// Sending half of an mpsc channel.
pub struct Sender<T> {
    inner: Arc<Mutex<Inner<T>>>,
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Sender {
            inner: self.inner.clone(),
        }
    }
}

impl<T> Sender<T> {
    /// Send a value into the channel.
    pub fn send(&self, value: T) -> Result<(), T> {
        let mut inner = self.inner.lock().unwrap();
        if inner.closed {
            return Err(value);
        }
        inner.queue.push_back(value);
        if let Some(waker) = inner.recv_waker.take() {
            waker.wake();
        }
        Ok(())
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        // Check if this is the last sender (Arc strong_count == 2: one for sender, one for receiver)
        // When this drops and only receiver remains, mark closed
        if Arc::strong_count(&self.inner) <= 2 {
            let mut inner = self.inner.lock().unwrap();
            inner.closed = true;
            if let Some(waker) = inner.recv_waker.take() {
                waker.wake();
            }
        }
    }
}

/// Receiving half of an mpsc channel.
pub struct Receiver<T> {
    inner: Arc<Mutex<Inner<T>>>,
}

impl<T> Receiver<T> {
    /// Receive the next value. Returns a future that resolves to `Some(T)` or
    /// `None` if all senders are dropped and the channel is empty.
    pub fn recv(&self) -> RecvFuture<'_, T> {
        RecvFuture { receiver: self }
    }
}

/// Future returned by `Receiver::recv()`.
pub struct RecvFuture<'a, T> {
    receiver: &'a Receiver<T>,
}

impl<'a, T> Future for RecvFuture<'a, T> {
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let mut inner = self.receiver.inner.lock().unwrap();
        if let Some(val) = inner.queue.pop_front() {
            Poll::Ready(Some(val))
        } else if inner.closed {
            Poll::Ready(None)
        } else {
            inner.recv_waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

/// Create an unbounded mpsc channel.
///
/// # Examples
/// ```
/// use lombokasync::{block_on, mpsc};
///
/// let (tx, rx) = mpsc::channel::<i32>();
/// tx.send(42).unwrap();
/// let val = block_on(async { rx.recv().await });
/// assert_eq!(val, Some(42));
/// ```
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let inner = Arc::new(Mutex::new(Inner {
        queue: VecDeque::new(),
        closed: false,
        recv_waker: None,
    }));
    (
        Sender { inner: inner.clone() },
        Receiver { inner },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_on;

    #[test]
    fn test_send_recv() {
        let (tx, rx) = channel();
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        let v1 = block_on(async { rx.recv().await });
        let v2 = block_on(async { rx.recv().await });
        assert_eq!(v1, Some(1));
        assert_eq!(v2, Some(2));
    }

    #[test]
    fn test_closed_channel() {
        let (tx, rx) = channel::<i32>();
        tx.send(99).unwrap();
        drop(tx);
        let v1 = block_on(async { rx.recv().await });
        let v2 = block_on(async { rx.recv().await });
        assert_eq!(v1, Some(99));
        assert_eq!(v2, None);
    }
}
