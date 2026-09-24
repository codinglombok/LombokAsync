//! One-shot channel — send exactly one value.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

struct Inner<T> {
    value: Option<T>,
    closed: bool,
    recv_waker: Option<Waker>,
}

/// Sending half of a oneshot channel.
pub struct Sender<T> {
    inner: Arc<Mutex<Inner<T>>>,
    sent: bool,
}

impl<T> Sender<T> {
    /// Send a value. Consumes the sender.
    pub fn send(mut self, value: T) -> Result<(), T> {
        let mut inner = self.inner.lock().unwrap();
        if inner.closed {
            return Err(value);
        }
        inner.value = Some(value);
        inner.closed = true;
        self.sent = true;
        if let Some(waker) = inner.recv_waker.take() {
            waker.wake();
        }
        Ok(())
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        if !self.sent {
            let mut inner = self.inner.lock().unwrap();
            inner.closed = true;
            if let Some(waker) = inner.recv_waker.take() {
                waker.wake();
            }
        }
    }
}

/// Receiving half of a oneshot channel. Implements Future.
pub struct Receiver<T> {
    inner: Arc<Mutex<Inner<T>>>,
}

impl<T> Future for Receiver<T> {
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let mut inner = self.inner.lock().unwrap();
        if let Some(val) = inner.value.take() {
            Poll::Ready(Some(val))
        } else if inner.closed {
            Poll::Ready(None) // sender dropped without sending
        } else {
            inner.recv_waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

/// Create a oneshot channel.
///
/// # Examples
/// ```
/// use lombokasync::{block_on, oneshot};
///
/// let (tx, rx) = oneshot::channel::<i32>();
/// tx.send(42).unwrap();
/// let val = block_on(async { rx.await });
/// assert_eq!(val, Some(42));
/// ```
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let inner = Arc::new(Mutex::new(Inner {
        value: None,
        closed: false,
        recv_waker: None,
    }));
    (
        Sender { inner: inner.clone(), sent: false },
        Receiver { inner },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_on;

    #[test]
    fn test_oneshot() {
        let (tx, rx) = channel();
        tx.send(42).unwrap();
        let val = block_on(async { rx.await });
        assert_eq!(val, Some(42));
    }

    #[test]
    fn test_oneshot_dropped() {
        let (tx, rx) = channel::<i32>();
        drop(tx);
        let val = block_on(async { rx.await });
        assert_eq!(val, None);
    }
}
