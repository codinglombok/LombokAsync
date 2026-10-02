//! One-shot channel: at most one value (SPEC section 4).
//!
//! [`Sender::send`] consumes the sender, so a second send cannot be written in
//! Rust; other ports report it as `already_sent`.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use super::lock;

struct Inner<T> {
    value: Option<T>,
    tx_gone: bool,
    rx_closed: bool,
    waker: Option<Waker>,
}

/// Error from [`Receiver::try_recv`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryRecvError {
    /// No value yet and the sender is alive (outcome `empty`).
    Empty,
    /// No value can arrive any more (outcome `closed`).
    Closed,
}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            TryRecvError::Empty => "EMPTY: no value yet",
            TryRecvError::Closed => "CLOSED: channel is closed",
        })
    }
}

impl std::error::Error for TryRecvError {}

/// Error from awaiting a [`Receiver`]: the sender went away without a value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecvError;

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CLOSED: channel is closed")
    }
}

impl std::error::Error for RecvError {}

/// Sending half.
pub struct Sender<T> {
    inner: Arc<Mutex<Inner<T>>>,
}

impl<T> Sender<T> {
    /// Sends the value. Returns it back when the receiver is closed or dropped.
    pub fn send(self, value: T) -> Result<(), T> {
        let mut inner = lock(&self.inner);
        if inner.rx_closed {
            return Err(value);
        }
        inner.value = Some(value);
        Ok(())
        // Drop marks the sender gone and wakes the receiver.
    }

    /// Returns true when the receiver is closed or dropped.
    pub fn is_closed(&self) -> bool {
        lock(&self.inner).rx_closed
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let mut inner = lock(&self.inner);
        inner.tx_gone = true;
        if let Some(w) = inner.waker.take() {
            w.wake();
        }
    }
}

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Sender { .. }")
    }
}

/// Receiving half. Await it to get the value.
pub struct Receiver<T> {
    inner: Arc<Mutex<Inner<T>>>,
    taken: bool,
}

impl<T> Receiver<T> {
    /// Takes the value without waiting.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let mut inner = lock(&self.inner);
        if let Some(v) = inner.value.take() {
            self.taken = true;
            return Ok(v);
        }
        if self.taken || inner.tx_gone || inner.rx_closed {
            Err(TryRecvError::Closed)
        } else {
            Err(TryRecvError::Empty)
        }
    }

    /// Stops the sender from sending; a value sent earlier can still be taken.
    pub fn close(&mut self) {
        lock(&self.inner).rx_closed = true;
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        lock(&self.inner).rx_closed = true;
    }
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Receiver { .. }")
    }
}

impl<T> Unpin for Receiver<T> {}

impl<T> Future for Receiver<T> {
    type Output = Result<T, RecvError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.try_recv() {
            Ok(v) => Poll::Ready(Ok(v)),
            Err(TryRecvError::Closed) => Poll::Ready(Err(RecvError)),
            Err(TryRecvError::Empty) => {
                let mut inner = lock(&self.inner);
                if inner.value.is_some() || inner.tx_gone {
                    drop(inner);
                    cx.waker().wake_by_ref();
                } else {
                    inner.waker = Some(cx.waker().clone());
                }
                Poll::Pending
            }
        }
    }
}

/// Creates a oneshot channel.
///
/// # Examples
/// ```
/// use lombokasync::{block_on, oneshot};
///
/// let (tx, rx) = oneshot::channel::<i32>();
/// tx.send(42).unwrap();
/// assert_eq!(block_on(rx), Ok(42));
/// ```
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let inner = Arc::new(Mutex::new(Inner {
        value: None,
        tx_gone: false,
        rx_closed: false,
        waker: None,
    }));
    (
        Sender {
            inner: inner.clone(),
        },
        Receiver {
            inner,
            taken: false,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{block_on, spawn, yield_now};

    #[test]
    fn value_then_closed() {
        let (tx, mut rx) = channel();
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
        tx.send(5).unwrap();
        assert_eq!(rx.try_recv(), Ok(5));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Closed));
    }

    #[test]
    fn dropped_sender() {
        let (tx, rx) = channel::<i32>();
        drop(tx);
        assert_eq!(block_on(rx), Err(RecvError));
    }

    #[test]
    fn await_value_from_task() {
        let v = block_on(async {
            let (tx, rx) = channel();
            spawn(async move {
                yield_now().await;
                tx.send("hi").unwrap();
            });
            rx.await
        });
        assert_eq!(v, Ok("hi"));
    }

    #[test]
    fn closed_receiver() {
        let (tx, mut rx) = channel();
        rx.close();
        assert!(tx.is_closed());
        assert_eq!(tx.send(1), Err(1));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Closed));
        let (tx, rx) = channel::<u8>();
        drop(rx);
        assert_eq!(tx.send(2), Err(2));
    }

    #[test]
    fn display_and_debug() {
        assert_eq!(TryRecvError::Empty.to_string(), "EMPTY: no value yet");
        assert_eq!(
            TryRecvError::Closed.to_string(),
            "CLOSED: channel is closed"
        );
        assert_eq!(RecvError.to_string(), "CLOSED: channel is closed");
        let (tx, rx) = channel::<u8>();
        assert_eq!(format!("{tx:?} {rx:?}"), "Sender { .. } Receiver { .. }");
    }
}
