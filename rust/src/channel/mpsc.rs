//! Multi-producer, single-consumer channel (SPEC section 3).
//!
//! The channel is closed for receiving once every [`Sender`] is dropped or the
//! [`Receiver`] is closed; values already in the queue are still delivered.

use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use super::lock;

struct Inner<T> {
    queue: VecDeque<T>,
    capacity: Option<usize>,
    senders: usize,
    rx_closed: bool,
    recv_waker: Option<Waker>,
    send_wakers: Vec<Waker>,
}

impl<T> Inner<T> {
    fn wake_senders(&mut self) {
        for w in self.send_wakers.drain(..) {
            w.wake();
        }
    }
    fn wake_receiver(&mut self) {
        if let Some(w) = self.recv_waker.take() {
            w.wake();
        }
    }
}

/// Error from [`Sender::try_send`]; the value is handed back.
#[derive(PartialEq, Eq)]
pub enum TrySendError<T> {
    /// The channel is bounded and holds `capacity` values (outcome `full`).
    Full(T),
    /// The receiver is closed or dropped (outcome `closed`).
    Closed(T),
}

impl<T> TrySendError<T> {
    /// Returns the value that was not sent.
    pub fn into_inner(self) -> T {
        match self {
            TrySendError::Full(v) | TrySendError::Closed(v) => v,
        }
    }
}

impl<T> fmt::Debug for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            TrySendError::Full(_) => "Full(..)",
            TrySendError::Closed(_) => "Closed(..)",
        })
    }
}

impl<T> fmt::Display for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            TrySendError::Full(_) => "FULL: channel is full",
            TrySendError::Closed(_) => "CLOSED: channel is closed",
        })
    }
}

impl<T> std::error::Error for TrySendError<T> {}

/// Error from [`Sender::send`]: the receiver is closed. The value is handed back.
#[derive(PartialEq, Eq)]
pub struct SendError<T>(pub T);

impl<T> fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SendError(..)")
    }
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CLOSED: channel is closed")
    }
}

impl<T> std::error::Error for SendError<T> {}

/// Error from [`Receiver::try_recv`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryRecvError {
    /// No value is queued but a sender is alive (outcome `empty`).
    Empty,
    /// No value is queued and none can arrive (outcome `closed`).
    Closed,
}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            TryRecvError::Empty => "EMPTY: channel is empty",
            TryRecvError::Closed => "CLOSED: channel is closed",
        })
    }
}

impl std::error::Error for TryRecvError {}

/// Error from [`bounded`] when the capacity is zero (code `INVALID_CAPACITY`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidCapacity;

impl fmt::Display for InvalidCapacity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("INVALID_CAPACITY: capacity must be at least 1")
    }
}

impl std::error::Error for InvalidCapacity {}

/// Sending half. Clone it to get more senders.
pub struct Sender<T> {
    inner: Arc<Mutex<Inner<T>>>,
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        lock(&self.inner).senders += 1;
        Sender {
            inner: self.inner.clone(),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let mut inner = lock(&self.inner);
        inner.senders -= 1;
        if inner.senders == 0 {
            inner.wake_receiver();
        }
    }
}

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Sender { .. }")
    }
}

impl<T> Sender<T> {
    /// Queues `value` without waiting.
    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        let mut inner = lock(&self.inner);
        if inner.rx_closed {
            return Err(TrySendError::Closed(value));
        }
        if inner.capacity.is_some_and(|c| inner.queue.len() >= c) {
            return Err(TrySendError::Full(value));
        }
        inner.queue.push_back(value);
        inner.wake_receiver();
        Ok(())
    }

    /// Queues `value`, waiting for space when the channel is bounded and full.
    pub fn send(&self, value: T) -> SendFuture<'_, T> {
        SendFuture {
            sender: self,
            value: Some(value),
        }
    }

    /// Returns true when the receiver is closed or dropped.
    pub fn is_closed(&self) -> bool {
        lock(&self.inner).rx_closed
    }
}

/// Future returned by [`Sender::send`].
#[must_use = "futures do nothing unless awaited"]
pub struct SendFuture<'a, T> {
    sender: &'a Sender<T>,
    value: Option<T>,
}

impl<T> Unpin for SendFuture<'_, T> {}

impl<T> Future for SendFuture<'_, T> {
    type Output = Result<(), SendError<T>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Some(value) = self.value.take() else {
            return Poll::Ready(Ok(()));
        };
        match self.sender.try_send(value) {
            Ok(()) => Poll::Ready(Ok(())),
            Err(TrySendError::Closed(v)) => Poll::Ready(Err(SendError(v))),
            Err(TrySendError::Full(v)) => {
                self.value = Some(v);
                lock(&self.sender.inner)
                    .send_wakers
                    .push(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

/// Receiving half.
pub struct Receiver<T> {
    inner: Arc<Mutex<Inner<T>>>,
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let mut inner = lock(&self.inner);
        inner.rx_closed = true;
        inner.wake_senders();
    }
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Receiver { .. }")
    }
}

impl<T> Receiver<T> {
    /// Takes the next value without waiting.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let mut inner = lock(&self.inner);
        if let Some(v) = inner.queue.pop_front() {
            inner.wake_senders();
            return Ok(v);
        }
        if inner.rx_closed || inner.senders == 0 {
            Err(TryRecvError::Closed)
        } else {
            Err(TryRecvError::Empty)
        }
    }

    /// Waits for the next value; `None` once the channel is closed and empty.
    pub fn recv(&mut self) -> RecvFuture<'_, T> {
        RecvFuture { receiver: self }
    }

    /// Polls for the next value; the building block of [`Receiver::recv`].
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        match self.try_recv() {
            Ok(v) => Poll::Ready(Some(v)),
            Err(TryRecvError::Closed) => Poll::Ready(None),
            Err(TryRecvError::Empty) => {
                let mut inner = lock(&self.inner);
                // A sender may have raced in between; check again under the lock.
                if !inner.queue.is_empty() || inner.senders == 0 {
                    drop(inner);
                    cx.waker().wake_by_ref();
                } else {
                    inner.recv_waker = Some(cx.waker().clone());
                }
                Poll::Pending
            }
        }
    }

    /// Stops new sends; values already queued can still be received.
    pub fn close(&mut self) {
        let mut inner = lock(&self.inner);
        inner.rx_closed = true;
        inner.wake_senders();
    }

    /// Number of values waiting in the queue.
    pub fn len(&self) -> usize {
        lock(&self.inner).queue.len()
    }

    /// Returns true when no value is waiting.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Future returned by [`Receiver::recv`].
#[must_use = "futures do nothing unless awaited"]
pub struct RecvFuture<'a, T> {
    receiver: &'a mut Receiver<T>,
}

impl<T> Future for RecvFuture<'_, T> {
    type Output = Option<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        self.receiver.poll_recv(cx)
    }
}

fn make<T>(capacity: Option<usize>) -> (Sender<T>, Receiver<T>) {
    let inner = Arc::new(Mutex::new(Inner {
        queue: VecDeque::new(),
        capacity,
        senders: 1,
        rx_closed: false,
        recv_waker: None,
        send_wakers: Vec::new(),
    }));
    (
        Sender {
            inner: inner.clone(),
        },
        Receiver { inner },
    )
}

/// Creates an unbounded channel.
///
/// # Examples
/// ```
/// use lombokasync::{block_on, mpsc};
///
/// let (tx, mut rx) = mpsc::channel::<i32>();
/// tx.try_send(1).unwrap();
/// drop(tx);
/// assert_eq!(block_on(rx.recv()), Some(1));
/// assert_eq!(block_on(rx.recv()), None);
/// ```
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    make(None)
}

/// Creates a channel that holds at most `capacity` values.
///
/// # Errors
///
/// [`InvalidCapacity`] when `capacity` is zero.
///
/// # Examples
/// ```
/// use lombokasync::mpsc::{self, TrySendError};
///
/// let (tx, _rx) = mpsc::bounded::<i32>(1).unwrap();
/// tx.try_send(1).unwrap();
/// assert_eq!(tx.try_send(2), Err(TrySendError::Full(2)));
/// assert!(mpsc::bounded::<i32>(0).is_err());
/// ```
pub fn bounded<T>(capacity: usize) -> Result<(Sender<T>, Receiver<T>), InvalidCapacity> {
    if capacity == 0 {
        return Err(InvalidCapacity);
    }
    Ok(make(Some(capacity)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{block_on, spawn, yield_now};

    #[test]
    fn fifo_and_close_on_last_sender() {
        let (tx, mut rx) = channel();
        let tx2 = tx.clone();
        tx.try_send(1).unwrap();
        tx2.try_send(2).unwrap();
        drop(tx);
        assert_eq!(rx.try_recv(), Ok(1));
        assert_eq!(rx.try_recv(), Ok(2));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
        drop(tx2);
        assert_eq!(rx.try_recv(), Err(TryRecvError::Closed));
    }

    #[test]
    fn recv_waits_for_sender_task() {
        let v = block_on(async {
            let (tx, mut rx) = channel();
            spawn(async move {
                for i in 0..5 {
                    yield_now().await;
                    tx.send(i).await.unwrap();
                }
            });
            let mut got = Vec::new();
            while let Some(v) = rx.recv().await {
                got.push(v);
            }
            got
        });
        assert_eq!(v, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn dropping_last_sender_wakes_receiver() {
        let r = block_on(async {
            let (tx, mut rx) = channel::<u8>();
            spawn(async move {
                yield_now().await;
                yield_now().await;
                drop(tx);
            });
            rx.recv().await
        });
        assert_eq!(r, None);
    }

    #[test]
    fn receive_wakes_blocked_sender() {
        block_on(async {
            let (tx, mut rx) = bounded(1).unwrap();
            tx.try_send(1).unwrap();
            let h = spawn(async move { tx.send(2).await });
            yield_now().await;
            assert!(!h.is_finished());
            assert_eq!(rx.try_recv(), Ok(1));
            assert_eq!(h.await, Ok(()));
            assert_eq!(rx.len(), 1);
        });
    }

    #[test]
    fn bounded_send_waits_for_space() {
        let got = block_on(async {
            let (tx, mut rx) = bounded(1).unwrap();
            let producer = spawn(async move {
                for i in 0..4 {
                    tx.send(i).await.unwrap();
                }
            });
            let mut got = Vec::new();
            while let Some(v) = rx.recv().await {
                got.push(v);
            }
            producer.await;
            got
        });
        assert_eq!(got, vec![0, 1, 2, 3]);
    }

    #[test]
    fn send_after_receiver_drop() {
        let (tx, rx) = channel::<u8>();
        drop(rx);
        assert!(tx.is_closed());
        assert_eq!(tx.try_send(3), Err(TrySendError::Closed(3)));
        assert_eq!(block_on(tx.send(4)), Err(SendError(4)));
    }

    #[test]
    fn pending_send_fails_when_receiver_closes() {
        let r = block_on(async {
            let (tx, mut rx) = bounded(1).unwrap();
            tx.try_send(0).unwrap();
            let h = spawn(async move { tx.send(1).await });
            yield_now().await;
            rx.close();
            h.await
        });
        assert_eq!(r, Err(SendError(1)));
    }

    #[test]
    fn errors_display_codes() {
        assert_eq!(TrySendError::Full(1).to_string(), "FULL: channel is full");
        assert_eq!(
            TrySendError::Closed(1).to_string(),
            "CLOSED: channel is closed"
        );
        assert_eq!(format!("{:?}", TrySendError::Full(1)), "Full(..)");
        assert_eq!(format!("{:?}", TrySendError::Closed(1)), "Closed(..)");
        assert_eq!(TrySendError::Full(7).into_inner(), 7);
        assert_eq!(SendError(1).to_string(), "CLOSED: channel is closed");
        assert_eq!(format!("{:?}", SendError(1)), "SendError(..)");
        assert_eq!(TryRecvError::Empty.to_string(), "EMPTY: channel is empty");
        assert_eq!(
            TryRecvError::Closed.to_string(),
            "CLOSED: channel is closed"
        );
        assert_eq!(
            InvalidCapacity.to_string(),
            "INVALID_CAPACITY: capacity must be at least 1"
        );
        let (tx, rx) = channel::<u8>();
        assert_eq!(format!("{tx:?} {rx:?}"), "Sender { .. } Receiver { .. }");
        assert!(rx.is_empty());
    }

    #[test]
    fn senders_on_other_threads() {
        let (tx, mut rx) = channel();
        let mut threads = Vec::new();
        for t in 0..4 {
            let tx = tx.clone();
            threads.push(std::thread::spawn(move || {
                for i in 0..100 {
                    tx.try_send(t * 100 + i).unwrap();
                }
            }));
        }
        drop(tx);
        let mut got = block_on(async {
            let mut got = Vec::new();
            while let Some(v) = rx.recv().await {
                got.push(v);
            }
            got
        });
        for t in threads {
            t.join().unwrap();
        }
        got.sort_unstable();
        assert_eq!(got, (0..400).collect::<Vec<_>>());
    }
}
