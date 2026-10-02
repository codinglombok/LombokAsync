//! Combinators (SPEC section 5): join, join3, join_all, try_join_all, select, select_all.
//!
//! Every combinator boxes its futures, so they are `Unpin` and no pin
//! projection (and no unsafe code) is needed.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

type Boxed<F> = Pin<Box<F>>;

/// Future returned by [`join`].
#[must_use = "futures do nothing unless awaited"]
pub struct Join<A: Future, B: Future> {
    a: Boxed<A>,
    b: Boxed<B>,
    ra: Option<A::Output>,
    rb: Option<B::Output>,
}

impl<A: Future, B: Future> Unpin for Join<A, B> {}

impl<A: Future, B: Future> Future for Join<A, B> {
    type Output = (A::Output, B::Output);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        if this.ra.is_none() {
            if let Poll::Ready(v) = this.a.as_mut().poll(cx) {
                this.ra = Some(v);
            }
        }
        if this.rb.is_none() {
            if let Poll::Ready(v) = this.b.as_mut().poll(cx) {
                this.rb = Some(v);
            }
        }
        match (this.ra.take(), this.rb.take()) {
            (Some(a), Some(b)) => Poll::Ready((a, b)),
            (a, b) => {
                this.ra = a;
                this.rb = b;
                Poll::Pending
            }
        }
    }
}

/// Runs two futures concurrently and returns both outputs.
///
/// # Examples
/// ```
/// let (a, b) = lombokasync::block_on(lombokasync::join(async { 1 }, async { "x" }));
/// assert_eq!((a, b), (1, "x"));
/// ```
pub fn join<A: Future, B: Future>(a: A, b: B) -> Join<A, B> {
    Join {
        a: Box::pin(a),
        b: Box::pin(b),
        ra: None,
        rb: None,
    }
}

/// Runs three futures concurrently and returns all outputs.
pub async fn join3<A: Future, B: Future, C: Future>(
    a: A,
    b: B,
    c: C,
) -> (A::Output, B::Output, C::Output) {
    let ((ra, rb), rc) = join(join(a, b), c).await;
    (ra, rb, rc)
}

/// Future returned by [`join_all`].
#[must_use = "futures do nothing unless awaited"]
pub struct JoinAll<F: Future> {
    futures: Vec<Option<Boxed<F>>>,
    results: Vec<Option<F::Output>>,
    remaining: usize,
}

impl<F: Future> Unpin for JoinAll<F> {}

impl<F: Future> Future for JoinAll<F> {
    type Output = Vec<F::Output>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        for (slot, result) in this.futures.iter_mut().zip(this.results.iter_mut()) {
            if let Some(f) = slot {
                if let Poll::Ready(v) = f.as_mut().poll(cx) {
                    *result = Some(v);
                    *slot = None;
                    this.remaining -= 1;
                }
            }
        }
        if this.remaining > 0 {
            return Poll::Pending;
        }
        Poll::Ready(this.results.iter_mut().filter_map(Option::take).collect())
    }
}

/// Runs every future concurrently and returns the outputs in input order.
///
/// # Examples
/// ```
/// use lombokasync::{block_on, join_all};
/// let out = block_on(join_all((1..=2).map(|i| async move { i }).collect()));
/// assert_eq!(out, vec![1, 2]);
/// ```
pub fn join_all<F: Future>(futures: Vec<F>) -> JoinAll<F> {
    let n = futures.len();
    JoinAll {
        futures: futures.into_iter().map(|f| Some(Box::pin(f))).collect(),
        results: (0..n).map(|_| None).collect(),
        remaining: n,
    }
}

/// Future returned by [`try_join_all`].
#[must_use = "futures do nothing unless awaited"]
pub struct TryJoinAll<F: Future> {
    inner: JoinAll<F>,
}

impl<F: Future> Unpin for TryJoinAll<F> {}

impl<T, E, F: Future<Output = Result<T, E>>> Future for TryJoinAll<F> {
    type Output = Result<Vec<T>, E>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.inner).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(results) => Poll::Ready(results.into_iter().collect()),
        }
    }
}

/// Runs every future to completion, then returns all values in input order or
/// the error of the lowest-index future that failed (not the earliest one).
///
/// # Examples
/// ```
/// use lombokasync::{block_on, try_join_all};
/// let ok: Result<Vec<i32>, String> = block_on(try_join_all((1..=2).map(|i| async move { Ok(i) }).collect()));
/// assert_eq!(ok, Ok(vec![1, 2]));
/// let err = block_on(try_join_all((1..=3).map(|i| async move { if i > 1 { Err(i) } else { Ok(i) } }).collect()));
/// assert_eq!(err, Err(2));
/// ```
pub fn try_join_all<T, E, F: Future<Output = Result<T, E>>>(futures: Vec<F>) -> TryJoinAll<F> {
    TryJoinAll {
        inner: join_all(futures),
    }
}

/// Which side of a [`select`] finished first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Either<A, B> {
    /// The first future finished first.
    Left(A),
    /// The second future finished first.
    Right(B),
}

/// Future returned by [`select`].
#[must_use = "futures do nothing unless awaited"]
pub struct Select<A: Future, B: Future> {
    a: Boxed<A>,
    b: Boxed<B>,
}

impl<A: Future, B: Future> Unpin for Select<A, B> {}

impl<A: Future, B: Future> Future for Select<A, B> {
    type Output = Either<A::Output, B::Output>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Poll::Ready(v) = self.a.as_mut().poll(cx) {
            return Poll::Ready(Either::Left(v));
        }
        if let Poll::Ready(v) = self.b.as_mut().poll(cx) {
            return Poll::Ready(Either::Right(v));
        }
        Poll::Pending
    }
}

/// Returns the output of whichever future finishes first; when both are ready
/// in the same step the first one wins. The other future is dropped.
///
/// # Examples
/// ```
/// use lombokasync::{block_on, select, Either};
/// assert_eq!(block_on(select(async { 1 }, async { 2 })), Either::Left(1));
/// ```
pub fn select<A: Future, B: Future>(a: A, b: B) -> Select<A, B> {
    Select {
        a: Box::pin(a),
        b: Box::pin(b),
    }
}

/// Future returned by [`select_all`].
#[must_use = "futures do nothing unless awaited"]
pub struct SelectAll<F: Future> {
    futures: Vec<Boxed<F>>,
}

impl<F: Future> Unpin for SelectAll<F> {}

impl<F: Future> Future for SelectAll<F> {
    type Output = (usize, F::Output);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        for (i, f) in self.futures.iter_mut().enumerate() {
            if let Poll::Ready(v) = f.as_mut().poll(cx) {
                return Poll::Ready((i, v));
            }
        }
        Poll::Pending
    }
}

/// Returns `(index, output)` of the first future to finish; when several are
/// ready in the same step the lowest index wins. The others are dropped.
///
/// # Panics
///
/// Panics when `futures` is empty.
///
/// # Examples
/// ```
/// use std::future::{pending, ready};
/// use std::pin::Pin;
/// use std::future::Future;
/// use lombokasync::{block_on, select_all};
///
/// let fs: Vec<Pin<Box<dyn Future<Output = i32>>>> = vec![Box::pin(pending()), Box::pin(ready(7))];
/// assert_eq!(block_on(select_all(fs)), (1, 7));
/// ```
pub fn select_all<F: Future>(futures: Vec<F>) -> SelectAll<F> {
    assert!(!futures.is_empty(), "select_all needs at least one future");
    SelectAll {
        futures: futures.into_iter().map(Box::pin).collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{block_on, sleep, yield_now};
    use std::time::Duration;

    async fn after(yields: usize, v: i32) -> i32 {
        for _ in 0..yields {
            yield_now().await;
        }
        v
    }

    #[test]
    fn join_variants() {
        assert_eq!(block_on(join(after(3, 1), after(0, 2))), (1, 2));
        assert_eq!(block_on(join(after(0, 1), after(3, 2))), (1, 2));
        assert_eq!(
            block_on(join3(after(2, 1), after(1, 2), after(0, 3))),
            (1, 2, 3)
        );
    }

    #[test]
    fn join_all_is_concurrent() {
        let start = std::time::Instant::now();
        let out = block_on(join_all(
            (0..5)
                .map(|i| async move {
                    sleep(Duration::from_millis(40)).await;
                    i
                })
                .collect(),
        ));
        assert_eq!(out, vec![0, 1, 2, 3, 4]);
        assert!(
            start.elapsed() < Duration::from_millis(190),
            "futures ran one after another"
        );
    }

    #[test]
    fn join_all_empty() {
        let out: Vec<i32> = block_on(join_all(Vec::<std::future::Ready<i32>>::new()));
        assert!(out.is_empty());
    }

    #[test]
    fn try_join_all_lowest_index_error() {
        type Fallible = Pin<Box<dyn Future<Output = Result<i32, &'static str>>>>;
        let fs: Vec<Fallible> = vec![
            Box::pin(async { Ok(1) }),
            Box::pin(async {
                after(4, 0).await;
                Err("first")
            }),
            Box::pin(async { Err("second") }),
        ];
        assert_eq!(block_on(try_join_all(fs)), Err("first"));
    }

    #[test]
    fn select_prefers_left_on_tie() {
        assert_eq!(block_on(select(after(0, 1), after(0, 2))), Either::Left(1));
        assert_eq!(block_on(select(after(2, 1), after(0, 2))), Either::Right(2));
    }

    #[test]
    fn select_all_first_finished() {
        let fs: Vec<Pin<Box<dyn Future<Output = i32>>>> = vec![
            Box::pin(after(3, 1)),
            Box::pin(after(1, 2)),
            Box::pin(after(1, 3)),
        ];
        assert_eq!(block_on(select_all(fs)), (1, 2));
    }

    #[test]
    #[should_panic(expected = "at least one")]
    fn select_all_empty_panics() {
        drop(select_all(Vec::<std::future::Ready<i32>>::new()));
    }
}
