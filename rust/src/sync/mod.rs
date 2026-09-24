//! Combinators — join and select for concurrent task composition.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Join two futures, running them concurrently and returning both results.
pub struct Join<A: Future, B: Future> {
    a: Pin<Box<A>>,
    b: Pin<Box<B>>,
    result_a: Option<A::Output>,
    result_b: Option<B::Output>,
}

impl<A: Future, B: Future> Future for Join<A, B> {
    type Output = (A::Output, B::Output);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<(A::Output, B::Output)> {
        let this = unsafe { self.get_unchecked_mut() };
        if this.result_a.is_none() {
            if let Poll::Ready(val) = this.a.as_mut().poll(cx) {
                this.result_a = Some(val);
            }
        }
        if this.result_b.is_none() {
            if let Poll::Ready(val) = this.b.as_mut().poll(cx) {
                this.result_b = Some(val);
            }
        }

        if this.result_a.is_some() && this.result_b.is_some() {
            Poll::Ready((
                this.result_a.take().unwrap(),
                this.result_b.take().unwrap(),
            ))
        } else {
            Poll::Pending
        }
    }
}

/// Run two futures concurrently and return both results.
///
/// # Examples
/// ```
/// use lombokasync::{block_on};
/// use lombokasync::sync::join;
///
/// let (a, b) = block_on(join(async { 1 }, async { 2 }));
/// assert_eq!((a, b), (1, 2));
/// ```
pub fn join<A: Future, B: Future>(a: A, b: B) -> Join<A, B> {
    Join {
        a: Box::pin(a),
        b: Box::pin(b),
        result_a: None,
        result_b: None,
    }
}

/// Join three futures concurrently.
pub async fn join3<A: Future, B: Future, C: Future>(a: A, b: B, c: C) -> (A::Output, B::Output, C::Output) {
    let ((ra, rb), rc) = join(join(a, b), c).await;
    (ra, rb, rc)
}

/// The result of a select operation — which future completed first.
pub enum Either<A, B> {
    Left(A),
    Right(B),
}

/// Select — returns whichever future completes first.
pub struct Select<A: Future, B: Future> {
    a: Pin<Box<A>>,
    b: Pin<Box<B>>,
}

impl<A: Future, B: Future> Future for Select<A, B> {
    type Output = Either<A::Output, B::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Either<A::Output, B::Output>> {
        let this = unsafe { self.get_unchecked_mut() };
        if let Poll::Ready(val) = this.a.as_mut().poll(cx) {
            return Poll::Ready(Either::Left(val));
        }
        if let Poll::Ready(val) = this.b.as_mut().poll(cx) {
            return Poll::Ready(Either::Right(val));
        }
        Poll::Pending
    }
}

/// Race two futures — returns the result of whichever completes first.
///
/// # Examples
/// ```
/// use lombokasync::{block_on};
/// use lombokasync::sync::{select, Either};
///
/// let result = block_on(select(async { 1 }, async { 2 }));
/// match result {
///     Either::Left(v) => assert_eq!(v, 1),
///     Either::Right(v) => assert_eq!(v, 2),
/// }
/// ```
pub fn select<A: Future, B: Future>(a: A, b: B) -> Select<A, B> {
    Select {
        a: Box::pin(a),
        b: Box::pin(b),
    }
}

/// Join a vector of futures, returning all results in order.
pub async fn join_all<F: Future>(futures: Vec<F>) -> Vec<F::Output> {
    let mut results = Vec::with_capacity(futures.len());
    for f in futures {
        results.push(f.await);
    }
    results
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_on;

    #[test]
    fn test_join() {
        let (a, b) = block_on(join(async { 1 }, async { "hello" }));
        assert_eq!(a, 1);
        assert_eq!(b, "hello");
    }

    #[test]
    fn test_join3() {
        let (a, b, c) = block_on(join3(async { 1 }, async { 2 }, async { 3 }));
        assert_eq!((a, b, c), (1, 2, 3));
    }

    #[test]
    fn test_select() {
        let result = block_on(select(async { 42 }, async { 99 }));
        match result {
            Either::Left(v) => assert_eq!(v, 42),
            Either::Right(v) => assert_eq!(v, 99),
        }
    }

    #[test]
    fn test_join_all() {
        let futures = vec![
            Box::pin(async { 1 }) as Pin<Box<dyn Future<Output = i32>>>,
            Box::pin(async { 2 }),
            Box::pin(async { 3 }),
        ];
        let results = block_on(join_all(futures));
        assert_eq!(results, vec![1, 2, 3]);
    }
}
