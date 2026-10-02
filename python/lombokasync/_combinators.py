"""Combinators (SPEC section 5): join_all, join, join3, select."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Awaitable, Generic, Iterator, List, Sequence, Tuple, TypeVar

T = TypeVar("T")
A = TypeVar("A")
B = TypeVar("B")
C = TypeVar("C")


async def join_all(awaitables: Sequence[Awaitable[T]]) -> List[T]:
    """Waits for every awaitable, then returns the results in input order or
    raises the error of the lowest-index awaitable that failed (not the
    earliest one)."""
    results = await asyncio.gather(*awaitables, return_exceptions=True)
    for r in results:
        if isinstance(r, BaseException):
            raise r
    return list(results)


async def join(a: Awaitable[A], b: Awaitable[B]) -> Tuple[A, B]:
    """:func:`join_all` for two awaitables."""
    ra, rb = await join_all([a, b])  # type: ignore[list-item]
    return ra, rb  # type: ignore[return-value]


async def join3(a: Awaitable[A], b: Awaitable[B], c: Awaitable[C]) -> Tuple[A, B, C]:
    """:func:`join_all` for three awaitables."""
    ra, rb, rc = await join_all([a, b, c])  # type: ignore[list-item]
    return ra, rb, rc  # type: ignore[return-value]


@dataclass(frozen=True)
class Selected(Generic[T]):
    """Result of :func:`select`: index of the winner and its value.
    Unpacks like a tuple: ``index, value = await select(...)``."""

    index: int
    value: T

    def __iter__(self) -> Iterator[Any]:
        return iter((self.index, self.value))


async def select(awaitables: Sequence[Awaitable[T]]) -> "Selected[T]":
    """Returns the first awaitable to finish as ``Selected(index, value)``, or
    raises its error. Ties go to the lowest index. The others are cancelled.

    Raises ``ValueError`` for an empty sequence.
    """
    if not awaitables:
        raise ValueError("select needs at least one awaitable")
    tasks = [asyncio.ensure_future(a) for a in awaitables]
    order: List[int] = []
    first: "asyncio.Future[None]" = asyncio.get_running_loop().create_future()

    def finished(i: int) -> Any:
        def cb(_: Any) -> None:
            order.append(i)
            if not first.done():
                first.set_result(None)

        return cb

    for i, t in enumerate(tasks):
        t.add_done_callback(finished(i))
    try:
        await first
    finally:
        for t in tasks:
            if not t.done():
                t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    winner = order[0]
    return Selected(winner, tasks[winner].result())
