"""Combinators — join, select for concurrent composition."""

from __future__ import annotations
import asyncio
from dataclasses import dataclass
from typing import Any, Awaitable, List, Tuple, TypeVar, Union

T = TypeVar('T')
A = TypeVar('A')
B = TypeVar('B')
C = TypeVar('C')


@dataclass
class Either:
    """Result of a select operation."""
    kind: str  # 'left' or 'right'
    value: Any


async def join(a: Awaitable[A], b: Awaitable[B]) -> Tuple[A, B]:
    """Run two awaitables concurrently and return both results.

    Example::

        x, y = await join(fetch_a(), fetch_b())
    """
    return await asyncio.gather(a, b)  # type: ignore


async def join3(a: Awaitable[A], b: Awaitable[B], c: Awaitable[C]) -> Tuple[A, B, C]:
    """Run three awaitables concurrently."""
    return await asyncio.gather(a, b, c)  # type: ignore


async def join_all(awaitables: List[Awaitable[T]]) -> List[T]:
    """Run a list of awaitables concurrently and return all results."""
    return await asyncio.gather(*awaitables)  # type: ignore


async def select(a: Awaitable[A], b: Awaitable[B]) -> Either:
    """Race two awaitables — returns whichever completes first.

    Example::

        result = await select(fetch_fast(), fetch_slow())
        if result.kind == 'left':
            print(result.value)
    """
    task_a = asyncio.ensure_future(a)  # type: ignore
    task_b = asyncio.ensure_future(b)  # type: ignore

    done, pending = await asyncio.wait(
        [task_a, task_b],
        return_when=asyncio.FIRST_COMPLETED,
    )

    for p in pending:
        p.cancel()
        try:
            await p
        except (asyncio.CancelledError, Exception):
            pass

    if task_a in done:
        return Either(kind='left', value=task_a.result())
    else:
        return Either(kind='right', value=task_b.result())
