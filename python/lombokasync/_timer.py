"""Timers: sleep, timeout, interval."""

from __future__ import annotations

import asyncio
from typing import AsyncIterator, Awaitable, TypeVar

from lombokasync._errors import Timeout

T = TypeVar("T")


async def sleep(seconds: float) -> None:
    """Waits ``seconds`` seconds."""
    await asyncio.sleep(seconds)


async def timeout(seconds: float, awaitable: Awaitable[T]) -> T:
    """Returns the result of ``awaitable`` or raises :class:`Timeout` after
    ``seconds``. Errors from the awaitable pass through; on timeout it is
    cancelled."""
    task = asyncio.ensure_future(awaitable)
    done, _ = await asyncio.wait({task}, timeout=seconds)
    if task in done:
        return task.result()
    task.cancel()
    try:
        await task
    except BaseException:  # noqa: BLE001 - the task's own outcome no longer matters
        pass
    raise Timeout(f"deadline of {seconds} s elapsed")


async def interval(seconds: float) -> AsyncIterator[int]:
    """Yields 0, 1, 2, ... every ``seconds``. Ticks are scheduled at fixed
    multiples of the period, so delays do not accumulate."""
    if not seconds > 0:
        raise ValueError("interval period must be greater than zero")
    loop = asyncio.get_running_loop()
    start = loop.time()
    n = 0
    while True:
        await asyncio.sleep(max(0.0, start + (n + 1) * seconds - loop.time()))
        yield n
        n += 1


__all__ = ["sleep", "timeout", "interval"]
