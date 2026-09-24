"""Timer utilities — sleep, timeout, interval."""

from __future__ import annotations
import asyncio
from typing import Any, AsyncIterator, Coroutine, Optional, TypeVar

T = TypeVar('T')


async def sleep(seconds: float) -> None:
    """Sleep for the given number of seconds.

    Example::

        await sleep(0.1)  # sleep 100ms
    """
    await asyncio.sleep(seconds)


async def timeout(seconds: float, coro: Coroutine) -> Optional[Any]:
    """Run a coroutine with a timeout. Returns None if the deadline elapses.

    Example::

        result = await timeout(1.0, fetch_data())
        if result is None:
            print("timed out")
    """
    try:
        return await asyncio.wait_for(coro, timeout=seconds)
    except asyncio.TimeoutError:
        return None


async def interval(seconds: float) -> AsyncIterator[int]:
    """Async generator that yields at regular intervals.

    Example::

        async for tick in interval(0.5):
            print(f"tick {tick}")
            if tick >= 2:
                break
    """
    count = 0
    while True:
        await asyncio.sleep(seconds)
        yield count
        count += 1
