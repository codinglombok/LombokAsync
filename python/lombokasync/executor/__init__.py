"""Executor — run and spawn tasks on asyncio."""

from __future__ import annotations
import asyncio
from typing import Any, Coroutine, TypeVar

T = TypeVar('T')


class JoinHandle:
    """Handle to a spawned task. Await it to get the result."""

    def __init__(self, task: asyncio.Task) -> None:
        self._task = task

    def __await__(self):
        return self._task.__await__()

    async def result(self):
        """Wait for and return the task result."""
        return await self._task

    def cancel(self) -> None:
        """Cancel the task."""
        self._task.cancel()


def spawn(coro: Coroutine) -> JoinHandle:
    """Spawn a coroutine as a concurrent task.

    Must be called from within an async context (inside `run`).

    Example::

        async def main():
            handle = spawn(compute())
            result = await handle
    """
    loop = asyncio.get_running_loop()
    task = loop.create_task(coro)
    return JoinHandle(task)


def run(coro: Coroutine) -> Any:
    """Run a coroutine to completion. Entry point for the async runtime.

    Example::

        import lombokasync

        result = lombokasync.run(async_main())
    """
    return asyncio.run(coro)
