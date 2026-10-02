"""Running and spawning tasks on asyncio."""

from __future__ import annotations

import asyncio
from typing import Any, Coroutine, Generator, Generic, TypeVar

T = TypeVar("T")


class JoinHandle(Generic[T]):
    """Handle to a task started with :func:`spawn`. Await it for the result."""

    def __init__(self, task: "asyncio.Task[T]") -> None:
        self._task = task

    def __await__(self) -> Generator[Any, None, T]:
        return self._task.__await__()

    def done(self) -> bool:
        """True once the task has finished."""
        return self._task.done()

    def cancel(self) -> bool:
        """Requests cancellation; awaiting the handle then raises ``CancelledError``."""
        return self._task.cancel()


def spawn(coro: Coroutine[Any, Any, T]) -> JoinHandle[T]:
    """Starts ``coro`` as a task on the running loop.

    Raises ``RuntimeError`` when no loop is running (call it inside :func:`run`).
    """
    return JoinHandle(asyncio.get_running_loop().create_task(coro))


def run(coro: Coroutine[Any, Any, T]) -> T:
    """Runs ``coro`` to completion on a new event loop and returns its result."""
    return asyncio.run(coro)


async def yield_now() -> None:
    """Lets other ready tasks run before continuing."""
    await asyncio.sleep(0)
