"""Async channels — mpsc and oneshot."""

from __future__ import annotations
import asyncio
from typing import Any, Generic, Optional, Tuple, TypeVar

T = TypeVar('T')


class Sender:
    """Sending half of an mpsc channel."""

    def __init__(self, queue: asyncio.Queue, closed: list) -> None:
        self._queue = queue
        self._closed = closed

    def send(self, value: Any) -> None:
        """Send a value into the channel."""
        if self._closed[0]:
            raise RuntimeError("channel closed")
        self._queue.put_nowait(value)

    def close(self) -> None:
        """Close the channel."""
        self._closed[0] = True


class Receiver:
    """Receiving half of an mpsc channel."""

    def __init__(self, queue: asyncio.Queue, closed: list) -> None:
        self._queue = queue
        self._closed = closed

    async def recv(self) -> Optional[Any]:
        """Receive the next value, or None if closed and empty."""
        # Drain any buffered values first
        if not self._queue.empty():
            return self._queue.get_nowait()
        # Nothing buffered — if closed, signal end
        if self._closed[0]:
            return None
        # Block until something arrives or channel closes
        while True:
            try:
                return await asyncio.wait_for(self._queue.get(), timeout=0.05)
            except asyncio.TimeoutError:
                if self._closed[0] and self._queue.empty():
                    return None

    async def __aiter__(self):
        while True:
            val = await self.recv()
            if val is None:
                break
            yield val


def mpsc_channel(maxsize: int = 0) -> Tuple[Sender, Receiver]:
    """Create an unbounded mpsc (multi-producer, single-consumer) channel.

    Example::

        tx, rx = mpsc_channel()
        tx.send(42)
        val = await rx.recv()
    """
    queue = asyncio.Queue(maxsize=maxsize)
    closed = [False]
    return Sender(queue, closed), Receiver(queue, closed)


class OneshotSender:
    """Sending half of a oneshot channel."""

    def __init__(self, future: asyncio.Future) -> None:
        self._future = future

    def send(self, value: Any) -> None:
        """Send a value. Can only be called once."""
        if self._future.done():
            raise RuntimeError("oneshot already sent")
        self._future.set_result(value)


class OneshotReceiver:
    """Receiving half of a oneshot channel."""

    def __init__(self, future: asyncio.Future) -> None:
        self._future = future

    async def recv(self) -> Optional[Any]:
        """Wait for the value."""
        return await self._future

    def __await__(self):
        return self._future.__await__()


def oneshot_channel() -> Tuple[OneshotSender, OneshotReceiver]:
    """Create a oneshot channel — send exactly one value.

    Example::

        tx, rx = oneshot_channel()
        tx.send(42)
        val = await rx.recv()
    """
    loop = asyncio.get_running_loop()
    future = loop.create_future()
    return OneshotSender(future), OneshotReceiver(future)
