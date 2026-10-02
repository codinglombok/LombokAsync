"""Channels: mpsc (SPEC section 3) and oneshot (SPEC section 4).

Channels can be created outside a running loop. Waiting operations must run
on one event loop; the channels are not thread-safe.
"""

from __future__ import annotations

import asyncio
from collections import deque
from typing import Any, AsyncIterator, Deque, Generic, List, Optional, Tuple, TypeVar

from lombokasync._errors import AlreadySent, ChannelClosed, ChannelEmpty, ChannelFull, InvalidCapacity

T = TypeVar("T")


def _wake(waiters: List["asyncio.Future[None]"]) -> None:
    for w in waiters:
        if not w.done():
            w.set_result(None)
    waiters.clear()


class _Mpsc(Generic[T]):
    __slots__ = ("queue", "capacity", "senders", "rx_closed", "recv_waiters", "send_waiters")

    def __init__(self, capacity: Optional[int]) -> None:
        self.queue: Deque[T] = deque()
        self.capacity = capacity
        self.senders = 0
        self.rx_closed = False
        self.recv_waiters: List["asyncio.Future[None]"] = []
        self.send_waiters: List["asyncio.Future[None]"] = []


class Sender(Generic[T]):
    """Sending half of an mpsc channel."""

    def __init__(self, state: _Mpsc[T]) -> None:
        self._state = state
        self._dropped = False
        state.senders += 1

    def try_send(self, value: T) -> None:
        """Queues ``value`` without waiting.

        Raises :class:`ChannelClosed` when the receiver is closed and
        :class:`ChannelFull` when a bounded channel is full.
        """
        self._alive()
        s = self._state
        if s.rx_closed:
            raise ChannelClosed("channel is closed")
        if s.capacity is not None and len(s.queue) >= s.capacity:
            raise ChannelFull("channel is full")
        s.queue.append(value)
        _wake(s.recv_waiters)

    async def send(self, value: T) -> None:
        """Queues ``value``, waiting for space; raises :class:`ChannelClosed` if the receiver closes."""
        while True:
            try:
                self.try_send(value)
                return
            except ChannelFull:
                waiter = asyncio.get_running_loop().create_future()
                self._state.send_waiters.append(waiter)
                await waiter

    def clone(self) -> "Sender[T]":
        """Returns another sender for the same channel."""
        self._alive()
        return Sender(self._state)

    def close(self) -> None:
        """Drops this sender; the channel closes after the last one. Idempotent."""
        if self._dropped:
            return
        self._dropped = True
        s = self._state
        s.senders -= 1
        if s.senders == 0:
            _wake(s.recv_waiters)

    @property
    def is_closed(self) -> bool:
        """True when the receiver is closed."""
        return self._state.rx_closed

    def _alive(self) -> None:
        if self._dropped:
            raise ChannelClosed("sender was closed")


class Receiver(Generic[T]):
    """Receiving half of an mpsc channel. Iterate it with ``async for``."""

    def __init__(self, state: _Mpsc[T]) -> None:
        self._state = state

    def try_recv(self) -> T:
        """Takes the next value without waiting.

        Raises :class:`ChannelEmpty` when nothing is queued but a sender is
        alive, and :class:`ChannelClosed` when nothing more can arrive.
        """
        s = self._state
        if s.queue:
            value = s.queue.popleft()
            _wake(s.send_waiters)
            return value
        if s.rx_closed or s.senders == 0:
            raise ChannelClosed("channel is closed")
        raise ChannelEmpty("channel is empty")

    async def recv(self) -> T:
        """Waits for the next value; raises :class:`ChannelClosed` once closed and empty."""
        while True:
            try:
                return self.try_recv()
            except ChannelEmpty:
                waiter = asyncio.get_running_loop().create_future()
                self._state.recv_waiters.append(waiter)
                await waiter

    def close(self) -> None:
        """Stops new sends; values already queued can still be received."""
        s = self._state
        s.rx_closed = True
        _wake(s.send_waiters)
        _wake(s.recv_waiters)

    def __len__(self) -> int:
        return len(self._state.queue)

    async def __aiter__(self) -> AsyncIterator[T]:
        while True:
            try:
                yield await self.recv()
            except ChannelClosed:
                return


def mpsc_channel(capacity: Optional[int] = None) -> Tuple[Sender[Any], Receiver[Any]]:
    """Creates an mpsc channel: unbounded when ``capacity`` is None, otherwise
    holding at most ``capacity`` values.

    Raises :class:`InvalidCapacity` when ``capacity`` is not an integer of at least 1.
    """
    if capacity is not None and (isinstance(capacity, bool) or not isinstance(capacity, int) or capacity < 1):
        raise InvalidCapacity("capacity must be an integer of at least 1")
    state: _Mpsc[Any] = _Mpsc(capacity)
    return Sender(state), Receiver(state)


class _Oneshot(Generic[T]):
    __slots__ = ("value", "has_value", "tx_used", "tx_dropped", "rx_closed", "taken", "waiters")

    def __init__(self) -> None:
        self.value: Optional[T] = None
        self.has_value = False
        self.tx_used = False
        self.tx_dropped = False
        self.rx_closed = False
        self.taken = False
        self.waiters: List["asyncio.Future[None]"] = []


class OneshotSender(Generic[T]):
    """Sending half of a oneshot channel."""

    def __init__(self, state: _Oneshot[T]) -> None:
        self._state = state

    def send(self, value: T) -> None:
        """Sends the value. Any attempt uses the sender up.

        Raises :class:`AlreadySent` on a second attempt and :class:`ChannelClosed`
        when the receiver is closed.
        """
        s = self._state
        if s.tx_used:
            raise AlreadySent("oneshot sender was already used")
        s.tx_used = True
        if s.rx_closed:
            raise ChannelClosed("channel is closed")
        s.value, s.has_value = value, True
        _wake(s.waiters)

    def close(self) -> None:
        """Drops the sender without sending. Idempotent."""
        self._state.tx_dropped = True
        _wake(self._state.waiters)

    @property
    def is_closed(self) -> bool:
        """True when the receiver is closed."""
        return self._state.rx_closed


class OneshotReceiver(Generic[T]):
    """Receiving half of a oneshot channel. ``await rx`` is ``await rx.recv()``."""

    def __init__(self, state: _Oneshot[T]) -> None:
        self._state = state

    def try_recv(self) -> T:
        """Takes the value without waiting; raises :class:`ChannelEmpty` or :class:`ChannelClosed`."""
        s = self._state
        if s.has_value and not s.taken:
            s.taken = True
            value, s.value = s.value, None
            return value  # type: ignore[return-value]
        if s.taken or s.rx_closed or s.tx_dropped:
            raise ChannelClosed("channel is closed")
        raise ChannelEmpty("no value yet")

    async def recv(self) -> T:
        """Waits for the value; raises :class:`ChannelClosed` when none can arrive."""
        while True:
            try:
                return self.try_recv()
            except ChannelEmpty:
                waiter = asyncio.get_running_loop().create_future()
                self._state.waiters.append(waiter)
                await waiter

    def close(self) -> None:
        """Stops the sender from sending; a value sent earlier can still be taken."""
        self._state.rx_closed = True

    def __await__(self):  # type: ignore[no-untyped-def]
        return self.recv().__await__()


def oneshot_channel() -> Tuple[OneshotSender[Any], OneshotReceiver[Any]]:
    """Creates a oneshot channel."""
    state: _Oneshot[Any] = _Oneshot()
    return OneshotSender(state), OneshotReceiver(state)
