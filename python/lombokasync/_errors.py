"""Errors with the codes shared by every LombokAsync port (SPEC section 2)."""

from __future__ import annotations


class AsyncError(Exception):
    """Base class; ``code`` is stable across ports."""

    code = ""

    def __init__(self, message: str) -> None:
        super().__init__(f"{self.code}: {message}")


class Timeout(AsyncError, TimeoutError):
    """The deadline of :func:`lombokasync.timeout` elapsed."""

    code = "TIMEOUT"


class ChannelClosed(AsyncError):
    """The channel is closed (outcome ``closed``)."""

    code = "CLOSED"


class ChannelFull(AsyncError):
    """A bounded channel is full (outcome ``full``)."""

    code = "FULL"


class ChannelEmpty(AsyncError):
    """No value is waiting yet (outcome ``empty``)."""

    code = "EMPTY"


class AlreadySent(AsyncError):
    """The oneshot sender was already used (outcome ``already_sent``)."""

    code = "ALREADY_SENT"


class InvalidCapacity(AsyncError, ValueError):
    """The capacity is not an integer of at least 1."""

    code = "INVALID_CAPACITY"
