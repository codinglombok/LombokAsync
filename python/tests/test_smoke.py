"""Smoke tests for LombokAsync Python."""

import asyncio
import time
import pytest
from lombokasync import (
    run, spawn, sleep, timeout, interval,
    mpsc_channel, oneshot_channel,
    join, join3, join_all, select,
)


class TestSleep:
    def test_sleep(self):
        async def _test():
            t0 = time.monotonic()
            await sleep(0.05)
            elapsed = time.monotonic() - t0
            assert elapsed >= 0.04

        run(_test())


class TestTimeout:
    def test_timeout_ok(self):
        async def _test():
            result = await timeout(1.0, asyncio.sleep(0, result=42))
            assert result == 42

        run(_test())

    def test_timeout_expired(self):
        async def _test():
            result = await timeout(0.01, asyncio.sleep(10))
            assert result is None

        run(_test())


class TestSpawn:
    def test_spawn(self):
        async def _test():
            handle = spawn(compute())
            result = await handle
            assert result == 30

        async def compute():
            return 10 + 20

        run(_test())

    def test_multiple_spawns(self):
        async def _test():
            h1 = spawn(make(1))
            h2 = spawn(make(2))
            h3 = spawn(make(3))
            r1 = await h1
            r2 = await h2
            r3 = await h3
            assert r1 + r2 + r3 == 6

        async def make(n):
            return n

        run(_test())


class TestMpscChannel:
    def test_send_recv(self):
        async def _test():
            tx, rx = mpsc_channel()
            tx.send(1)
            tx.send(2)
            v1 = await rx.recv()
            v2 = await rx.recv()
            assert v1 == 1
            assert v2 == 2

        run(_test())

    def test_close(self):
        async def _test():
            tx, rx = mpsc_channel()
            tx.send(99)
            tx.close()
            v1 = await rx.recv()
            v2 = await rx.recv()
            assert v1 == 99
            assert v2 is None

        run(_test())


class TestOneshotChannel:
    def test_oneshot(self):
        async def _test():
            tx, rx = oneshot_channel()
            tx.send(42)
            val = await rx.recv()
            assert val == 42

        run(_test())


class TestJoin:
    def test_join(self):
        async def _test():
            a, b = await join(ret(1), ret(2))
            assert a == 1
            assert b == 2

        async def ret(n):
            return n

        run(_test())

    def test_join3(self):
        async def _test():
            a, b, c = await join3(ret(1), ret(2), ret(3))
            assert a + b + c == 6

        async def ret(n):
            return n

        run(_test())

    def test_join_all(self):
        async def _test():
            results = await join_all([ret(1), ret(2), ret(3)])
            assert results == [1, 2, 3]

        async def ret(n):
            return n

        run(_test())


class TestSelect:
    def test_select(self):
        async def _test():
            result = await select(ret(42), slow())
            assert result.kind == 'left'
            assert result.value == 42

        async def ret(n):
            return n

        async def slow():
            await asyncio.sleep(10)
            return 99

        run(_test())


class TestInterval:
    def test_interval(self):
        async def _test():
            count = 0
            async for _tick in interval(0.02):
                count += 1
                if count >= 3:
                    break
            assert count == 3

        run(_test())
