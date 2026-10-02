import asyncio
import time

import pytest

import lombokasync as la


def run(coro):
    return la.run(coro)


def test_run_spawn_join_handle():
    async def main():
        h = la.spawn(asyncio.sleep(0, result=5))
        assert not h.done()
        assert await h == 5
        assert h.done()
        slow = la.spawn(asyncio.sleep(10))
        assert slow.cancel()
        with pytest.raises(asyncio.CancelledError):
            await slow
        return "done"

    assert run(main()) == "done"


def test_spawn_outside_loop_raises():
    coro = asyncio.sleep(0)
    with pytest.raises(RuntimeError):
        la.spawn(coro)
    coro.close()


def test_sleep_and_timeout():
    async def main():
        t0 = time.monotonic()
        await la.sleep(0.02)
        assert time.monotonic() - t0 > 0  # asyncio may wake up to one clock tick early (about 15.6 ms on Windows)
        assert await la.timeout(1, asyncio.sleep(0, result=3)) == 3
        with pytest.raises(la.Timeout) as ei:
            await la.timeout(0.01, asyncio.sleep(5))
        assert ei.value.code == "TIMEOUT"
        assert isinstance(ei.value, TimeoutError)
        assert str(ei.value).startswith("TIMEOUT:")
        with pytest.raises(KeyError):
            await la.timeout(1, _raise(KeyError("k")))

    run(main())


async def _raise(exc):
    raise exc


def test_interval():
    async def main():
        ticks = []
        async for n in la.interval(0.005):
            ticks.append(n)
            if n == 2:
                break
        assert ticks == [0, 1, 2]
        with pytest.raises(ValueError):
            async for _ in la.interval(0):
                pass

    run(main())


def test_mpsc_async_iteration_and_clones():
    async def main():
        tx, rx = la.mpsc_channel()
        tx2 = tx.clone()

        async def produce(s, values):
            for v in values:
                await la.yield_now()
                await s.send(v)
            s.close()

        la.spawn(produce(tx, [0, 1, 2]))
        la.spawn(produce(tx2, [100]))
        return sorted([v async for v in rx])

    assert run(main()) == [0, 1, 2, 100]


def test_mpsc_bounded_send_waits():
    async def main():
        tx, rx = la.mpsc_channel(1)

        async def produce():
            for i in range(5):
                await tx.send(i)
            tx.close()

        p = la.spawn(produce())
        got = [v async for v in rx]
        await p
        return got

    assert run(main()) == [0, 1, 2, 3, 4]


def test_mpsc_close_wakes_waiters():
    async def main():
        tx, rx = la.mpsc_channel(1)
        tx.try_send(1)
        pending = la.spawn(tx.send(2))
        waiting_rx_tx, waiting_rx = la.mpsc_channel()
        recv = la.spawn(waiting_rx.recv())
        await la.yield_now()
        rx.close()
        waiting_rx.close()
        assert tx.is_closed
        with pytest.raises(la.ChannelClosed):
            await pending
        with pytest.raises(la.ChannelClosed):
            await recv
        assert len(rx) == 1
        waiting_rx_tx.close()

    run(main())


def test_mpsc_closed_sender_and_capacity_errors():
    tx, rx = la.mpsc_channel()
    tx.close()
    tx.close()
    with pytest.raises(la.ChannelClosed):
        tx.try_send(1)
    with pytest.raises(la.ChannelClosed):
        tx.clone()
    for bad in (0, -1, 1.5, True, "2"):
        with pytest.raises(la.InvalidCapacity) as ei:
            la.mpsc_channel(bad)
        assert isinstance(ei.value, ValueError)
        assert ei.value.code == "INVALID_CAPACITY"


def test_oneshot():
    async def main():
        tx, rx = la.oneshot_channel()
        asyncio.get_running_loop().call_later(0.005, tx.send, "hi")
        assert await rx == "hi"
        with pytest.raises(la.ChannelClosed):
            await rx.recv()
        tx2, rx2 = la.oneshot_channel()
        asyncio.get_running_loop().call_later(0.005, tx2.close)
        with pytest.raises(la.ChannelClosed):
            await rx2.recv()
        tx3, rx3 = la.oneshot_channel()
        rx3.close()
        assert tx3.is_closed
        with pytest.raises(la.ChannelClosed):
            tx3.send(1)
        with pytest.raises(la.AlreadySent) as ei:
            tx3.send(2)
        assert ei.value.code == "ALREADY_SENT"

    run(main())


def test_combinators():
    async def late_error():
        await asyncio.sleep(0.01)
        raise ValueError("late")

    async def early_error():
        raise KeyError("early")

    async def main():
        assert await la.join(asyncio.sleep(0, 1), asyncio.sleep(0, "a")) == (1, "a")
        assert await la.join3(asyncio.sleep(0, 1), asyncio.sleep(0, 2), asyncio.sleep(0, 3)) == (1, 2, 3)
        assert await la.join_all([]) == []
        with pytest.raises(ValueError):
            await la.join_all([late_error(), early_error()])
        r = await la.select([asyncio.sleep(0.05, 1), asyncio.sleep(0.001, 2)])
        assert r == la.Selected(1, 2)
        assert (r.index, r.value) == (1, 2)
        with pytest.raises(ValueError):
            await la.select([])

    run(main())


def test_error_hierarchy():
    for cls, code in [(la.ChannelFull, "FULL"), (la.ChannelEmpty, "EMPTY"), (la.ChannelClosed, "CLOSED")]:
        e = cls("x")
        assert isinstance(e, la.AsyncError)
        assert e.code == code
        assert str(e) == f"{code}: x"
    assert la.__version__ == "0.2.0"
