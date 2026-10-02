import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
    AsyncError,
    interval,
    join,
    join3,
    joinAll,
    mpscChannel,
    oneshotChannel,
    select,
    sleep,
    spawn,
    timeout,
    yieldNow,
} from '../src/index.js';

test('sleep waits and can be aborted', async () => {
    const t0 = Date.now();
    await sleep(20);
    assert.ok(Date.now() - t0 >= 15);
    const ac = new AbortController();
    const p = sleep(10_000, ac.signal);
    ac.abort(new Error('stop'));
    await assert.rejects(p, /stop/);
    await assert.rejects(sleep(10, AbortSignal.abort(new Error('early'))), /early/);
    const ac2 = new AbortController();
    await sleep(1, ac2.signal);
});

test('timeout resolves, rejects with TIMEOUT, passes errors, accepts a function', async () => {
    assert.equal(await timeout(100, Promise.resolve(1)), 1);
    assert.equal(await timeout(100, async () => 2), 2);
    await assert.rejects(timeout(10, sleep(1000)), (e: unknown) => e instanceof AsyncError && e.code === 'TIMEOUT' && e.message.startsWith('TIMEOUT:'));
    await assert.rejects(timeout(100, Promise.reject(new Error('boom'))), /boom/);
});

test('interval ticks in order and validates the period', async () => {
    const ticks: number[] = [];
    for await (const n of interval(5)) {
        ticks.push(n);
        if (n === 2) break;
    }
    assert.deepEqual(ticks, [0, 1, 2]);
    await assert.rejects(interval(0).next(), RangeError);
});

test('spawn runs later and reports finished', async () => {
    const order: string[] = [];
    const h = spawn(() => {
        order.push('task');
        return 7;
    });
    order.push('caller');
    assert.equal(h.finished, false);
    assert.equal(await h.promise, 7);
    assert.equal(h.finished, true);
    assert.deepEqual(order, ['caller', 'task']);
    const bad = spawn(async () => {
        throw new Error('x');
    });
    await assert.rejects(bad.promise, /x/);
});

test('mpsc: async iteration ends when every sender closes', async () => {
    const [tx, rx] = mpscChannel<number>();
    const tx2 = tx.clone();
    void (async () => {
        for (let i = 0; i < 3; i++) {
            await yieldNow();
            await tx.send(i);
        }
        tx.close();
    })();
    void (async () => {
        await tx2.send(100);
        tx2.close();
    })();
    const got: number[] = [];
    for await (const v of rx) got.push(v);
    assert.deepEqual(got.sort((a, b) => a - b), [0, 1, 2, 100]);
});

test('mpsc: bounded send waits for space', async () => {
    const [tx, rx] = mpscChannel<number>(1);
    const producer = (async () => {
        for (let i = 0; i < 5; i++) await tx.send(i);
        tx.close();
    })();
    const got: number[] = [];
    for (;;) {
        const r = await rx.recv();
        if (r.done) break;
        got.push(r.value);
    }
    await producer;
    assert.deepEqual(got, [0, 1, 2, 3, 4]);
});

test('mpsc: a receive wakes a blocked sender', async () => {
    const [tx, rx] = mpscChannel<number>(1);
    assert.equal(tx.trySend(1), 'ok');
    let sent = false;
    const pending = tx.send(2).then(() => {
        sent = true;
    });
    await yieldNow();
    assert.equal(sent, false);
    assert.deepEqual(rx.tryRecv(), { status: 'value', value: 1 });
    await pending;
    assert.equal(rx.length, 1);
});

test('mpsc: dropping the last sender wakes a waiting receiver', async () => {
    const [tx, rx] = mpscChannel<number>();
    const waiting = rx.recv();
    await yieldNow();
    tx.close();
    assert.deepEqual(await waiting, { done: true });
});

test('mpsc: closing the receiver fails pending sends and wakes recv', async () => {
    const [tx, rx] = mpscChannel<number>(1);
    assert.equal(tx.trySend(1), 'ok');
    const pending = tx.send(2);
    rx.close();
    assert.equal(tx.isClosed, true);
    await assert.rejects(pending, (e: unknown) => e instanceof AsyncError && e.code === 'CLOSED');
    const [tx2, rx2] = mpscChannel<number>();
    const waiting = rx2.recv();
    rx2.close();
    assert.deepEqual(await waiting, { done: true });
    assert.equal(tx2.trySend(1), 'closed');
});

test('mpsc: closed sender cannot be used; close is idempotent', () => {
    const [tx] = mpscChannel<number>();
    tx.close();
    tx.close();
    assert.throws(() => tx.trySend(1), (e: unknown) => e instanceof AsyncError && e.code === 'CLOSED');
    assert.throws(() => tx.clone(), AsyncError);
});

test('mpsc: invalid capacities', () => {
    for (const c of [0, -1, 1.5, Number.NaN]) {
        assert.throws(() => mpscChannel(c), (e: unknown) => e instanceof AsyncError && e.code === 'INVALID_CAPACITY');
    }
    assert.doesNotThrow(() => mpscChannel(null));
});

test('mpsc: long queues compact without losing order', () => {
    const [tx, rx] = mpscChannel<number>();
    for (let i = 0; i < 5000; i++) tx.trySend(i);
    for (let i = 0; i < 5000; i++) {
        const r = rx.tryRecv();
        assert.ok(r.status === 'value' && r.value === i);
        if (i % 1000 === 0) tx.trySend(-1);
    }
    assert.equal(rx.length, 5);
});

test('oneshot: recv waits for the value or the drop', async () => {
    const [tx, rx] = oneshotChannel<string>();
    setTimeout(() => tx.send('hi'), 5);
    assert.equal(await rx.recv(), 'hi');
    await assert.rejects(rx.recv(), (e: unknown) => e instanceof AsyncError && e.code === 'CLOSED');
    const [tx2, rx2] = oneshotChannel<string>();
    setTimeout(() => tx2.close(), 5);
    await assert.rejects(rx2.recv(), AsyncError);
    const [tx3, rx3] = oneshotChannel<string>();
    rx3.close();
    assert.equal(tx3.isClosed, true);
    assert.equal(tx3.send('x'), 'closed');
});

test('combinators', async () => {
    assert.deepEqual(await join(Promise.resolve(1), Promise.resolve('a')), [1, 'a']);
    assert.deepEqual(await join3(Promise.resolve(1), Promise.resolve(2), Promise.resolve(3)), [1, 2, 3]);
    assert.deepEqual(await joinAll([]), []);
    await assert.rejects(joinAll([sleep(5).then(() => Promise.reject(new Error('late'))), Promise.reject(new Error('early'))]), /late/);
    assert.deepEqual(await select([sleep(50).then(() => 1), sleep(1).then(() => 2)]), { index: 1, value: 2 });
    await assert.rejects(select([]), RangeError);
});

test('AsyncError shape', () => {
    const e = new AsyncError('ALREADY_SENT', 'x');
    assert.equal(e.name, 'AsyncError');
    assert.equal(e.code, 'ALREADY_SENT');
    assert.equal(e.message, 'ALREADY_SENT: x');
    assert.ok(e instanceof Error);
});
