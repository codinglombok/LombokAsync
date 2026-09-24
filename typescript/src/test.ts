/**
 * Smoke tests for LombokAsync TypeScript.
 */

import {
    sleep, timeout, interval, spawn,
    mpscChannel, oneshotChannel,
    join, join3, joinAll, select,
} from './index.js';

let pass = 0;
let fail = 0;

function assertEq<T>(a: T, b: T, name: string): void {
    if (a === b) {
        pass++;
    } else {
        fail++;
        console.log(`FAIL: ${name}`);
        console.log(`  expected: ${JSON.stringify(b)}`);
        console.log(`  got:      ${JSON.stringify(a)}`);
    }
}

async function run() {
    // --- Sleep ---
    const t0 = Date.now();
    await sleep(50);
    const elapsed = Date.now() - t0;
    assertEq(elapsed >= 40, true, 'sleep 50ms');

    // --- Timeout OK ---
    const val = await timeout(100, Promise.resolve(42));
    assertEq(val, 42, 'timeout ok');

    // --- Timeout expired ---
    const val2 = await timeout(10, sleep(200).then(() => 99));
    assertEq(val2, undefined, 'timeout expired');

    // --- Spawn ---
    const handle = spawn(async () => 10 + 20);
    const result = await handle.promise;
    assertEq(result, 30, 'spawn');

    // --- MPSC Channel ---
    const [tx, rx] = mpscChannel<number>();
    tx.send(1);
    tx.send(2);
    const v1 = await rx.recv();
    const v2 = await rx.recv();
    assertEq(v1, 1, 'mpsc recv 1');
    assertEq(v2, 2, 'mpsc recv 2');

    // --- MPSC close ---
    tx.close();
    const v3 = await rx.recv();
    assertEq(v3, undefined, 'mpsc closed');

    // --- Oneshot ---
    const [otx, orx] = oneshotChannel<number>();
    otx.send(42);
    const ov = await orx.recv();
    assertEq(ov, 42, 'oneshot');

    // --- Join ---
    const [ja, jb] = await join(Promise.resolve(1), Promise.resolve(2));
    assertEq(ja, 1, 'join a');
    assertEq(jb, 2, 'join b');

    // --- Join3 ---
    const [j3a, j3b, j3c] = await join3(
        Promise.resolve(1),
        Promise.resolve(2),
        Promise.resolve(3),
    );
    assertEq(j3a + j3b + j3c, 6, 'join3');

    // --- JoinAll ---
    const jall = await joinAll([
        Promise.resolve(1),
        Promise.resolve(2),
        Promise.resolve(3),
    ]);
    assertEq(jall.length, 3, 'joinAll length');
    assertEq(jall[0] + jall[1] + jall[2], 6, 'joinAll sum');

    // --- Select ---
    const sel = await select(Promise.resolve(42), sleep(100).then(() => 99));
    assertEq(sel.kind, 'left', 'select kind');
    if (sel.kind === 'left') {
        assertEq(sel.value, 42, 'select value');
    }

    // --- Interval ---
    let count = 0;
    for await (const _n of interval(20)) {
        count++;
        if (count >= 3) break;
    }
    assertEq(count, 3, 'interval 3 ticks');

    console.log(`\n${pass} passed, ${fail} failed`);
    process.exit(fail > 0 ? 1 : 0);
}

run().catch(err => {
    console.error(err);
    process.exit(1);
});
