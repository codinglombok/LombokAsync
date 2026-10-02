/**
 * Differential fuzz: random byte strings are decoded into mpsc and oneshot
 * operation sequences, run against the library and against a plain model of
 * SPEC sections 3-4; any difference or exception is a crash.
 *
 * Run: npm run fuzz   (FUZZ_EXECUTIONS sets the number of executions)
 */
import { LombokFuzzer, FuzzMode, HarnessMode, FuzzEvent } from 'lombokfuzzer';
import { mpscChannel, oneshotChannel } from '../../src/index.js';
import type { Sender } from '../../src/index.js';

function mpscCase(data: Uint8Array): void {
    if (data.length === 0) return;
    const capByte = data[0] % 5;
    const cap = capByte === 0 ? null : capByte;
    const [tx0, rx] = mpscChannel<number>(cap);
    const senders: (Sender<number> | null)[] = [tx0];
    // model
    const queue: number[] = [];
    let live = 1;
    let rxClosed = false;
    for (let i = 1; i < data.length; i++) {
        const b = data[i];
        const pick = senders.length ? (b >> 3) % senders.length : 0;
        switch (b % 6) {
            case 0: {
                const s = senders[pick];
                if (!s) break;
                const want = rxClosed ? 'closed' : cap !== null && queue.length >= cap ? 'full' : 'ok';
                if (want === 'ok') queue.push(i);
                const got = s.trySend(i);
                if (got !== want) throw new Error(`send: got ${got} want ${want} at ${i}`);
                break;
            }
            case 1: {
                const got = rx.tryRecv();
                const want = queue.length ? 'value' : rxClosed || live === 0 ? 'closed' : 'empty';
                if (got.status !== want) throw new Error(`recv: got ${got.status} want ${want} at ${i}`);
                if (got.status === 'value' && got.value !== queue.shift()) throw new Error(`recv value mismatch at ${i}`);
                break;
            }
            case 2: {
                const s = senders[pick];
                if (!s || senders.length > 8) break;
                senders.push(s.clone());
                live++;
                break;
            }
            case 3: {
                const s = senders[pick];
                if (!s) break;
                s.close();
                senders[pick] = null;
                live--;
                break;
            }
            case 4:
                rx.close();
                rxClosed = true;
                break;
            default:
                if (rx.length !== queue.length) throw new Error(`len: got ${rx.length} want ${queue.length}`);
        }
    }
}

function oneshotCase(data: Uint8Array): void {
    const [tx, rx] = oneshotChannel<number>();
    let has = false;
    let used = false;
    let dropped = false;
    let closed = false;
    let taken = false;
    for (let i = 0; i < data.length; i++) {
        switch (data[i] % 4) {
            case 0: {
                if (dropped) break;
                const want = used ? 'already_sent' : closed ? 'closed' : 'ok';
                if (!used) {
                    used = true;
                    if (!closed) has = true;
                }
                const got = tx.send(i);
                if (got !== want) throw new Error(`oneshot send: got ${got} want ${want}`);
                break;
            }
            case 1: {
                const got = rx.tryRecv().status;
                const want = has && !taken ? 'value' : taken || closed || dropped ? 'closed' : 'empty';
                if (want === 'value') taken = true;
                if (got !== want) throw new Error(`oneshot recv: got ${got} want ${want}`);
                break;
            }
            case 2:
                tx.close();
                dropped = true;
                break;
            default:
                rx.close();
                closed = true;
        }
    }
}

const fuzzer = new LombokFuzzer({
    name: 'lombokasync-channels',
    mode: FuzzMode.Mutation,
    maxExecutions: Number(process.env.FUZZ_EXECUTIONS ?? 50_000),
    maxInputSize: 4096,
    timeoutMs: 2_000,
    harness: {
        mode: HarnessMode.InProcess,
        targetFunction: (data: Uint8Array) => {
            mpscCase(data);
            oneshotCase(data);
        },
    },
});

for (const s of [[0, 0, 1, 0, 1, 1], [2, 0, 0, 0, 1, 5, 4, 0, 1], [1, 2, 0, 3, 0, 3, 1], [0, 1, 0, 3, 1, 2]]) {
    fuzzer.addSeed(new Uint8Array(s));
}

let crashes = 0;
fuzzer.on(FuzzEvent.CrashFound, ({ crash }) => {
    crashes++;
    console.error(`[CRASH] ${crash.id} ${crash.category}: ${JSON.stringify(Array.from(crash.input.data.slice(0, 64)))}`);
});

const stats = await fuzzer.run();
console.log(`executions: ${stats.totalExecutions}, crashes: ${crashes}`);
if (crashes > 0) process.exit(1);
