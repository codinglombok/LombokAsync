// Runs the shared cross-language vectors (vectors/lombokasync-vectors-v1.json).
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { AsyncError, joinAll, mpscChannel, oneshotChannel, select, timeout, yieldNow } from '../src/index.js';
import type { Receiver, Sender } from '../src/index.js';

type V = number | string;
type Op = [string, ...unknown[]];
interface TaskSpec { yields?: number; value?: V; error?: string; never?: boolean }
interface Case { id: string; kind: string; ops?: Op[]; tasks?: TaskSpec[]; task?: TaskSpec; expected: unknown }

const path = fileURLToPath(new URL('../../../vectors/lombokasync-vectors-v1.json', import.meta.url));
const doc = JSON.parse(readFileSync(path, 'utf8')) as { timeout_deadline_ms: number; cases: Case[] };

function task(spec: TaskSpec): Promise<V> {
    if (spec.never) return new Promise(() => undefined);
    return (async () => {
        for (let i = 0; i < (spec.yields ?? 0); i++) await yieldNow();
        if (spec.error !== undefined) throw new Error(spec.error);
        return spec.value as V;
    })();
}

function runMpsc(ops: Op[]): unknown[] {
    const out: unknown[] = [];
    const senders: Sender<V>[] = [];
    let rx: Receiver<V> | undefined;
    for (const [name, a, b] of ops) {
        switch (name) {
            case 'new':
                try {
                    const [tx, r] = mpscChannel<V>(a as number | null);
                    senders.push(tx);
                    rx = r;
                    out.push('ok');
                } catch (e) {
                    assert.ok(e instanceof AsyncError && e.code === 'INVALID_CAPACITY');
                    out.push('invalid_capacity');
                    return out;
                }
                break;
            case 'clone':
                senders.push(senders[a as number].clone());
                out.push({ sender: senders.length - 1 });
                break;
            case 'send':
                out.push(senders[a as number].trySend(b as V));
                break;
            case 'recv': {
                const r = rx!.tryRecv();
                out.push(r.status === 'value' ? { value: r.value } : r.status);
                break;
            }
            case 'drop':
                senders[a as number].close();
                out.push('ok');
                break;
            case 'close':
                rx!.close();
                out.push('ok');
                break;
            case 'len':
                out.push({ len: rx!.length });
                break;
            default:
                throw new Error(`unknown op ${name}`);
        }
    }
    return out;
}

function runOneshot(ops: Op[]): unknown[] {
    const [tx, rx] = oneshotChannel<V>();
    return ops.map(([name, a]) => {
        switch (name) {
            case 'new':
                return 'ok';
            case 'send':
                return tx.send(a as V);
            case 'recv': {
                const r = rx.tryRecv();
                return r.status === 'value' ? { value: r.value } : r.status;
            }
            case 'drop_tx':
                tx.close();
                return 'ok';
            case 'close':
                rx.close();
                return 'ok';
            default:
                throw new Error(`unknown op ${name}`);
        }
    });
}

async function run(c: Case): Promise<unknown> {
    switch (c.kind) {
        case 'mpsc':
            return runMpsc(c.ops!);
        case 'oneshot':
            return runOneshot(c.ops!);
        case 'join_all':
            return joinAll(c.tasks!.map(task)).then(ok => ({ ok }), (e: Error) => ({ error: e.message }));
        case 'select':
            return select(c.tasks!.map(task)).then(r => ({ index: r.index, value: r.value }), (e: Error) => ({ error: e.message }));
        case 'timeout':
            return timeout(doc.timeout_deadline_ms, task(c.task!)).then(
                ok => ({ ok }),
                (e: Error) => (e instanceof AsyncError && e.code === 'TIMEOUT' ? 'timeout' : { error: e.message }),
            );
        default:
            throw new Error(`unknown kind ${c.kind}`);
    }
}

test('vector file has at least 100 cases', () => {
    assert.ok(doc.cases.length >= 100);
});

for (const c of doc.cases) {
    test(`vector ${c.id}`, async () => {
        assert.deepStrictEqual(await run(c), c.expected);
    });
}
