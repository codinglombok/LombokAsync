/**
 * LombokAsync for TypeScript/JavaScript: timers, mpsc and oneshot channels,
 * and combinators on top of native promises. Behaviour shared with the other
 * ports is specified in docs/SPEC_LombokAsync_v0.2.0.md.
 */

/** Error codes shared by every LombokAsync port (SPEC section 2). */
export type AsyncErrorCode = 'TIMEOUT' | 'CLOSED' | 'INVALID_CAPACITY' | 'ALREADY_SENT';

/** Error thrown by LombokAsync; `code` is stable across ports. */
export class AsyncError extends Error {
    readonly code: AsyncErrorCode;

    constructor(code: AsyncErrorCode, message: string) {
        super(`${code}: ${message}`);
        this.name = 'AsyncError';
        this.code = code;
    }
}

// --- Timers -----------------------------------------------------------------

/** Resolves after `ms` milliseconds. Rejects with the signal's reason when aborted. */
export function sleep(ms: number, signal?: AbortSignal): Promise<void> {
    return new Promise((resolve, reject) => {
        if (signal?.aborted) {
            reject(signal.reason);
            return;
        }
        const onAbort = (): void => {
            clearTimeout(timer);
            reject(signal!.reason);
        };
        const timer = setTimeout(() => {
            signal?.removeEventListener('abort', onAbort);
            resolve();
        }, ms);
        signal?.addEventListener('abort', onAbort, { once: true });
    });
}

/** Lets other queued callbacks run before continuing (one macrotask turn). */
export function yieldNow(): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, 0));
}

/**
 * Resolves with the value of `task` or rejects with `AsyncError('TIMEOUT')`
 * after `ms` milliseconds. A function is called to create the task. A task
 * error passes through unchanged. The timer is always cleared.
 */
export function timeout<T>(ms: number, task: Promise<T> | (() => Promise<T>)): Promise<T> {
    const promise = typeof task === 'function' ? task() : task;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const deadline = new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new AsyncError('TIMEOUT', `deadline of ${ms} ms elapsed`)), ms);
    });
    return Promise.race([promise, deadline]).finally(() => clearTimeout(timer));
}

/**
 * Yields 0, 1, 2, ... every `ms` milliseconds. Ticks are scheduled at fixed
 * multiples of the period, so delays do not accumulate. Stop with `break`.
 */
export async function* interval(ms: number): AsyncGenerator<number, void, void> {
    if (!(ms > 0)) throw new RangeError('interval period must be greater than zero');
    const start = Date.now();
    for (let n = 0; ; n++) {
        await sleep(Math.max(0, start + (n + 1) * ms - Date.now()));
        yield n;
    }
}

// --- Tasks ------------------------------------------------------------------

/** Handle to a task started with {@link spawn}. */
export interface JoinHandle<T> {
    /** Settles with the task's result. */
    readonly promise: Promise<T>;
    /** True once the task has settled. */
    readonly finished: boolean;
}

/** Starts `fn` on the next macrotask and returns a handle to its result. */
export function spawn<T>(fn: () => Promise<T> | T): JoinHandle<T> {
    let finished = false;
    const promise = yieldNow().then(fn).finally(() => {
        finished = true;
    });
    // The caller decides whether to observe failures; avoid unhandled-rejection noise for the handle itself.
    promise.catch(() => undefined);
    return {
        promise,
        get finished() {
            return finished;
        },
    };
}

// --- Channels ---------------------------------------------------------------

/** Outcome of a non-blocking send (SPEC section 3.2). */
export type SendStatus = 'ok' | 'full' | 'closed';

/** Outcome of a non-blocking receive (SPEC section 3.3). */
export type TryRecvResult<T> = { status: 'value'; value: T } | { status: 'empty' } | { status: 'closed' };

/** Outcome of a waiting receive: a value, or `done` once the channel is closed and empty. */
export type RecvResult<T> = { done: false; value: T } | { done: true };

interface MpscState<T> {
    queue: T[];
    head: number;
    capacity: number | null;
    senders: number;
    rxClosed: boolean;
    recvWaiter: (() => void) | null;
    sendWaiters: (() => void)[];
}

function queued<T>(s: MpscState<T>): number {
    return s.queue.length - s.head;
}

function wakeSenders<T>(s: MpscState<T>): void {
    const waiters = s.sendWaiters;
    s.sendWaiters = [];
    for (const w of waiters) w();
}

function wakeReceiver<T>(s: MpscState<T>): void {
    const w = s.recvWaiter;
    s.recvWaiter = null;
    w?.();
}

/** Sending half of an mpsc channel. */
export class Sender<T> {
    #state: MpscState<T>;
    #dropped = false;

    /** @internal */
    constructor(state: MpscState<T>) {
        this.#state = state;
        state.senders++;
    }

    /** Queues `value` without waiting. */
    trySend(value: T): SendStatus {
        this.#alive();
        const s = this.#state;
        if (s.rxClosed) return 'closed';
        if (s.capacity !== null && queued(s) >= s.capacity) return 'full';
        s.queue.push(value);
        wakeReceiver(s);
        return 'ok';
    }

    /** Queues `value`, waiting for space; rejects with `AsyncError('CLOSED')` if the receiver closes. */
    async send(value: T): Promise<void> {
        for (;;) {
            const r = this.trySend(value);
            if (r === 'ok') return;
            if (r === 'closed') throw new AsyncError('CLOSED', 'channel is closed');
            await new Promise<void>(resolve => this.#state.sendWaiters.push(resolve));
        }
    }

    /** Returns another sender for the same channel. */
    clone(): Sender<T> {
        this.#alive();
        return new Sender(this.#state);
    }

    /** Drops this sender. When the last sender is dropped the channel closes. Idempotent. */
    close(): void {
        if (this.#dropped) return;
        this.#dropped = true;
        const s = this.#state;
        s.senders--;
        if (s.senders === 0) wakeReceiver(s);
    }

    /** True when the receiver is closed. */
    get isClosed(): boolean {
        return this.#state.rxClosed;
    }

    #alive(): void {
        if (this.#dropped) throw new AsyncError('CLOSED', 'sender was closed');
    }
}

/** Receiving half of an mpsc channel. Iterate it with `for await`. */
export class Receiver<T> {
    #state: MpscState<T>;

    /** @internal */
    constructor(state: MpscState<T>) {
        this.#state = state;
    }

    /** Takes the next value without waiting. */
    tryRecv(): TryRecvResult<T> {
        const s = this.#state;
        if (queued(s) > 0) {
            const value = s.queue[s.head];
            s.queue[s.head] = undefined as T;
            s.head++;
            if (s.head > 1024 && s.head * 2 > s.queue.length) {
                s.queue = s.queue.slice(s.head);
                s.head = 0;
            }
            wakeSenders(s);
            return { status: 'value', value };
        }
        if (s.rxClosed || s.senders === 0) return { status: 'closed' };
        return { status: 'empty' };
    }

    /** Waits for the next value; `{ done: true }` once closed and empty. */
    async recv(): Promise<RecvResult<T>> {
        for (;;) {
            const r = this.tryRecv();
            if (r.status === 'value') return { done: false, value: r.value };
            if (r.status === 'closed') return { done: true };
            await new Promise<void>(resolve => {
                this.#state.recvWaiter = resolve;
            });
        }
    }

    /** Stops new sends; values already queued can still be received. */
    close(): void {
        this.#state.rxClosed = true;
        wakeSenders(this.#state);
        wakeReceiver(this.#state);
    }

    /** Number of queued values. */
    get length(): number {
        return queued(this.#state);
    }

    async *[Symbol.asyncIterator](): AsyncIterableIterator<T> {
        for (;;) {
            const r = await this.recv();
            if (r.done) return;
            yield r.value;
        }
    }
}

/**
 * Creates an mpsc channel. Without `capacity` it is unbounded; otherwise it
 * holds at most `capacity` values. Throws `AsyncError('INVALID_CAPACITY')`
 * when `capacity` is not an integer of at least 1.
 */
export function mpscChannel<T>(capacity?: number | null): [Sender<T>, Receiver<T>] {
    if (capacity !== undefined && capacity !== null && !(Number.isInteger(capacity) && capacity >= 1)) {
        throw new AsyncError('INVALID_CAPACITY', 'capacity must be an integer of at least 1');
    }
    const state: MpscState<T> = {
        queue: [],
        head: 0,
        capacity: capacity ?? null,
        senders: 0,
        rxClosed: false,
        recvWaiter: null,
        sendWaiters: [],
    };
    return [new Sender(state), new Receiver(state)];
}

interface OneshotState<T> {
    value: T | undefined;
    hasValue: boolean;
    txUsed: boolean;
    txDropped: boolean;
    rxClosed: boolean;
    taken: boolean;
    waiter: (() => void) | null;
}

/** Outcome of a oneshot send (SPEC section 4). */
export type OneshotSendStatus = 'ok' | 'closed' | 'already_sent';

/** Sending half of a oneshot channel. */
export class OneshotSender<T> {
    #state: OneshotState<T>;

    /** @internal */
    constructor(state: OneshotState<T>) {
        this.#state = state;
    }

    /** Sends the value. Any attempt uses the sender up; later attempts return `already_sent`. */
    send(value: T): OneshotSendStatus {
        const s = this.#state;
        if (s.txUsed) return 'already_sent';
        s.txUsed = true;
        if (s.rxClosed) return 'closed';
        s.value = value;
        s.hasValue = true;
        s.waiter?.();
        return 'ok';
    }

    /** Drops the sender without sending. Idempotent. */
    close(): void {
        this.#state.txDropped = true;
        this.#state.waiter?.();
    }

    /** True when the receiver is closed. */
    get isClosed(): boolean {
        return this.#state.rxClosed;
    }
}

/** Receiving half of a oneshot channel. */
export class OneshotReceiver<T> {
    #state: OneshotState<T>;

    /** @internal */
    constructor(state: OneshotState<T>) {
        this.#state = state;
    }

    /** Takes the value without waiting. */
    tryRecv(): TryRecvResult<T> {
        const s = this.#state;
        if (s.hasValue && !s.taken) {
            s.taken = true;
            const value = s.value as T;
            s.value = undefined;
            return { status: 'value', value };
        }
        if (s.taken || s.rxClosed || s.txDropped) return { status: 'closed' };
        return { status: 'empty' };
    }

    /** Waits for the value; rejects with `AsyncError('CLOSED')` when none can arrive. */
    async recv(): Promise<T> {
        for (;;) {
            const r = this.tryRecv();
            if (r.status === 'value') return r.value;
            if (r.status === 'closed') throw new AsyncError('CLOSED', 'channel is closed');
            await new Promise<void>(resolve => {
                this.#state.waiter = resolve;
            });
        }
    }

    /** Stops the sender from sending; a value sent earlier can still be taken. */
    close(): void {
        this.#state.rxClosed = true;
    }
}

/** Creates a oneshot channel. */
export function oneshotChannel<T>(): [OneshotSender<T>, OneshotReceiver<T>] {
    const state: OneshotState<T> = {
        value: undefined,
        hasValue: false,
        txUsed: false,
        txDropped: false,
        rxClosed: false,
        taken: false,
        waiter: null,
    };
    return [new OneshotSender(state), new OneshotReceiver(state)];
}

// --- Combinators ------------------------------------------------------------

/**
 * Waits for every promise, then resolves with the values in input order or
 * rejects with the reason of the lowest-index promise that rejected (not the
 * earliest one).
 */
export async function joinAll<T>(promises: readonly Promise<T>[]): Promise<T[]> {
    const settled = await Promise.allSettled(promises);
    const failed = settled.find((r): r is PromiseRejectedResult => r.status === 'rejected');
    if (failed) throw failed.reason;
    return settled.map(r => (r as PromiseFulfilledResult<T>).value);
}

/** {@link joinAll} for two promises. */
export async function join<A, B>(a: Promise<A>, b: Promise<B>): Promise<[A, B]> {
    return (await joinAll<unknown>([a, b])) as [A, B];
}

/** {@link joinAll} for three promises. */
export async function join3<A, B, C>(a: Promise<A>, b: Promise<B>, c: Promise<C>): Promise<[A, B, C]> {
    return (await joinAll<unknown>([a, b, c])) as [A, B, C];
}

/** Result of {@link select}: the index of the winner and its value. */
export interface Selected<T> {
    index: number;
    value: T;
}

/**
 * Settles with the first promise to settle: `{ index, value }`, or the
 * rejection reason. When several are already settled, the lowest index wins.
 * Throws `RangeError` for an empty list.
 */
export function select<T>(promises: readonly Promise<T>[]): Promise<Selected<T>> {
    if (promises.length === 0) return Promise.reject(new RangeError('select needs at least one promise'));
    return Promise.race(promises.map((p, index) => Promise.resolve(p).then(value => ({ index, value }))));
}
