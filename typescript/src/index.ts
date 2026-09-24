/**
 * LombokAsync — lightweight async utilities for TypeScript/JavaScript.
 *
 * Uses native Promise/async-await with ergonomic task combinators,
 * timers, and channels.
 */

// --- Sleep / Timer ---

/** Returns a promise that resolves after the given milliseconds. */
export function sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
}

/** Wraps a promise with a timeout. Returns undefined if the deadline elapses. */
export async function timeout<T>(ms: number, promise: Promise<T>): Promise<T | undefined> {
    let timer: ReturnType<typeof setTimeout>;
    const timeoutPromise = new Promise<undefined>(resolve => {
        timer = setTimeout(() => resolve(undefined), ms);
    });
    try {
        const result = await Promise.race([promise, timeoutPromise]);
        clearTimeout(timer!);
        return result;
    } catch (err) {
        clearTimeout(timer!);
        throw err;
    }
}

/** Async generator that yields at regular intervals. */
export async function* interval(ms: number): AsyncGenerator<number, never, void> {
    let count = 0;
    while (true) {
        await sleep(ms);
        yield count++;
    }
}

// --- Spawn ---

export interface JoinHandle<T> {
    /** Wait for the task result. */
    readonly promise: Promise<T>;
}

/**
 * Spawn a task (microtask). Returns a JoinHandle with a promise to await.
 * The task starts immediately.
 */
export function spawn<T>(fn: () => Promise<T>): JoinHandle<T> {
    const promise = fn();
    return { promise };
}

// --- Channel: MPSC ---

export interface Sender<T> {
    send(value: T): void;
    close(): void;
}

export interface Receiver<T> {
    recv(): Promise<T | undefined>;
    [Symbol.asyncIterator](): AsyncIterableIterator<T>;
}

/** Create an unbounded mpsc (multi-producer, single-consumer) channel. */
export function mpscChannel<T>(): [Sender<T>, Receiver<T>] {
    const queue: T[] = [];
    let closed = false;
    let resolver: ((value: T | undefined) => void) | null = null;

    const sender: Sender<T> = {
        send(value: T): void {
            if (closed) throw new Error('channel closed');
            if (resolver) {
                const r = resolver;
                resolver = null;
                r(value);
            } else {
                queue.push(value);
            }
        },
        close(): void {
            closed = true;
            if (resolver) {
                const r = resolver;
                resolver = null;
                r(undefined);
            }
        },
    };

    const receiver: Receiver<T> = {
        recv(): Promise<T | undefined> {
            if (queue.length > 0) {
                return Promise.resolve(queue.shift()!);
            }
            if (closed) {
                return Promise.resolve(undefined);
            }
            return new Promise<T | undefined>(resolve => {
                resolver = resolve;
            });
        },
        async *[Symbol.asyncIterator](): AsyncIterableIterator<T> {
            while (true) {
                const val = await this.recv();
                if (val === undefined) break;
                yield val;
            }
        },
    };

    return [sender, receiver];
}

// --- Channel: Oneshot ---

export interface OneshotSender<T> {
    send(value: T): void;
}

export interface OneshotReceiver<T> {
    recv(): Promise<T | undefined>;
}

/** Create a oneshot channel — send exactly one value. */
export function oneshotChannel<T>(): [OneshotSender<T>, OneshotReceiver<T>] {
    let resolver: ((value: T | undefined) => void) | null = null;
    let result: T | undefined;
    let settled = false;

    const promise = new Promise<T | undefined>(resolve => {
        resolver = resolve;
    });

    const sender: OneshotSender<T> = {
        send(value: T): void {
            if (settled) throw new Error('oneshot already sent');
            settled = true;
            result = value;
            resolver!(value);
        },
    };

    const receiver: OneshotReceiver<T> = {
        recv(): Promise<T | undefined> {
            return promise;
        },
    };

    return [sender, receiver];
}

// --- Combinators ---

/** Type for select results. */
export type Either<A, B> =
    | { kind: 'left'; value: A }
    | { kind: 'right'; value: B };

/** Race two promises — returns whichever settles first. */
export async function select<A, B>(
    a: Promise<A>,
    b: Promise<B>,
): Promise<Either<A, B>> {
    return Promise.race([
        a.then(value => ({ kind: 'left' as const, value })),
        b.then(value => ({ kind: 'right' as const, value })),
    ]);
}

/** Join two promises — run concurrently, return both results. */
export async function join<A, B>(
    a: Promise<A>,
    b: Promise<B>,
): Promise<[A, B]> {
    return Promise.all([a, b]);
}

/** Join three promises concurrently. */
export async function join3<A, B, C>(
    a: Promise<A>,
    b: Promise<B>,
    c: Promise<C>,
): Promise<[A, B, C]> {
    return Promise.all([a, b, c]);
}

/** Join a list of promises concurrently. */
export async function joinAll<T>(promises: Promise<T>[]): Promise<T[]> {
    return Promise.all(promises);
}
