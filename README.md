# LombokAsync

> Small async toolkit with one behaviour in five languages: bounded and unbounded mpsc channels, oneshot channels, `join_all`, `select`, and `timeout`, plus a single-threaded Rust executor. Zero runtime dependencies.

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![CI](https://github.com/codinglombok/LombokAsync/actions/workflows/ci.yml/badge.svg)](https://github.com/codinglombok/LombokAsync/actions/workflows/ci.yml)
[![Vectors](https://img.shields.io/badge/shared%20vectors-141%20x%205%20ports-success)](vectors/)
[![Lombok Ecosystem](https://img.shields.io/badge/Lombok-Ecosystem-2e7d5b?logo=github)](https://github.com/codinglombok)

Part of the [Lombok Ecosystem](https://github.com/codinglombok).

## Mengapa library ini? (Why this library?)

- **Same rules everywhere.** Rust, TypeScript, Python, Go, and PHP follow one written contract ([SPEC](docs/SPEC_LombokAsync_v0.2.0.md)): when a send is `full` or `closed`, when a receive is `empty`, which task wins a `select` tie (the lowest index), which error `join_all` reports (the lowest index, not the earliest). All five ports run the same 141 vector cases in CI.
- **Explicit channel states.** Non-blocking `try_send` / `try_recv` with named outcomes, bounded capacity for backpressure, sender cloning, and receiver `close()` that still drains queued values.
- **A real small executor in Rust.** Ready queue, timer heap, thread parking instead of busy-polling, wakers usable from other threads, and `#![forbid(unsafe_code)]`.
- **Idiomatic per language.** Rust `Result`, TypeScript status objects and `AsyncError`, Python exceptions on `asyncio`, Go sentinel errors with `context` variants, PHP Fibers with a timer-driven event loop.

## Installation

| Language | Package | Status |
|---|---|---|
| Rust | `lombokasync` (crates.io) | not yet published |
| TypeScript / JavaScript | `lombokasync` (npm) | not yet published |
| Python | `lombokasync` (PyPI) | not yet published |
| Go | `github.com/codinglombok/lombokasync/go` | tag `go/v0.2.0` on release |
| PHP | `codinglombok/lombokasync` (Packagist) | not yet published |

Until then, use the source in this repository. See [docs/how_to_dist_LombokAsync_v0.2.0.md](docs/how_to_dist_LombokAsync_v0.2.0.md).

## Quick start

### Rust

```rust
use lombokasync::{block_on, mpsc, spawn, timeout};
use std::time::Duration;

let total = block_on(async {
    let (tx, mut rx) = mpsc::bounded::<u32>(8).unwrap();
    for i in 0..3 {
        let tx = tx.clone();
        spawn(async move { tx.send(i).await.unwrap() });
    }
    drop(tx);
    let mut sum = 0;
    while let Some(v) = rx.recv().await {
        sum += v;
    }
    sum
});
assert_eq!(total, 3);
assert_eq!(block_on(timeout(Duration::from_millis(50), async { 1 })), Ok(1));
```

### TypeScript

```ts
import { mpscChannel, select, timeout, AsyncError } from 'lombokasync';

const [tx, rx] = mpscChannel<number>(2);
tx.trySend(1);       // 'ok'
tx.trySend(2);       // 'ok'
tx.trySend(3);       // 'full'
rx.tryRecv();        // { status: 'value', value: 1 }

const { index, value } = await select([fetchA(), fetchB()]);
try {
    await timeout(500, fetchSlow());
} catch (e) {
    if (e instanceof AsyncError && e.code === 'TIMEOUT') { /* ... */ }
}
```

### Python

```python
import lombokasync as la

async def main():
    tx, rx = la.mpsc_channel(capacity=8)
    la.spawn(producer(tx))
    async for item in rx:
        print(item)
    results = await la.join_all([fetch(1), fetch(2)])
    index, value = await la.select([fast(), slow()])

la.run(main())
```

### Go

```go
import lombokasync "github.com/codinglombok/lombokasync/go"

tx, rx, _ := lombokasync.NewBoundedMpsc[int](8)
go func() { defer tx.Close(); for i := 0; i < 3; i++ { tx.Send(i) } }()
for v, ok := rx.Recv(); ok; v, ok = rx.Recv() {
    fmt.Println(v)
}
v, err := lombokasync.Timeout(time.Second, func() int { return 42 })
```

### PHP

```php
use LombokAsync\Async;
use LombokAsync\Channel\MpscChannel;

$sum = Async::run(function (): int {
    [$tx, $rx] = MpscChannel::bounded(8);
    Async::spawn(function () use ($tx) {
        foreach ([1, 2, 3] as $v) { $tx->send($v); }
        $tx->close();
    });
    $sum = 0;
    foreach ($rx as $v) { $sum += $v; }
    return $sum;
});
```

## Ports

| Feature | Rust | TypeScript | Python | Go | PHP |
|---|---|---|---|---|---|
| mpsc (bounded/unbounded, clone, close) | YES | YES | YES | YES | YES |
| oneshot | YES | YES | YES | YES | YES |
| join_all / select / timeout | YES | YES | YES | YES | YES |
| own executor | YES (`block_on`) | JS event loop | asyncio | goroutines | YES (Fibers) |
| shared vectors | 141/141 | 141/141 | 141/141 | 141/141 | 141/141 |

## Known limitations

No I/O reactor (sockets, files), no broadcast or watch channels, no thread pool; the Rust executor is single-threaded. Go `Timeout` cannot stop the goroutine it started (use `TimeoutCtx`). PHP needs a split repository or a root `composer.json` before it can be published to Packagist. Full list: [docs/full_summary_project_LombokAsync_v0.2.0.md](docs/full_summary_project_LombokAsync_v0.2.0.md#2-batasan-yang-diketahui).

## Upgrading from 0.1.0

0.2.0 changes every port: channel outcomes are explicit, `timeout` reports a `TIMEOUT` error instead of `None`/`undefined`, `select` takes a list and returns the winning index, and the Go module path is now lowercase. See [CHANGELOG.md](CHANGELOG.md).

## Development

```bash
cd rust && cargo test && cargo clippy --all-targets -- -D warnings
cd typescript && npm ci && npm run lint && npm run coverage
cd python && python -m pytest
cd go && go test -race ./...
cd php && php tests/run.php
bash scripts/lombok-doctor.sh LombokAsync
```

## License

Apache-2.0. See [LICENSE](LICENSE).
