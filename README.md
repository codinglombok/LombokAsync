# LombokAsync

Lightweight async runtime — task spawning, timers, channels, and combinators for RAG pipeline orchestration.

Part of the [LombokRAGFrameworks](https://github.com/codinglombok) ecosystem.

## Features

- **Task spawning** — spawn concurrent tasks with join handles
- **Timers** — sleep, timeout, interval
- **Channels** — mpsc (multi-producer, single-consumer) and oneshot
- **Combinators** — join, join3, join_all, select

## Quick Start

### Rust
```rust
use lombokasync::{block_on, spawn, sleep, mpsc, timeout};
use std::time::Duration;

let result = block_on(async {
    let h1 = spawn(async { 1 });
    let h2 = spawn(async { 2 });
    h1.await + h2.await
});
assert_eq!(result, 3);
```

### TypeScript
```typescript
import { spawn, sleep, join, mpscChannel, timeout } from 'lombokasync';

const [a, b] = await join(
    Promise.resolve(1),
    Promise.resolve(2),
);
```

### Python
```python
import lombokasync

async def main():
    h1 = lombokasync.spawn(compute_a())
    h2 = lombokasync.spawn(compute_b())
    a, b = await lombokasync.join(h1, h2)

lombokasync.run(main())
```

### Go
```go
import async "github.com/codinglombok/LombokAsync/go"

h := async.Spawn(func() int { return 42 })
result := h.Await() // 42

a, b := async.Join(
    func() int { return 1 },
    func() string { return "hello" },
)
```

### PHP
```php
use LombokAsync\Executor\EventLoop;
use LombokAsync\Channel\MpscChannel;

$loop = new EventLoop();
$loop->spawn(fn() => 42);
$result = $loop->run(); // 42

[$tx, $rx] = MpscChannel::create();
$tx->send(1);
[$val, $ok] = $rx->recv();
```

## License

Apache-2.0
