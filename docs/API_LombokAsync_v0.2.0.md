# LombokAsync — API v0.2.0

Perilaku normatif ada di SPEC. Dokumen ini memetakan konsep SPEC ke nama di setiap port.

## 1. Ringkasan lintas port

| Konsep (SPEC) | Rust | TypeScript | Python | Go | PHP |
|---|---|---|---|---|---|
| mpsc tak terbatas | `mpsc::channel()` | `mpscChannel()` | `mpsc_channel()` | `NewMpsc[T]()` | `MpscChannel::unbounded()` |
| mpsc terbatas | `mpsc::bounded(n)?` | `mpscChannel(n)` | `mpsc_channel(n)` | `NewBoundedMpsc[T](n)` | `MpscChannel::bounded($n)` |
| try_send | `tx.try_send(v)` | `tx.trySend(v)` | `tx.try_send(v)` | `tx.TrySend(v)` | `$tx->trySend($v)` |
| send (menunggu) | `tx.send(v).await` | `await tx.send(v)` | `await tx.send(v)` | `tx.Send(v)` / `SendContext` | `$tx->send($v)` |
| clone / drop pengirim | `tx.clone()` / `drop(tx)` | `tx.clone()` / `tx.close()` | `tx.clone()` / `tx.close()` | `tx.Clone()` / `tx.Close()` | `$tx->clone()` / `$tx->close()` |
| try_recv | `rx.try_recv()` | `rx.tryRecv()` | `rx.try_recv()` | `rx.TryRecv()` | `$rx->tryRecv()` |
| recv (menunggu) | `rx.recv().await` | `await rx.recv()` | `await rx.recv()` | `rx.Recv()` / `RecvContext` | `$rx->recv()` |
| close penerima | `rx.close()` / drop | `rx.close()` | `rx.close()` | `rx.Close()` | `$rx->close()` |
| len | `rx.len()` | `rx.length` | `len(rx)` | `rx.Len()` | `count($rx)` |
| oneshot | `oneshot::channel()` | `oneshotChannel()` | `oneshot_channel()` | `NewOneshot[T]()` | `OneshotChannel::create()` |
| join_all | `join_all(v)` / `try_join_all(v)` | `joinAll(ps)` | `join_all(aws)` | `JoinAll(fns)` / `TryJoinAll(fns)` | `Async::joinAll($fns)` |
| select | `select_all(v)` / `select(a, b)` | `select(ps)` | `select(aws)` | `Select(chans...)` | `Async::select($fns)` |
| timeout | `timeout(d, f)` | `timeout(ms, p)` | `timeout(s, aw)` | `Timeout` / `TimeoutCtx` / `TimeoutErr` | `Async::timeout($ms, $fn)` |
| beri giliran | `yield_now()` | `yieldNow()` | `yield_now()` | `runtime.Gosched()` | `Async::yield()` |

## 2. Rust (`lombokasync`)

| Item | Tanda tangan |
|---|---|
| `block_on` | `fn block_on<F: Future>(f: F) -> F::Output` (panic bila bersarang) |
| `spawn` | `fn spawn<F: Future + 'static>(f: F) -> JoinHandle<F::Output>`; `JoinHandle: Future`, `is_finished()` |
| `yield_now` | `fn yield_now() -> YieldNow` |
| `sleep` / `timeout` / `interval` | `sleep(Duration) -> Sleep`; `timeout(Duration, F) -> Timeout<F>` dengan output `Result<F::Output, Elapsed>`; `interval(Duration) -> Interval` dengan `async fn tick(&mut self) -> u64` (panic bila periode nol) |
| `mpsc` | `channel<T>()`; `bounded<T>(usize) -> Result<(Sender, Receiver), InvalidCapacity>`; `Sender: Clone + Send` dengan `try_send`, `send`, `is_closed`; `Receiver` dengan `try_recv`, `recv`, `poll_recv`, `close`, `len`, `is_empty`; error `TrySendError<T>{Full, Closed}`, `SendError<T>`, `TryRecvError{Empty, Closed}` |
| `oneshot` | `channel<T>()`; `Sender::send(self, T) -> Result<(), T>`, `is_closed`; `Receiver: Future<Output = Result<T, RecvError>>`, `try_recv`, `close` |
| combinator | `join`, `join3`, `join_all(Vec<F>) -> Vec<Output>`, `try_join_all(Vec<F>) -> Result<Vec<T>, E>`, `select(a, b) -> Either<A, B>`, `select_all(Vec<F>) -> (usize, Output)` (panic bila kosong) |
| kompatibilitas | modul `sync` mengekspor ulang `join`, `join3`, `join_all`, `select`, `Either` |

## 3. TypeScript (`lombokasync`)

| Item | Tanda tangan |
|---|---|
| `AsyncError` | `class AsyncError extends Error { code: 'TIMEOUT' \| 'CLOSED' \| 'INVALID_CAPACITY' \| 'ALREADY_SENT' }` |
| timer | `sleep(ms, signal?)`, `yieldNow()`, `timeout<T>(ms, Promise<T> \| () => Promise<T>)`, `interval(ms): AsyncGenerator<number>` |
| task | `spawn<T>(fn): JoinHandle<T>` dengan `promise` dan `finished` |
| mpsc | `mpscChannel<T>(capacity?): [Sender<T>, Receiver<T>]`; `Sender.trySend(v): 'ok' \| 'full' \| 'closed'`, `send(v)`, `clone()`, `close()`, `isClosed`; `Receiver.tryRecv(): {status:'value', value} \| {status:'empty'} \| {status:'closed'}`, `recv(): Promise<{done:false, value} \| {done:true}>`, `close()`, `length`, `for await` |
| oneshot | `oneshotChannel<T>()`; `OneshotSender.send(v): 'ok' \| 'closed' \| 'already_sent'`, `close()`, `isClosed`; `OneshotReceiver.tryRecv()`, `recv(): Promise<T>`, `close()` |
| combinator | `joinAll(ps)`, `join(a, b)`, `join3(a, b, c)`, `select(ps): Promise<{index, value}>` |

## 4. Python (`lombokasync`)

| Item | Tanda tangan |
|---|---|
| error | `AsyncError` (atribut `code`), `Timeout` (juga `TimeoutError`), `ChannelClosed`, `ChannelFull`, `ChannelEmpty`, `AlreadySent`, `InvalidCapacity` (juga `ValueError`) |
| eksekutor | `run(coro)`, `spawn(coro) -> JoinHandle` (`await`, `done()`, `cancel()`), `yield_now()` |
| timer | `sleep(s)`, `timeout(s, aw)`, `interval(s)` (async generator) |
| mpsc | `mpsc_channel(capacity=None) -> (Sender, Receiver)`; `Sender.try_send`, `send`, `clone`, `close`, `is_closed`; `Receiver.try_recv`, `recv`, `close`, `len()`, `async for` |
| oneshot | `oneshot_channel()`; `OneshotSender.send`, `close`, `is_closed`; `OneshotReceiver.try_recv`, `recv`, `close`, `await rx` |
| combinator | `join_all(aws)`, `join(a, b)`, `join3(a, b, c)`, `select(aws) -> Selected(index, value)` |

## 5. Go (`github.com/codinglombok/lombokasync/go`, package `lombokasync`)

| Item | Tanda tangan |
|---|---|
| error | `type Error struct{ Code, Msg string }`; `ErrTimeout`, `ErrClosed`, `ErrFull`, `ErrEmpty`, `ErrAlreadySent`, `ErrInvalidCapacity` (bandingkan dengan `errors.Is`) |
| task | `Spawn[T](func() T) *JoinHandle[T]` dengan `Await()`, `Done()` |
| timer | `Sleep(d)`, `Timeout[T](d, func() T) (T, error)`, `TimeoutCtx[T](ctx, d, func(context.Context) T) (T, error)`, `TimeoutErr[T](d, func() (T, error)) (T, error)`, `Interval(d) (<-chan int, func())` |
| mpsc | `NewMpsc[T]()`, `NewBoundedMpsc[T](n) (..., error)`; `MpscSender.TrySend`, `Send`, `SendContext`, `Clone`, `Close`, `IsClosed`; `MpscReceiver.TryRecv`, `Recv() (T, bool)`, `RecvContext`, `Close`, `Len` |
| oneshot | `NewOneshot[T]()`; `OneshotSender.Send`, `Close`, `IsClosed`; `OneshotReceiver.TryRecv`, `Recv`, `RecvContext`, `Close` |
| combinator | `Join`, `Join3`, `JoinAll`, `TryJoinAll`, `Select[T](chans ...<-chan T) (int, T, bool)` |

## 6. PHP (`codinglombok/lombokasync`, namespace `LombokAsync`)

| Item | Tanda tangan |
|---|---|
| error | `AsyncException extends RuntimeException`, properti `errorCode` (konstanta `TIMEOUT`, `CLOSED`, `FULL`, `EMPTY`, `ALREADY_SENT`, `INVALID_CAPACITY`, `CANCELLED`) |
| fasad | `Async::run(callable)`, `spawn`, `yield`, `sleep(float $ms)`, `timeout(float $ms, callable)`, `joinAll(array)`, `select(array): array{int, mixed}` |
| eksekutor | `Executor\EventLoop::run`, `current`, `spawn`, `yield`, `sleep`; `Executor\Task::await()`, `isDone()`, `cancel()` |
| timer | `Timer\Timer::sleep`, `timeout`, `interval(float $ms): Generator` |
| mpsc | `Channel\MpscChannel::unbounded()`, `bounded(int)`, `create()` (usang); `MpscSender::trySend`, `send`, `clone`, `close`, `isClosed`; `MpscReceiver::tryRecv`, `recv`, `close`, `count`, `foreach` |
| oneshot | `Channel\OneshotChannel::create()`; `OneshotSender::send`, `close`, `isClosed`; `OneshotReceiver::tryRecv`, `recv`, `close` |

## 7. Kompatibilitas dengan 0.1.0

0.2.0 mengubah API di semua port; lihat CHANGELOG bagian Changed. Nama yang dipertahankan: Rust `block_on`, `spawn`, `sleep`, `timeout`, `interval`, `mpsc::channel`, `oneshot::channel`, modul `sync`; TS `sleep`, `timeout`, `interval`, `spawn`, `mpscChannel`, `oneshotChannel`, `join`, `join3`, `joinAll`, `select`; Python nama fungsi yang sama; Go `Spawn`, `Sleep`, `Timeout`, `TimeoutCtx`, `Join`, `Join3`, `JoinAll`, `Select`, `Interval`; PHP `EventLoop`, `MpscChannel::create`, `OneshotChannel::create`, `Timer`.

*Lisensi dokumen: Apache-2.0 · © codinglombok*
