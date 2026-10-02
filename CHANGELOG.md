# Changelog

All notable changes to **LombokAsync** are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

## [0.2.0] — 2026-10-02

Brings the library in line with the Lombok Ecosystem v3.6 standards: a normative
cross-language specification, shared vectors executed by all five ports, and
claims that match the code. Every port changes its API (0.x release).

### Added
- `docs/SPEC_LombokAsync_v0.2.0.md`: normative contract for mpsc and oneshot channels, `join_all`, `select` and `timeout`.
- `vectors/lombokasync-vectors-v1.json`: 141 cases built from an independent state-machine model in `vectors/build_vectors.py`; runners in Rust, TypeScript, Python, Go and PHP.
- Bounded mpsc channels with `try_send` / `try_recv`, sender `clone` / `close`, receiver `close` (queued values still drain) and `len` in every port.
- Oneshot `try_recv` and receiver `close`; a second send is reported as `already_sent`.
- Stable error codes `TIMEOUT`, `CLOSED`, `FULL`, `EMPTY`, `ALREADY_SENT`, `INVALID_CAPACITY` (PHP also `CANCELLED`).
- Rust: `try_join_all`, `select_all`, `yield_now`, `JoinHandle::is_finished`, `examples/quickstart.rs`.
- TypeScript: `AsyncError`, `yieldNow`, `sleep` with `AbortSignal`, `select` over a list, async iteration of receivers; differential fuzz harness (lombokfuzzer).
- Python: exception classes, `yield_now`, `Selected`, `py.typed`, `pyproject.toml`.
- Go: `TrySend` / `TryRecv`, `SendContext` / `RecvContext`, `TryJoinAll`, `TimeoutErr`, `JoinHandle.Done`.
- PHP: `Async` facade, `Task` handles with `await` / `cancel`, `Timer::interval`, deadlock detection.
- Ten standard documents under `docs/`, `scripts/lombok-doctor.sh`, CI for all five ports with coverage thresholds.

### Changed
- Rust executor rewritten: ready queue, timer heap and thread parking instead of polling every task and sleeping 1 ms; wakers work from other threads; no `unsafe` code (`#![forbid(unsafe_code)]`).
- Rust `join_all` now runs futures concurrently (0.1.0 awaited them one after another).
- `timeout` reports a `TIMEOUT` error in every port instead of returning `None` / `undefined` / `(zero, false)`, so a task that legitimately returns nothing is no longer mistaken for a timeout.
- `select` takes a list and returns the winning index; ties go to the lowest index in every port (Go previously picked randomly).
- `join_all` failures report the lowest-index error in every port.
- PHP event loop sleeps until the next timer instead of calling `usleep(1000)` in a loop; one class per file (PSR-4).
- Python `recv` waits on futures instead of polling every 50 ms; channels can be created outside a running loop.
- Go module path is now `github.com/codinglombok/lombokasync/go` (lowercase).

### Fixed
- Rust: `Sender` drop closed the channel based on `Arc::strong_count`, which miscounted clones; senders are now counted explicitly.
- Rust: `JoinHandle`, `Sleep` and `Timeout` woke themselves on every poll (busy loop).
- PHP: `MpscSender`, `MpscReceiver`, `OneshotSender` and `OneshotReceiver` were declared in other classes' files and could not be autoloaded.
- PHP: `Timer::timeout` read private loop state through reflection.

### Removed
- README claims that tied the library to one application domain.
- Rust `std` feature flag (it had no effect).

## [0.1.0] — 2026-09-24

Initial version: executor, timers, mpsc and oneshot channels, combinators in Rust, TypeScript, Python, Go and PHP (not published to any registry).
