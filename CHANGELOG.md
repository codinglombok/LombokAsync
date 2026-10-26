# Changelog

All notable changes to **LombokAsync** are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [0.2.0] — 2026-10-26

Brings the library in line with the Lombok Ecosystem v3.6 standards: a normative
cross-language specification, shared vectors executed by all five ports, and
claims that match the code. Every port changes its API (0.x release).

### Added
- `docs/SPEC_LombokAsync_0.2.0.md`: normative contract for mpsc and oneshot channels, `join_all`, `select` and `timeout`.
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
- Dual ESM + CJS build for the TypeScript port: `dist/` (ESM) and `dist-cjs/` (CJS, marked via `dist-cjs/package.json`), `exports` map with `import`/`require`, and `npm run smoke` that exercises both entry points. `npm pack` now ships both builds.
- `go-module` job in `release.yml`: pushing a `v*` tag automatically creates and pushes the `go/v<version>` module tag (previously a manual step documented in the README).
- PHP: `phpstan.neon` (level max) and `composer validate --strict` in CI; docblock generics added to satisfy max level.
- Python: `[tool.ruff]` (E/F/W/I/B/UP, line-length 100) and `[tool.mypy] strict` configuration in `pyproject.toml`; both run in CI.
- Ecosystem `.gitignore` block for internal documents (`Map_*.md`, `Stucture Repo_*.md`, `TECH DEBT_*.md`, `Architecture_*.md`, `Masterplan_*.md`) with an explicit exception keeping `docs/SPEC_*.md` public, since CI and downstream users require the normative contract.
- `go/LICENSE` (Apache-2.0) so every port directory carries the license.
- `github-release` job in `release.yml`: builds and attaches every port's package artifact to the GitHub Release of the pushed `v*` tag — the npm tarball (dual ESM + CJS), the Python sdist + wheel, the Rust `.crate`, and a zip archive of `php/`.
- `packagist` job in `release.yml`: runs the PHP smoke test, then subtree-splits `php/` into the split repository `codinglombok/lombokasync-php` (branch `main` + tag `v*`) and pings the Packagist update API. Gated on the `SPLIT_TOKEN`, `PACKAGIST_USERNAME` and `PACKAGIST_TOKEN` secrets; skipped with an explicit warning until the one-time setup is done.

### Changed
- Rust executor rewritten: ready queue, timer heap and thread parking instead of polling every task and sleeping 1 ms; wakers work from other threads; no `unsafe` code (`#![forbid(unsafe_code)]`).
- Rust `join_all` now runs futures concurrently (0.1.0 awaited them one after another).
- `timeout` reports a `TIMEOUT` error in every port instead of returning `None` / `undefined` / `(zero, false)`, so a task that legitimately returns nothing is no longer mistaken for a timeout.
- `select` takes a list and returns the winning index; ties go to the lowest index in every port (Go previously picked randomly).
- `join_all` failures report the lowest-index error in every port.
- PHP event loop sleeps until the next timer instead of calling `usleep(1000)` in a loop; one class per file (PSR-4).
- Python `recv` waits on futures instead of polling every 50 ms; channels can be created outside a running loop.
- Go module path is now `github.com/codinglombok/lombokasync/go` (lowercase).
- Documentation renamed to the canonical DOCS PACK naming without the `v` prefix (`docs/[Name]_LombokAsync_0.2.0.md`). Committed public set: API, Changelog, Development_idea, Full Summary Project, Guide How to Use, How To Dist, Language (now including the per-language port map), SPEC. Internal local-only set: Map, Stucture Repo, TECH DEBT, Architecture, Masterplan. `scripts/lombok-doctor.sh` updated accordingly (8 public docs; `--internal` checks the 5 internal ones plus the U1–U13 table).
- Coverage thresholds raised to the ecosystem standard: Rust `cargo llvm-cov --fail-under-lines 95`, TypeScript 95 line / 90 branch / 90 function, Python `--fail-under=95` (branch mode), Go ≥ 95% statements, PHP `--coverage=95`. Measured locally: Rust 98.4% lines, TS 100/98.4/98.3, Python 99%, Go 99.2%.
- Python `join_all` now filters results through a typed loop (mypy-strict clean); typing modernized per ruff UP rules (`list`/`tuple`/`collections.abc`, PEP 604 unions) — annotations are lazy (`from __future__ import annotations`), runtime behaviour unchanged.
- PHP `Async::joinAll`/`Async::select` accept `array<array-key, callable>` and keep re-indexing via `array_values`, so map input still yields 0-based indices; docblocks updated for phpstan max.

### Fixed
- Rust: `Sender` drop closed the channel based on `Arc::strong_count`, which miscounted clones; senders are now counted explicitly.
- Rust: `JoinHandle`, `Sleep` and `Timeout` woke themselves on every poll (busy loop).
- PHP: `MpscSender`, `MpscReceiver`, `OneshotSender` and `OneshotReceiver` were declared in other classes' files and could not be autoloaded.
- PHP: `Timer::timeout` read private loop state through reflection.
- TypeScript coverage thresholds (previously 90/90/85) now match the standard and pass at 100/98.4/98.3.
- README "Development" commands updated to include the new lint/build/smoke gates.

### Removed
- README claims that tied the library to one application domain.
- Rust `std` feature flag (it had no effect).

## [0.1.0] — 2026-09-24

Initial version: executor, timers, mpsc and oneshot channels, combinators in Rust, TypeScript, Python, Go and PHP (not published to any registry).
