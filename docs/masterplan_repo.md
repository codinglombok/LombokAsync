# LombokAsync — Masterplan Repository

## Overview
Async runtime library for concurrent operations in RAG pipelines.
Provides task spawning, timers, channels, and I/O across 5 languages.

## Architecture
- **Rust**: Custom single-threaded executor with epoll-based I/O reactor
- **TypeScript**: Wraps native Promise/async-await with task combinators
- **Python**: Wraps asyncio with ergonomic task/channel/timer APIs
- **Go**: Wraps goroutines/channels with unified API
- **PHP**: Uses Fibers for cooperative multitasking

## Scope
1. Event loop / executor
2. Task spawner + JoinHandle
3. Timer — sleep, timeout, interval
4. Channel — mpsc, oneshot, broadcast
5. Select/Join — concurrent task combinators
6. Thread pool — spawn_blocking (Rust only)

## Package
- **Name:** lombokasync
- **GitHub:** codinglombok/LombokAsync
- **License:** Apache 2.0
- **Wave:** 1

## Non-Goals
- Full I/O reactor (TCP/UDP sockets) — minimal for this wave
- Signal handling, fs, process
- io-uring support
- Target: <3000 LOC total across all languages
