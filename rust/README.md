# LombokAsync for Rust

Rust crate `lombokasync`: mpsc and oneshot channels with explicit outcomes, `join_all`, `select`, and `timeout`. Zero runtime dependencies.

```
cargo add lombokasync
```

This port follows the shared contract in [docs/SPEC_LombokAsync_v0.2.0.md](https://github.com/codinglombok/LombokAsync/blob/main/docs/SPEC_LombokAsync_v0.2.0.md) and runs all 141 shared vector cases in CI. Usage, the other ports, and the API reference are in the [main README](https://github.com/codinglombok/LombokAsync#readme) and [docs/API_LombokAsync_v0.2.0.md](https://github.com/codinglombok/LombokAsync/blob/main/docs/API_LombokAsync_v0.2.0.md).

License: Apache-2.0.
