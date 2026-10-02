# LombokAsync — Development IDE v0.2.0

## 1. Lingkungan

| Alat | Versi |
|---|---|
| Rust | stable (MSRV 1.70, diperiksa CI); `cargo-llvm-cov` untuk coverage |
| Node.js | 20 LTS atau lebih baru (CI: 20, 22, 24) |
| Python | 3.9+ (CI: 3.9, 3.11, 3.13); `pytest`, `coverage` |
| Go | 1.22+ (CI: 1.22, 1.24) |
| PHP | 8.1+ (CI: 8.1, 8.3); ekstensi `pcov` untuk coverage |
| Editor | VS Code, RustRover/GoLand/PhpStorm/PyCharm, atau editor lain |
| Bash | untuk `scripts/lombok-doctor.sh` (Windows: Git Bash atau WSL) |

## 2. Perintah

| Direktori | Perintah | Fungsi |
|---|---|---|
| `rust/` | `cargo test` | unit + doctest + runner vector |
| `rust/` | `cargo clippy --all-targets -- -D warnings` · `cargo fmt --check` | lint |
| `rust/` | `cargo llvm-cov --fail-under-lines 90` | coverage |
| `typescript/` | `npm ci` · `npm run lint` · `npm test` | tipe strict, test |
| `typescript/` | `npm run coverage` | test dengan ambang baris 90, cabang 90, fungsi 85 |
| `typescript/` | `npm run fuzz` | fuzz diferensial channel (`FUZZ_EXECUTIONS` mengatur jumlah) |
| `python/` | `python -m pytest` · `coverage run --branch --source=lombokasync -m pytest && coverage report --fail-under=90` | test, coverage |
| `go/` | `go vet ./...` · `go test -race -cover ./...` | lint, test |
| `php/` | `php tests/run.php` · `php -d pcov.enabled=1 tests/run.php --coverage=90` | test, coverage |
| root | `python3 vectors/build_vectors.py` | bangun ulang berkas vector |
| root | `bash scripts/lombok-doctor.sh LombokAsync` | pemeriksaan standar Lombok v3.6 |

## 3. Alur mengubah perilaku

1. Ubah SPEC lebih dulu.
2. Ubah model di `vectors/build_vectors.py` dan tambah kasus. Nilai harapan kasus tulis tangan harus cocok dengan model; builder menolak bila tidak.
3. Jalankan builder, perbarui `vectors/SHA256SUMS` dan hash di SPEC.
4. Ubah kelima port sampai semua runner hijau.
5. Catat di `CHANGELOG.md`.

## 4. Arah pengembangan

- Channel broadcast dan watch (SPEC §3 bagian baru, vector v2).
- Token pembatalan yang sama di semua port.
- Eksekutor Rust opsional multi-thread di balik fitur Cargo, tanpa mengubah kontrak.

*Lisensi dokumen: Apache-2.0 · © codinglombok*
