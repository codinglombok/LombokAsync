# LombokAsync — Development Idea 0.2.0

Dokumen ini menggabungkan lingkungan pengembangan (IDE) dan backlog ide (P0–P3) beserta kriteria selesai dan non-goals.

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
| `rust/` | `cargo llvm-cov --fail-under-lines 95` | coverage (S4: baris ≥ 95) |
| `typescript/` | `npm ci` · `npm run lint` · `npm test` | tipe strict, test |
| `typescript/` | `npm run coverage` | test dengan ambang baris 95, cabang 90, fungsi 90 (S4) |
| `typescript/` | `npm run build` · `npm run build:cjs` · `npm run smoke` | build dual ESM (dist/) + CJS (dist-cjs/) dan uji kedua entry |
| `typescript/` | `npm run fuzz` | fuzz diferensial channel (`FUZZ_EXECUTIONS` mengatur jumlah) |
| `python/` | `python -m pytest` · `coverage run --branch --source=lombokasync -m pytest && coverage report --fail-under=95` | test, coverage (S4) |
| `python/` | `ruff check .` · `mypy` | lint + tipe strict (CLEAN wajib) |
| `go/` | `go vet ./...` · `go test -race -cover ./...` | lint, test |
| `php/` | `php tests/run.php` · `php -d pcov.enabled=1 tests/run.php --coverage=95` | test, coverage (S4) |
| `php/` | `phpstan analyse --no-progress --memory-limit=1G` · `composer validate --strict` | analisis statis level max, validasi manifest |
| root | `python3 vectors/build_vectors.py` | bangun ulang berkas vector |
| root | `bash scripts/lombok-doctor.sh LombokAsync` | pemeriksaan standar Lombok v3.6 |

## 3. Alur mengubah perilaku

1. Ubah SPEC lebih dulu.
2. Ubah model di `vectors/build_vectors.py` dan tambah kasus. Nilai harapan kasus tulis tangan harus cocok dengan model; builder menolak bila tidak.
3. Jalankan builder, perbarui `vectors/SHA256SUMS` dan hash di SPEC.
4. Ubah kelima port sampai semua runner hijau.
5. Catat di `CHANGELOG.md`.

## 4. Backlog (Development idea) P0–P3

| Prioritas | Ide | Kriteria selesai |
|---|---|---|
| P0 (sesi ini) | Dual ESM+CJS untuk TS | `npm run smoke` lulus untuk kedua entry; `npm pack` memuat `dist/` + `dist-cjs/` |
| P0 (sesi ini) | Threshold coverage 95/90 semua port (S4) | Gate CI mematok angka; bukti lokal ≥ 98,4 di port terukur |
| P0 (sesi ini) | ruff + mypy strict, phpstan max, composer validate strict | Kelima alat CLEAN di lokal dan CI |
| P0 (sesi ini) | DOCS PACK 13 dokumen kanonik + blok [GITIGNORE] ekosistem | doctor 8/8 publik + `--internal` lulus; dokumen internal tak terlacak |
| P1 | Rilis pertama ke registry (push tag `v0.2.0`) | npm/crates/PyPI + GitHub Release terisi; tag `go/v<versi>` terdorong otomatis oleh release.yml |
| P1 | Packagist: sekali-submit repo ini + secrets `PACKAGIST_USERNAME`/`PACKAGIST_TOKEN` (job otomatis sudah ada di `release.yml`) | `composer require codinglombok/lombokasync` jalan | 
| P1 | Hapus shim kompat: `rust::sync` dan `MpscChannel::create()` | CHANGELOG 0.3.0 mencatat breaking; doctor tetap hijau |
| P2 | Channel broadcast dan watch (SPEC §3 bagian baru) | Vector v2 + runner 5 port lulus; SPEC v0.3.x |
| P2 | Token pembatalan yang sama di semua port (`cancel` seragam) | SPEC bagian baru; runner vector memuat kasus cancel |
| P2 | Fuzz harness untuk Python/Go/PHP | Harness porting dari model `build_vectors.py`; 20.000 eksekusi tanpa crash |
| P3 | Eksekutor Rust opsional multi-thread di balik fitur Cargo | Kontrak tidak berubah; feature flag terdokumentasi |
| P3 | I/O reaktor minimal (socket) di balik fitur | SPEC bagian baru + vector; tanpa dependensi |

## 5. Non-goals (0.2.x)

1. I/O reaktor (socket, berkas) — keluar ruang lingkup SPEC 0.2.0; gunakan runtime lain atau thread terpisah yang mengirim ke channel.
2. Thread pool dan eksekutor multi-thread bawaan — eksekutor Rust sengaja satu thread.
3. Kompatibilitas API tokio/asyncio penuh — bukan jembatan, melainkan kontrak mandiri.
4. Thread-safety channel di luar acuan SPEC §7 (Python/PHP/TS satu event loop; Go dan Rust lintas thread).

*Lisensi dokumen: Apache-2.0 · © codinglombok*
