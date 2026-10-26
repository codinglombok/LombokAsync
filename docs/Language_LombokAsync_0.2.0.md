# LombokAsync — Language 0.2.0

## 1. Peta port per bahasa

| Bahasa | Peran | Lokasi | Runner vector | Catatan platform |
|---|---|---|---|---|
| Rust | **Bahasa acuan** kontrak; semantik baru dieksplorasi lebih dulu di sini | `rust/` | `cargo test` (tests/vectors.rs) | MSRV 1.70; `#![forbid(unsafe_code)]`; channel aman lintas thread |
| TypeScript | Jalur publikasi npm (target utama distribusi) | `typescript/` | `npm test` (tests/vectors.test.ts) | Dual ESM (`dist/`) + CJS (`dist-cjs/`); Node 20+; ordering mengikat event order, bukan timing |
| Python | Port asyncio idiomatik; jalur PyPI | `python/` | `pytest` (tests/test_vectors.py) | Python 3.9+; ruff + mypy strict wajib CLEAN |
| Go | Port goroutine; modul terbit via tag `go/v*` | `go/` | `go test -race` (vectors_test.go) | `Select` atas channel; varian `context` untuk pembatalan |
| PHP | Port Fiber; jalur Packagist (menunggu repositori split) | `php/` | `php tests/run.php` (tests/vectors.php) | PHP 8.1+ Fiber; phpstan level max; kode `CANCELLED` khusus port |

Kelima port menjalankan vector yang sama (141 kasus) dan wajib menghasilkan JSON kanonik yang identik (SPEC §0.2, §2).

## 2. i18n

| Atribut | Nilai |
|---|---|
| Versi | 0.2.0 |
| Tingkat i18n (masterplan §13) | **E**: kode error dan dokumentasi; perilaku bebas locale |
| Katalog pesan | belum ada berkas `locales/`; ID pesan dicadangkan di §4 |
| Fallback | teks bahasa Inggris tertanam di kode |
| Cakupan katalog saat ini | en + id (2/20) di tabel §4; Nusantara 0/6 |

## 3. Prinsip

1. Setiap error membawa kode stabil (SPEC §2). Program MUST memeriksa kode, bukan teks.
2. Library tidak membaca locale sistem, zona waktu, atau variabel lingkungan. Waktu diukur dengan jam monoton (`Instant`, `performance`/`setTimeout`, `loop.time()`, `time`, `microtime`).
3. Nilai di channel diteruskan apa adanya; tidak ada normalisasi teks.

## 4. Katalog ID pesan (dicadangkan)

| ID | Kode | en | id |
|---|---|---|---|
| `lombokasync.timeout` | `TIMEOUT` | Deadline of {$ms} ms elapsed. | Tenggat {$ms} ms terlewati. |
| `lombokasync.closed` | `CLOSED` | Channel is closed. | Channel sudah ditutup. |
| `lombokasync.full` | `FULL` | Channel is full. | Channel penuh. |
| `lombokasync.empty` | `EMPTY` | Channel is empty. | Channel kosong. |
| `lombokasync.already_sent` | `ALREADY_SENT` | Oneshot sender was already used. | Pengirim oneshot sudah dipakai. |
| `lombokasync.invalid_capacity` | `INVALID_CAPACITY` | Capacity must be at least 1. | Kapasitas minimal 1. |

## 5. RTL

Library tidak menghasilkan UI atau teks untuk pengguna akhir; RTL tidak berlaku.

## 6. Rencana

- Berkas `locales/en` dan `locales/id` dimuat lewat LombokLocale bila terpasang (dependensi opsional, bukan wajib).

*Lisensi dokumen: Apache-2.0 · © codinglombok*
