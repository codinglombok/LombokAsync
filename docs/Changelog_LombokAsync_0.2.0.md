# LombokAsync — Changelog 0.2.0

Ringkasan; rincian lengkap di [CHANGELOG.md](../CHANGELOG.md). Entri terbaru di depan.

| Versi | Tanggal | Jenis | Ringkasan |
|---|---|---|---|
| 0.2.0 | 2026-10-26 | perubahan API (0.x) + perbaikan | SPEC lintas bahasa + 141 kasus vector yang dijalankan kelima port; channel mpsc terbatas/tak terbatas dengan outcome berkode, `clone`/`close`; oneshot `try_recv`/`close`; `select` n-ary (seri ke indeks terkecil), `join_all` melaporkan error indeks terkecil, `timeout` berupa error `TIMEOUT`; eksekutor Rust ditulis ulang tanpa busy-poll dan tanpa `unsafe`; event loop PHP berbasis timer; path modul Go huruf kecil; dual ESM+CJS untuk npm; threshold coverage 95/90 semua port (S4); ruff + mypy strict (Python); phpstan max + composer validate strict (PHP); DOCS PACK 13 dokumen kanonik; blok [GITIGNORE] ekosistem (SPEC tetap publik lewat pengecualian); job release baru: `go-module` (tag `go/v*` otomatis), `github-release` (artefak semua port ke GitHub Release), `packagist` (ping API Packagist; `composer.json` dipindah ke root + `.gitattributes` export-ignore) |
| 0.1.0 | 2026-09-24 | awal | Eksekutor, timer, channel, combinator di 5 bahasa (tidak terbit di registry) |

*Lisensi dokumen: Apache-2.0 · © codinglombok*
