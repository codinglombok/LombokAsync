# LombokAsync — Changelog v0.2.0

Ringkasan; rincian lengkap di [CHANGELOG.md](../CHANGELOG.md). Entri terbaru di depan.

| Versi | Tanggal | Jenis | Ringkasan |
|---|---|---|---|
| 0.2.0 | 2026-10-02 | perubahan API (0.x) + perbaikan | SPEC lintas bahasa + 141 kasus vector yang dijalankan kelima port; channel mpsc terbatas/tak terbatas dengan outcome berkode, `clone`/`close`; oneshot `try_recv`/`close`; `select` n-ary (seri ke indeks terkecil), `join_all` melaporkan error indeks terkecil, `timeout` berupa error `TIMEOUT`; eksekutor Rust ditulis ulang tanpa busy-poll dan tanpa `unsafe`; event loop PHP berbasis timer; path modul Go huruf kecil; 10 dokumen standar; klaim README dikoreksi |
| 0.1.0 | 2026-09-24 | awal | Eksekutor, timer, channel, combinator di 5 bahasa (tidak terbit di registry) |

*Lisensi dokumen: Apache-2.0 · © codinglombok*
