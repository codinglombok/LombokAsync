# LombokAsync — Full Summary Project v0.2.0

| Item | Nilai |
|---|---|
| Deskripsi | Channel mpsc/oneshot dengan outcome eksplisit, `join_all`, `select`, `timeout`, dan eksekutor Rust satu thread; perilaku sama di 5 bahasa; tanpa dependensi runtime |
| Cluster · tingkat | 01 Fondasi & Runtime · L0 |
| Referensi | SPEC + model keadaan Python di `vectors/build_vectors.py`; kelima port setara |
| Port | Rust, TypeScript, Python, Go, PHP |
| Vector | 141 kasus (mpsc 82, join_all 20, select 19, oneshot 13, timeout 7) · SHA-256 `dda4edb6...106376` |
| Test | Rust 36 unit + 12 doctest + runner vector · TS 157 · Python 153 · Go unit + runner (dengan `-race`) · PHP 33 cek API + 142 cek vector |
| Coverage | Rust 98,2% baris (cargo-llvm-cov) · TS 99,5% baris / 98,3% cabang · Python 99% (baris + cabang) · Go 99,6% pernyataan · PHP 97,5% baris (pcov) |
| Fuzz | lombokfuzzer, fuzz diferensial channel TS vs model SPEC, 20.000 eksekusi lokal tanpa crash |
| Uji mutasi | TS 12 mutan: 10 terbunuh, 2 ekuivalen · Rust 10 mutan: 9 terbunuh, 1 dipertahankan (lihat §2.7) |
| Registry | crates.io, npm, PyPI `lombokasync`; Go `github.com/codinglombok/lombokasync/go`; Packagist `codinglombok/lombokasync` (semua belum terbit) |
| Lisensi | Apache-2.0 |

## 1. Tabel gap vs pembanding (jujur)

| Kemampuan | LombokAsync 0.2.0 | tokio (Rust) | asyncio (Python) | Go stdlib | ReactPHP / Amp (PHP) |
|---|---|---|---|---|---|
| mpsc terbatas + `try_send` / `try_recv` | YA | YA | parsial (`Queue`, tanpa konsep pengirim/close) | parsial (channel bawaan tanpa banyak-pengirim-close) | parsial |
| oneshot | YA | YA | Future | channel buffer 1 | Deferred/Future |
| `select` deterministik (seri ke indeks terkecil) | YA | `biased;` opsional | TIDAK | TIDAK (acak) | TIDAK |
| Kontrak yang sama di 5 bahasa (SPEC + vector) | YA | TIDAK | TIDAK | TIDAK | TIDAK |
| Eksekutor multi-thread / work stealing | TIDAK | YA | TIDAK | YA (runtime) | TIDAK |
| I/O reaktor (socket, berkas) | TIDAK | YA | YA | YA | YA |
| Channel broadcast / watch | TIDAK | YA | TIDAK | TIDAK | parsial |
| Tanpa dependensi | YA | TIDAK | YA (stdlib) | YA (stdlib) | TIDAK |

Posisi unik yang dibuktikan test: aturan channel dan combinator yang identik dan teruji di Rust, TS, Python, Go, dan PHP, sehingga logika koordinasi yang dipindah antar-bahasa tetap berperilaku sama.

## 2. Batasan yang Diketahui

1. Tidak ada I/O reaktor; eksekutor Rust hanya menjalankan future dan timer. Untuk socket/berkas gunakan runtime lain atau thread terpisah yang mengirim ke channel.
2. Eksekutor Rust satu thread; `spawn` menempatkan task di thread pemanggil, dan task yang belum selesai saat `block_on` berakhir menunggu `block_on` berikutnya di thread yang sama.
3. Go `Timeout` tidak dapat menghentikan goroutine (keterbatasan bahasa); gunakan `TimeoutCtx`.
4. TS dan Go tidak membatalkan task yang kalah di `select`; hasilnya diabaikan.
5. Channel Python dan PHP hanya aman di dalam satu event loop; channel TS hanya di satu realm.
6. Paket PHP berada di subdirektori `php/`; Packagist membutuhkan repositori split atau `composer.json` di root sebelum dapat diterbitkan.
7. Mutan Rust yang dipertahankan: penanda `taken` di oneshot. Tes satu thread tidak dapat membedakannya karena `send` memakai habis pengirim, tetapi penanda itu menutup race lintas thread antara pelepasan lock di `send` dan drop pengirim.
8. Waktu timer bergantung pada OS; SPEC hanya mengikat urutan dan outcome.

## 3. Prinsip Universal (ringkas, untuk publik)

| Prinsip | Status | Bukti |
|---|---|---|
| U1 Mandiri | YA | README tanpa klaim kepemilikan; skenario netral di guide_ §6 |
| U2 Modern | YA | SPEC: acuan bahasa dan runtime dengan tanggal tinjauan |
| U3 Multi-platform | YA | CI ubuntu/windows/macos untuk Rust dan TS; Python, Go, PHP di Linux dan Windows |
| U4 Multi-bahasa | YA | 5 port, runner vector 141/141 di semua port |
| U5 Rentang skala | SEBAGIAN | tanpa dependensi; tanpa multi-thread/I-O (Batasan 1-2) |
| U6 Lengkap & unik | SEBAGIAN | tabel gap di atas |
| U7 Aman & teruji | YA | SPEC §7; `forbid(unsafe_code)`; coverage ≥ 97% di semua port; fuzz; uji mutasi |
| U8 Ekosistem tanpa kopling | YA | 0 dependensi wajib |
| U9 Internasional | SEBAGIAN | Lang_: tingkat E |
| U10 Lisensi | YA | Apache-2.0 di root dan setiap port |
| U11 Siap registri | SEBAGIAN | npm/crates/PyPI/Go siap; Packagist butuh split repo |
| U12 Dokumentasi | YA | 10 dokumen publik + 2 internal |
| U13 Kerahasiaan & dokumen bersih | YA | `lombok-doctor.sh`: 0 emoji, `.gitignore` ADR-024 |

*Lisensi dokumen: Apache-2.0 · © codinglombok*
