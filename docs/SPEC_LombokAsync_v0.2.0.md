# LombokAsync — SPEC v0.2.0

This document is the normative cross-language contract. Every language port MUST produce byte-identical output for all specified inputs. Deviations from this specification are bugs.

| Atribut | Nilai |
|---|---|
| Versi SPEC | 0.2.0 (berlaku untuk paket `lombokasync` 0.2.x di Rust, npm, PyPI, Packagist, dan modul Go) |
| Acuan | Rust `std::future` / `std::task` (stabil sejak 1.36); ECMAScript 2024 Promise; Python `asyncio` (3.9+); Go spesifikasi bahasa 1.22 (goroutine, channel, `select`); PHP 8.1 Fibers (RFC "Fibers"); semantik channel mengikuti model mpsc/oneshot yang lazim (tokio `sync::mpsc`/`oneshot`, tinjauan 2026-10-02) |
| Vector | `vectors/lombokasync-vectors-v1.json` — 141 kasus (mpsc 82, join_all 20, select 19, oneshot 13, timeout 7) — SHA-256 `dda4edb6f77c77a2c419d7ccbad172a60c2499a09c9aba450b5229af80106376` |
| Pemeriksa referensi | model keadaan Python independen di `vectors/build_vectors.py` (bukan asyncio) |
| Port | Rust (`rust/`), TypeScript (`typescript/`), Python (`python/`), Go (`go/`), PHP (`php/`); kelimanya menjalankan seluruh vector |
| Tanggal tinjauan | 2026-10-02 |

Kata MUST, MUST NOT, SHOULD, MAY mengikuti RFC 2119.

## 0. Konvensi

1. Kontrak ini mengatur **hasil yang dapat diamati**: outcome operasi channel, urutan nilai, dan pemilihan pemenang pada combinator. Lamanya waktu (jitter timer, penjadwalan OS) bukan bagian kontrak, kecuali urutan relatif yang dinyatakan eksplisit.
2. Outcome ditulis dengan nama abstrak (`ok`, `full`, `closed`, `empty`, `already_sent`, `invalid_capacity`, `timeout`). §2 memetakan nama ini ke bentuk idiomatis tiap bahasa. Runner vector mengubah bentuk idiomatis kembali ke nama abstrak, lalu membandingkan hasilnya sebagai JSON kanonik.
3. Nilai dalam vector berupa bilangan bulat dalam rentang aman ±(2^53 − 1) atau string Unicode. Port MUST meneruskan nilai apa adanya, tanpa salinan yang mengubah tipe atau isi.
4. "Task" berarti unit kerja konkuren menurut idiom bahasa: future (Rust), promise (TS), coroutine/task (Python), goroutine atau channel (Go), Fiber (PHP).

## 1. Ruang lingkup

| Bagian | Isi |
|---|---|
| §3 | channel mpsc: banyak pengirim, satu penerima, terbatas atau tak terbatas |
| §4 | channel oneshot: paling banyak satu nilai |
| §5 | combinator: `join_all` (dan `join`, `join3`), `select`, `timeout` |
| §6 | eksekutor dan timer (normatif per port, tanpa vector) |
| §7 | keamanan |

Di luar lingkup 0.2.0: I/O reaktor (socket, berkas), channel broadcast/watch, thread pool, pembatalan berbasis token lintas port.

## 2. Kode error dan pemetaan outcome

| Outcome / kode | Arti | Rust | TypeScript | Python | Go | PHP |
|---|---|---|---|---|---|---|
| `ok` | berhasil | `Ok(..)` | `'ok'` / nilai | tanpa exception | `nil` | tanpa exception |
| `full` / `FULL` | channel terbatas penuh | `TrySendError::Full(v)` | `'full'` | `ChannelFull` | `ErrFull` | `AsyncException` `FULL` |
| `closed` / `CLOSED` | channel tertutup | `TrySendError::Closed`, `TryRecvError::Closed`, `SendError`, `RecvError` | `'closed'`, `{status:'closed'}`, `AsyncError('CLOSED')` | `ChannelClosed` | `ErrClosed` | `CLOSED` |
| `empty` / `EMPTY` | belum ada nilai | `TryRecvError::Empty` | `{status:'empty'}` | `ChannelEmpty` | `ErrEmpty` | `EMPTY` |
| `already_sent` / `ALREADY_SENT` | pengirim oneshot sudah dipakai | tidak dapat ditulis (lihat §4.4) | `'already_sent'` | `AlreadySent` | `ErrAlreadySent` | `ALREADY_SENT` |
| `invalid_capacity` / `INVALID_CAPACITY` | kapasitas < 1 | `mpsc::InvalidCapacity` | `AsyncError('INVALID_CAPACITY')` | `InvalidCapacity` (juga `ValueError`) | `ErrInvalidCapacity` | `INVALID_CAPACITY` |
| `timeout` / `TIMEOUT` | tenggat lewat | `Elapsed` | `AsyncError('TIMEOUT')` | `Timeout` (juga `TimeoutError`) | `ErrTimeout` | `TIMEOUT` |

1. Teks pesan error MUST diawali kode lalu `": "` (misalnya `CLOSED: channel is closed`). Program MUST memeriksa kode, bukan teks.
2. Kode tambahan khusus port: PHP `CANCELLED` (menunggu task yang dibatalkan). Kode ini tidak muncul di vector.

## 3. Channel mpsc

### 3.1 Keadaan

Satu channel memiliki: antrean FIFO `Q`, kapasitas `cap` (tak terbatas, atau bilangan bulat ≥ 1), himpunan pengirim hidup `S`, dan penanda `rx_closed`.

1. Membuat channel dengan `cap` < 1 MUST gagal dengan `invalid_capacity`. Tidak ada channel yang terbentuk.
2. Channel baru: `Q` kosong, `S = {0}` (satu pengirim), `rx_closed = false`.
3. `clone(s)` menambah pengirim baru ke `S`. Di vector, pengirim baru diberi nomor berikutnya (1, 2, ...).
4. `drop(s)` (Rust: drop; port lain: `close()` pada pengirim) mengeluarkan `s` dari `S`. Operasi ini idempoten per handle. Pengirim yang sudah di-drop tidak boleh dipakai lagi; port yang masih bisa memanggilnya MUST melaporkan `closed`.
5. `close()` pada penerima mengubah `rx_closed` menjadi true. Operasi ini idempoten. Menjatuhkan penerima (Rust `Drop`) sama dengan `close()`.

### 3.2 Kirim tanpa menunggu (`try_send`)

Berurutan, aturan pertama yang cocok berlaku:

1. `rx_closed` → `closed`, nilai dikembalikan ke pemanggil (Rust) atau dibuang.
2. `cap` terbatas dan `|Q| ≥ cap` → `full`; `Q` tidak berubah.
3. Selain itu nilai ditambahkan di ekor `Q` → `ok`.

`closed` didahulukan atas `full` (vector `mpsc-014`).

### 3.3 Terima tanpa menunggu (`try_recv`)

1. `Q` tidak kosong → nilai di kepala `Q` dikeluarkan (`value`).
2. `Q` kosong dan (`rx_closed` atau `S` kosong) → `closed`.
3. Selain itu → `empty`.

Nilai yang sudah ada di antrean tetap dapat diterima setelah penerima ditutup atau semua pengirim pergi (`mpsc-004`, `mpsc-013`).

### 3.4 Operasi menunggu

1. `send(v)` menunggu selama `try_send` menghasilkan `full`. Ia selesai dengan `ok`, atau gagal dengan `closed` begitu aturan 3.2.1 berlaku, termasuk saat sedang menunggu.
2. `recv()` menunggu selama `try_recv` menghasilkan `empty`. Ia selesai dengan nilai, atau dengan tanda selesai (Rust `None`, TS `{done:true}`, Python `ChannelClosed`, Go `ok=false` / `ErrClosed`, PHP `CLOSED`).
3. Setiap perubahan keadaan yang dapat mengubah hasil operasi yang sedang menunggu MUST membangunkannya: penambahan ke `Q`, pengeluaran dari `Q` (bagi pengirim yang menunggu ruang), `S` menjadi kosong, dan `rx_closed`.
4. Iterasi (`for await`, `async for`, `foreach`, `while let Some`) berhenti tepat ketika `recv` melaporkan selesai.

### 3.5 Panjang

`len` = `|Q|`.

## 4. Channel oneshot

### 4.1 Keadaan

`value` (ada/tidak), `tx_used`, `tx_dropped`, `rx_closed`, `taken`.

### 4.2 `send(v)`

1. `tx_used` → `already_sent`.
2. `tx_used` menjadi true; setiap percobaan mengirim, berhasil atau tidak, memakai habis pengirim.
3. `rx_closed` → `closed`.
4. Selain itu `value = v` → `ok`.

### 4.3 `try_recv`

1. Ada `value` dan belum `taken` → `value`, lalu `taken` menjadi true.
2. `taken` atau `rx_closed` atau `tx_dropped` → `closed`.
3. Selain itu → `empty`.

Nilai yang terkirim sebelum `close()` pada penerima atau sebelum pengirim di-drop tetap dapat diambil (`oneshot-004`, `oneshot-006`). `recv` versi menunggu selesai dengan nilai, atau gagal dengan `closed`.

### 4.4 Rust

`Sender::send(self, v)` memakai habis pengirim, sehingga pengiriman kedua tidak dapat ditulis. Runner Rust menyimpan `Option<Sender>` dan melaporkan `already_sent` bila pengirim sudah terpakai. Ini pemetaan sah untuk §4.2.1.

## 5. Combinator

Di vector, satu task dijelaskan dengan:

- `{"value": v}`: selesai dengan `v`;
- `{"error": m}`: gagal dengan pesan `m`;
- `{"never": true}`: tidak pernah selesai;
- `{"yields": k, ...}`: memberi giliran k kali (Rust `yield_now`, TS `yieldNow`, Python `yield_now`, Go `runtime.Gosched`, PHP `Async::yield`) sebelum selesai.

### 5.1 `join_all(tasks)`

1. Semua task berjalan konkuren dan `join_all` menunggu semuanya selesai.
2. Bila tidak ada yang gagal, hasilnya daftar nilai dalam **urutan masukan**, bukan urutan selesai (`join_all-003`, `join_all-004`).
3. Bila ada yang gagal, `join_all` gagal dengan error milik task yang gagal dengan **indeks terkecil**, bukan yang gagal paling awal (`join_all-006`).
4. Daftar kosong menghasilkan daftar kosong.
5. `join(a, b)` dan `join3(a, b, c)` adalah `join_all` dengan dua dan tiga task. Rust juga menyediakan `try_join_all` untuk task bertipe `Result`, sedangkan `join_all` Rust mengembalikan seluruh output tanpa membedakan gagal; aturan 3 berlaku untuk `try_join_all`.

### 5.2 `select(tasks)`

1. Hasilnya `(index, value)` milik task pertama yang selesai. Bila task itu gagal, `select` gagal dengan error tersebut.
2. Bila beberapa task sudah selesai pada langkah penjadwalan yang sama, **indeks terkecil** menang (`select-003`, `select-007`, `select-008`).
3. Task lain dibatalkan atau dijatuhkan bila bahasanya memungkinkan (Rust: drop; Python: `cancel`; PHP: `Task::cancel`). Di TS dan Go task lain tetap berjalan tetapi hasilnya diabaikan.
4. Daftar kosong adalah kesalahan pemanggil (Rust panic, TS `RangeError`, Python `ValueError`, Go panic, PHP `InvalidArgumentException`).
5. Go: `Select(chans ...<-chan T)` bekerja atas channel. Channel yang sudah siap saat dipanggil diperiksa menurut urutan indeks. Channel `nil` tidak pernah siap. Vector memetakan task siap menjadi channel berisi hasil dan `never` menjadi `nil`.
6. Vector `select` hanya memakai task `value`, `error`, dan `never`, sehingga pemenangnya ditentukan oleh aturan 2 tanpa bergantung pada waktu.

### 5.3 `timeout(deadline, task)`

1. Bila task selesai sebelum tenggat → nilainya (`{"ok": v}`); bila gagal → error-nya diteruskan apa adanya.
2. Bila tenggat lewat lebih dulu → `timeout`. Task dibatalkan bila bahasanya memungkinkan. Go `Timeout` tidak dapat menghentikan goroutine; `TimeoutCtx` membatalkan context.
3. Task diperiksa sebelum tenggat, sehingga hasil yang siap pada saat tenggat tetap menang.
4. Vector memakai tenggat `timeout_deadline_ms` = 50 ms. Kasus `never` harus menghasilkan `timeout`; kasus dengan `yields` ≤ 5 harus selesai jauh sebelum tenggat.

## 6. Eksekutor dan timer (per port)

| Port | Eksekutor | Timer |
|---|---|---|
| Rust | `block_on` satu thread: ready queue, timer heap; thread di-park sampai tenggat terdekat atau sampai waker dipanggil (juga dari thread lain); `spawn` ke thread yang sama; tanpa `unsafe` | `sleep`, `timeout`, `interval` (tick pada kelipatan periode tetap) |
| TypeScript | event loop JavaScript; `spawn` mulai pada macrotask berikutnya | `setTimeout`; `sleep` dapat dibatalkan dengan `AbortSignal` |
| Python | `asyncio` (`run`, `spawn` = `create_task`) | `asyncio.sleep`, `wait` |
| Go | goroutine; `Spawn` menangkap panic dan melemparnya lagi di `Await` | `time`, `context` |
| PHP | `EventLoop::run` atas Fiber: ready queue, timer heap, tidur sampai tenggat terdekat (tanpa polling); deteksi deadlock (`LogicException`) | `Timer::sleep`, `timeout`, `interval` |

1. Eksekutor MUST NOT busy-wait. Bila tidak ada pekerjaan siap, ia menunggu event atau tenggat timer terdekat.
2. `interval` menjadwalkan tick ke-n pada `start + (n + 1) · periode`, sehingga keterlambatan tidak menumpuk. Periode ≤ 0 adalah kesalahan pemanggil.
3. Rust `block_on` tidak boleh bersarang di thread yang sama (panic).

## 7. Keamanan (normatif)

1. Kode Rust MUST bebas `unsafe` (`#![forbid(unsafe_code)]`).
2. Tidak ada operasi yang tumbuh tanpa batas karena masukan pemanggil, selain antrean channel tak terbatas, yang memang dikendalikan pemanggil. Gunakan channel terbatas untuk backpressure.
3. Antrean TS dan Go memadatkan buffer setelah > 1024 elemen terbuang dan lebih dari separuh buffer kosong, sehingga memori tidak bocor pada penggunaan panjang.
4. Channel Rust aman lintas thread (`Send` bila `T: Send`). Channel di port lain hanya aman di dalam satu event loop (Python, PHP, TS). Channel Go aman untuk goroutine.
5. Tidak ada dependensi runtime di semua port.

## 8. Perubahan dari 0.1.0

0.2.0 tidak kompatibel dengan 0.1.0 (rilis 0.x). Lihat CHANGELOG untuk rinciannya. Ringkasnya:

- eksekutor Rust ditulis ulang (0.1 melakukan busy-poll dan memakai `unsafe`);
- `join_all` Rust kini konkuren;
- channel memperoleh kapasitas, `try_send`/`try_recv`, `clone`/`close`, dan outcome berkode;
- `select` menjadi n-ary dengan indeks terkecil sebagai pemenang seri;
- `timeout` melaporkan error `TIMEOUT` (bukan `None`/`undefined`);
- event loop PHP tidak lagi melakukan polling `usleep(1000)`;
- path modul Go menjadi `github.com/codinglombok/lombokasync/go`.

*Lisensi dokumen: Apache-2.0 · © codinglombok*
