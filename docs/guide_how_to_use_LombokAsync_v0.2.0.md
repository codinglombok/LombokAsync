# LombokAsync — Guide How to Use v0.2.0

## 1. Pemasangan

| Bahasa | Perintah | Syarat |
|---|---|---|
| Rust | `cargo add lombokasync` | Rust 1.70+ |
| TypeScript/JS | `npm install lombokasync` | Node.js 20+, peramban modern, Deno, Bun |
| Python | `pip install lombokasync` | Python 3.9+ |
| Go | `go get github.com/codinglombok/lombokasync/go` | Go 1.22+ |
| PHP | `composer require codinglombok/lombokasync` | PHP 8.1+ |

Selama belum terbit, pakai sumber di repositori ini (lihat `how_to_dist_`).

## 2. Channel mpsc

Satu antrean, banyak pengirim, satu penerima. Tanpa kapasitas berarti tak terbatas; dengan kapasitas, `try_send` melaporkan `full` dan `send` menunggu ruang (backpressure).

```rust
let (tx, mut rx) = lombokasync::mpsc::bounded::<String>(100)?;
let tx2 = tx.clone();
tx.try_send("a".into())?;            // Err(TrySendError::Full(v)) bila penuh
drop(tx); drop(tx2);                  // channel tertutup setelah pengirim terakhir pergi
while let Some(v) = rx.recv().await { /* ... */ }
```

```ts
const [tx, rx] = mpscChannel<string>(100);
await tx.send('a');                  // menunggu bila penuh
tx.close();                          // drop pengirim ini
for await (const v of rx) { /* ... */ }
```

Aturan penting (SPEC §3):
- `rx.close()` menolak kiriman baru, tetapi nilai yang sudah mengantre tetap diterima.
- `closed` didahulukan atas `full`.
- `try_recv` membedakan `empty` (masih ada pengirim) dari `closed` (tidak ada nilai yang bisa datang lagi).

## 3. Channel oneshot

```python
tx, rx = la.oneshot_channel()
tx.send(42)          # kedua kalinya: AlreadySent
value = await rx     # ChannelClosed bila pengirim pergi tanpa nilai
```

## 4. Combinator

| Fungsi | Hasil | Seri / gagal |
|---|---|---|
| `join_all(tasks)` | semua nilai dalam urutan masukan | gagal dengan error indeks terkecil |
| `select(tasks)` | `(index, value)` task pertama yang selesai | seri: indeks terkecil |
| `timeout(t, task)` | nilai task | error `TIMEOUT` bila tenggat lewat |

```go
vals, err := lombokasync.TryJoinAll([]func() (int, error){fetchA, fetchB})
i, v, ok := lombokasync.Select(chA, chB)               // channel yang siap, indeks terkecil dulu
v2, err := lombokasync.TimeoutCtx(ctx, time.Second, work) // work menerima ctx yang dibatalkan
```

```php
$results = Async::joinAll([fn () => fetch(1), fn () => fetch(2)]);
[$index, $value] = Async::select([fn () => fast(), fn () => slow()]);
$v = Async::timeout(500, fn () => slow());   // AsyncException TIMEOUT
```

## 5. Eksekutor Rust

`block_on(future)` menjalankan future dan semua task dari `spawn` di thread pemanggil. Thread tidur di antara event (tanpa busy-wait). Channel dan `oneshot::Sender` boleh dipakai dari thread lain untuk membangunkan eksekutor. Jangan memanggil `block_on` di dalam `block_on`.

## 6. Skenario pemakaian

1. **Pipeline kerja dengan backpressure.** Produsen membaca berkas dan mengirim baris ke channel terbatas; beberapa worker memproses. Bila worker lambat, produsen menunggu dan memori tetap terkendali.
2. **Permintaan ke beberapa cermin (mirror).** `select` atas tiga unduhan; yang pertama selesai dipakai, sisanya dibatalkan (Rust, Python, PHP).
3. **Gateway IoT.** Pembacaan sensor dari beberapa perangkat masuk ke satu mpsc; `timeout` membatasi tiap pembacaan, dan perangkat yang macet tidak menahan siklus.
4. **Logika koordinasi yang sama di backend dan frontend.** Aturan antrean ditulis sekali di Go (server) dan TypeScript (klien), dengan perilaku `full`/`closed`/`select` yang identik.
5. **Skrip PHP yang memanggil beberapa layanan.** Tiga panggilan berjalan bergantian dalam satu proses tanpa ekstensi tambahan, dibatasi `Async::timeout`.

## 7. Batasan

Lihat `full_summary_project_` §2: tanpa I/O reaktor, eksekutor Rust satu thread, Go `Timeout` tidak menghentikan goroutine.

*Lisensi dokumen: Apache-2.0 · © codinglombok*
