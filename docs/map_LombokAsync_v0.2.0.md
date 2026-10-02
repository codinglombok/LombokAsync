# LombokAsync — Map v0.2.0

## 1. Posisi di ekosistem

```
Cluster 01 Fondasi & Runtime · tingkat L0 (tanpa dependensi Lombok wajib)

L0  LombokAsync
     dependensi wajib    : (tidak ada)
     dependensi opsional : (tidak ada)
     dependensi dev      : serde_json (runner vector Rust), typescript + @types/node + lombokfuzzer (TS),
                           pytest + coverage (Python, CI saja)
```

## 2. Contoh pemakai di ekosistem

Library ini mandiri dan dapat dipakai siapa pun. Library dan aplikasi Lombok yang dapat memakainya (arah dependensi selalu pemakai ke library):

| Pemakai | Pemakaian |
|---|---|
| LombokDNSProxy (aplikasi) | antrean permintaan terbatas antar-worker, batas waktu upstream |
| LombokMiner (aplikasi) | menjalankan job konkuren dan mengumpulkan hasil dengan `join_all` |
| LombokRAGFrameworks (aplikasi) | memanggil beberapa layanan sekaligus dengan `select`/`timeout` |
| LombokFuzzer (library) | kanal hasil antar-worker fuzz (rencana) |

## 3. Peta fitur x port

| Fitur | Rust | TypeScript | Python | Go | PHP |
|---|---|---|---|---|---|
| mpsc (SPEC §3) | YA | YA | YA | YA | YA |
| oneshot (SPEC §4) | YA | YA | YA | YA | YA |
| join_all / select / timeout (SPEC §5) | YA | YA | YA | YA | YA |
| eksekutor sendiri (SPEC §6) | YA | event loop JS | asyncio | goroutine | YA (Fiber) |
| interval | YA | YA | YA | YA | YA |

YA = runner vector lulus 141/141 (GP-11).

*Lisensi dokumen: Apache-2.0 · © codinglombok*
