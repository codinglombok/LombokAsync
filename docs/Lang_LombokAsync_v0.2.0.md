# LombokAsync — Bahasa & i18n v0.2.0

| Atribut | Nilai |
|---|---|
| Versi | 0.2.0 |
| Tingkat i18n (masterplan §13) | **E**: kode error dan dokumentasi; perilaku bebas locale |
| Katalog pesan | belum ada berkas `locales/`; ID pesan dicadangkan di §2 |
| Fallback | teks bahasa Inggris tertanam di kode |
| Cakupan katalog saat ini | en + id (2/20) di tabel §2; Nusantara 0/6 |

## 1. Prinsip

1. Setiap error membawa kode stabil (SPEC §2). Program MUST memeriksa kode, bukan teks.
2. Library tidak membaca locale sistem, zona waktu, atau variabel lingkungan. Waktu diukur dengan jam monoton (`Instant`, `performance`/`setTimeout`, `loop.time()`, `time`, `microtime`).
3. Nilai di channel diteruskan apa adanya; tidak ada normalisasi teks.

## 2. Katalog ID pesan (dicadangkan)

| ID | Kode | en | id |
|---|---|---|---|
| `lombokasync.timeout` | `TIMEOUT` | Deadline of {$ms} ms elapsed. | Tenggat {$ms} ms terlewati. |
| `lombokasync.closed` | `CLOSED` | Channel is closed. | Channel sudah ditutup. |
| `lombokasync.full` | `FULL` | Channel is full. | Channel penuh. |
| `lombokasync.empty` | `EMPTY` | Channel is empty. | Channel kosong. |
| `lombokasync.already_sent` | `ALREADY_SENT` | Oneshot sender was already used. | Pengirim oneshot sudah dipakai. |
| `lombokasync.invalid_capacity` | `INVALID_CAPACITY` | Capacity must be at least 1. | Kapasitas minimal 1. |

## 3. RTL

Library tidak menghasilkan UI atau teks untuk pengguna akhir; RTL tidak berlaku.

## 4. Rencana

- Berkas `locales/en` dan `locales/id` dimuat lewat LombokLocale bila terpasang (dependensi opsional, bukan wajib).

*Lisensi dokumen: Apache-2.0 · © codinglombok*
