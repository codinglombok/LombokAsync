# LombokAsync — How to Dist 0.2.0

## 1. Registry

| Registry | Nama | Status | Mekanisme |
|---|---|---|---|
| crates.io | `lombokasync` | belum terbit | job `crates` di `release.yml` pada tag `v*`, `cargo publish` dari `rust/`, rahasia `CARGO_REGISTRY_TOKEN` |
| npm | `lombokasync` | belum terbit | job `npm`, `npm publish --provenance` dari `typescript/`, paket dual ESM (`dist/`) + CJS (`dist-cjs/`), rahasia `NPM_TOKEN` |
| PyPI | `lombokasync` | belum terbit | job `pypi`, build `python/`, trusted publishing (OIDC) |
| Go | `github.com/codinglombok/lombokasync/go` | tag otomatis saat rilis | job `go-module` di `release.yml` membuat dan mendorong tag `go/v<versi>` saat tag `v*` di-push |
| GitHub Release | artefak semua port | otomatis | job `github-release` di `release.yml` melampirkan tarball npm (dual ESM+CJS), sdist + wheel PyPI, `.crate` crates.io, dan zip `php/` ke rilis tag `v*` |
| Packagist | `codinglombok/lombokasync` | sekali-setup, lalu otomatis | job `packagist` di `release.yml`: uji smoke PHP, subtree-split `php/` ke `codinglombok/lombokasync-php` (branch `main` + tag `v*`), lalu ping API Packagist; rahasia `SPLIT_TOKEN`, `PACKAGIST_USERNAME`, `PACKAGIST_TOKEN`; sekali-setup: §3 |

## 2. Alur rilis

1. Semua perubahan masuk lewat PR dengan CI hijau (5 port, vector, doctor).
2. Versi di `rust/Cargo.toml`, `typescript/package.json`, `python/pyproject.toml`, `python/lombokasync/__init__.py`, nama berkas `docs/[Nama]_<versi>.md`, dan entri teratas `CHANGELOG.md` harus sama (doctor memeriksa manifest dan dokumen).
3. Buat tag `v<versi>` dari `main`. `release.yml` memeriksa tag = versi manifest, menjalankan ulang test, menerbitkan crates/npm/PyPI, mendorong tag `go/v<versi>` (job `go-module`), membuat GitHub Release beserta artefak semua port (job `github-release`), dan menyinkronkan Packagist (job `packagist`).

```powershell
git switch main ; git pull
bash scripts/lombok-doctor.sh LombokAsync
git tag v0.2.0
git push origin v0.2.0   # go/v0.2.0 didorong otomatis oleh job go-module
```

## 3. Sekali-setup Packagist

1. Buat repositori kosong `codinglombok/lombokasync-php` (tanpa README agar push pertama tidak konflik).
2. Buat PAT GitHub (klasik, scope `repo`) yang berhak mendorong ke repositori split; simpan sebagai secret `SPLIT_TOKEN`.
3. Masuk ke Packagist → Submit → isikan `https://github.com/codinglombok/lombokasync-php` → verifikasi. Simpan username Packagist sebagai secret `PACKAGIST_USERNAME` dan API token sebagai secret `PACKAGIST_TOKEN`.
4. Sejak itu setiap tag `v*` otomatis memperbarui repositori split dan memicu sinkronisasi Packagist. Selama secrets belum dipasang, job `packagist` dilewati dengan peringatan eksplisit — job rilis lain tetap berjalan.

## 4. Pasca-rilis

```powershell
cargo search lombokasync
npm view lombokasync@0.2.0 version
pip index versions lombokasync
go list -m github.com/codinglombok/lombokasync/go@v0.2.0
curl -fsS https://repo.packagist.org/p2/codinglombok/lombokasync.json   # metadata Packagist
gh release view v0.2.0 --repo codinglombok/LombokAsync                  # artefak GitHub Release
```

Rollback: `cargo yank --version 0.2.0`, `npm deprecate lombokasync@0.2.0 "gunakan 0.2.1"`, PyPI "yank" lewat antarmuka web. Tag Go tidak dihapus; terbitkan versi perbaikan. Packagist tidak mendukung yank (composer membaca versi dari tag repositori split) — terbitkan versi perbaikan.

*Lisensi dokumen: Apache-2.0 · © codinglombok*
