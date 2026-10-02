# LombokAsync — How to Dist v0.2.0

## 1. Registry

| Registry | Nama | Status | Mekanisme |
|---|---|---|---|
| crates.io | `lombokasync` | belum terbit | job `crates` di `release.yml` pada tag `v*`, `cargo publish` dari `rust/`, rahasia `CARGO_REGISTRY_TOKEN` |
| npm | `lombokasync` | belum terbit | job `npm`, `npm publish --provenance` dari `typescript/`, rahasia `NPM_TOKEN` |
| PyPI | `lombokasync` | belum terbit | job `pypi`, build `python/`, trusted publishing (OIDC) |
| Go | `github.com/codinglombok/lombokasync/go` | tag saat rilis | tag tambahan `go/v<versi>` (modul di subdirektori) |
| Packagist | `codinglombok/lombokasync` | belum dapat terbit | Packagist membaca `composer.json` di root repositori; butuh repositori split (`LombokAsync-php`) yang disinkronkan dari `php/`, atau `composer.json` root yang menunjuk `php/src` |

## 2. Alur rilis

1. Semua perubahan masuk lewat PR dengan CI hijau (5 port, vector, doctor).
2. Versi di `rust/Cargo.toml`, `typescript/package.json`, `python/pyproject.toml`, `python/lombokasync/__init__.py`, nama berkas `docs/*_v<versi>.md`, dan entri teratas `CHANGELOG.md` harus sama (doctor memeriksa manifest dan dokumen).
3. Buat tag `v<versi>` dan `go/v<versi>` dari `main`. `release.yml` memeriksa tag = versi manifest, menjalankan ulang test, lalu menerbitkan.

```powershell
git switch main ; git pull
bash scripts/lombok-doctor.sh LombokAsync
git tag v0.2.0 ; git tag go/v0.2.0
git push origin v0.2.0 go/v0.2.0
```

## 3. Pasca-rilis

```powershell
cargo search lombokasync
npm view lombokasync@0.2.0 version
pip index versions lombokasync
go list -m github.com/codinglombok/lombokasync/go@v0.2.0
```

Rollback: `cargo yank --version 0.2.0`, `npm deprecate lombokasync@0.2.0 "gunakan 0.2.1"`, PyPI "yank" lewat antarmuka web. Tag Go tidak dihapus; terbitkan versi perbaikan.

*Lisensi dokumen: Apache-2.0 · © codinglombok*
