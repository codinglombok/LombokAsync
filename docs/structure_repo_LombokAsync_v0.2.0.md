# LombokAsync — Structure Repo v0.2.0

```
LombokAsync/
├── README.md · CHANGELOG.md · LICENSE (Apache-2.0)
├── .github/workflows/  ci.yml (5 port + standards) · release.yml (terbit per registry pada tag v*)
├── docs/               10 dokumen publik; masterplan_ dan architecture_ adalah dokumen internal (ADR-024), tidak di-commit
├── vectors/            lombokasync-vectors-v1.json · SHA256SUMS · build_vectors.py (model keadaan independen)
├── scripts/            lombok-doctor.sh (pemeriksaan standar v3.6)
├── rust/               Cargo.toml · src/{lib,executor,timer,combinators}.rs · src/channel/{mpsc,oneshot}.rs
│                       tests/vectors.rs · examples/quickstart.rs
├── typescript/         package.json · src/index.ts · tests/{api,vectors}.test.ts · tests/fuzz/channels.fuzz.ts
│                       scripts/coverage.mjs
├── python/             pyproject.toml · lombokasync/{_executor,_timer,_channel,_combinators,_errors}.py
│                       tests/{test_api,test_vectors}.py
├── go/                 go.mod · {task,mpsc,oneshot,combinators,errors}.go · {async,vectors}_test.go
└── php/                composer.json · src/{Async,AsyncException}.php · src/Executor/{EventLoop,Task}.php
                        src/Channel/*.php · src/Timer/Timer.php · tests/{run,api,vectors,bootstrap}.php
```

Aturan: setiap perubahan perilaku mengubah SPEC dan vector lebih dulu, lalu kelima port. Setiap port punya README, LICENSE, dan manifest sendiri supaya dapat diterbitkan dari subdirektorinya. Hasil build tidak di-commit.

*Lisensi dokumen: Apache-2.0 · © codinglombok*
