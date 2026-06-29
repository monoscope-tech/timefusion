# Contributing to TimeFusion

Thanks for contributing. This guide covers local setup, the test suite, and the
conventions we hold PRs to.

## Local setup

You need a recent stable Rust toolchain (edition 2024 → **Rust 1.85+**) and
Docker (for MinIO and the end-to-end tests).

```bash
git clone https://github.com/monoscope-tech/timefusion.git
cd timefusion
cargo build

# Run against a local MinIO
docker compose up -d minio createbucket
export AWS_S3_BUCKET=timefusion \
       AWS_S3_ENDPOINT=http://localhost:9000 \
       AWS_ALLOW_HTTP=true \
       AWS_ACCESS_KEY_ID=minioadmin \
       AWS_SECRET_ACCESS_KEY=minioadmin
cargo run
```

The `Makefile` has shortcuts (`make minio-start`, `make run-minio`,
`make test-minio`) if you'd rather run MinIO as a local binary than in Docker.

## Tests

```bash
cargo test                          # unit tests (fast)
cargo test --test sqllogictest      # SQL logic tests
cargo test --test integration_test  # write-path integration
cargo test --test e2e               # end-to-end (Docker required; testcontainers MinIO)
RUST_LOG=debug cargo test           # with debug logging
make test-all                       # everything, including slow integration tests
```

- **Dev builds compile far faster than release** — use them while iterating.
- Tests that mutate process env (`std::env::set_var`) must be `#[serial]` (via
  `serial_test`) with a SAFETY comment explaining the race they avoid.
- The `e2e` suite exercises the full prod path (pgwire → WAL → MemBuffer →
  flush → Delta on MinIO → query) with a virtual clock; a failure there mirrors
  a prod failure.

## Bug-fix workflow (required)

When fixing a bug, **write a failing test first**, at the level closest to where
the bug manifests:

- pure logic / parsing → unit test
- SQL behavior → `sqllogictest` case
- end-to-end write path → `integration_test` / `e2e`

The test must fail on the *specific* symptom (an error string, a row count, an
error code) — "errors somewhere" isn't enough. Then write the fix, confirm the
test passes, run the suite for regressions, and keep the test as a guard named
after the bug. Don't skip the failing-test step because the fix looks obvious.

## Code style

TimeFusion is maintained by a small team; **conciseness and zero boilerplate are
top priorities**. Before opening a PR:

- Prefer extending existing functions over adding new ones; generalize rather
  than duplicate.
- Make surgical changes — touch only what the change requires, match the
  surrounding style, and don't refactor unrelated code or delete pre-existing
  dead code unless that's the point of the PR.
- Use `ArrayData::try_new` (not `new_unchecked`), named constants for magic
  numbers, and bounded deserialization.
- Run `cargo fmt` and `cargo clippy` before pushing; fix new warnings.

See [CLAUDE.md](CLAUDE.md) for the full code philosophy.

## Commits & pull requests

- Keep commits focused; write clear messages describing the *why*.
- Make sure `cargo test`, `cargo fmt --check`, and `cargo clippy` pass.
- Open the PR against `master` with a description of the change and how you
  verified it.

By contributing, you agree your contributions are licensed under the
[MIT License](LICENSE).
