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
make test                           # the whole suite in one parallel run (~74s)
cargo nextest run <substring>       # one test or one .slt file, e.g. `dedup_compaction`
cargo nextest run --lib             # unit tests only
make test-e2e                       # end-to-end (Docker required; testcontainers MinIO)
RUST_LOG=debug cargo nextest run --no-capture   # with debug logging
make test-all                       # also the #[ignore]d tests
```

- **Dev builds compile far faster than release** — use them while iterating.
- Tests that mutate process env (`std::env::set_var`) must be `#[serial]` (via
  `serial_test`) with a SAFETY comment explaining the race they avoid.
- The `e2e` suite exercises the full prod path (pgwire → WAL → MemBuffer →
  flush → Delta on MinIO → query) with a virtual clock; a failure there mirrors
  a prod failure.

## Running CI locally

Our GitHub runners are 4 vCPU; your machine almost certainly isn't. `make ci`
runs **the same checks CI runs, from the same definition** — `ci/checks.tsv`,
which both the workflow and the Makefile read, so the two cannot drift:

```bash
make ci                      # everything CI runs: fmt, clippy, test, pg-smoke, e2e
make ci CHECKS="fmt clippy"  # just these
make ci-status               # what CI would run right now, without running it
make ci-down                 # stop the MinIO it started
```

**Use it before you push.** Each check is fingerprinted over the content it
depends on, and a pass publishes that result as a git ref — so CI's gate skips
any check already proven for the exact tree it is about to test. Run `make ci`
and CI has little or nothing left to do.

This is unusually faithful for a Rust project: `rust-toolchain.toml` pins the
channel and `ci.yml` installs exactly that, so your `cargo` *is* CI's compiler.
There is no container to reproduce — `make ci` runs natively and only supplies
the MinIO that CI starts by hand, from the same pinned image.

`make prepush` is still the right fast inner-loop gate. The difference is that
`make ci` covers everything (including `e2e` and the pg client smoke) and its
results are published, so they count.

Two things worth knowing:

- **Publishing requires push access.** From a fork, `make ci` still gives you
  fast local feedback but CI re-runs the checks itself. That's deliberate: an
  attestation is only as trustworthy as the ability to push code in the first
  place.
- **Reuse is conservative.** Each check declares the capabilities it needs
  (`rust`, `protoc`, `nextest`, `minio`, `docker`) and each result records what
  the environment actually had, so a run without MinIO can never stand in for one
  that needs it.

Full reference — every knob, and how to add or change a check:
[docs/local-ci.md](docs/local-ci.md).

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

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the current design rules.

## Commits & pull requests

- Keep commits focused; write clear messages describing the *why*.
- Make sure `make prepush` (`cargo lint` + the full suite) and `cargo fmt --check` pass.
- Open the PR against `master` with a description of the change and how you
  verified it.

By contributing, you agree your contributions are licensed under the
[MIT License](LICENSE).
