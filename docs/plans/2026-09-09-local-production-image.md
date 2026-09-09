# Local production image and deployment

The ARM64 laptop cross-compiles the Linux amd64 production image. The compiler
and dependency tools run natively. Runtime libraries match the target, including
libunwind and liblzma. Profiling, frame pointers, and x86-64-v3 remain enabled.

`make ci-signoff` runs the local checks and helper tests. When all required
checks have matching passing records, it builds, smoke-tests, and publishes a
candidate image. Limited signoff leaves image publication pending when required
checks remain. No failed image smoke produces a published candidate.

The helper archives a captured Git tree using a separate index. Build inputs
and the smoke recipe determine the candidate fingerprint. User staging is
preserved, and later edits cannot alter the archived build. Master deployment
resolves that candidate to an immutable digest and skips compilation when it
exists. The native CI build remains the fallback.

`make deploy` requires a clean checkout of current master, matching checks,
a published image, and PGURL/CAPROVER_SERVER/CAPROVER_APP/CAPROVER_TOKEN.
It runs the same handoff, rollout, recovery verification, and readiness soak
as CI. The five shell stages were extracted from the existing workflow without
changing their thresholds. A shared Git lease serializes local and CI rollouts.
A digest plus live boot receipt avoids a duplicate restart. Failure from the first
handoff mutation retains the lease until the remote rollout is investigated.
Private diagnostics remain under `.ci/deploy/rollout-*`.

## Validation

- ARM64 Rust and C toolchain probes produced runnable x86-64 executables.
- Full cross-build exited 0. Image `timefusion-production-cross:local` has ID
  `sha256:4cdb3458c830f0b65acf55e9e4582926f527c5b404985c9a617eb43afd63c7ae`.
  The first uncached build took about 40 minutes. Warm speed is unmeasured.
- Default local amd64 execution exited 139 for both this image and the known-good
  production image a31c7b5. Explicit QEMU also failed with its default layout.
- QEMU 10.0.11 with `-B 0x800000000000 -cpu max` started both unchanged images.
  Both passed their actual PGWire healthcheck. A syscall trace of the failing
  layout showed high guest mappings before SIGSEGV. This diagnoses the local
  harness; it does not establish the exact allocator fault instruction.
- The helper now selects QEMU automatically on ARM64 Docker hosts, waits through
  the 12-second startup window, requires the real PGWire listener, and executes
  the image's protocol healthcheck. Native amd64 runs the same checks directly.
- Real temporary-Git tests cover fingerprint invalidation, frozen source,
  preservation of staging, lease exclusion, receipt updates, interrupted rollout
  retention, and verified release. No production lease has been created yet.
- Actionlint 1.7.7 passed with shellcheck disabled. Ruby YAML parsing and Bash
  syntax checks passed. The native deployment client image built successfully.

Full updated `CARGO_TARGET_DIR=/tmp/timefusion-isolated-target make ci-signoff`
passed (session 1851). All five unchanged Rust checks reused matching passing
records; both helper tests ran and passed. The frozen image build reused its
cached layers, passed the updated smoke, and published the production digest:

`sha256:61104180933df79eb6f2c1ad2dcd15eedf71bb8769183578bc874f58439e2836`

No required check remains for GitHub. CI candidate reuse and local production
rollout remain to be exercised after merge. The existing enabled Timefusion
app deployment token was located on the authorized CapRover host. Only its
presence was reported. The local client is CapRover 2.3.1 and PostgreSQL 15.19.

Two review passes checked artifact attribution and deployment interruption.
The fixes preserve frozen smoke inputs, reject unexpected entrypoints, retain
the lease from HANDOFF onward, and prevent Python imports from dirtying the
checkout. The current diff has no Rust implementation changes.

## Hash-query goal remains open

Production a31c7b5 deployed successfully in run 34351293065. Startup backfill
selected 34 otel files within 1,910 MiB and logged a completed indexing unit.
The post-deployment SQL smoke still failed: only 8 of 16 result pairs completed.
Sep 6 day and Sep 2–9 week queries exceeded the 3-second probe limit.
A Delta/manifest audit found only 14 physical hash candidates among 1,406 live
project files for Sep 2–8. Sep 6 had none. Complete warm benchmark queries are
fast, but partial coverage and cold index loading remain expensive. Continue
coverage and visibility optimization after the local release path is usable.

## First production deployment and digest correction

PR 240 merged as 750742d6. GitHub run 34358189411 found the local candidate,
skipped compilation, passed native smoke, deployed, and passed its readiness
soak: 46 probes with zero transient failures. The workflow completed successfully.

During concurrent local preflight, inspection found that promotion wrapped the
single image in a manifest index. Its outer digest differed from the candidate
although the amd64 image manifest was identical. The local lease waiter was
stopped before it acquired ownership or changed production. Comparing the two
registry references reproduced a failing assertion before the fix.

Digest resolution now selects exactly one Linux amd64 manifest. It rejects
missing or ambiguous platform matches. Existing receipts are normalized too,
so the first index-based receipt remains usable. The real registry comparison
now passes; a regression test covers index wrappers and ambiguous platforms.

Updated local signoff passed, reusing all five matching Rust results and running
both helper suites. Image smoke passed and published digest
`sha256:5f00b4db1acefb426fc8522b540fa4b75a8961805b3fee91087e98915d4d1c2d`.
No required check remains for GitHub. The next rollout will exercise the local
command directly; production hash latency acceptance remains open.
