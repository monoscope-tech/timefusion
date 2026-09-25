# Local CI

Run checks locally before pushing changes. Prefer laptop build caches and local services.
Use `make ci-signoff` to run checks and publish passing results for GitHub to reuse.
For a limited change, use `make ci-signoff CHECKS="..."` with the relevant checks from `ci/checks.tsv`.
The final status output lists every check that still needs GitHub.

Record the local commands, results, and outstanding checks in the PR description.
If a required service or tool is unavailable, record that limitation and let GitHub run the affected checks.
Never publish an attestation for a check that did not pass.

Use standard GitHub-hosted runners for remote jobs. Do not add Blacksmith runners or actions without an explicit user request.
See [the local CI guide](docs/local-ci.md) for setup, capabilities, and cache controls.

## Batch work and release validation

- **Freeze the release batch.** Do not apply optional refactors while release checks run. Change the frozen batch only to fix a required gate failure.
- **Reuse valid results.** Rerun only checks affected by changed code, dependencies, toolchains, or build settings. Record the checked inputs, command, result, and log location. Missing output is not a passing result. Preserve valid results when another check fails.
- **Batch dependency pins.** Update related dependency revisions together, regenerate the lockfile, then run one integrated release validation. Do not repeat full validation after each individual pin.
- **Avoid competing builds.** Do not queue multiple Cargo processes against the same build cache, including from separate worktrees. Use the wait for independent review or documentation work. Do not restart a live check because it produces no output.
- **Deploy fewer, coherent releases.** Combine changes with compatible safety and rollback requirements. Keep broader rollup optimizations separate from the first resource-safety release. Defer optional changes instead of repeatedly expanding a release that is ready for validation.

These rules reduce repeated work, not acceptance requirements. Keep correctness, resource limits, rollback, and local CI signoff as release gates.
