# Rust distillation workflow

Run `scripts/rs-distill-all.sh` from a clean worktree to review every handwritten
Rust file in the repository except vendored/generated code. Each file gets an
independent Codex pass with the `rs-distill` rules, formatting, and a whitespace
check. Compile and test the integrated result in batches instead of rebuilding
the dependency graph after every file.

The workflow records completed paths in `.git/rs-distill/completed`, so an
interrupted run resumes without repeating finished files. Pass paths explicitly
to review a subset:

```sh
scripts/rs-distill-all.sh src/config.rs src/main.rs
```

Inspect and commit the accumulated diff in small batches. Run `make prepush`
before publishing changes. Delete the state file to repeat the complete review.
