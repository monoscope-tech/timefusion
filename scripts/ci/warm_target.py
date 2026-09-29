#!/usr/bin/env python3
"""Seed a cold target/ with a copy-on-write clone of a sibling worktree's.

A new worktree otherwise rebuilds ~900 dependency crates (~5 min, more under
load). sccache does not prevent it: proc-macro dylibs are uncacheable and
build-script OUT_DIRs are per target dir, so the misses cascade to nearly every
dependent. A clone costs seconds and no disk (APFS clonefile / Linux reflink),
and Cargo's own fingerprints then rebuild only what differs here: this crate
and the vendored path dependencies, whose paths are per worktree.
"""
import fcntl
import os
from pathlib import Path
import platform
import subprocess
import sys

root = Path.cwd()
dest = Path(os.environ.get('CARGO_TARGET_DIR') or root / 'target')


def worktrees():
    out = subprocess.run(['git', 'worktree', 'list', '--porcelain'], capture_output=True, text=True, check=True).stdout
    return [Path(line[9:]) for line in out.splitlines() if line.startswith('worktree ')]


def donors(sub):
    # Same lock AND manifest (profiles, features) reuses every dependency; same
    # lock nearly all; any other tree still reuses most. Newest first within each.
    manifests = [(root / name).read_bytes() for name in ('Cargo.lock', 'Cargo.toml')]
    found = []
    for tree in worktrees():
        debug = tree / 'target' / sub / 'debug'
        if tree.resolve() == root.resolve() or not (debug / '.fingerprint').is_dir():
            continue
        try:
            same = [(tree / name).read_bytes() == own for name, own in zip(('Cargo.lock', 'Cargo.toml'), manifests)]
        except OSError:
            same = [False, False]
        found.append((all(same), same[0], (debug / '.fingerprint').stat().st_mtime, debug))
    return [debug for *_, debug in sorted(found, reverse=True)]


def clone(debug, dest):
    # Cargo holds .cargo-build-lock exclusively while it builds. Sharing it keeps
    # a half-written artifact from being cloned under a fingerprint that calls it fresh.
    with open(debug / '.cargo-build-lock', 'ab') as held:
        try:
            fcntl.flock(held, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except OSError:
            return False
        dest.mkdir(parents=True, exist_ok=True)
        flags = ['-c', '-R', '-p'] if platform.system() == 'Darwin' else ['-a', '--reflink=auto']
        subprocess.run(['cp', *flags, str(debug), str(dest) + '/'], check=True)
    print(f'── seeded {dest}/debug from {debug} (copy-on-write)', file=sys.stderr)
    return True


# `clippy` is the dir ci.sh lints in, beside the test build. A donor's main
# dir still holds check-mode artifacts if its owner ran `cargo lint` there.
for sub, sources in (('', ('',)), ('clippy', ('clippy', ''))):
    if (dest / sub / 'debug').exists():
        continue
    if not any(clone(debug, dest / sub) for source in sources for debug in donors(source)):
        print(f'── no idle sibling target/{sub} to seed from; building from scratch', file=sys.stderr)
