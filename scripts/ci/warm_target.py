#!/usr/bin/env python3
"""Seed a cold target/ with a copy-on-write clone of a sibling worktree's.

A new worktree otherwise rebuilds ~900 dependency crates (~5 min, more under
load). sccache does not prevent it: proc-macro dylibs are uncacheable and
build-script OUT_DIRs are per target dir, so the misses cascade to nearly every
dependent. A clone costs seconds and no disk (APFS clonefile / Linux reflink),
and Cargo's own fingerprints then rebuild only what differs here: this crate
and the vendored path dependencies, whose paths are per worktree. A warm
target/ gains the dependencies an identically-configured sibling already built,
so a Cargo.toml profile or lock change is compiled once, not once per worktree.
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
    # (exact, debug): exact = same lock AND manifest (profiles, features), so the
    # donor holds every dependency this tree needs. Same lock next, then any.
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
    return [(exact, debug) for exact, _, _, debug in sorted(found, reverse=True)]


def locked(debug, mode):
    # Cargo holds .cargo-build-lock exclusively while it builds: sharing the
    # donor's keeps a half-written artifact from being cloned under a fingerprint
    # that calls it fresh; owning ours keeps a build from racing the merge.
    held = open(debug / '.cargo-build-lock', 'ab')
    try:
        fcntl.flock(held, mode | fcntl.LOCK_NB)
        return held
    except OSError:
        held.close()


def cp(*args, check=True):
    flags = ['-c', '-R', '-p'] if platform.system() == 'Darwin' else ['-a', '--reflink=auto']
    subprocess.run(['cp', *flags, *args], check=check)


def seed(sub, sources):
    mine = dest / sub / 'debug'
    for source in sources:
        for exact, debug in donors(source):
            # An existing dir only gains what an exact donor has and it lacks: the
            # hash-named deps/build/.fingerprint entries, never overwriting one.
            # That is how a profile or lock change is paid once, by one worktree.
            if mine.exists() and not exact:
                continue
            donor = locked(debug, fcntl.LOCK_SH)
            if not donor:
                continue
            verb = 'merged into' if mine.exists() else 'seeded'
            with donor:
                if not mine.exists():
                    mine.parent.mkdir(parents=True, exist_ok=True)
                    cp(str(debug), str(mine.parent) + '/')
                else:
                    own = locked(mine, fcntl.LOCK_EX)
                    if not own:
                        return
                    with own:
                        for part in ('deps', 'build', '.fingerprint'):
                            (mine / part).mkdir(exist_ok=True)
                            # macOS exits 1 for every file -n skips; a gap only costs a rebuild.
                            cp('-n', f'{debug / part}/.', f'{mine / part}/', check=False)
            print(f'── {verb} {mine} from {debug} (copy-on-write)', file=sys.stderr)
            return
    if not mine.exists():
        print(f'── no idle sibling target/{sub} to seed from; building from scratch', file=sys.stderr)


# `clippy` is the dir ci.sh lints in, beside the test build. A donor's main
# dir still holds check-mode artifacts if its owner ran `cargo lint` there.
seed('', ('',))
seed('clippy', ('clippy', ''))
