#!/usr/bin/env python3
"""Run the real CI self-test in a disposable repository with retained receipts."""
import io
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import unittest

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts/ci/ci.sh"


class SelftestPreservesEvidence(unittest.TestCase):
    def test_existing_and_fresh_ci_directories(self):
        with tempfile.TemporaryDirectory(prefix="timefusion-ci-selftest-") as directory:
            fixture = Path(directory)
            archive = subprocess.check_output(["git", "archive", "HEAD"], cwd=ROOT)
            with tarfile.open(fileobj=io.BytesIO(archive)) as tree:
                tree.extractall(fixture, filter="data")
            shutil.copy2(SCRIPT, fixture / "scripts/ci/ci.sh")
            subprocess.run(["git", "init", "--quiet"], cwd=fixture, check=True)
            subprocess.run(["git", "add", "."], cwd=fixture, check=True)
            subprocess.run([
                "git", "-c", "user.name=CI fixture", "-c", "user.email=ci@example.invalid",
                "-c", "commit.gpgsign=false", "-c", "core.hooksPath=/dev/null",
                "commit", "--quiet", "-m", "fixture",
            ], cwd=fixture, check=True)
            for retained in ({}, {"receipts/keep.bin": b"receipt\0\xff"}, {
                "receipts/keep.bin": b"receipt\0\xff", "fingerprints.tsv": b"previous\tfingerprint\n",
            }):
                with self.subTest(files=list(retained)):
                    ci = fixture / ".ci"
                    for name, content in retained.items():
                        path = ci / name
                        path.parent.mkdir(parents=True, exist_ok=True)
                        path.write_bytes(content)
                    result = subprocess.run(
                        ["bash", "scripts/ci/ci.sh", "selftest"], cwd=fixture,
                        env={**os.environ, "CI_FINGERPRINTS": ""}, capture_output=True, text=True, timeout=120,
                    )
                    self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                    actual = {str(path.relative_to(ci)): path.read_bytes() for path in ci.rglob("*") if path.is_file()}
                    self.assertEqual(actual, retained, "self-test must preserve every existing evidence byte")
                    self.assertEqual(ci.exists(), bool(retained))


if __name__ == "__main__":
    unittest.main()
