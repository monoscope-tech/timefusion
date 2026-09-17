#!/usr/bin/env python3
"""Regression tests for the fail-closed production handoff gate."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
PREPARE = ROOT / "scripts/deploy/prepare.sh"


class PrepareHandoffTest(unittest.TestCase):
    def run_prepare(self, status: int) -> tuple[subprocess.CompletedProcess[str], str, str]:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            timeout = root / "timeout"
            timeout.write_text('#!/bin/sh\nprintf "%s\\n" "$*" >> "$CALL_LOG"\nexit "$MOCK_STATUS"\n')
            timeout.chmod(0o755)
            output = root / "output"
            calls = root / "calls"
            output.write_text("")
            calls.write_text("")
            env = dict(
                os.environ,
                PATH=f"{root}:{os.environ['PATH']}",
                PGURL="postgres://test",
                GITHUB_OUTPUT=str(output),
                CALL_LOG=str(calls),
                MOCK_STATUS=str(status),
            )
            result = subprocess.run(["bash", str(PREPARE)], text=True, capture_output=True, env=env)
            return result, output.read_text(), calls.read_text()

    def assert_single_handoff(self, calls: str) -> None:
        self.assertEqual(1, len(calls.splitlines()))
        self.assertIn("HANDOFF", calls)
        self.assertNotIn("FLUSH", calls)

    def test_success_proves_drained_fence(self) -> None:
        result, output, calls = self.run_prepare(0)
        self.assertEqual(0, result.returncode)
        self.assertEqual("drained=true\n", output)
        self.assert_single_handoff(calls)

    def test_server_failure_aborts_without_claiming_drained(self) -> None:
        result, output, calls = self.run_prepare(1)
        self.assertEqual(1, result.returncode)
        self.assertEqual("", output)
        self.assertIn("did not prove a drained write fence", result.stdout)
        self.assert_single_handoff(calls)

    def test_client_timeout_aborts_without_claiming_drained(self) -> None:
        result, output, calls = self.run_prepare(124)
        self.assertEqual(1, result.returncode)
        self.assertEqual("", output)
        self.assert_single_handoff(calls)


if __name__ == "__main__":
    unittest.main()
