"""A docs-only merge must not cancel the code deploy it lands behind."""
import os
import pathlib
import re
import subprocess
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from lease import DeploymentLease
from run import UNDEPLOYABLE, deployment_target_error, only_undeployable, prepare_handoff, reconcile_replacement

ROOT = pathlib.Path(__file__).resolve().parents[2]


def repo(root, *rounds):
    """A clone whose origin/master carries each round of paths as one commit."""
    remote, work = root / 'remote', root / 'work'
    run = lambda *args, **kw: subprocess.run(['git', *map(str, args)], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, **kw)
    run('init', '--bare', '--initial-branch=master', remote)
    run('clone', remote, work)
    run('-C', work, 'config', 'user.email', 'test@example.com')
    run('-C', work, 'config', 'user.name', 'test')
    commits = []
    for round_index, paths in enumerate(rounds):
        for path in paths:
            target = work / path
            target.parent.mkdir(parents=True, exist_ok=True)
            # Content must differ per round, or a rewritten path is not a diff.
            target.write_text(f'{path} round {round_index}')
        run('-C', work, 'add', '-A')
        run('-C', work, 'commit', '-m', ' '.join(paths))
        commits.append(subprocess.run(['git', '-C', work, 'rev-parse', 'HEAD'], check=True, text=True, stdout=subprocess.PIPE).stdout.strip())
    run('-C', work, 'push', '--quiet', 'origin', 'master')
    return DeploymentLease(work), commits


class SupersedeTest(unittest.TestCase):
    def check(self, later):
        with tempfile.TemporaryDirectory() as directory:
            lease, commits = repo(pathlib.Path(directory), ['src/main.rs'], later)
            return only_undeployable(lease, commits[0], commits[1])

    def test_docs_only_does_not_supersede(self):
        for paths in (['docs/plan.md'], ['README.md'], ['bench/probe.py'], ['docs/a.md', 'bench/b.py']):
            with self.subTest(paths=paths):
                self.assertTrue(self.check(paths), f'{paths} cannot change the image, so it must not cancel a rollout')

    def test_code_still_supersedes(self):
        for paths in (['src/main.rs'], ['Cargo.toml'], ['docs/a.md', 'src/main.rs']):
            with self.subTest(paths=paths):
                self.assertFalse(self.check(paths), f'{paths} can change the image, so it must still defer the rollout')

    def test_identical_commits_are_not_treated_as_undeployable(self):
        with tempfile.TemporaryDirectory() as directory:
            lease, commits = repo(pathlib.Path(directory), ['src/main.rs'])
            self.assertFalse(only_undeployable(lease, commits[0], commits[0]), 'an empty diff must not be read as a docs-only difference')

    def test_an_autofmt_commit_does_not_supersede(self):
        """An autofmt commit changes `.rs`, so only the AUTHOR distinguishes it.

        It is pushed with GITHUB_TOKEN, which triggers no workflow, so deferring
        to it leaves production on the old image indefinitely — three hours on
        2026-09-20, across two commits, every deploy job green.
        """
        with tempfile.TemporaryDirectory() as directory:
            lease, commits = repo(pathlib.Path(directory), ['src/main.rs'], ['src/main.rs'])
            subprocess.run(
                ['git', '-C', str(lease.root), 'commit', '--amend', '--no-edit', '--author',
                 'github-actions[bot] <github-actions[bot]@users.noreply.github.com>'],
                check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            head = subprocess.run(['git', '-C', str(lease.root), 'rev-parse', 'HEAD'], check=True, text=True, stdout=subprocess.PIPE).stdout.strip()
            subprocess.run(['git', '-C', str(lease.root), 'push', '--quiet', '--force', 'origin', 'master'], check=True)
            self.assertTrue(only_undeployable(lease, commits[0], head), 'a bot commit starts no rollout of its own, so it must not cancel this one')

    def test_a_human_commit_after_a_bot_one_still_supersedes(self):
        """Mixed authorship means a real push followed, and that one WILL deploy."""
        with tempfile.TemporaryDirectory() as directory:
            lease, commits = repo(pathlib.Path(directory), ['src/main.rs'], ['src/a.rs'], ['src/b.rs'])
            self.assertFalse(only_undeployable(lease, commits[0], commits[2]), 'a human commit in the range must still defer the rollout')

    def test_the_autofmt_workflow_starts_its_own_rollout(self):
        """The net above only exists because the dispatch can fail; both must hold."""
        autofmt = (ROOT / '.github/workflows/autoformat.yml').read_text()
        self.assertIn('gh workflow run', autofmt, 'autoformat must dispatch its own CI and deploy')
        for flow in ('ci.yml', 'deploy.yml'):
            text = (ROOT / '.github/workflows' / flow).read_text()
            self.assertRegex(text, r'(?m)^\s*workflow_dispatch:', f'{flow} must accept the dispatch autoformat sends')

    def test_ignore_list_matches_the_workflow(self):
        """Drift here is silent: the workflow would skip a path this still defers on."""
        # Parsed rather than yaml-loaded so the check needs no third-party module.
        text = (ROOT / '.github/workflows/deploy.yml').read_text()
        block = re.search(r'^\s*paths-ignore:\n((?:\s*-\s.*\n)+)', text, re.M)
        self.assertIsNotNone(block, 'deploy.yml no longer declares paths-ignore')
        ignored = {re.sub(r"^\s*-\s*|['\"]", '', line) for line in block.group(1).splitlines() if line.strip()}
        covered = {p.rstrip('/') + '/**' for p in UNDEPLOYABLE} | {'**/*.md'}
        self.assertEqual(ignored, covered, 'deploy.yml paths-ignore and run.py UNDEPLOYABLE disagree')


class DeploymentTargetTest(unittest.TestCase):
    def test_only_timefusion_can_reach_the_deployment_lease(self):
        self.assertIsNone(deployment_target_error('timefusion'))
        self.assertIn('monoscope', deployment_target_error('monoscope'))

    def run_preflight(self, status=1108, raw=None, delay=0):
        seen = {}

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                seen['path'] = self.path
                seen['token'] = self.headers.get('x-captain-app-token')
                seen['namespace'] = self.headers.get('x-namespace')
                seen['content_type'] = self.headers.get('content-type')
                seen['body'] = self.rfile.read(int(self.headers.get('content-length', 0)))
                time.sleep(delay)
                payload = raw if raw is not None else ('{"status":%d,"description":"test"}' % status).encode()
                self.send_response(200)
                self.send_header('content-type', 'application/json')
                self.send_header('content-length', str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, _format, *_args):
                pass

        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        try:
            with tempfile.NamedTemporaryFile() as output:
                env = {'PATH': os.environ['PATH'], 'CAPROVER_SERVER': f'http://127.0.0.1:{server.server_port}',
                       'CAPROVER_APP': 'timefusion', 'CAPROVER_TOKEN': 'scoped-token',
                       'CAPROVER_PREFLIGHT_TIMEOUT_MS': '50' if delay else '10000', 'GITHUB_OUTPUT': output.name}
                result = subprocess.run(['bash', ROOT / 'scripts/deploy/preflight.sh'], env=env, text=True,
                                        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                output.seek(0)
                result.identity_record = output.read().decode()
        finally:
            server.shutdown()
            thread.join()
            server.server_close()
        self.assertEqual(seen, {'path': '/api/v2/user/apps/appData/timefusion/', 'token': 'scoped-token',
                                'namespace': 'captain', 'content_type': 'application/x-www-form-urlencoded',
                                'body': b''})
        return result

    def test_preflight_proves_the_app_token_pair_without_starting_a_build(self):
        result = self.run_preflight()
        self.assertEqual(result.returncode, 0)
        self.assertRegex(result.identity_record, r'target_server_host=127\.0\.0\.1:\d+\n'
                         r'target_app=timefusion\ntoken_bound=true\nverified_at=.+Z\n')

    def test_preflight_rejects_a_token_for_another_app(self):
        result = self.run_preflight(1106)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('rejected', result.stderr)

    def test_preflight_rejects_a_non_json_proxy_response(self):
        result = self.run_preflight(raw=b'Internal Server Error')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('non-JSON', result.stderr)

    def test_preflight_times_out_closed(self):
        self.assertNotEqual(self.run_preflight(delay=0.2).returncode, 0)

    def test_failed_preflight_cannot_announce_or_prepare_a_handoff(self):
        calls = []

        def stage(name):
            calls.append(name)
            raise RuntimeError('wrong token')

        class Guard:
            def submitted(self):
                calls.append('submitted')

        with self.assertRaisesRegex(RuntimeError, 'wrong token'):
            prepare_handoff(stage, Guard())
        self.assertEqual(calls, ['preflight'])


class ReplacementReconciliationTest(unittest.TestCase):
    def run_reconciliation(self, availability_slo_met):
        calls = []

        def stage(name):
            calls.append(name)
            if name == 'verify-recovery':
                return {'availability_slo_met': availability_slo_met}
            if name == 'record-boot':
                return {'boot_micros': '1789610570795968'}
            return {}

        class Lease:
            def complete(self, image, boot):
                calls.append(('complete', image, boot))

        class Guard:
            def verified(self):
                calls.append('verified')

        return calls, lambda: reconcile_replacement(stage, Lease(), Guard(), 'image@sha256:digest')

    def test_availability_miss_is_reported_after_soak_receipt_and_verification(self):
        calls, reconcile = self.run_reconciliation('false')
        with self.assertRaisesRegex(RuntimeError, 'missed the availability SLO'):
            reconcile()
        self.assertEqual(calls, [
            'verify-recovery', 'soak', 'record-boot',
            ('complete', 'image@sha256:digest', '1789610570795968'), 'verified',
        ])

    def test_available_replacement_completes_normally(self):
        calls, reconcile = self.run_reconciliation('true')
        self.assertEqual(reconcile(), '1789610570795968')
        self.assertEqual(calls[-2:], [('complete', 'image@sha256:digest', '1789610570795968'), 'verified'])


if __name__ == '__main__':
    unittest.main()
