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
from run import UNDEPLOYABLE, deployment_target_error, only_undeployable, prepare_handoff

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


if __name__ == '__main__':
    unittest.main()
