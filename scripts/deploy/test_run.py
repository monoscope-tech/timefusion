"""A docs-only merge must not cancel the code deploy it lands behind."""
import pathlib
import re
import subprocess
import tempfile
import unittest

from lease import DeploymentLease
from run import UNDEPLOYABLE, only_undeployable

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


if __name__ == '__main__':
    unittest.main()
