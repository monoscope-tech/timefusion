"""A shared Git lease for local and GitHub production rollouts."""
import contextlib
from dataclasses import dataclass
import json
import os
import subprocess
import time
import uuid

LEASE_REF = 'refs/timefusion-deploy/lease'
RECEIPT_REF = 'refs/timefusion-deploy/completed'


@dataclass
class RolloutGuard:
    owner: str
    unresolved: bool = False

    def submitted(self):
        self.unresolved = True

    def verified(self):
        self.unresolved = False


class DeploymentLease:
    def __init__(self, root):
        self.root = root

    def git(self, *args, **kwargs):
        return subprocess.run(['git', *args], cwd=self.root, text=True, check=True,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs).stdout.strip()

    def remote(self, ref):
        value = self.git('ls-remote', 'origin', ref)
        return value.split()[0] if value else None

    def record(self, ref):
        commit = self.remote(ref)
        if commit is None:
            return None
        self.git('fetch', '--no-write-fetch-head', '--no-tags', 'origin', ref)
        return json.loads(self.git('show', commit + ':record.json'))

    def commit(self, record, parent=None):
        blob = self.git('hash-object', '-w', '--stdin', input=json.dumps(record))
        tree = self.git('mktree', input='100644 blob ' + blob + '\trecord.json\n')
        parents = ['-p', parent] if parent else []
        return self.git('-c', 'user.name=Timefusion deployment', '-c', 'user.email=deploy@timefusion.invalid',
                        'commit-tree', tree, *parents, '-m', 'Production rollout record')

    @contextlib.contextmanager
    def hold(self, image, wait_seconds=2700):
        owner = self.commit({'image': image, 'nonce': uuid.uuid4().hex,
                             'run_id': os.environ.get('GITHUB_RUN_ID'), 'pid': os.getpid(), 'started': time.time()})
        deadline = time.monotonic() + wait_seconds
        last_notice = 0
        while True:
            try:
                # Parentless commits cannot fast-forward an existing owner's ref.
                self.git('push', 'origin', owner + ':' + LEASE_REF)
                break
            except subprocess.CalledProcessError:
                occupied = self.remote(LEASE_REF)
                if occupied is None or time.monotonic() >= deadline:
                    raise
                if time.monotonic() - last_notice >= 30:
                    print('Production lease ' + occupied[:12] + ' is occupied; waiting.', flush=True)
                    last_notice = time.monotonic()
                time.sleep(5)
        guard = RolloutGuard(owner)
        try:
            yield guard
        finally:
            if guard.unresolved:
                print('Rollout state is unresolved; retaining production lease ' + owner + ' for recovery.', flush=True)
            else:
                # Never delete a lease whose owner has changed.
                self.git('push', '--force-with-lease=' + LEASE_REF + ':' + owner, 'origin', ':' + LEASE_REF)

    def complete(self, image, boot_micros):
        previous = self.remote(RECEIPT_REF)
        if previous:
            self.git('fetch', '--no-write-fetch-head', '--no-tags', 'origin', RECEIPT_REF)
        commit = self.commit({'image': image, 'boot_micros': boot_micros, 'completed': time.time()}, parent=previous)
        self.git('push', 'origin', commit + ':' + RECEIPT_REF)
