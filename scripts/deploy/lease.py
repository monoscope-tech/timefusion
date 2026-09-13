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

# A holder that never released its lease wedges EVERY later rollout until a
# person deletes the ref by hand. Prod 2026-09-13: a superseded rollout mutated
# nothing, then its release push failed; the three runs behind it each waited
# out the full timeout and failed, and production could not be deployed for ~5
# hours. Releasing is bookkeeping, so it must neither fail a good rollout nor be
# the only way the ref ever comes back.
#
# Only a lease that provably never reached a production mutation is reclaimable.
# One retained for recovery carries `unresolved` and is left for a person.
STALE_SECONDS = 1800


@dataclass
class RolloutGuard:
    owner: str
    publish: 'callable' = None
    unresolved: bool = False

    def submitted(self):
        # Announce BEFORE the first production mutation, and fail the rollout if
        # the announcement does not land: a lease that never says it may be
        # mutating is reclaimable, so mutating without it risks two concurrent
        # rollouts. Aborting here is safe precisely because nothing has changed yet.
        self.owner = self.publish(mutating=True)
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

    def abandoned(self, commit):
        """Whether `commit` is a dead holder's lease rather than a live or mutating one.

        Fail-safe in every direction: an unreadable record, a missing
        `mutating` key (an older writer, or one that never got to declare), or
        `mutating` set all mean "leave it alone".
        """
        try:
            self.git('fetch', '--no-write-fetch-head', '--no-tags', 'origin', LEASE_REF)
            record = json.loads(self.git('show', commit + ':record.json'))
        except (subprocess.CalledProcessError, ValueError):
            return False
        if record.get('mutating') is not False:
            return False
        return time.time() - record.get('started', time.time()) >= STALE_SECONDS

    @contextlib.contextmanager
    def hold(self, image, wait_seconds=2700):
        record = {'image': image, 'nonce': uuid.uuid4().hex, 'mutating': False,
                  'run_id': os.environ.get('GITHUB_RUN_ID'), 'pid': os.getpid(), 'started': time.time()}
        owner = self.commit(record)
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
                # Reclaim only against the exact commit just observed, so a
                # holder that comes back to life between the read and the push
                # keeps its lease.
                if self.abandoned(occupied):
                    print('Production lease ' + occupied[:12] + ' was abandoned; reclaiming.', flush=True)
                    try:
                        self.git('push', '--force-with-lease=' + LEASE_REF + ':' + occupied, 'origin', owner + ':' + LEASE_REF)
                        break
                    except subprocess.CalledProcessError:
                        pass
                elif time.monotonic() - last_notice >= 30:
                    print('Production lease ' + occupied[:12] + ' is occupied; waiting.', flush=True)
                    last_notice = time.monotonic()
                time.sleep(5)
        def publish(**changes):
            updated = self.commit(dict(record, **changes), parent=guard.owner)
            self.git('push', '--force-with-lease=' + LEASE_REF + ':' + guard.owner, 'origin', updated + ':' + LEASE_REF)
            return updated

        guard = RolloutGuard(owner, publish)
        try:
            yield guard
        finally:
            owner = guard.owner
            if guard.unresolved:
                print('Rollout state is unresolved; retaining production lease ' + owner + ' for recovery.', flush=True)
            else:
                # Never delete a lease whose owner has changed. A transient
                # failure here must not fail an otherwise-good rollout — the
                # reclaim above is what recovers the ref when this loses.
                for delay in (0, 2, 5):
                    time.sleep(delay)
                    try:
                        self.git('push', '--force-with-lease=' + LEASE_REF + ':' + owner, 'origin', ':' + LEASE_REF)
                        break
                    except subprocess.CalledProcessError:
                        pass
                else:
                    print('Could not release production lease ' + owner[:12]
                          + '; a later rollout will reclaim it as abandoned.', flush=True)

    def complete(self, image, boot_micros):
        previous = self.remote(RECEIPT_REF)
        if previous:
            self.git('fetch', '--no-write-fetch-head', '--no-tags', 'origin', RECEIPT_REF)
        commit = self.commit({'image': image, 'boot_micros': boot_micros, 'completed': time.time()}, parent=previous)
        self.git('push', 'origin', commit + ':' + RECEIPT_REF)
