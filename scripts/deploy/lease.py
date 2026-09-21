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

# A MUTATING lease is not reclaimable on age alone — its holder may still be
# changing production. But refusing forever turns a transient fault into a
# permanent outage: prod 2026-09-14 lost a deploy mid-rollout, and the lease it
# left behind (mutating, holder long dead) blocked EVERY later rollout for 15.6
# hours until it was deleted by hand — the second such wedge in one night.
#
# So the question is not "is it old" but "is its holder still alive". A GitHub run
# that has completed is proof the holder is gone; the long grace on top is for the
# case where the API answers wrongly or the holder is local (no run id), where this
# still refuses.
MUTATING_DEAD_SECONDS = 7200

# Clock skew allowance between the runner (which stamps `started`) and the box
# (which stamps `boot_micros`). Applied so that only a CLEARLY older boot counts
# as proof, since the dangerous mistake is calling a live rollout dead.
BOOT_SKEW_SECONDS = 120


def production_booted_before(started):
    """Whether the process serving production now predates `started`.

    Proof that a rollout never replaced anything. A mutating lease is otherwise
    unreclaimable until `MUTATING_DEAD_SECONDS`, which blocks every later deploy
    for two hours — and on 2026-09-21 three drain timeouts in a row meant that
    penalty landed three times, each needing a person to check the same two facts
    and delete the ref by hand.

    Those two facts are mechanical, so this asks them instead. `boot_micros` is
    the serving process's own boot stamp, the same value `record-boot.sh` reads,
    so it reports the process actually answering queries rather than whatever an
    orchestrator believes it scheduled.

    Fail-safe in every direction: no `PGURL`, an unreachable database, a missing
    row, an unparsable answer, or a boot that is merely close to `started` all
    return False — "cannot prove it, leave the lease alone".
    """
    url = os.environ.get('PGURL')
    if not url or not started:
        return False
    query = "SELECT value FROM timefusion_stats WHERE component = 'buffered_layer' AND key = 'boot_micros'"
    try:
        probe = subprocess.run(['psql', url, '-v', 'ON_ERROR_STOP=1', '-Atqc', query], capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.SubprocessError):
        return False
    if probe.returncode != 0:
        return False
    try:
        boot_seconds = int(probe.stdout.strip()) / 1_000_000
    except ValueError:
        return False
    return boot_seconds < started - BOOT_SKEW_SECONDS


def run_is_finished(run_id):
    """True only when GitHub states this run is no longer active.

    Every other outcome — no id, a local holder, an API error, a timeout, an
    unexpected answer — is False, i.e. "assume alive, do not reclaim".
    """
    if not run_id:
        return False
    try:
        done = subprocess.run(['gh', 'run', 'view', str(run_id), '--json', 'status', '-q', '.status'],
                              capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.SubprocessError):
        return False
    return done.returncode == 0 and done.stdout.strip() == 'completed'


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

        Fail-safe in every direction: an unreadable record, or a missing
        `mutating` key (an older writer, or one that never got to declare), mean
        "leave it alone". A lease that DID declare a mutation is reclaimable only
        once GitHub says its run finished AND it is well past
        `MUTATING_DEAD_SECONDS` — otherwise a holder that dies mid-rollout blocks
        every later deploy forever, which has now happened twice.
        """
        try:
            self.git('fetch', '--no-write-fetch-head', '--no-tags', 'origin', LEASE_REF)
            record = json.loads(self.git('show', commit + ':record.json'))
        except (subprocess.CalledProcessError, ValueError):
            return False
        started = record.get('started', time.time())
        age = time.time() - started
        if record.get('mutating') is not False:
            # Reclaimable only once the holder is PROVABLY gone. Age alone never
            # qualifies a mutating lease, and a holder we cannot ask about never does.
            if not run_is_finished(record.get('run_id')):
                return False
            # A finished run cannot still be mid-rollout. What remains is whether
            # it ever REPLACED anything, and there are two independent witnesses —
            # either suffices, because both only say "this rollout never became
            # production".
            if production_booted_before(started):
                print('Production still serves a process that predates lease ' + commit[:12] + '; it never replaced anything.', flush=True)
                return True
            if self.receipt_image_differs(record.get('image')):
                print('Lease ' + commit[:12] + ' never reached the completion receipt; it never became production.', flush=True)
                return True
            return age >= MUTATING_DEAD_SECONDS
        return age >= STALE_SECONDS

    def receipt_image_differs(self, image):
        """Whether the last COMPLETED rollout served a different image than `image`.

        The restart-independent half of the reclaim proof, and the reason it
        exists: `production_booted_before` is defeated by ANY unrelated restart.
        Bounce the container for an incident and every wedged lease suddenly
        predates nothing, so the check declines and the two-hour grace applies
        again. That happened three times on 2026-09-21, each time costing an hour
        of blocked deploys during an incident — precisely when deploying matters.

        The receipt moves only when a rollout actually completes, so restarts
        cannot perturb it. If the last completed rollout is a DIFFERENT image than
        this lease's, the lease never became production.

        Fails safe: an unreadable or missing receipt, or a matching image, both
        return False and leave the lease alone.
        """
        if not image:
            return False
        try:
            receipt = self.remote(RECEIPT_REF)
            if receipt is None:
                return False
            self.git('fetch', '--no-write-fetch-head', '--no-tags', 'origin', RECEIPT_REF)
            served = json.loads(self.git('show', receipt + ':record.json')).get('image')
        except (subprocess.CalledProcessError, ValueError):
            return False
        return bool(served) and served != image

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
