import contextlib
import pathlib
import subprocess
import tempfile
import time
import unittest
from unittest import mock
import lease as lease_module
from lease import DeploymentLease, LEASE_REF, RECEIPT_REF


@contextlib.contextmanager
def repo():
    """A bare origin with two independent checkouts holding leases against it."""
    with tempfile.TemporaryDirectory() as directory:
        root = pathlib.Path(directory)
        remote, first, second = (root / name for name in ('remote', 'first', 'second'))
        def git(*args):
            subprocess.run(['git', *map(str, args)], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        git('init', '--bare', remote)
        for checkout in (first, second):
            git('clone', remote, checkout)
        yield DeploymentLease(first), DeploymentLease(second)


class LeaseTest(unittest.TestCase):
    def test_exclusion_release_and_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            remote, first, second = (root / name for name in ('remote', 'first', 'second'))
            def git(*args):
                subprocess.run(['git', *map(str, args)], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            git('init', '--bare', remote)
            for checkout in (first, second):
                git('clone', remote, checkout)
            a, b = DeploymentLease(first), DeploymentLease(second)
            with a.hold('image-a'):
                with self.assertRaises(subprocess.CalledProcessError):
                    with b.hold('image-b', wait_seconds=0):
                        self.fail('Concurrent owner acquired an occupied lease')
                a.complete('image-a', '123')
                self.assertEqual(b.record(RECEIPT_REF)['boot_micros'], '123')
            self.assertIsNone(a.remote(LEASE_REF))
            with self.assertRaisesRegex(RuntimeError, 'failed rollout'):
                with b.hold('image-b'):
                    raise RuntimeError('failed rollout')
            self.assertIsNone(b.remote(LEASE_REF))
            with b.hold('image-b'):
                b.complete('image-b', '456')
            self.assertEqual(a.record(RECEIPT_REF)['image'], 'image-b')
            with self.assertRaisesRegex(RuntimeError, 'unknown remote state'):
                with a.hold('image-c') as guard:
                    guard.submitted()
                    raise RuntimeError('unknown remote state')
            self.assertEqual(a.remote(LEASE_REF), guard.owner)
            # Explicit recovery releases only the owner verified by this test.
            a.git('push', '--force-with-lease=' + LEASE_REF + ':' + guard.owner, 'origin', ':' + LEASE_REF)
            with a.hold('image-d') as guard:
                guard.submitted()
                guard.verified()
            self.assertIsNone(a.remote(LEASE_REF))


class AbandonedLeaseTest(unittest.TestCase):
    """Prod 2026-09-13: a release push failed and wedged every deploy for ~5 hours."""

    @contextlib.contextmanager
    def stale_after(self, seconds):
        previous, lease_module.STALE_SECONDS = lease_module.STALE_SECONDS, seconds
        try:
            yield
        finally:
            lease_module.STALE_SECONDS = previous

    def test_a_failed_release_does_not_fail_the_rollout(self):
        with repo() as (a, _):
            with a.hold('image-a') as guard:
                # Exactly the 09-13 shape: the rollout itself is fine, only the
                # bookkeeping push loses. It must not turn a good rollout red.
                a.git('push', '--force-with-lease=' + LEASE_REF + ':' + guard.owner, 'origin', ':' + LEASE_REF)
            self.assertIsNone(a.remote(LEASE_REF))

    def test_a_lease_that_never_mutated_is_reclaimed(self):
        with repo() as (a, b), self.stale_after(0):
            # A holder that died before releasing: the ref is left behind by a
            # rollout that never declared a production mutation.
            a.git('push', 'origin', a.commit({'image': 'image-a', 'mutating': False, 'started': 0}) + ':' + LEASE_REF)
            self.assertIsNotNone(b.remote(LEASE_REF))
            with b.hold('image-b', wait_seconds=30) as guard:
                self.assertEqual(b.remote(LEASE_REF), guard.owner, 'the abandoned lease was not reclaimed')

    def test_a_mutating_lease_is_never_reclaimed(self):
        with repo() as (a, b), self.stale_after(0):
            with contextlib.suppress(RuntimeError):
                with a.hold('image-a') as guard:
                    guard.submitted()
                    raise RuntimeError('unknown remote state')
            held = a.remote(LEASE_REF)
            self.assertIsNotNone(held)
            # Stale by the clock, but it declared that it may have changed
            # production — stealing it would run two rollouts at once.
            self.assertFalse(b.abandoned(held))
            with self.assertRaises(subprocess.CalledProcessError):
                with b.hold('image-b', wait_seconds=0):
                    self.fail('reclaimed a lease that reached a production mutation')

    @contextlib.contextmanager
    def serving_boot(self, boot_micros):
        """Stand in for the production `boot_micros` probe."""
        def fake(started):
            if boot_micros is None:
                return False
            return boot_micros / 1_000_000 < started - lease_module.BOOT_SKEW_SECONDS
        previous, lease_module.production_booted_before = lease_module.production_booted_before, fake
        try:
            yield
        finally:
            lease_module.production_booted_before = previous

    def test_a_mutating_lease_is_reclaimed_when_production_predates_it(self):
        """The 2026-09-21 shape: three drain timeouts in a row, each leaving a
        mutating lease that blocked deploys for two hours and needed a person to
        check the same two facts by hand.

        A finished run cannot still be mid-rollout, and a serving process older
        than the rollout was never replaced by it. Together that is conclusive,
        so the grace that exists for "we cannot establish either" does not apply.
        """
        started = 1_000_000
        with repo() as (a, b), self.stale_after(0), self.serving_boot(boot_micros=(started - 3600) * 1_000_000):
            record = {'image': 'image-a', 'mutating': True, 'started': started, 'run_id': '123'}
            a.git('push', 'origin', a.commit(record) + ':' + LEASE_REF)
            held = a.remote(LEASE_REF)
            with mock.patch.object(lease_module, 'run_is_finished', return_value=True):
                self.assertTrue(b.abandoned(held), 'a finished run whose rollout never replaced production must be reclaimable')
            # The run being alive still outranks everything: a rollout mid-flight
            # keeps its lease however old the serving process is.
            with mock.patch.object(lease_module, 'run_is_finished', return_value=False):
                self.assertFalse(b.abandoned(held), 'a live holder keeps its lease')

    def test_a_lease_that_did_replace_production_is_not_reclaimed_early(self):
        """The direction that must never be wrong.

        A rollout that DID restart production leaves a serving process younger
        than itself. That lease may have mutated, so it falls back to the
        two-hour grace rather than being stolen.
        """
        started = time.time()
        with repo() as (a, b), self.stale_after(0), self.serving_boot(boot_micros=(started + 60) * 1_000_000):
            record = {'image': 'image-a', 'mutating': True, 'started': started, 'run_id': '123'}
            a.git('push', 'origin', a.commit(record) + ':' + LEASE_REF)
            held = a.remote(LEASE_REF)
            with mock.patch.object(lease_module, 'run_is_finished', return_value=True):
                self.assertFalse(b.abandoned(held), 'a rollout that replaced production must keep its lease until the grace expires')

    def test_an_unprovable_boot_leaves_the_lease_alone(self):
        """No PGURL, an unreachable database, a missing row: all mean "cannot
        prove it", never "safe to steal"."""
        started = time.time()
        with repo() as (a, b), self.stale_after(0), self.serving_boot(boot_micros=None):
            record = {'image': 'image-a', 'mutating': True, 'started': started, 'run_id': '123'}
            a.git('push', 'origin', a.commit(record) + ':' + LEASE_REF)
            held = a.remote(LEASE_REF)
            with mock.patch.object(lease_module, 'run_is_finished', return_value=True):
                self.assertFalse(b.abandoned(held), 'an unprovable probe must fall back to the age grace')

    def test_the_boot_probe_needs_a_clear_margin(self):
        """Runner and box clocks are independent, so a boot merely CLOSE to the
        rollout proves nothing — and the dangerous mistake is calling a live
        rollout dead."""
        started = 1_000_000
        just_inside = (started - lease_module.BOOT_SKEW_SECONDS + 1) * 1_000_000
        with self.serving_boot(boot_micros=just_inside):
            self.assertFalse(lease_module.production_booted_before(started), 'a boot within the skew window is not proof')
        with self.serving_boot(boot_micros=(started - lease_module.BOOT_SKEW_SECONDS - 1) * 1_000_000):
            self.assertTrue(lease_module.production_booted_before(started), 'a boot clearly older than the rollout is proof')

    def test_the_receipt_reclaims_a_lease_a_restart_would_have_stranded(self):
        """The restart-independent witness, and why it had to exist.

        `production_booted_before` is defeated by ANY unrelated restart: bounce
        the container during an incident and a wedged lease suddenly predates
        nothing, so the reclaim declines and deploys stay blocked for two hours.
        That happened three times on 2026-09-21, each during an incident.

        The receipt only moves when a rollout COMPLETES, so restarts cannot
        perturb it. Here the boot stamp says nothing useful (production restarted
        AFTER the lease) and the receipt still settles it.
        """
        started = time.time()
        with repo() as (a, b), self.stale_after(0), self.serving_boot(boot_micros=(started + 60) * 1_000_000):
            # A completed rollout of a DIFFERENT image than the wedged lease's.
            b.git('push', 'origin', b.commit({'image': 'image-served', 'boot_micros': '1', 'completed': started}) + ':' + RECEIPT_REF)
            a.git('push', 'origin', a.commit({'image': 'image-wedged', 'mutating': True, 'started': started, 'run_id': '123'}) + ':' + LEASE_REF)
            held = a.remote(LEASE_REF)
            with mock.patch.object(lease_module, 'run_is_finished', return_value=True):
                self.assertTrue(b.abandoned(held), 'a lease whose image never reached the receipt never became production')

    def test_a_lease_matching_the_receipt_is_left_alone(self):
        """The direction that must not be wrong: if the receipt says this image
        IS what production serves, the rollout completed and the lease is not
        evidence of a dead one."""
        started = time.time()
        with repo() as (a, b), self.stale_after(0), self.serving_boot(boot_micros=(started + 60) * 1_000_000):
            b.git('push', 'origin', b.commit({'image': 'image-same', 'boot_micros': '1', 'completed': started}) + ':' + RECEIPT_REF)
            a.git('push', 'origin', a.commit({'image': 'image-same', 'mutating': True, 'started': started, 'run_id': '123'}) + ':' + LEASE_REF)
            held = a.remote(LEASE_REF)
            with mock.patch.object(lease_module, 'run_is_finished', return_value=True):
                self.assertFalse(b.abandoned(held), 'a lease whose image IS the served one must keep its lease until the grace expires')

    def test_a_missing_receipt_proves_nothing(self):
        """No receipt at all — a fresh repo, a pruned ref — must not be read as
        "never completed"."""
        started = time.time()
        with repo() as (a, b), self.stale_after(0), self.serving_boot(boot_micros=(started + 60) * 1_000_000):
            a.git('push', 'origin', a.commit({'image': 'image-wedged', 'mutating': True, 'started': started, 'run_id': '123'}) + ':' + LEASE_REF)
            held = a.remote(LEASE_REF)
            with mock.patch.object(lease_module, 'run_is_finished', return_value=True):
                self.assertFalse(b.abandoned(held), 'an absent receipt must fall back to the age grace')

    def test_a_mutating_lease_whose_run_is_dead_is_reclaimed(self):
        """The 2026-09-14 wedge: a rollout died mid-flight and its lease blocked
        every later deploy for 15.6 hours, because `mutating` refused reclaim at
        any age. A completed GitHub run is proof the holder is gone."""
        with repo() as (a, b), self.stale_after(0):
            a.git('push', 'origin', a.commit(
                {'image': 'image-a', 'mutating': True, 'started': 0, 'run_id': '12345'}) + ':' + LEASE_REF)
            with mock.patch.object(lease_module, 'MUTATING_DEAD_SECONDS', 0), \
                 mock.patch.object(lease_module, 'run_is_finished', lambda run_id: run_id == '12345'):
                with b.hold('image-b', wait_seconds=30) as guard:
                    self.assertEqual(b.remote(LEASE_REF), guard.owner, 'a dead mutating holder must not wedge deploys forever')

    def test_a_mutating_lease_whose_run_still_runs_is_left_alone(self):
        """The safety half: an ACTIVE holder may still be changing production,
        so no amount of age may steal its lease."""
        with repo() as (a, b), self.stale_after(0):
            held = a.commit({'image': 'image-a', 'mutating': True, 'started': 0, 'run_id': '999'})
            a.git('push', 'origin', held + ':' + LEASE_REF)
            with mock.patch.object(lease_module, 'MUTATING_DEAD_SECONDS', 0), \
                 mock.patch.object(lease_module, 'run_is_finished', lambda _run_id: False):
                self.assertFalse(b.abandoned(held), 'a live mutating holder must never be reclaimed')


if __name__ == '__main__':
    unittest.main()
