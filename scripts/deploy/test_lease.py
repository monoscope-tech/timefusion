import contextlib
import pathlib
import subprocess
import tempfile
import unittest
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


if __name__ == '__main__':
    unittest.main()
