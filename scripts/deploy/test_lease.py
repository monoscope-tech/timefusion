import pathlib
import subprocess
import tempfile
import unittest
from lease import DeploymentLease, LEASE_REF, RECEIPT_REF


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


if __name__ == '__main__':
    unittest.main()
