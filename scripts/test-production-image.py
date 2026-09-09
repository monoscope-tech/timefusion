#!/usr/bin/env python3
"""Exercise image attribution against a real temporary Git repository."""
import importlib.util
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


class ImageSnapshotTest(unittest.TestCase):
    def test_promoted_index_identifies_runtime_image(self):
        spec = importlib.util.spec_from_file_location('production_image', Path(__file__).with_name('production-image.py'))
        image = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(image)
        runtime = {'digest': 'sha256:' + 'a' * 64, 'platform': {'os': 'linux', 'architecture': 'amd64'}}
        attestation = {'digest': 'sha256:' + 'b' * 64, 'platform': {'os': 'unknown', 'architecture': 'unknown'}}
        index = {'digest': 'sha256:' + 'c' * 64, 'manifests': [attestation, runtime]}
        self.assertEqual(image.runtime_digest(index), image.runtime_digest(runtime))
        for entries in ([], [attestation], [runtime, runtime]):
            with self.subTest(entries=entries), self.assertRaises(RuntimeError):
                image.runtime_digest(dict(index, manifests=entries))

    def test_build_inputs_and_frozen_context(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            script = root / 'scripts/production-image.py'
            script.parent.mkdir()
            shutil.copyfile(Path(__file__).with_name('production-image.py'), script)
            spec = importlib.util.spec_from_file_location('production_image', script)
            image = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(image)
            for name in image.INPUTS:
                path = root / name
                if name in ('src', 'schemas', 'vendor'):
                    path.mkdir()
                    (path / 'input').write_text('initial')
                elif not path.exists():
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text('initial')
            def git(*args):
                return subprocess.check_output(['git', *args], cwd=root, stderr=subprocess.DEVNULL)
            git('init')
            git('add', '.')
            git('-c', 'user.name=Test', '-c', 'user.email=test@example.invalid', 'commit', '-m', 'fixture')
            tree, original = image.snapshot()
            (root / 'notes.md').write_text('Documentation does not change the image')
            self.assertEqual(image.snapshot()[1], original)
            for name in ('Dockerfile', 'Cargo.lock', 'src/input', 'schemas/input', 'vendor/input', 'ci/smoke.Dockerfile'):
                path = root / name
                before = path.read_bytes()
                path.write_bytes(before + b' changed')
                with self.subTest(path=name):
                    self.assertNotEqual(image.snapshot()[1], original)
                path.write_bytes(before)
            (root / 'src/input').write_text('staged')
            git('add', 'src/input')
            staged = git('diff', '--cached', '--binary')
            (root / 'src/input').write_text('working')
            frozen, _ = image.snapshot()
            self.assertEqual(git('diff', '--cached', '--binary'), staged)
            (root / 'src/input').write_text('later edit')
            self.assertEqual(git('show', frozen + ':src/input'), b'working')
            self.assertEqual(git('show', tree + ':src/input'), b'initial')
            archive = root / 'snapshot.tar'
            image.run('git', 'archive', '--format=tar', '-o', str(archive), frozen, '--', *image.INPUTS)
            self.assertGreater(archive.stat().st_size, 0)


if __name__ == '__main__':
    unittest.main()
