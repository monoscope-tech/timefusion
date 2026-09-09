#!/usr/bin/env python3
"""Build, smoke-test, and publish immutable production image candidates."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tarfile
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
REGISTRY = 'ghcr.io/monoscope-tech/timefusion'
INPUTS = ('Dockerfile', '.dockerignore', 'Cargo.toml', 'Cargo.lock', 'src', 'schemas', 'vendor', 'scripts/production-image.py', 'ci/smoke.Dockerfile')


def run(*args, **kwargs):
    return subprocess.run(args, cwd=ROOT, check=True, **kwargs)


def output(*args, **kwargs):
    return run(*args, stdout=subprocess.PIPE, text=True, **kwargs).stdout.strip()


def snapshot():
    # A separate index preserves the user's staging area. Archive this exact tree,
    # so edits during a long build cannot silently change the published candidate.
    with tempfile.TemporaryDirectory(prefix='tf-image-index-') as directory:
        env = dict(os.environ, GIT_INDEX_FILE=str(Path(directory) / 'index'))
        run('git', 'read-tree', 'HEAD', env=env)
        run('git', 'add', '-A', '--', *INPUTS, env=env)
        tree = output('git', 'write-tree', env=env)
    entries = run('git', 'ls-tree', '-rz', tree, '--', *INPUTS, stdout=subprocess.PIPE).stdout
    return tree, hashlib.sha256(b'timefusion-linux-amd64-image-v1\0' + entries).hexdigest()


def digest(image):
    if not image.startswith((REGISTRY + ':', REGISTRY + '@')):
        raise ValueError('Only production-registry images can be resolved')
    manifest = json.loads(output('docker', 'buildx', 'imagetools', 'inspect', image, '--format', '{{json .Manifest}}'))
    return REGISTRY + '@' + runtime_digest(manifest)


def runtime_digest(manifest):
    # Promotion may wrap an unchanged image in an index. Deployment receipts
    # identify the actual Linux amd64 manifest, not that incidental wrapper.
    if 'manifests' in manifest:
        candidates = [entry for entry in manifest['manifests']
                      if entry.get('platform', {}).get('os') == 'linux'
                      and entry['platform'].get('architecture') == 'amd64']
        if len(candidates) != 1:
            raise RuntimeError('Production index must identify exactly one Linux amd64 image')
        manifest = candidates[0]
    value = manifest['digest']
    if not re.fullmatch(r'sha256:[0-9a-f]{64}', value):
        raise RuntimeError('Registry returned an invalid image digest')
    return value


def smoke(image, recipe=ROOT / 'ci/smoke.Dockerfile'):
    config = json.loads(output('docker', 'image', 'inspect', image, '--format', '{{json .Config}}'))
    if config['Entrypoint'] != ['/usr/local/bin/timefusion'] or config.get('Cmd'):
        raise RuntimeError('Smoke harness must be updated for the changed production entrypoint')
    name = 'tf-image-smoke-' + uuid.uuid4().hex
    state = ROOT / '.ci'
    state.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='image-smoke-', dir=state) as directory:
        command = ['/usr/local/bin/timefusion']
        options = []
        if output('docker', 'info', '--format', '{{.Architecture}}') in ('arm64', 'aarch64'):
            emulator = 'timefusion-smoke-emulator:local'
            run('docker', 'build', '-f', str(recipe), '-t', emulator, str(recipe.parent))
            try:
                run('docker', 'create', '--name', name + '-emulator', emulator)
                run('docker', 'cp', name + '-emulator:/usr/bin/qemu-x86_64', directory + '/qemu')
            finally:
                subprocess.run(['docker', 'rm', '-f', name + '-emulator'], check=False)
            options = ['-v', directory + '/qemu:/qemu:ro', '--entrypoint', '/qemu']
            # Keep guest mappings in the canonical x86-64 address range. The
            # default ARM64 placement crashes the unchanged production allocator.
            command = ['/qemu', '-B', '0x800000000000', '-cpu', 'max', *command]
        try:
            run('docker', 'run', '--platform', 'linux/amd64', '-d', '--name', name,
                '--no-healthcheck', '--ulimit', 'core=0', *options,
                '-e', 'TIMEFUSION_ALLOW_INSECURE_AUTH=true', '-e', 'AWS_S3_BUCKET=smoke',
                '-e', 'AWS_REGION=us-east-1', '-e', 'AWS_ACCESS_KEY_ID=smoke',
                '-e', 'AWS_SECRET_ACCESS_KEY=smoke', '-e', 'RUST_LOG=info',
                image, *command[1:])
            deadline = time.monotonic() + 12
            while True:
                status = json.loads(output('docker', 'inspect', '--format', '{{json .State}}', name))
                if not status['Running']:
                    raise RuntimeError('Production image exited: ' + json.dumps(status))
                if time.monotonic() >= deadline:
                    break
                time.sleep(1)
            logs = subprocess.run(['docker', 'logs', name], capture_output=True, text=True, check=True)
            if not re.search(r'Listening on 0\.0\.0\.0:5432', logs.stdout + logs.stderr):
                raise RuntimeError('Production image did not reach PGWire within 12 seconds')
            # Run the image's real protocol probe explicitly on both platforms.
            # Docker's normal probe would bypass the emulator on ARM64.
            run('docker', 'exec', name, *command, 'healthcheck', timeout=5)
            if output('docker', 'inspect', '--format', '{{.State.Running}}', name) != 'true':
                raise RuntimeError('Production image exited during its health probe')
            print('smoke: production image stayed running and passed PGWire probe', flush=True)
        except Exception:
            subprocess.run(['docker', 'logs', '--tail', '40', name], check=False)
            raise
        finally:
            subprocess.run(['docker', 'rm', '-f', name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def signed_off():
    expected = set(output('bash', 'scripts/ci/ci.sh', 'checks').splitlines())
    gate = output('bash', 'scripts/ci/ci.sh', 'gate')
    passed = {line.split()[1] for line in gate.splitlines() if line.startswith('skip ')}
    return passed == expected


def publish():
    tree, fingerprint = snapshot()
    if not signed_off() or snapshot()[1] != fingerprint:
        raise RuntimeError('Matching local signoff is required; run make ci-signoff with stable inputs')
    image = REGISTRY + ':input-' + fingerprint
    try:
        existing = digest(image)
    except subprocess.CalledProcessError:
        existing = None
    if existing:
        print('Reusing previously published image: ' + existing)
        return
    with tempfile.TemporaryDirectory(prefix='tf-image-context-') as directory:
        archive = Path(directory) / 'context.tar'
        run('git', 'archive', '--format=tar', '-o', str(archive), tree, '--', *INPUTS)
        with tarfile.open(archive) as source:
            source.extractall(directory, filter='data')
        archive.unlink()
        run('docker', 'buildx', 'build', '--load', '--platform', 'linux/amd64',
            '--build-arg', 'CARGO_BUILD_JOBS=2', '--label', 'io.timefusion.source-fingerprint=' + fingerprint,
            '-t', image, directory)
        smoke(image, Path(directory) / 'ci/smoke.Dockerfile')
    run('docker', 'push', image)
    pushed = json.loads(output('docker', 'image', 'inspect', image, '--format', '{{json .RepoDigests}}'))
    print(next(value for value in pushed if value.startswith(REGISTRY + '@')))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('fingerprint', 'resolve', 'publish', 'signoff', 'smoke', 'digest'))
    parser.add_argument('image', nargs='?')
    args = parser.parse_args()
    if args.command == 'fingerprint':
        print(snapshot()[1])
    elif args.command == 'resolve':
        print(digest(REGISTRY + ':input-' + snapshot()[1]))
    elif args.command == 'publish':
        publish()
    elif args.command == 'signoff':
        if signed_off():
            publish()
        else:
            print('Image publication pending: required checks still need to pass.')
    elif args.image is None:
        parser.error('This command requires an image')
    elif args.command == 'smoke':
        smoke(args.image)
    else:
        print(digest(args.image))


if __name__ == '__main__':
    main()
