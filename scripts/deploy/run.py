#!/usr/bin/env python3
"""Run the same production handoff and verification locally or in GitHub."""
import argparse
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile
import tarfile

# Importing the helper must not dirty the checkout before the clean-tree check.
sys.dont_write_bytecode = True
from lease import DeploymentLease, RECEIPT_REF

ROOT = Path(__file__).resolve().parents[2]
CLIENT = 'timefusion-deploy-client:local'
CREDENTIALS = ('PGURL', 'CAPROVER_SERVER', 'CAPROVER_APP', 'CAPROVER_TOKEN')
LIMITS = {'MAX_WAL_RECOVERY_MS': '5000', 'MAX_READY_WAIT_SECS': '10', 'RECOVERY_WAIT_SECS': '720',
          'SOAK_SECS': '120', 'PROBE_INTERVAL_SECS': '2', 'MAX_CONSECUTIVE_FAILURES': '2'}


def execute(*args, **kwargs):
    return subprocess.run(args, cwd=ROOT, check=True, **kwargs)


def output(*args):
    return execute(*args, stdout=subprocess.PIPE, text=True).stdout.strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('local', 'ci'))
    args = parser.parse_args()
    missing = [name for name in CREDENTIALS if not os.environ.get(name)]
    if missing:
        parser.error('Deployment credentials are required: ' + ', '.join(missing))
    local = args.mode == 'local'
    if local:
        if output('git', 'status', '--porcelain'):
            parser.error('Deploy from a clean checkout of the merged master commit')
        expected = set(output('bash', 'scripts/ci/ci.sh', 'checks').splitlines())
        passed = {line.split()[1] for line in output('bash', 'scripts/ci/ci.sh', 'gate').splitlines() if line.startswith('skip ')}
        if passed != expected:
            parser.error('Matching local checks are required; run make ci-signoff')
        image = output('python3', 'scripts/production-image.py', 'resolve')
        execute('docker', 'pull', image)
        execute('python3', 'scripts/production-image.py', 'smoke', image)
        execute('docker', 'build', '-f', 'ci/deploy.Dockerfile', '-t', CLIENT, 'ci')
    else:
        image = output('python3', 'scripts/production-image.py', 'digest', os.environ['IMAGE_URL'])
    if not re.fullmatch(r'ghcr\.io/monoscope-tech/timefusion@sha256:[0-9a-f]{64}', image):
        parser.error('Deployment requires an immutable production image digest')
    values = {name: os.environ[name] for name in CREDENTIALS}
    values.update(LIMITS, IMAGE_URL=image)
    lease = DeploymentLease(ROOT)
    # A signal releases admission only before handoff or after verification.
    # An unresolved remote rollout retains its owner record for recovery.
    def interrupted(signum, frame):
        raise SystemExit(128 + signum)
    signal.signal(signal.SIGTERM, interrupted)
    state_root = ROOT / '.ci/deploy'
    state_root.mkdir(parents=True, exist_ok=True)
    directory = tempfile.mkdtemp(prefix='rollout-', dir=state_root)
    print('Deployment diagnostics: ' + directory, flush=True)
    with lease.hold(image) as guard:
        current = lease.git('rev-parse', 'HEAD')
        master = lease.remote('refs/heads/master')
        if current != master:
            if local:
                parser.error('The checkout must be the current merged master commit')
            print('A newer master commit superseded this rollout; production is unchanged.')
            return
        archive = Path(directory) / 'scripts.tar'
        execute('git', 'archive', '--format=tar', '-o', str(archive), current, 'scripts/deploy')
        with tarfile.open(archive) as source:
            source.extractall(directory, filter='data')
        archive.unlink()
        stage_root = Path(directory) / 'scripts/deploy'

        def stage(name):
            result = Path(directory) / ('outputs-' + name)
            result.write_text('')
            stage_values = dict(values, GITHUB_OUTPUT='/state/' + result.name if local else str(result))
            env = dict(os.environ, **stage_values)
            if local:
                args = ['docker', 'run', '--rm', '-v', str(stage_root) + ':/deploy:ro',
                        '-v', directory + ':/state', '-w', '/state']
                for key in stage_values:
                    args.extend(('-e', key))
                args.extend((CLIENT, 'bash', '/deploy/' + name + '.sh'))
            else:
                args = ['bash', str(stage_root / (name + '.sh'))]
            subprocess.run(args, cwd=directory, env=env, check=True)
            return dict(line.split('=', 1) for line in result.read_text().splitlines() if '=' in line)

        previous = stage('record-boot')
        receipt = lease.record(RECEIPT_REF)
        receipt_image = output('python3', 'scripts/production-image.py', 'digest', receipt['image']) if receipt else None
        if receipt_image == image and previous.get('boot_micros') and receipt.get('boot_micros') == previous['boot_micros']:
            stage('soak')
            print('The tested image is already deployed on this boot; readiness soak passed.')
            return
        # HANDOFF itself can fence writes. Retain ownership from the first
        # production mutation, including interruption before CapRover submission.
        guard.submitted()
        handoff = stage('prepare')
        previous = stage('record-boot')
        values.update(PREVIOUS_BOOT_MICROS=previous['boot_micros'],
                      LAST_OLD_RESPONSE_EPOCH_MS=previous['last_old_response_epoch_ms'],
                      HANDOFF_STARTED_EPOCH_MS=previous['handoff_started_epoch_ms'],
                      PREFLUSHED_HANDOFF=handoff['drained'])
        rollout = stage('rollout')
        values.update(OBSERVED_UNREADY_MS=rollout['observed_unready_ms'], QUERY_HANDOFF_MS=rollout['query_handoff_ms'])
        stage('verify-recovery')
        stage('soak')
        boot = stage('record-boot')['boot_micros']
        if not re.fullmatch(r'[0-9]+', boot):
            raise RuntimeError('Cannot record deployment success without a valid live boot identifier')
        lease.complete(image, boot)
        guard.verified()
        print('Production deployment and recovery verification completed.')


if __name__ == '__main__':
    main()
