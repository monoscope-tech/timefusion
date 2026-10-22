#!/usr/bin/env python3
"""Run the coverage audit regression against local MinIO on port 9000.

Requires the same boto3, deltalake and pyarrow dependencies as the audit.
Creates and removes one uniquely named local test bucket.
"""
import datetime
import json
import os
from pathlib import Path
import tempfile
import uuid

import boto3
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from hash_index_coverage import audit


def test_current_and_legacy_manifest_coverage():
    with tempfile.TemporaryDirectory() as temp:
        endpoint = 'http://127.0.0.1:9000'
        os.environ['AWS_ALLOW_HTTP'] = 'true'
        client = boto3.client('s3', endpoint_url=endpoint, region_name='us-east-1', aws_access_key_id='minioadmin', aws_secret_access_key='minioadmin')
        bucket = 'tf-coverage-audit-' + uuid.uuid4().hex
        project = str(uuid.uuid4())
        foreign = str(uuid.uuid4())
        today = datetime.datetime.now(datetime.timezone.utc).date()
        future = today + datetime.timedelta(days=1)
        env_file = Path(temp) / 'audit.env'
        env_file.write_text('\n'.join(['AWS_S3_BUCKET=' + bucket, 'AWS_S3_ENDPOINT=' + endpoint, 'AWS_REGION=us-east-1', 'AWS_ACCESS_KEY_ID=minioadmin', 'AWS_SECRET_ACCESS_KEY=minioadmin']))
        options = {'AWS_ACCESS_KEY_ID': 'minioadmin', 'AWS_SECRET_ACCESS_KEY': 'minioadmin', 'AWS_REGION': 'us-east-1', 'AWS_ENDPOINT_URL': endpoint, 'AWS_ALLOW_HTTP': 'true'}
        uri = f's3://{bucket}/timefusion/otel_logs_and_spans'
        prefix = f'index_manifests/otel_logs_and_spans/{project}'
        client.create_bucket(Bucket=bucket)

        def put(key, value):
            client.put_object(Bucket=bucket, Key=key, Body=json.dumps(value).encode())
        try:
            write_deltalake(uri, pa.table({'project_id': [project, project, foreign], 'date': pa.array([today, future, today], type=pa.date32()), 'id': ['today', 'future', 'foreign']}), partition_by=['project_id', 'date'], storage_options=options)
            table = DeltaTable(uri, storage_options=options)
            live = [u for u in table.file_uris() if '/project_id=' + project + '/' in u]
            assert len(live) == 2
            legacy = {u: {'schema_version': 1, 'index': 'unused-index', 'error': None, 'covered_files': [u], 'ordinals_valid': True, 'element_fields': ['hashes'], 'fields': []} for u in live}
            put(prefix + '/manifest.json', {'version': 1, 'entries': legacy})
            put(prefix + '/shards/_root.json', {'version': 1, 'shards': [str(today), str(future)]})
            current = next((u for u in live if '/date=' + str(today) + '/' in u))
            put(prefix + '/shards/' + str(today) + '.json', {'entries': {current: {**legacy[current], 'element_fields': [], 'fields': ['body']}}})
            report = audit(project, env_file)
            assert sum((d['physical_hash_candidates'] for d in report['by_date'].values())) == 0, 'current shards must override stale legacy hash coverage'
            assert report['manifest_format'] == 'sharded'
            assert report['missing_shards'] == [str(future)]
            assert report['live_project_files'] == 2, 'foreign project must remain excluded'
            assert report['by_date'][str(today)]['physical_body_candidates'] == 1
            assert report['by_date'][str(today)]['body_index_candidates'] == 1
            assert len(report['files']) == 2
            assert report['by_date'][str(today)]['age_class'] == 'today'
            assert report['by_date'][str(future)]['age_class'] == 'future'
            assert report['by_date'][str(future)]['body_uncovered_compressed_bytes'] > 0
            put(prefix + '/shards/' + str(today) + '.json', {'entries': {current: {**legacy[current], 'element_fields': [], 'fields': ['body'], 'ordinals_valid': False, 'covered_files': live}}})
            carried = audit(project, env_file)
            assert sum((d['physical_body_candidates'] for d in carried['by_date'].values())) == 0
            assert sum((d['body_index_candidates'] for d in carried['by_date'].values())) == 2
            assert sum((d['body_uncovered_compressed_bytes'] for d in carried['by_date'].values())) == 0
            client.delete_object(Bucket=bucket, Key=prefix + '/shards/_root.json')
            fallback = audit(project, env_file)
            assert fallback['manifest_format'] == 'legacy'
            assert sum((d['physical_hash_candidates'] for d in fallback['by_date'].values())) == 2
            client.put_object(Bucket=bucket, Key=prefix + '/shards/_root.json', Body=b'not json')
            try:
                audit(project, env_file)
            except json.JSONDecodeError:
                pass
            else:
                raise AssertionError('corrupt root must fail, not fall back to stale legacy data')
            print('PASS: real Delta/MinIO shard priority, missing shard, legacy fallback, corrupt root, project isolation, body byte prices and future classification')
        finally:
            for page in client.get_paginator('list_objects_v2').paginate(Bucket=bucket):
                objects = [{'Key': o['Key']} for o in page.get('Contents', [])]
                if objects:
                    client.delete_objects(Bucket=bucket, Delete={'Objects': objects})
            client.delete_bucket(Bucket=bucket)
            env_file.unlink(missing_ok=True)


if __name__ == '__main__':
    test_current_and_legacy_manifest_coverage()
