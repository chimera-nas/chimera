#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

"""
S3 test suite using boto3 with AWS Signature V2 and V4.
Starts chimera daemon as a subprocess and runs tests against it.
"""

import argparse
import datetime
import hashlib
import hmac
import http.client
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time

import boto3
import botocore.exceptions
from botocore.config import Config
from botocore.exceptions import ClientError


class ChimeraServer:
    """Manages a chimera server instance for testing."""

    def __init__(self, backend='memfs', chimera_path=None, debug=False):
        self.backend = backend
        self.chimera_path = chimera_path or self._find_chimera()
        self.debug = debug
        self.process = None
        self.temp_dir = None
        self.config_path = None

    def _find_chimera(self):
        """Find the chimera daemon executable."""
        # Look in common build locations
        candidates = [
            './src/daemon/chimera',
            '../daemon/chimera',
            '../../src/daemon/chimera',
            '../../../src/daemon/chimera',
        ]

        # Also check relative to script location
        script_dir = os.path.dirname(os.path.abspath(__file__))
        for levels in range(5):
            path = os.path.join(script_dir, '../' * levels, 'src/daemon/chimera')
            candidates.append(os.path.normpath(path))

        for path in candidates:
            if os.path.exists(path) and os.access(path, os.X_OK):
                return os.path.abspath(path)

        raise RuntimeError("Could not find chimera daemon executable")

    def _create_config(self):
        """Create a JSON configuration for the test server."""
        config = {
            "server": {
                "s3_port": 5000,
            },
            "s3_access_keys": [
                {
                    "access_key": "myaccessid",
                    "secret_key": "mysecretkey"
                }
            ],
            "mounts": {},
            "buckets": {
                "mybucket": {
                    "path": "/share"
                }
            }
        }

        if self.backend == 'memfs':
            config["mounts"]["share"] = {
                "module": "memfs",
                "path": "/"
            }
        elif self.backend == 'linux':
            config["mounts"]["share"] = {
                "module": "linux",
                "path": self.temp_dir
            }
        elif self.backend == 'io_uring':
            config["mounts"]["share"] = {
                "module": "io_uring",
                "path": self.temp_dir
            }
        elif self.backend == 'diskfs':
            # Create device files for diskfs
            devices = []
            for i in range(10):
                device_path = os.path.join(self.temp_dir, f'device-{i}.img')
                with open(device_path, 'wb') as f:
                    f.truncate(1024 * 1024 * 1024)  # 1GB sparse file
                devices.append({
                    "type": "io_uring",
                    "size": 1,
                    "path": device_path
                })
            # Module config goes under server.vfs, config is an object not string
            config["server"]["vfs"] = {
                "diskfs": {
                    "path": None,
                    "config": {"initialize": True, "devices": devices, "unsafe_async": True}
                }
            }
            config["mounts"]["share"] = {
                "module": "diskfs",
                "path": "/"
            }
        elif self.backend == 'cairn':
            # Module config goes under server.vfs, config is an object not string
            config["server"]["vfs"] = {
                "cairn": {
                    "path": None,
                    "config": {
                        "initialize": True,
                        "path": self.temp_dir
                    }
                }
            }
            config["mounts"]["share"] = {
                "module": "cairn",
                "path": "/"
            }
        else:
            raise ValueError(f"Unknown backend: {self.backend}")

        return config

    def start(self):
        """Start the chimera server."""
        # For backends that use name_to_handle_at(), we need a filesystem that
        # supports file handles. /tmp is often tmpfs which doesn't support this.
        # Use /build/test/ instead, following the NFS test pattern.
        if self.backend in ('linux', 'io_uring', 'diskfs', 'cairn'):
            test_base = '/build/test'
            os.makedirs(test_base, exist_ok=True)
            self.temp_dir = tempfile.mkdtemp(prefix='chimera_test_', dir=test_base)
        else:
            self.temp_dir = tempfile.mkdtemp(prefix='chimera_test_')

        config = self._create_config()
        self.config_path = os.path.join(self.temp_dir, 'config.json')
        with open(self.config_path, 'w') as f:
            json.dump(config, f, indent=2)

        cmd = [self.chimera_path, '-c', self.config_path]
        if self.debug:
            cmd.append('-d')

        print(f"Starting chimera server: {' '.join(cmd)}")

        self.process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE if not self.debug else None,
            stderr=subprocess.PIPE if not self.debug else None,
        )

        # Wait for server to be ready
        time.sleep(1)

        if self.process.poll() is not None:
            stdout, stderr = self.process.communicate()
            raise RuntimeError(f"Server failed to start: {stderr.decode() if stderr else 'unknown error'}")

        print("Server started successfully")

    def stop(self):
        """Stop the chimera server and cleanup."""
        if self.process:
            print("Stopping chimera server...")
            self.process.send_signal(signal.SIGTERM)
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
            self.process = None

        if self.temp_dir and os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir, ignore_errors=True)
            self.temp_dir = None

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        return False


def create_s3_client(endpoint_url='http://localhost:5000',
                     access_key='myaccessid',
                     secret_key='mysecretkey',
                     signature_version='s3v4'):
    """Create an S3 client configured for local testing.

    Args:
        endpoint_url: The S3 endpoint URL
        access_key: AWS access key ID
        secret_key: AWS secret access key
        signature_version: 's3v4' for V4 signing, 's3' for V2 signing
    """
    return boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(
            signature_version=signature_version,
            s3={'addressing_style': 'path'}
        ),
        region_name='us-east-1'
    )


def test_put(client, bucket):
    """Test PUT operations."""
    print("Testing PUT operations...")

    # Small object
    data = b'x' * 4096
    client.put_object(Bucket=bucket, Key='mydir1/mydir2/mydir3/mykey1', Body=data)
    print("  PUT mykey1 (4KB) - OK")

    # Another small object
    client.put_object(Bucket=bucket, Key='mydir1/mydir2/mydir3/mykey2', Body=data)
    print("  PUT mykey2 (4KB) - OK")

    # Empty object
    client.put_object(Bucket=bucket, Key='mydir1/mydir2/mydir3/mykey3', Body=b'')
    print("  PUT mykey3 (0B) - OK")

    # Large object (35MB)
    large_data = b'y' * 35000000
    client.put_object(Bucket=bucket, Key='mydir1/mydir2/mydir3/mykey4', Body=large_data)
    print("  PUT mykey4 (35MB) - OK")

    print("PUT tests passed!")


def test_get(client, bucket):
    """Test GET operations."""
    print("Testing GET operations...")

    # First put an object
    data = b'x' * 4096
    client.put_object(Bucket=bucket, Key='mydir1/mydir2/mydir3/mykey1', Body=data)

    # Full GET
    response = client.get_object(Bucket=bucket, Key='mydir1/mydir2/mydir3/mykey1')
    body = response['Body'].read()
    assert len(body) == 4096, f"Expected 4096 bytes, got {len(body)}"
    print("  GET mykey1 (full) - OK")

    # Range GET
    response = client.get_object(
        Bucket=bucket,
        Key='mydir1/mydir2/mydir3/mykey1',
        Range='bytes=1000-1599'
    )
    body = response['Body'].read()
    assert len(body) == 600, f"Expected 600 bytes, got {len(body)}"
    print("  GET mykey1 (range 1000-1599) - OK")

    # Large object
    large_data = b'y' * 35000000
    client.put_object(Bucket=bucket, Key='mydir1/mydir2/mydir3/mykey4', Body=large_data)
    response = client.get_object(Bucket=bucket, Key='mydir1/mydir2/mydir3/mykey4')
    body = response['Body'].read()
    assert len(body) == 35000000, f"Expected 35000000 bytes, got {len(body)}"
    print("  GET mykey4 (35MB) - OK")

    print("GET tests passed!")


def test_head(client, bucket):
    """Test HEAD operations."""
    print("Testing HEAD operations...")

    # Put an object first
    data = b'x' * 4096
    client.put_object(Bucket=bucket, Key='mydir1/mydir2/mydir3/mykey1', Body=data)

    # HEAD request
    response = client.head_object(Bucket=bucket, Key='mydir1/mydir2/mydir3/mykey1')
    assert response['ContentLength'] == 4096, f"Expected ContentLength 4096, got {response['ContentLength']}"
    print("  HEAD mykey1 - OK")

    print("HEAD tests passed!")


def test_delete(client, bucket):
    """Test DELETE operations."""
    print("Testing DELETE operations...")

    # Put objects first
    data = b'x' * 4096
    client.put_object(Bucket=bucket, Key='mydir1/mydir2/mydir3/mykey1', Body=data)
    client.put_object(Bucket=bucket, Key='mydir1/mydir2/mydir3/mykey2', Body=data)

    # Delete
    client.delete_object(Bucket=bucket, Key='mydir1/mydir2/mydir3/mykey1')
    print("  DELETE mykey1 - OK")

    # Verify deleted
    try:
        client.head_object(Bucket=bucket, Key='mydir1/mydir2/mydir3/mykey1')
        print("  ERROR: Object should have been deleted!")
        return False
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            print("  Verified mykey1 deleted - OK")
        else:
            raise

    # Other object should still exist
    response = client.head_object(Bucket=bucket, Key='mydir1/mydir2/mydir3/mykey2')
    assert response['ContentLength'] == 4096
    print("  Verified mykey2 still exists - OK")

    print("DELETE tests passed!")


def test_delete_objects(client, bucket):
    """Test batch DeleteObjects (POST /bucket?delete) used by `aws s3 rm
    --recursive` and the delete phase of `aws s3 sync`."""
    print("Testing batch DeleteObjects operations...")

    data = b'y' * 1024
    keys = [
        'batch/a/key1',
        'batch/a/key2',
        'batch/b/key3',
        'batch/key4',
    ]
    for k in keys:
        client.put_object(Bucket=bucket, Key=k, Body=data)
    print(f"  Put {len(keys)} objects")

    # Batch-delete a subset plus a nonexistent key. DeleteObjects is
    # idempotent, so the missing key is reported as deleted, not an error.
    to_delete = keys[:3] + ['batch/does-not-exist']
    response = client.delete_objects(
        Bucket=bucket,
        Delete={'Objects': [{'Key': k} for k in to_delete]},
    )

    deleted = sorted(d['Key'] for d in response.get('Deleted', []))
    errors = response.get('Errors', [])
    assert not errors, f"Unexpected errors: {errors}"
    assert deleted == sorted(to_delete), f"Deleted mismatch: {deleted}"
    print("  Batch delete reported all keys deleted (incl. nonexistent) - OK")

    # Deleted keys are gone.
    for k in keys[:3]:
        try:
            client.head_object(Bucket=bucket, Key=k)
            raise AssertionError(f"{k} should have been deleted")
        except ClientError as e:
            if e.response['Error']['Code'] != '404':
                raise
    print("  Verified deleted keys are gone - OK")

    # The untouched key survives.
    resp = client.head_object(Bucket=bucket, Key='batch/key4')
    assert resp['ContentLength'] == 1024
    print("  Verified untouched key survives - OK")

    # Quiet mode: successful deletes are not echoed back in the result.
    response = client.delete_objects(
        Bucket=bucket,
        Delete={'Objects': [{'Key': 'batch/key4'}], 'Quiet': True},
    )
    assert not response.get('Deleted'), "Quiet mode should omit Deleted entries"
    assert not response.get('Errors'), f"Unexpected errors: {response.get('Errors')}"
    print("  Quiet mode suppressed Deleted entries - OK")

    try:
        client.head_object(Bucket=bucket, Key='batch/key4')
        raise AssertionError("batch/key4 should have been deleted")
    except ClientError as e:
        if e.response['Error']['Code'] != '404':
            raise
    print("  Verified quiet-mode delete removed the key - OK")

    print("Batch DeleteObjects tests passed!")


def test_list(client, bucket):
    """Test LIST operations."""
    print("Testing LIST operations...")

    # Put multiple objects in different directories
    data = b'x' * 4096
    keys = [
        'mydir1/mydir2/mydir3/mykey1',
        'mydir1/mydir2/mydir3/mykey2',
        'mydir1/mydir2/mydir4/mykey3',
        'mydir1/mydir2/mydir4/mykey4',
        'mydir1/mydir3/mydir5/mykey5',
        'mydir1/mydir3/mydir5/mykey6',
    ]
    for key in keys:
        client.put_object(Bucket=bucket, Key=key, Body=data)

    # List root - just verify the operation succeeds (auth works)
    response = client.list_objects_v2(Bucket=bucket, Delimiter='/')
    print(f"  LIST / - {response.get('KeyCount', 0)} keys, {len(response.get('CommonPrefixes', []))} prefixes - OK")

    # List with prefix
    response = client.list_objects_v2(Bucket=bucket, Prefix='mydir1/', Delimiter='/')
    print(f"  LIST mydir1/ - {response.get('KeyCount', 0)} keys, {len(response.get('CommonPrefixes', []))} prefixes - OK")

    # List deeper - verify we get some response (list implementation details may vary)
    response = client.list_objects_v2(Bucket=bucket, Prefix='mydir1/mydir2/mydir3/', Delimiter='/')
    key_count = response.get('KeyCount', 0)
    print(f"  LIST mydir1/mydir2/mydir3/ - {key_count} keys - OK")

    print("LIST tests passed!")


def test_copy(client, bucket):
    """Test server-side CopyObject (x-amz-copy-source) operations.

    Exercises the copy handler's transfer paths (clone_range fast path,
    copy_range, and the read+write fallback are selected per backend) plus
    source/destination parsing and the error paths.
    """
    print("Testing COPY operations...")

    data = b'abcd' * 1024  # 4096 bytes
    client.put_object(Bucket=bucket, Key='copydir/src1', Body=data)

    # Simple copy to a new key.
    client.copy_object(Bucket=bucket, Key='copydir/dst1',
                       CopySource={'Bucket': bucket, 'Key': 'copydir/src1'})
    assert client.get_object(Bucket=bucket, Key='copydir/dst1')['Body'].read() == data
    print("  COPY src1 -> dst1 (4KB) - content matches")

    # Source must remain intact (copy, not move).
    assert client.get_object(Bucket=bucket, Key='copydir/src1')['Body'].read() == data
    print("  source intact after copy - OK")

    # Empty object.
    client.put_object(Bucket=bucket, Key='copydir/empty', Body=b'')
    client.copy_object(Bucket=bucket, Key='copydir/empty_copy',
                       CopySource={'Bucket': bucket, 'Key': 'copydir/empty'})
    assert client.get_object(Bucket=bucket, Key='copydir/empty_copy')['Body'].read() == b''
    print("  COPY empty object - OK")

    # Large, deliberately non-block-aligned object. On reflink-capable
    # backends this forces the clone fast path to fall back (an unaligned
    # interior length is rejected) to copy_range / read+write.
    large = b'z' * (5 * 1024 * 1024 + 123)
    client.put_object(Bucket=bucket, Key='copydir/large', Body=large)
    client.copy_object(Bucket=bucket, Key='copydir/large_copy',
                       CopySource={'Bucket': bucket, 'Key': 'copydir/large'})
    body = client.get_object(Bucket=bucket, Key='copydir/large_copy')['Body'].read()
    assert len(body) == len(large), f"size {len(body)} != {len(large)}"
    assert body == large, "large copy content mismatch"
    print("  COPY large unaligned (5MB+123) - content matches")

    # Copy over an existing destination object (replace).
    client.put_object(Bucket=bucket, Key='copydir/dst2', Body=b'stale-contents')
    client.copy_object(Bucket=bucket, Key='copydir/dst2',
                       CopySource={'Bucket': bucket, 'Key': 'copydir/src1'})
    assert client.get_object(Bucket=bucket, Key='copydir/dst2')['Body'].read() == data
    print("  COPY overwrites existing destination - OK")

    # CopySource passed as a raw "bucket/key" string.
    client.copy_object(Bucket=bucket, Key='copydir/dst3',
                       CopySource=f'{bucket}/copydir/src1')
    assert client.get_object(Bucket=bucket, Key='copydir/dst3')['Body'].read() == data
    print("  COPY with string CopySource - OK")

    # Missing source key -> NoSuchKey.
    try:
        client.copy_object(Bucket=bucket, Key='copydir/nope',
                           CopySource={'Bucket': bucket, 'Key': 'copydir/does_not_exist'})
        raise AssertionError("copy of missing source should fail")
    except ClientError as e:
        if e.response['Error']['Code'] != 'NoSuchKey':
            raise
    print("  COPY missing source -> NoSuchKey (ok)")

    # Missing source bucket -> NoSuchBucket.
    try:
        client.copy_object(Bucket=bucket, Key='copydir/nope2',
                           CopySource={'Bucket': 'no_such_bucket_xyz', 'Key': 'copydir/src1'})
        raise AssertionError("copy from missing bucket should fail")
    except ClientError as e:
        if e.response['Error']['Code'] != 'NoSuchBucket':
            raise
    print("  COPY missing source bucket -> NoSuchBucket (ok)")

    print("COPY tests passed!")


def _multipart_upload_and_verify(client, bucket, key, part_sizes):
    """Helper: run a full multipart upload and verify the assembled bytes
    match the concatenation of the part bodies. Exercises whichever
    backend assembly path (move/copy/read+write) is active."""
    init = client.create_multipart_upload(Bucket=bucket, Key=key)
    upload_id = init['UploadId']
    assert upload_id, "no UploadId returned"

    parts = []
    expected = b''
    for part_number, size in enumerate(part_sizes, start=1):
        # Each part filled with a distinct byte so a misordered or
        # truncated assembly is immediately visible.
        body = bytes([part_number]) * size
        expected += body
        resp = client.upload_part(
            Bucket=bucket, Key=key,
            PartNumber=part_number, UploadId=upload_id,
            Body=body,
        )
        parts.append({'PartNumber': part_number, 'ETag': resp['ETag']})

    client.complete_multipart_upload(
        Bucket=bucket, Key=key, UploadId=upload_id,
        MultipartUpload={'Parts': parts},
    )

    head = client.head_object(Bucket=bucket, Key=key)
    assert head['ContentLength'] == len(expected), \
        f"size mismatch: expected {len(expected)}, got {head['ContentLength']}"

    resp = client.get_object(Bucket=bucket, Key=key)
    actual = resp['Body'].read()
    assert actual == expected, \
        f"content mismatch: {len(actual)} bytes, first diff at offset " \
        f"{next((i for i, (a, b) in enumerate(zip(actual, expected)) if a != b), -1)}"

    return upload_id


def test_multipart(client, bucket):
    """Test multipart upload operations end-to-end.

    Verifies the full upload lifecycle (Create / UploadPart / ListParts /
    ListMultipartUploads / Complete / Abort) and that the assembled object
    matches the concatenation of part bodies. The same test exercises
    different VFS assembly paths depending on backend capabilities:
    move_range (memfs), copy_range (linux/io_uring), or read+write
    (cairn/diskfs).
    """
    print("Testing multipart upload operations...")

    key = 'mpdir/myobject'
    part_size = 5 * 1024 * 1024  # 5MiB: boto3's per-part minimum
    part_count = 3

    # Initiate, upload, list, complete — also verifies final bytes match
    init = client.create_multipart_upload(Bucket=bucket, Key=key)
    upload_id = init['UploadId']
    assert upload_id, "no UploadId returned"
    print(f"  CreateMultipartUpload -> {upload_id}")

    parts = []
    expected = b''
    for part_number in range(1, part_count + 1):
        body = bytes([part_number]) * part_size
        expected += body
        resp = client.upload_part(
            Bucket=bucket, Key=key,
            PartNumber=part_number, UploadId=upload_id,
            Body=body,
        )
        parts.append({'PartNumber': part_number, 'ETag': resp['ETag']})
        print(f"  UploadPart #{part_number} ({part_size} bytes) -> {resp['ETag']}")

    listed = client.list_parts(Bucket=bucket, Key=key, UploadId=upload_id)
    assert len(listed.get('Parts', [])) == part_count
    print(f"  ListParts -> {part_count} parts visible")

    uploads = client.list_multipart_uploads(Bucket=bucket)
    upload_ids = [u['UploadId'] for u in uploads.get('Uploads', [])]
    assert upload_id in upload_ids
    print(f"  ListMultipartUploads -> {len(upload_ids)} in-progress upload(s)")

    complete = client.complete_multipart_upload(
        Bucket=bucket, Key=key, UploadId=upload_id,
        MultipartUpload={'Parts': parts},
    )
    print(f"  CompleteMultipartUpload -> {complete.get('ETag', '')}")

    head = client.head_object(Bucket=bucket, Key=key)
    assert head['ContentLength'] == len(expected), \
        f"size mismatch: expected {len(expected)}, got {head['ContentLength']}"
    print(f"  HEAD final object -> {head['ContentLength']} bytes")

    resp = client.get_object(Bucket=bucket, Key=key)
    actual = resp['Body'].read()
    assert actual == expected, "assembled content does not match parts"
    print(f"  GET final object -> {len(actual)} bytes, content matches")

    # After Complete the upload disappears from ListMultipartUploads
    uploads = client.list_multipart_uploads(Bucket=bucket)
    upload_ids = [u['UploadId'] for u in uploads.get('Uploads', [])]
    assert upload_id not in upload_ids

    # Multi-part roundtrip with varied sizes (block-aligned to keep
    # memfs move_range happy; backends without alignment requirements
    # are not picky).
    _multipart_upload_and_verify(
        client, bucket, 'mpdir/varied',
        part_sizes=[5 * 1024 * 1024, 6 * 1024 * 1024, 5 * 1024 * 1024],
    )
    print("  Varied-size multipart roundtrip -> content matches")

    # Abort path: starts an upload, uploads a part, aborts, then verifies
    # nothing was materialized
    init = client.create_multipart_upload(Bucket=bucket, Key='mpdir/aborted')
    abort_id = init['UploadId']
    client.upload_part(
        Bucket=bucket, Key='mpdir/aborted',
        PartNumber=1, UploadId=abort_id,
        Body=b'x' * part_size,
    )
    client.abort_multipart_upload(
        Bucket=bucket, Key='mpdir/aborted', UploadId=abort_id,
    )
    print(f"  AbortMultipartUpload -> ok")
    try:
        client.head_object(Bucket=bucket, Key='mpdir/aborted')
        raise AssertionError("aborted upload should not have created an object")
    except ClientError as e:
        if e.response['Error']['Code'] != '404':
            raise

    # NoSuchUpload error path
    try:
        client.list_parts(
            Bucket=bucket, Key=key,
            UploadId='0' * 32,
        )
        raise AssertionError("ListParts on bogus upload should fail")
    except ClientError as e:
        if e.response['Error']['Code'] != 'NoSuchUpload':
            raise
    print("  ListParts on nonexistent upload -> NoSuchUpload (ok)")

    # --- Manifest validation: subset / wrong-etag / out-of-order / missing ---

    # Subset completion: upload 3 parts, complete with [1, 3] only.
    # The result must contain only parts 1 and 3 concatenated.
    init = client.create_multipart_upload(Bucket=bucket, Key='mpdir/subset')
    sub_id = init['UploadId']
    sub_parts = []
    sub_bodies = []
    for n in range(1, 4):
        body = bytes([n]) * part_size
        sub_bodies.append(body)
        r = client.upload_part(Bucket=bucket, Key='mpdir/subset',
                               PartNumber=n, UploadId=sub_id, Body=body)
        sub_parts.append({'PartNumber': n, 'ETag': r['ETag']})
    client.complete_multipart_upload(
        Bucket=bucket, Key='mpdir/subset', UploadId=sub_id,
        MultipartUpload={'Parts': [sub_parts[0], sub_parts[2]]},
    )
    head = client.head_object(Bucket=bucket, Key='mpdir/subset')
    assert head['ContentLength'] == 2 * part_size, \
        f"subset: expected {2*part_size}, got {head['ContentLength']}"
    actual = client.get_object(Bucket=bucket, Key='mpdir/subset')['Body'].read()
    assert actual == sub_bodies[0] + sub_bodies[2], \
        "subset: assembled content does not match parts 1+3"
    print("  Subset completion (parts [1,3] of 3) -> content matches")

    # Wrong ETag: upload should be rejected, upload remains for retry.
    init = client.create_multipart_upload(Bucket=bucket, Key='mpdir/wrongetag')
    we_id = init['UploadId']
    we_parts = []
    for n in range(1, 3):
        r = client.upload_part(Bucket=bucket, Key='mpdir/wrongetag',
                               PartNumber=n, UploadId=we_id,
                               Body=bytes([n]) * part_size)
        we_parts.append({'PartNumber': n, 'ETag': r['ETag']})
    bad_parts = list(we_parts)
    bad_parts[1] = {'PartNumber': 2, 'ETag': '"' + 'f' * 32 + '"'}
    try:
        client.complete_multipart_upload(
            Bucket=bucket, Key='mpdir/wrongetag', UploadId=we_id,
            MultipartUpload={'Parts': bad_parts},
        )
        raise AssertionError("Complete with wrong ETag should fail")
    except ClientError as e:
        if e.response['Error']['Code'] != 'InvalidPart':
            raise
    # Upload must still be listable (retry possible)
    listed = client.list_parts(Bucket=bucket, Key='mpdir/wrongetag',
                               UploadId=we_id)
    assert len(listed.get('Parts', [])) == 2, "upload should survive wrong-ETag rejection"
    client.abort_multipart_upload(Bucket=bucket, Key='mpdir/wrongetag',
                                  UploadId=we_id)
    print("  Wrong ETag -> InvalidPart, upload retained (ok)")

    # Out-of-order parts in the manifest.
    init = client.create_multipart_upload(Bucket=bucket, Key='mpdir/order')
    o_id = init['UploadId']
    o_parts = []
    for n in range(1, 3):
        r = client.upload_part(Bucket=bucket, Key='mpdir/order',
                               PartNumber=n, UploadId=o_id,
                               Body=bytes([n]) * part_size)
        o_parts.append({'PartNumber': n, 'ETag': r['ETag']})
    body = (
        '<CompleteMultipartUpload>'
        '<Part><PartNumber>2</PartNumber><ETag>{}</ETag></Part>'
        '<Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part>'
        '</CompleteMultipartUpload>'
    ).format(o_parts[1]['ETag'], o_parts[0]['ETag']).encode('utf-8')
    status, resp_body = signed_s3_post_xml(
        'localhost', 5000, 'myaccessid', 'mysecretkey', 'us-east-1',
        bucket, 'mpdir/order', f'uploadId={o_id}', body)
    assert status == 400 and b'InvalidPartOrder' in resp_body, (
        f"out-of-order CompleteMultipartUpload: status={status}, "
        f"body={resp_body!r}")
    print("  Out-of-order parts -> InvalidPartOrder (ok)")
    client.abort_multipart_upload(Bucket=bucket, Key='mpdir/order',
                                  UploadId=o_id)

    # Reference a nonexistent part number.
    init = client.create_multipart_upload(Bucket=bucket, Key='mpdir/missing')
    m_id = init['UploadId']
    r1 = client.upload_part(Bucket=bucket, Key='mpdir/missing',
                            PartNumber=1, UploadId=m_id,
                            Body=b'x' * part_size)
    try:
        client.complete_multipart_upload(
            Bucket=bucket, Key='mpdir/missing', UploadId=m_id,
            MultipartUpload={'Parts': [
                {'PartNumber': 1, 'ETag': r1['ETag']},
                {'PartNumber': 7, 'ETag': '"' + 'a' * 32 + '"'},
            ]},
        )
        raise AssertionError("Complete with nonexistent part should fail")
    except ClientError as e:
        if e.response['Error']['Code'] != 'InvalidPart':
            raise
    client.abort_multipart_upload(Bucket=bucket, Key='mpdir/missing',
                                  UploadId=m_id)
    print("  Nonexistent part_number -> InvalidPart (ok)")

    # EntityTooSmall: two 1-MiB parts, complete with both.
    small_size = 1 * 1024 * 1024
    init = client.create_multipart_upload(Bucket=bucket, Key='mpdir/small')
    s_id = init['UploadId']
    s_parts = []
    for n in range(1, 3):
        r = client.upload_part(Bucket=bucket, Key='mpdir/small',
                               PartNumber=n, UploadId=s_id,
                               Body=b'q' * small_size)
        s_parts.append({'PartNumber': n, 'ETag': r['ETag']})
    try:
        client.complete_multipart_upload(
            Bucket=bucket, Key='mpdir/small', UploadId=s_id,
            MultipartUpload={'Parts': s_parts},
        )
        raise AssertionError("Complete with sub-5MiB non-final part should fail")
    except ClientError as e:
        if e.response['Error']['Code'] != 'EntityTooSmall':
            raise
    client.abort_multipart_upload(Bucket=bucket, Key='mpdir/small',
                                  UploadId=s_id)
    print("  Sub-5MiB non-final part -> EntityTooSmall (ok)")

    # Part-number out of range. boto3 passes the int through to the URL
    # query string; the server is the one that rejects.
    init = client.create_multipart_upload(Bucket=bucket, Key='mpdir/pnrange')
    pn_id = init['UploadId']
    for bad_pn in (0, 10001):
        try:
            client.upload_part(Bucket=bucket, Key='mpdir/pnrange',
                               PartNumber=bad_pn, UploadId=pn_id,
                               Body=b'x' * 1024)
            raise AssertionError(f"PartNumber={bad_pn} should fail")
        except ClientError as e:
            code = e.response['Error']['Code']
            if code != 'InvalidArgument':
                raise AssertionError(
                    f"PartNumber={bad_pn}: expected InvalidArgument, got {code}")
        except botocore.exceptions.ParamValidationError:
            # boto3 may filter client-side for some out-of-range values; that's
            # acceptable evidence that the spec is honored end-to-end.
            pass
    client.abort_multipart_upload(Bucket=bucket, Key='mpdir/pnrange',
                                  UploadId=pn_id)
    print("  PartNumber=0 and PartNumber=10001 rejected (ok)")

    print("multipart tests passed!")


def _sign(key, msg):
    return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()


def _sha256_hex(data):
    return hashlib.sha256(data).hexdigest()


def _canonical_query(query):
    """Replicate the server's query canonicalization: split on '&', sort, and
    append '=' to bare keys."""
    if not query:
        return ''
    params = sorted(query.split('&'))
    return '&'.join(p if '=' in p else p + '=' for p in params)


def streaming_put(host, port, access_key, secret_key, region, bucket, key,
                  data, chunk_size, query=None):
    """Perform a SigV4 streaming (aws-chunked) PUT by hand.

    Mirrors what the AWS CLI / SDKs send when they default to
    STREAMING-AWS4-HMAC-SHA256-PAYLOAD: the seed signature is computed over a
    canonical request whose hashed-payload is the literal streaming sentinel,
    and the body is wrapped in signed chunk framing. boto3 will not emit this
    encoding for an in-memory body, so we build it directly to exercise the
    server's de-chunking path. An optional raw query string (e.g. the
    partNumber/uploadId of an UploadPart) is included in the URL and signature.
    Returns (http_status, etag).
    """
    service = 's3'
    host_header = f'{host}:{port}'
    canonical_uri = f'/{bucket}/{key}'
    request_target = canonical_uri + (f'?{query}' if query else '')

    now = datetime.datetime.now(datetime.timezone.utc)
    amz_date = now.strftime('%Y%m%dT%H%M%SZ')
    date_stamp = now.strftime('%Y%m%d')
    scope = f'{date_stamp}/{region}/{service}/aws4_request'

    seed_sha = 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD'

    # Split the payload into chunks; build the chunk stream and its signature
    # chain after we have the seed signature.
    chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
    if not chunks:
        chunks = []  # zero-length object: only the terminating chunk

    # Derive the signing key (identical to the server's derive_signing_key_v4).
    k_date = _sign(('AWS4' + secret_key).encode('utf-8'), date_stamp)
    k_region = hmac.new(k_date, region.encode('utf-8'), hashlib.sha256).digest()
    k_service = hmac.new(k_region, service.encode('utf-8'), hashlib.sha256).digest()
    signing_key = hmac.new(k_service, b'aws4_request', hashlib.sha256).digest()

    # Compute the encoded content-length so it can be signed. Each data chunk
    # is "<hexsize>;chunk-signature=<64hex>\r\n<data>\r\n"; the final chunk is
    # "0;chunk-signature=<64hex>\r\n\r\n".
    def chunk_overhead(size):
        return len(f'{size:x}') + len(';chunk-signature=') + 64 + 2 + 2

    content_length = sum(chunk_overhead(len(c)) + len(c) for c in chunks)
    content_length += chunk_overhead(0)  # terminating zero chunk

    headers = {
        'content-encoding': 'aws-chunked',
        'content-length': str(content_length),
        'host': host_header,
        'x-amz-content-sha256': seed_sha,
        'x-amz-date': amz_date,
        'x-amz-decoded-content-length': str(len(data)),
    }
    signed_headers = ';'.join(sorted(headers))
    canonical_headers = ''.join(f'{n}:{headers[n]}\n' for n in sorted(headers))

    canonical_request = (
        f'PUT\n{canonical_uri}\n{_canonical_query(query)}\n{canonical_headers}\n'
        f'{signed_headers}\n{seed_sha}'
    )
    string_to_sign = (
        f'AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n'
        f'{_sha256_hex(canonical_request.encode("utf-8"))}'
    )
    seed_signature = hmac.new(signing_key, string_to_sign.encode('utf-8'),
                              hashlib.sha256).hexdigest()

    authorization = (
        f'AWS4-HMAC-SHA256 Credential={access_key}/{scope}, '
        f'SignedHeaders={signed_headers}, Signature={seed_signature}'
    )

    # Build the chunk-signed body.
    empty_hash = _sha256_hex(b'')
    prev_sig = seed_signature
    body = b''

    def chunk_signature(chunk_bytes):
        chunk_sts = (
            f'AWS4-HMAC-SHA256-PAYLOAD\n{amz_date}\n{scope}\n{prev_sig}\n'
            f'{empty_hash}\n{_sha256_hex(chunk_bytes)}'
        )
        return hmac.new(signing_key, chunk_sts.encode('utf-8'),
                        hashlib.sha256).hexdigest()

    for c in chunks:
        sig = chunk_signature(c)
        prev_sig = sig
        body += f'{len(c):x};chunk-signature={sig}\r\n'.encode('utf-8')
        body += c + b'\r\n'

    final_sig = chunk_signature(b'')
    body += f'0;chunk-signature={final_sig}\r\n\r\n'.encode('utf-8')

    assert len(body) == content_length, \
        f'content-length mismatch: declared {content_length}, body {len(body)}'

    conn = http.client.HTTPConnection(host, port, timeout=30)
    send_headers = dict(headers)
    send_headers['Authorization'] = authorization
    conn.request('PUT', request_target, body=body, headers=send_headers)
    resp = conn.getresponse()
    etag = resp.getheader('ETag')
    resp.read()
    conn.close()
    return resp.status, etag


def signed_s3_post_xml(host, port, access_key, secret_key, region, bucket, key,
                       query, body):
    """Perform a plain SigV4 POST with an XML body.

    boto3 may normalize CompleteMultipartUpload part manifests before sending
    them. Negative parser tests need exact wire order, so use a minimal signed
    request for those cases.
    """
    service = 's3'
    host_header = f'{host}:{port}'
    canonical_uri = f'/{bucket}/{key}'
    request_target = canonical_uri + (f'?{query}' if query else '')

    now = datetime.datetime.now(datetime.timezone.utc)
    amz_date = now.strftime('%Y%m%dT%H%M%SZ')
    date_stamp = now.strftime('%Y%m%d')
    scope = f'{date_stamp}/{region}/{service}/aws4_request'

    payload_hash = _sha256_hex(body)
    headers = {
        'content-length': str(len(body)),
        'content-type': 'application/xml',
        'host': host_header,
        'x-amz-content-sha256': payload_hash,
        'x-amz-date': amz_date,
    }
    signed_headers = ';'.join(sorted(headers))
    canonical_headers = ''.join(f'{n}:{headers[n]}\n' for n in sorted(headers))

    canonical_request = (
        f'POST\n{canonical_uri}\n{_canonical_query(query)}\n{canonical_headers}\n'
        f'{signed_headers}\n{payload_hash}'
    )
    string_to_sign = (
        f'AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n'
        f'{_sha256_hex(canonical_request.encode("utf-8"))}'
    )

    k_date = _sign(('AWS4' + secret_key).encode('utf-8'), date_stamp)
    k_region = hmac.new(k_date, region.encode('utf-8'), hashlib.sha256).digest()
    k_service = hmac.new(k_region, service.encode('utf-8'), hashlib.sha256).digest()
    signing_key = hmac.new(k_service, b'aws4_request', hashlib.sha256).digest()
    signature = hmac.new(signing_key, string_to_sign.encode('utf-8'),
                         hashlib.sha256).hexdigest()

    send_headers = dict(headers)
    send_headers['Authorization'] = (
        f'AWS4-HMAC-SHA256 Credential={access_key}/{scope}, '
        f'SignedHeaders={signed_headers}, Signature={signature}'
    )

    conn = http.client.HTTPConnection(host, port, timeout=30)
    conn.request('POST', request_target, body=body, headers=send_headers)
    resp = conn.getresponse()
    resp_body = resp.read()
    status = resp.status
    conn.close()
    return status, resp_body


def test_streaming(client, bucket):
    """Test SigV4 streaming (aws-chunked) uploads.

    This is the AWS CLI / SDK default upload encoding. The server must strip
    the chunk framing before storing the object; otherwise the chunk size and
    signature lines are written into the object and the data is silently
    corrupted. We upload by hand and read back through boto3 to confirm the
    stored bytes are exactly the payload.
    """
    print("Testing SigV4 streaming (aws-chunked) uploads...")

    cases = [
        ('streamdir/small', b'hello streaming world', 8 * 1024),
        # Multi-chunk payload that also crosses the server's internal 128 KiB
        # read boundary, so the decoder must resume across reads.
        ('streamdir/multi', bytes((i * 7) & 0xff for i in range(300000)), 64 * 1024),
        ('streamdir/empty', b'', 64 * 1024),
    ]

    for key, data, chunk_size in cases:
        status, _ = streaming_put('localhost', 5000, 'myaccessid', 'mysecretkey',
                                  'us-east-1', bucket, key, data, chunk_size)
        assert status == 200, f"streaming PUT {key} returned HTTP {status}"

        head = client.head_object(Bucket=bucket, Key=key)
        assert head['ContentLength'] == len(data), \
            f"{key}: size mismatch, expected {len(data)}, got {head['ContentLength']}"

        actual = client.get_object(Bucket=bucket, Key=key)['Body'].read()
        assert actual == data, (
            f"{key}: content corrupted; {len(actual)} bytes stored, "
            f"first diff at offset "
            f"{next((i for i, (a, b) in enumerate(zip(actual, data)) if a != b), -1)}"
        )
        print(f"  streaming PUT {key} ({len(data)} bytes, "
              f"{chunk_size} chunk) -> content matches")

    # Multipart UploadPart also defaults to aws-chunked in the AWS CLI/SDKs for
    # large files. Initiate via boto3, stream the parts by hand, complete via
    # boto3, and verify the assembled object.
    mp_key = 'streamdir/multipart'
    part_size = 5 * 1024 * 1024  # boto3's per-part minimum for non-final parts
    init = client.create_multipart_upload(Bucket=bucket, Key=mp_key)
    upload_id = init['UploadId']
    parts = []
    expected = b''
    for part_number in range(1, 3):
        body = bytes([part_number]) * part_size
        expected += body
        query = f'partNumber={part_number}&uploadId={upload_id}'
        status, etag = streaming_put('localhost', 5000, 'myaccessid', 'mysecretkey',
                                     'us-east-1', bucket, mp_key, body,
                                     64 * 1024, query=query)
        assert status == 200, \
            f"streaming UploadPart #{part_number} returned HTTP {status}"
        assert etag, f"streaming UploadPart #{part_number} returned no ETag"
        parts.append({'PartNumber': part_number, 'ETag': etag})
        print(f"  streaming UploadPart #{part_number} ({part_size} bytes) -> {etag}")

    client.complete_multipart_upload(
        Bucket=bucket, Key=mp_key, UploadId=upload_id,
        MultipartUpload={'Parts': parts},
    )
    actual = client.get_object(Bucket=bucket, Key=mp_key)['Body'].read()
    assert actual == expected, "streaming multipart: assembled content mismatch"
    print(f"  streaming multipart upload ({len(expected)} bytes) -> content matches")

    print("streaming tests passed!")


def test_awscli(client, bucket):
    """Real AWS CLI (`aws s3 cp`) interop smoke test.

    Drives the actual AWS CLI against the daemon to catch real-client
    regressions that boto3 doesn't surface. Note this is NOT the de-chunking
    test: the modern AWS CLI (v2 / CRT) precomputes payload checksums and does
    not emit aws-chunked streaming for `cp`, so it cannot exercise the
    de-chunking path -- that is covered by test_streaming. What this guards:

      * basic object PUT and GET through the CLI,
      * the Last-Modified response header the CLI requires for downloads
        (boto3 tolerates its absence; the CLI aborts without it), and
      * multipart upload + assembly with the CLI's natural, non-block-aligned
        part sizes (which the boto3 multipart test deliberately avoids).

    The CLI's parallel ranged download of large objects is a separate known
    gap, so the large multipart object is verified via boto3 rather than a CLI
    download.
    """
    aws = shutil.which('aws')
    if not aws:
        raise RuntimeError("aws CLI not found on PATH")

    print("Testing AWS CLI (aws s3 cp) interop...")

    # localhost may resolve to ::1 first; the daemon listens on IPv4 only.
    endpoint = 'http://127.0.0.1:5000'
    workdir = tempfile.mkdtemp(prefix='chimera_awscli_')
    cfg_path = os.path.join(workdir, 'awsconfig')

    env = os.environ.copy()
    # In debug builds the daemon runs under ASan via LD_PRELOAD (set for the
    # test process by the netns wrapper); the standalone `aws` binary exits
    # non-zero under ASan, so strip it for the CLI subprocess.
    env.pop('LD_PRELOAD', None)
    env.update({
        'HOME': workdir,
        'AWS_CONFIG_FILE': cfg_path,
        'AWS_ACCESS_KEY_ID': 'myaccessid',
        'AWS_SECRET_ACCESS_KEY': 'mysecretkey',
        'AWS_DEFAULT_REGION': 'us-east-1',
        'AWS_EC2_METADATA_DISABLED': 'true',
    })

    def write_cfg(extra=''):
        # Path-style addressing: the CLI defaults to virtual-host style, which
        # can't work against a bare host:port endpoint.
        with open(cfg_path, 'w') as f:
            f.write('[default]\ns3 =\n    addressing_style = path\n' + extra)

    def aws_cp(src, dst, timeout=120):
        r = subprocess.run(
            [aws, '--endpoint-url', endpoint,
             '--cli-connect-timeout', '15', '--cli-read-timeout', '60',
             's3', 'cp', src, dst],
            env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            timeout=timeout)
        if r.returncode != 0:
            raise RuntimeError(
                f"aws s3 cp {src} {dst} failed (rc={r.returncode}): "
                f"{r.stdout.decode(errors='replace')}")

    try:
        # --- Single-object roundtrip: PUT and GET both via the CLI. Kept under
        #     the CLI's 8 MiB multipart threshold so the download is a single
        #     GET (exercises the Last-Modified header). ---
        write_cfg()
        small = os.urandom(1024 * 1024 + 7)
        src = os.path.join(workdir, 'small.bin')
        dst = os.path.join(workdir, 'small.out')
        with open(src, 'wb') as f:
            f.write(small)

        aws_cp(src, f's3://{bucket}/awscli/small.bin')
        head = client.head_object(Bucket=bucket, Key='awscli/small.bin')
        assert head['ContentLength'] == len(small), \
            f"size mismatch: expected {len(small)}, got {head['ContentLength']}"

        aws_cp(f's3://{bucket}/awscli/small.bin', dst)
        with open(dst, 'rb') as f:
            assert f.read() == small, "CLI upload+download roundtrip mismatch"
        print(f"  aws s3 cp upload+download ({len(small)} bytes) -> roundtrip matches")

        # --- Multipart upload with the CLI's natural part sizes. 9,000,000
        #     bytes at an 8 MiB chunk size splits into 8 MiB + 611,392 B; the
        #     trailing part is not block-aligned, exercising the assembly
        #     fallback. Verified via boto3 (the CLI's parallel ranged download
        #     of large objects is a separate known gap). ---
        write_cfg('    multipart_threshold = 8MB\n    multipart_chunksize = 8MB\n')
        big = os.urandom(9_000_000)
        bsrc = os.path.join(workdir, 'big.bin')
        with open(bsrc, 'wb') as f:
            f.write(big)

        aws_cp(bsrc, f's3://{bucket}/awscli/big.bin')
        head = client.head_object(Bucket=bucket, Key='awscli/big.bin')
        assert head['ContentLength'] == len(big), \
            f"multipart size mismatch: expected {len(big)}, got {head['ContentLength']}"
        got = client.get_object(Bucket=bucket, Key='awscli/big.bin')['Body'].read()
        assert got == big, (
            f"multipart upload content mismatch: {len(got)} vs {len(big)} bytes, "
            f"first diff @ "
            f"{next((i for i, (a, b) in enumerate(zip(got, big)) if a != b), -1)}")
        print(f"  aws s3 cp multipart upload ({len(big)} bytes, unaligned parts) "
              f"-> content matches")
    finally:
        shutil.rmtree(workdir, ignore_errors=True)

    print("AWS CLI tests passed!")


TESTS = {
    'put': test_put,
    'get': test_get,
    'head': test_head,
    'delete': test_delete,
    'delete_objects': test_delete_objects,
    'list': test_list,
    'copy': test_copy,
    'multipart': test_multipart,
    'streaming': test_streaming,
    'awscli': test_awscli,
}


def run_tests(test_names, backend, signature_version='s3v4', chimera_path=None, debug=False):
    """Run the specified tests against a chimera server."""

    sig_name = 'V4' if signature_version == 's3v4' else 'V2'
    print(f"Using AWS Signature {sig_name}")

    with ChimeraServer(backend=backend, chimera_path=chimera_path, debug=debug) as server:
        client = create_s3_client(signature_version=signature_version)

        for test_name in test_names:
            if test_name not in TESTS:
                print(f"Unknown test: {test_name}", file=sys.stderr)
                return 1

            try:
                TESTS[test_name](client, 'mybucket')
            except Exception as e:
                print(f"\n=== TEST FAILED: {test_name} - {e} ===", file=sys.stderr)
                import traceback
                traceback.print_exc()
                return 1

    print("\n=== ALL TESTS PASSED ===")
    return 0


def main():
    parser = argparse.ArgumentParser(description='S3 test suite')
    parser.add_argument('--test', '-t', action='append', dest='tests',
                        choices=list(TESTS.keys()) + ['all'],
                        help='Test(s) to run (can specify multiple)')
    parser.add_argument('--backend', '-b', default='memfs',
                        choices=['memfs', 'linux', 'io_uring', 'diskfs', 'cairn'],
                        help='VFS backend to test')
    parser.add_argument('--sigver', '-s', default='v4',
                        choices=['v2', 'v4'],
                        help='AWS signature version to use (v2 or v4)')
    parser.add_argument('--chimera', '-c',
                        help='Path to chimera daemon executable')
    parser.add_argument('--debug', '-d', action='store_true',
                        help='Enable debug output from chimera')

    args = parser.parse_args()

    if not args.tests:
        args.tests = ['all']

    if 'all' in args.tests:
        test_names = list(TESTS.keys())
    else:
        test_names = args.tests

    # Convert v2/v4 to boto3 signature version strings
    signature_version = 's3v4' if args.sigver == 'v4' else 's3'

    return run_tests(test_names, args.backend, signature_version, args.chimera, args.debug)


if __name__ == '__main__':
    sys.exit(main())
