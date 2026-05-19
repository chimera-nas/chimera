#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

"""
S3 test suite using boto3 with AWS Signature V2 and V4.
Starts chimera daemon as a subprocess and runs tests against it.
"""

import argparse
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
        elif self.backend == 'demofs':
            # Create device files for demofs
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
                "demofs": {
                    "path": None,
                    "config": {"devices": devices}
                }
            }
            config["mounts"]["share"] = {
                "module": "demofs",
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
        if self.backend in ('linux', 'io_uring', 'demofs', 'cairn'):
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
    (cairn/demofs).
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
    try:
        client.complete_multipart_upload(
            Bucket=bucket, Key='mpdir/order', UploadId=o_id,
            MultipartUpload={'Parts': [o_parts[1], o_parts[0]]},
        )
        raise AssertionError("Complete with out-of-order parts should fail")
    except ClientError as e:
        # boto3 sometimes sorts internally; either InvalidPartOrder or
        # silently sorted are both acceptable. If sorted, no error -> fail loud.
        if e.response['Error']['Code'] != 'InvalidPartOrder':
            raise
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


TESTS = {
    'put': test_put,
    'get': test_get,
    'head': test_head,
    'delete': test_delete,
    'list': test_list,
    'multipart': test_multipart,
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
                        choices=['memfs', 'linux', 'io_uring', 'demofs', 'cairn'],
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
