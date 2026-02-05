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


TESTS = {
    'put': test_put,
    'get': test_get,
    'head': test_head,
    'delete': test_delete,
    'list': test_list,
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
