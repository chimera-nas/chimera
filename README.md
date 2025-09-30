<!--
SPDX-FileCopyrightText: 2025 Ben Jarvis

SPDX-License-Identifier: LGPL-2.1-only
-->

# Chimera

Chimera is a userspace virtual file system (VFS) aimed primarily at network attached storage (NAS).   The chimera virtual file system aims to allow applications to serve and consume NAS storage using standard protocols such as NFS, SMB, and S3 without a dependency on kernel-resident storage code.

# Features

* Protocol support for NFS3, NFS4, SMB2, SMB3, and S3 with a single library, single set of RCU caches, single thread pool, single VFS module.
* A fully asynchronous, event-driven API for file operations.   Not just read/write, but dispatch and callback style APIs for all operations.
* A zero-copy I/O path even for data arriving embedded in unaligned message encodings.  Traditional storage stacks require buffer alignment for DMA (O_DIRECT).   There is no basis for this requirement on modern hardware.
* A richer VFS API that aligns to NAS protocol requirements, not UNIX tradition.  In NFS4 and SMB2, it is possible to create or consume an entire small file in one RPC round trip.   Applications often know what they will do a few steps in advance, but the POSIX API provides no opportunity to convey this knowledge.
* Hardware acceleration.  Chimera is built on top of libevpl, an event loop library that provides hybrid event/poll mode based on load and support for hardware offload and kernel bypass of both network and storage I/O.   RDMA, io_uring, NVIDIA XLIO, etc, can be utilized.   With the right stack, it is possible to run an end-to-end workflow entirely in userspace, without system calls.

# Status

Chimera is currently in an experimental state of development.  The focus so far has been on the server side of the equation.   NFS3, NFS4, SMB2, SMB3, and S3 have been sufficiently implemented to be demonstrable and capture performance measurements, but none are complete.

# Documentation

Currently, there is none beyond this README.   More to come.

# License

Chimera is licensed under LGPL-2.1. See [LICENSE](LICENSE) for details.


# Development

There is a devcontainer in the source tree suitable for use with VS Code or Cursor.

The project uses CMake and there is a makefile wrapper at the root of the tree for non-containerized builds.  Use .devcontainer/Dockerfile as a reference for dependencies.

# Using Docker

The latest chimera build is published by CI to ghcr.io:

```bash
docker run -v /path/to/config:/etc/chimera.json ghcr.io/chimera-nas/chimera:latest
```

The Dockerfile used to generate this image is in the root of the tree.

# Configuration

Chimera uses JSON configuration files to define shares and runtime parameters:

```json
{
    "server": {
        "threads": 16,
        "delegation_threads": 64,
        "rdma": true,
        "smb_multichannel": [
            {
                "address": "192.168.1.1",
                "speed": 200
            }
        ],
        "vfs": {
            "demofs": {
                "path": "/usr/local/lib/chimera_vfs_demofs.so",
                "config": "/usr/local/etc/demofs.json"
            }
        }
    },
    "buckets": {
        "linux": {
            "path": "/linux"
        },
        "demofs": {
            "path": "/demofs"
        },
        "memfs": {
            "path": "/memfs"
        }
    },
    "mounts": {
        "memfs": {
            "module": "memfs",
            "path": "/"
        },
        "linux": {
            "module": "io_uring",
            "path": "/share"
        },
        "demofs": {
            "module": "demofs",
            "path": "/"
        }
    },
    "shares": {
        "memfs": {
            "path": "/memfs"
        },
        "linux": {
            "path": "/linux"
        },
        "demofs": {
            "path": "/demofs"
        }
    }
}
```