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

# Community

Join the conversation on [Discord](https://discord.gg/hnRkvQFecq) to ask questions, share what you're building, or follow along with development.

# License

Chimera is licensed under LGPL-2.1. See [LICENSE](LICENSE) for details.


# Development

There is a devcontainer in the source tree suitable for use with VS Code or Cursor.

The project uses CMake and there is a makefile wrapper at the root of the tree for non-containerized builds.  Use .devcontainer/Dockerfile as a reference for dependencies.

# Using Docker

The latest chimera build is published by CI to ghcr.io. The image ships with a
default config that exports two filesystems:

* `/export` — `linux` backend, bound to the container path `/export`. Mount a
  host directory there to serve files from disk.
* `/memfs` — `memfs` backend, an in-memory filesystem useful for smoke tests
  and benchmarking.

Each is published as an NFS export, an SMB share, and an S3 bucket.

> **Warning:** Chimera binds the standard NFS, SMB, and Sun RPC portmap ports.
> If the host is already running `rpcbind`, `nfs-server`, `smbd`, or any other
> service on ports 111, 2049, or 445, either stop them or remap chimera to
> different host ports (e.g. `-p 12049:2049`).

```bash
mkdir -p ./export
docker run --rm -it \
    -v "$(pwd)/export":/export \
    -p 111:111 -p 2049:2049 \
    -p 445:445 \
    -p 5000:5000 \
    -p 8080:8080 \
    -p 9000:9000 \
    ghcr.io/chimera-nas/chimera:latest
```

| Port | Protocol |
|------|----------|
| 111  | Sun RPC portmap (required for NFSv3 mounts) |
| 2049 | NFSv3 / NFSv4 |
| 445  | SMB2 / SMB3 |
| 5000 | S3 |
| 8080 | Admin REST API |
| 9000 | Prometheus metrics |

From the host:

```bash
mount -t nfs localhost:/export /mnt/export
mount -t nfs localhost:/memfs  /mnt/memfs

mount -t cifs //localhost/export /mnt/export -o guest
mount -t cifs //localhost/memfs  /mnt/memfs  -o guest

aws --endpoint-url http://localhost:5000 s3 ls s3://export/
aws --endpoint-url http://localhost:5000 s3 ls s3://memfs/

curl http://localhost:8080/api/v1/exports
curl http://localhost:9000/metrics
```

## Overriding the bundled config

To run with a custom configuration, bind-mount your own file over
`/usr/local/etc/chimera.json`:

```bash
docker run --rm -it \
    -v "$(pwd)/chimera.json":/usr/local/etc/chimera.json:ro \
    -v "$(pwd)/export":/export \
    -p 111:111 -p 2049:2049 -p 445:445 -p 5000:5000 -p 8080:8080 -p 9000:9000 \
    ghcr.io/chimera-nas/chimera:latest
```

A minimal example that exports a single in-memory filesystem over NFS at
`/mnt`, with RDMA enabled:

```json
{
    "server": {
        "threads": 32,
        "sync_delegation_threads": 32,
        "async_delegation": false,
        "async_delegation_threads": 8,
        "preallocate_slabs": 8,
        "preallocate_threads": 4,
        "max_open_files": 262144,
        "external_portmap": 0,
        "rdma": true,
        "rdma_hostname": "0.0.0.0",
        "rdma_port": 20049
    },
    "mounts": {
        "memfs": {
            "module": "memfs",
            "path": "/"
        }
    },
    "exports": {
        "/mnt": {
            "path": "/memfs"
        }
    }
}
```

The Dockerfile used to generate this image is in the root of the tree.

# Configuration

Chimera uses JSON configuration files to define shares and runtime parameters:

```json
{
    "server": {
        "threads": 16,
        "sync_delegation_threads": 64,
        "async_delegation": false,
        "async_delegation_threads": 8,
        "rdma": true,
        "smb_multichannel": [
            {
                "address": "192.168.1.1",
                "speed": 200
            }
        ],
        "vfs": {
            "diskfs": {
                "path": "/usr/local/lib/chimera_vfs_diskfs.so",
                "config": "/usr/local/etc/diskfs.json"
            }
        }
    },
    "buckets": {
        "linux": {
            "path": "/linux"
        },
        "diskfs": {
            "path": "/diskfs"
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
        "diskfs": {
            "module": "diskfs",
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
        "diskfs": {
            "path": "/diskfs"
        }
    }
}
```

# Questions or feedback?

Come say hi on [Discord](https://discord.gg/hnRkvQFecq) — it's the best place to ask questions, share what you're building, or follow along with development.
