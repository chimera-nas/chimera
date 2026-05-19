---
title: Quick Start
layout: default
nav_order: 1
permalink: /quickstart
---

# Quick Start

The fastest way to try chimera is the prebuilt container image published by CI
to `ghcr.io`. The image ships with a working default config that exports two
filesystems out of the box:

| Mount    | Backend | Description                                        |
|----------|---------|----------------------------------------------------|
| `/export`| `linux` | A bind-mount target inside the container. Mount any host directory here to serve files from disk. |
| `/memfs` | `memfs` | An in-memory filesystem. Useful for smoke tests and benchmarking; contents do not persist across restarts. |

Each is published as an NFS export, an SMB share, and an S3 bucket
simultaneously.

## Run the server

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

The ports map as follows:

| Port | Protocol                                            |
|------|-----------------------------------------------------|
| 111  | Sun RPC portmap (required for NFSv3 mounts)         |
| 2049 | NFSv3 / NFSv4                                       |
| 445  | SMB2 / SMB3                                         |
| 5000 | S3                                                  |
| 8080 | Admin REST API                                      |
| 9000 | Prometheus metrics                                  |

> **Warning:** Chimera binds the standard NFS, SMB, and Sun RPC portmap ports.
> If the host is already running `rpcbind`, `nfs-server`, `smbd`, or any other
> service on ports 111, 2049, or 445, either stop them or remap chimera to
> different host ports (e.g. `-p 12049:2049`).

## Connect a client

From another shell on the same host:

```bash
# NFS
mount -t nfs localhost:/export /mnt/export
mount -t nfs localhost:/memfs  /mnt/memfs

# SMB
mount -t cifs //localhost/export /mnt/export -o guest
mount -t cifs //localhost/memfs  /mnt/memfs  -o guest

# S3
aws --endpoint-url http://localhost:5000 s3 ls s3://export/
aws --endpoint-url http://localhost:5000 s3 ls s3://memfs/

# Admin REST API
curl http://localhost:8080/api/v1/exports

# Metrics
curl http://localhost:9000/metrics
```

## Overriding the bundled config

Chimera is configured via a JSON file at `/usr/local/etc/chimera.json` inside
the container. To run with your own config, bind-mount over it:

```bash
docker run --rm -it \
    -v "$(pwd)/chimera.json":/usr/local/etc/chimera.json:ro \
    -v "$(pwd)/export":/export \
    -p 111:111 -p 2049:2049 -p 445:445 -p 5000:5000 -p 8080:8080 -p 9000:9000 \
    ghcr.io/chimera-nas/chimera:latest
```

A minimal example exporting a single in-memory filesystem over NFS at `/mnt`,
with RDMA enabled:

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

The full set of `server` parameters, VFS modules, and protocol options is
covered in the project README and source.
