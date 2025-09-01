<!--
SPDX-FileCopyrightText: 2025 Ben Jarvis

SPDX-License-Identifier: Unlicense
-->

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Chimera is a high-performance multi-protocol Network Attached Storage (NAS) stack with an asynchronous Virtual File System (VFS) backend. It provides unified server implementations for NFS (v3/v4), SMB2, and S3 protocols, all backed by a pluggable VFS layer.

## Build Commands

```bash
# Build release version (optimized)
make

# Build debug version (with AddressSanitizer)
make debug

# Build without running tests
make build_release  # or make build_debug

# Run tests only
make test_release  # or make test_debug

# Clean all build artifacts
make clean

# Direct CMake commands (if needed)
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -S . -B build/release
ninja -C build/release
```

## Running Tests

```bash
# Run all tests (after building)
cd build/release && ctest --output-on-failure

# Run specific test
cd build/release && ctest -R <test_name> --output-on-failure
```

## Architecture Overview

### Core Components

1. **VFS Modules** (`src/vfs/`):
   - `memfs`: In-memory filesystem
   - `linux`: Linux filesystem passthrough
   - `demofs`: Demo/test filesystem
   - `cairn`: Custom persistent filesystem
   - `io_uring`: io_uring-based async filesystem

2. **Protocol Servers** (`src/server/`):
   - `nfs`: NFSv3 and NFSv4 implementation
   - `smb`: SMB2 protocol server
   - `s3`: S3-compatible object storage

3. **Client Library** (`src/client/`): Application client interface
4. **POSIX Layer** (`src/posix/`): POSIX compatibility layer
5. **Metrics** (`src/metrics/`): Prometheus metrics support

### Key Design Patterns

- **Asynchronous I/O**: Uses libevpl for event-driven architecture
- **File Handles**: 32-byte opaque handles (CHIMERA_VFS_FH_SIZE)
- **VFS Operations**: All VFS modules implement common interface with chimera_vfs_attrs
- **Threading**: Core threads + delegation threads model
- **High-Performance Networking**: Support for kernel bypass (RDMA, XLIO)

### Configuration

Chimera uses JSON configuration files. Example:

```json
{
    "shares": {
        "share_name": {
            "module": "linux",  // VFS module type
            "path": "/path/to/share"
        }
    },
    "core_threads": 4,
    "delegation_threads": 16,
    "enable_rdma": false
}
```

## Development Guidelines

### Code Style
- C code with 4-space indentation (no tabs)
- Use uncrustify with `/chimera/etc/uncrustify.cfg`
- SPDX license headers required
- Follow existing naming conventions in each module

### Adding New Features
- VFS modules go in `src/vfs/<module_name>/`
- Protocol features go in `src/server/<protocol>/`
- Add tests in corresponding `tests/` directories
- Update CMakeLists.txt for new components

### Dependencies
- External dependencies are in `ext/` (libevpl, libsmb2)
- System libraries: liburing, librdmacm, libjansson, liburcu, librocksdb
- Optional: CUDA toolkit, FIO

### Common Development Tasks

```bash
# Start the server
./build/release/daemon/chimera -c chimera.json

# Enable debug logging
./build/debug/daemon/chimera -c chimera.json -d

# Run with Docker
docker run -v /path/to/config:/etc/chimera.json chimera/chimera:latest

# Format code (if uncrustify is installed)
uncrustify -c etc/uncrustify.cfg --no-backup src/path/to/file.c
```

## Important Notes

- The project is under active development (v0.1.0)
- Some features marked with TODO/FIXME comments
- Debug builds include AddressSanitizer for memory safety
- Prometheus metrics available on port 9000
- File operations use asynchronous patterns throughout