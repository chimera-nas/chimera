<!--
SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors

SPDX-License-Identifier: Unlicense
-->

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Chimera is a high-performance multi-protocol Network Attached Storage (NAS) stack with an asynchronous Virtual File System (VFS) backend. It provides unified server implementations for NFS (v3/v4), SMB2, and S3 protocols, all backed by a pluggable VFS layer.

## Build Trees

Build outputs are located based on source tree location:
- **Main tree (`/chimera`)**: Uses `/build/Release` and `/build/Debug`
- **Worktrees (`/worktrees/*`)**: Uses `./build/Release` and `./build/Debug` within the worktree

This allows multiple worktrees to build independently without conflicts.

**Note:** The user will typically have already built the project. You can run `ninja` directly in these directories to rebuild, or run `ctest` to execute tests.

## Build Commands

```bash
# Build debug version (default)
make

# Build release version
make release

# Build without running tests
make build_release  # or make build_debug

# Run tests only
make test_release  # or make test_debug

# Clean all build artifacts
make clean

# Fix code formatting
make syntax

# Run directly in build directory (if already configured)
# Use ./build for worktrees, /build for main tree
ninja -C build/Debug
ninja -C build/Release
```

## Running Tests

All testing is done via ctest:

```bash
# Run all tests in a build directory
cd build/Debug && ctest --output-on-failure

# Run specific test
cd build/Debug && ctest -R <test_name> --output-on-failure

# Run tests with parallel execution
cd build/Release && ctest --output-on-failure -j 8
```

## Pre-Completion Verification

**IMPORTANT:** After finishing code changes, follow these steps:

1. **Format code** - Run `make syntax` to auto-format all code with uncrustify:
```bash
make syntax
```

2. **Verify all checks pass** - Run `make check` to verify all CI checks pass:
```bash
make check
```

This runs:
- `syntax-check` - Verifies code formatting with uncrustify
- `build_release` / `test_release` - Release build and tests
- `build_debug` / `test_debug` - Debug build and tests
- `build_clang` - Clang static analysis (scan-build)
- `reuse-lint` - SPDX license header compliance

All of these checks are verified by CI, so running `make check` locally ensures your changes will pass CI.

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
            "module": "linux",
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
- Run `make syntax` to auto-fix formatting issues

### Adding New Features
- VFS modules go in `src/vfs/<module_name>/`
- Protocol features go in `src/server/<protocol>/`
- Add tests in corresponding `tests/` directories
- Update CMakeLists.txt for new components

### Dependencies
- External dependencies are in `ext/` (libevpl)
- System libraries: liburing, librdmacm, libjansson, liburcu, librocksdb
- Optional: CUDA toolkit, FIO

## Pull Requests

- Do not include a "Test plan" section in PR descriptions.

## Important Notes

- The project is under active development (v0.1.0)
- Some features marked with TODO/FIXME comments
- Debug builds include AddressSanitizer for memory safety
- Prometheus metrics available on port 9000
- File operations use asynchronous patterns throughout
