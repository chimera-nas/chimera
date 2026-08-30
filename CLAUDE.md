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

All testing is done via ctest. Tests are split into two tiers:

- **quick** (the default) - the quint model-based tests. This is what a plain
  `ctest` runs.
- **extended** - the quick tier plus everything else: pynfs, pjdfstest, cthon,
  ltp, pike, smbtorture, wpts, the KVM suites, and the unit tests. Selected
  with `ctest -C extended`, and what the extended workflow runs.

```bash
# Quick tier: the model-based tests (default)
cd build/Debug && ctest --output-on-failure

# Extended tier: everything
cd build/Debug && ctest -C extended --output-on-failure

# Run specific test (add -C extended if it is not a model-based test)
cd build/Debug && ctest -R <test_name> --output-on-failure

# Run tests with parallel execution
cd build/Release && ctest --output-on-failure -j 8
```

`make check` runs the CI sweep over the quick tier; `make check_extended` runs
the same sweep over the extended tier.

The merge queue runs the quick tier and static analysis on every supported OS,
so the quick tier is what gates a merge. The extended tier runs in the Extended workflow, four times a day. Run it
locally as needed - in particular when proving out a fix to one of those tests,
because a break there will not surface until the next night.

The model-based tests are meant to be a cheap proxy for the full suite, so an
extended-tier failure that the model tests did not catch should be read as a
suggestion to improve the model: consider whether the quint specification or its
trace corpus can be extended to cover that case, so the next such regression is
caught by the quick tier.

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

`make check` runs the tests at the quick tier, which is what the merge queue
gates on. `make check_extended` runs the identical sweep with the full test
suite; only the Extended workflow runs that, so use it when a change reaches beyond
what the model-based tests cover, or when proving out a fix to an extended
test.

## Architecture Overview

### Core Components

1. **VFS Modules** (`src/vfs/`):
   - `memfs`: In-memory filesystem
   - `linux`: Linux filesystem passthrough
   - `diskfs`: Demo/test filesystem
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
- **File Handles**: variable-length opaque handles, at most CHIMERA_VFS_FH_SIZE
  (64) bytes; the actual length travels in `va_fh_len` and depends on the
  backend — roughly 18-22 for the inum-varint modules (memfs, diskfs, cairn),
  33 for smb, 26-42 for linux, and up to 64 for the nfs proxy
- **VFS Operations**: All VFS modules implement common interface with chimera_vfs_attrs
- **Threading**: Core threads + delegation threads model
- **High-Performance Networking**: Support for kernel bypass (RDMA, XLIO)

### Configuration

Chimera uses JSON configuration files. Example:

```json
{
    "server": {
        "nfs_enabled": true,
        "smb_enabled": true,
        "s3_enabled": true
    },
    "shares": {
        "share_name": {
            "module": "linux",
            "path": "/path/to/share"
        }
    },
    "core_threads": 4,
    "sync_delegation": true,
    "sync_delegation_threads": 16,
    "async_delegation": false,
    "async_delegation_threads": 8,
    "enable_rdma": false
}
```

Protocols are opt-in: each of NFS, SMB, and S3 serves only when its
`*_enabled` flag is set (all default to false); the `nfs_port` / `smb_port` /
`s3_port` settings keep the customary defaults and matter only once the
protocol is enabled.

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
