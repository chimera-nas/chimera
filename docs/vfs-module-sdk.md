<!--
SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors

SPDX-License-Identifier: LGPL-2.1-only
-->

# VFS Module SDK

`src/vfs/sdk/` is the complete compile-time contract between the chimera
VFS core and backend modules.  A module — in-tree or out-of-tree —
includes the umbrella header and nothing else from the chimera tree:

```c
#include "vfs/sdk/chimera_vfs_sdk.h"
```

An out-of-tree module compiles with `src/vfs/sdk` on its include path
plus the SDK's four declared external dependencies: libevpl
(`evpl/evpl.h`), prometheus-c, oteltracing-c, and xxhash (`<xxhash.h>`,
which the file-handle codec uses to derive a mount_id).
`examples/vfs_module/` is a buildable skeleton compiled exactly that way;
it doubles as the CI proof that the SDK is self-contained.

The SDK is also installed: `cmake --install` places the headers under
`<prefix>/include/chimera/vfs/sdk`, with prometheus-c.h, stopwatch.h and
oteltracing.h co-installed at `<prefix>/include/chimera` (those projects
install no headers of their own) and libevpl's headers at
`<prefix>/include/evpl` via libevpl's own install rules.  A module builds
against the installed tree with

```
-I<prefix>/include -I<prefix>/include/chimera
```

and the same `#include "vfs/sdk/chimera_vfs_sdk.h"` form used in-tree.
xxhash comes from the system (`libxxhash-dev` / `xxhash`).

## What is in the SDK

| Header | Contents |
| --- | --- |
| `vfs_fh_magic.h` | File-handle magic registry, including the VENDOR0–15 values (240–255) reserved for out-of-tree modules.  MIT licensed. |
| `vfs_module.h` | `struct chimera_vfs_module`, capability flags, `CHIMERA_VFS_SDK_VERSION` |
| `vfs_request.h` | Opcodes, per-op flag vocabularies, `struct chimera_vfs_open_handle`, `struct chimera_vfs_request` with the per-op payload union |
| `vfs_attrs.h` | `struct chimera_vfs_attrs`, attribute masks, time sentinels |
| `vfs_cred.h` | Credential layout and helper declarations (impersonation, hashing, kill-priv) |
| `vfs_error.h` | `enum chimera_vfs_error` |
| `vfs_claim_types.h` | Module-facing slice of the claim vocabulary: claim capability masks, owner/actor identity, the CAP_LEASE backend wire shapes, and the pending-acquire ticket the request embeds.  The claim machinery itself (`struct chimera_vfs_claim`, triggers, breaks) stays core-private and opaque. |
| `vfs_pnfs_layout.h` | Protocol-neutral pNFS layout descriptors for `CAP_LAYOUT_SOURCE` backends |
| `vfs_utils.h` | Exported helpers: `chimera_vfs_hash` (readdir-cookie contract), `chimera_vfs_realtime`, `chimera_vfs_request_tcp_flavor` |
| `vfs_log.h` | `chimera_vfs_debug/info/error/fatal/abort` macros, self-contained |
| `vfs_fh.h` | **The file-handle routing contract** (see below) and the encoders that satisfy it |
| `vfs_varint.h` | Varint primitives the inum-style file-handle encoders are built on |
| `vfs_acl.h` | `struct chimera_acl` / `struct chimera_ace` — the canonical ACL carried by `chimera_vfs_attrs.va_acl` |
| `vfs_acl_serialize.h` | Stable, versioned encoding of a `chimera_acl` for a backend's own persistence |
| `vfs_access.h` | Access-mask evaluation, so `ACCESS` answers agree across backends |
| `vfs_xattr_name.h` | The protocol-exported `user.` xattr keyspace NFS and SMB both normalize into |
| `vfs_tcp_flavor.h` | `enum chimera_tcp_flavor`, the value `chimera_vfs_request_tcp_flavor` returns |

## The file-handle contract

This is the one part of the SDK a backend cannot opt out of.  Core routes
every handle-addressed operation by looking the handle up in the mount
table, keyed on its **leading 16 bytes** (`CHIMERA_VFS_MOUNT_ID_SIZE`); the
entry is registered from the root handle the backend returns out of its
mount op.  A backend must guarantee:

1. every handle it emits for an object in a mount starts with the same 16
   bytes as that mount's root handle;
2. those 16 bytes are unique across all mounts live in the process,
   including mounts served by other backends;
3. `CHIMERA_VFS_MOUNT_ID_SIZE <= va_fh_len <= CHIMERA_VFS_FH_SIZE`.

Miss this and core resolves no module for the handle: every operation fails
`ESTALE`/`BADHANDLE`.  Everything after the mount_id is the backend's own
fragment, which core never interprets.  `chimera_vfs_encode_fh_mount()`
derives a conforming mount_id as `XXH3_128(fsid || fragment)` and
`chimera_vfs_encode_fh_parent()` copies a parent's onto a child; a backend
may compute the 16 bytes another way, but then it owns the uniqueness
argument in (2).

Note that `fh_magic` is **not** part of the handle — it identifies the
module in `struct chimera_vfs_module`.  See `vfs_fh_magic.h`.

## Boundary rules

* SDK headers carry **type/struct definitions, constants, and trivial
  inline accessors (ten lines or fewer) only**.  Substantive logic lives
  in the VFS core behind exported functions (`vfs_utils.h`,
  `vfs_cred.h`), so a module object incorporates no VFS code at compile
  time — the LGPL-2.1 §5 posture for proprietary modules dynamically
  linking against chimera.

  The file-handle codec (`vfs_fh.h`, `vfs_varint.h`) is the deliberate
  exception: it encodes the routing contract, runs on the per-attribute
  hot path, and no module can mint a routable handle without it, so it is
  inline and a module object does incorporate that much.  If the §5
  posture needs to be airtight, these are the functions to move behind
  exported symbols.
* Core structures a module holds pointers to but must not inspect
  (`struct chimera_vfs`, `struct chimera_vfs_thread`,
  `struct chimera_vfs_mount`, `struct chimera_vfs_file_state`, ...) stay
  opaque: forward declarations only.  When a module legitimately needs a
  core-owned value, it gets an exported accessor
  (`chimera_vfs_request_tcp_flavor`) rather than struct visibility.
* In-tree modules build against the same surface.
  `scripts/check_vfs_sdk_includes.sh` (run by `make check`) fails the
  build if a module includes a chimera-internal header.  The test for
  keeping something out of the SDK is **"could a third-party backend
  interoperate without it?"** — if not, it belongs here.  What remains
  allowed is in-tree convenience no out-of-tree module would reach for:
  `common/` utilities (a third party brings its own; module logging is
  `vfs_log.h`), `vfs_fsid.h` (Linux-passthrough `st_dev` mapping),
  `server/smb/smb2.h` (for speaking SMB to a third party, not to
  chimera), and `../linux/` (io_uring is the linux backend with a
  different I/O engine).  The `root` and `nfs`
  modules are exempt: root is VFS-core plumbing and the NFS client
  backend is still entangled with the open cache.

## Versioning

The SDK contract is versioned by `CHIMERA_VFS_SDK_VERSION`
(`vfs_module.h`).  A module stamps it into
`chimera_vfs_module.sdk_version` (the struct's first member);
`chimera_vfs_register()` aborts on a mismatch, so a stale out-of-tree
binary fails loudly at load time instead of misinterpreting the request
structures.  Any incompatible change to an SDK header — struct layout,
enum values, capability semantics — must bump the version.

The `struct chimera_vfs_request` layout is exposed in full and is
therefore ABI-stable only within an SDK version.  A public-head /
private-tail split of the request is possible future work if a stable
cross-version ABI is ever wanted.

## Writing a module

See `examples/vfs_module/vfs_example.c` for the skeleton.  In short:

1. Reserve a magic value: in-tree modules take the next low value in
   `vfs_fh_magic.h`; proprietary out-of-tree modules use one of
   `CHIMERA_VFS_FH_MAGIC_VENDOR0..15`.
2. Define `struct chimera_vfs_module vfs_<name>` with default symbol
   visibility, `.sdk_version = CHIMERA_VFS_SDK_VERSION`, the magic,
   capability flags, and the five callbacks (`init`, `destroy`,
   `thread_init`, `thread_destroy`, `dispatch`).
3. `dispatch` receives a `struct chimera_vfs_request`; switch on
   `request->opcode`, use the per-op payload union, set
   `request->status`, and call `request->complete(request)` exactly once
   (synchronously or later from an async completion).
4. Build as a shared object; reference the share in the chimera config
   with `"module": "<name>"` and `"module_path": "/path/to/vfs_<name>.so"`.
   The loader dlopens the object, resolves `vfs_<name>`, and validates
   `sdk_version`.
