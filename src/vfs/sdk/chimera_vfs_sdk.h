// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/*
 * Chimera VFS module SDK.
 *
 * This directory is the complete compile-time contract between the VFS
 * core and backend modules: a module -- in-tree or out-of-tree -- should
 * include this umbrella header and nothing else from the chimera source
 * tree.  The SDK depends on four external header sets that must be on
 * the include path: libevpl (evpl/evpl.h), prometheus-c, oteltracing-c,
 * and xxhash (<xxhash.h>, used by the file-handle codec).
 *
 * Boundary rules for anything added here:
 *
 *   - Type/struct definitions, constants, and trivial inline accessors
 *     (ten lines or fewer) only.  Substantive logic belongs in the VFS
 *     core behind an exported function (vfs_utils.h), so that a module
 *     object incorporates no VFS code at build time.
 *
 *     One deliberate exception: the file-handle codec (vfs_fh.h) and the
 *     varint primitives it is built on (vfs_varint.h) are inline.  They
 *     encode the routing contract described in vfs_fh.h, they run on the
 *     per-attribute hot path, and a module cannot produce a routable
 *     handle without them.  A module object therefore does incorporate
 *     this much VFS code; if the LGPL section-5 posture for proprietary
 *     modules needs to be airtight, these are the functions to move
 *     behind exported symbols.
 *
 *   - The core-side structures a module receives pointers to but has no
 *     business inspecting (struct chimera_vfs, struct chimera_vfs_thread,
 *     struct chimera_vfs_mount, ...) stay opaque: forward declarations
 *     only.
 *
 * The contract is versioned by CHIMERA_VFS_SDK_VERSION (vfs_module.h);
 * a module stamps it into struct chimera_vfs_module.sdk_version and
 * registration rejects a mismatch.
 */

#include "vfs_fh_magic.h"
#include "vfs_error.h"
#include "vfs_attrs.h"
#include "vfs_cred.h"
#include "vfs_lease_types.h"
#include "vfs_pnfs_layout.h"
#include "vfs_request.h"
#include "vfs_module.h"
#include "vfs_utils.h"
#include "vfs_log.h"
#include "vfs_tcp_flavor.h"

/* File handles: the routing contract every module must satisfy, plus the
 * varint primitives the inum-style encoders are built on. */
#include "vfs_varint.h"
#include "vfs_fh.h"

/* Attribute vocabularies a module fills in or interprets: the canonical
 * ACL carried by chimera_vfs_attrs.va_acl, the access-mask evaluator that
 * keeps ACCESS answers consistent across backends, and the protocol-
 * exported "user." xattr keyspace. */
#include "vfs_acl.h"
#include "vfs_acl_serialize.h"
#include "vfs_access.h"
#include "vfs_xattr_name.h"
