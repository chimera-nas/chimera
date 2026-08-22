// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>

/*
 * nodeid <-> file-handle table for one FUSE session.
 *
 * The kernel names inodes by 64-bit nodeids it learns from our LOOKUP-style
 * replies and reference-counts with FORGET; chimera names objects by opaque
 * variable-length file handles.  This table owns the mapping in both
 * directions: repeated lookups of the same handle return the same nodeid
 * (the kernel requires a live inode's nodeid to be stable) with its lookup
 * count bumped, and a FORGET that drains the count retires the entry.
 * Nodeids are never recycled within a session, so the entry generation is a
 * constant 1.
 *
 * Shared by every channel of the session (multi-queue delivery means any
 * thread can receive a request for any nodeid); a single mutex guards it,
 * and every hold is an O(1) hash op plus a <=64-byte memcpy.  Shard by
 * handle hash if this ever shows in profiles.
 *
 * FUSE_ROOT_ID (1) is not stored here: the caller resolves it to the mount's
 * root handle directly, and it can never be forgotten.
 */

struct chimera_fuse_node_table;

struct chimera_fuse_node_table *
chimera_fuse_node_table_create(
    void);

void
chimera_fuse_node_table_destroy(
    struct chimera_fuse_node_table *table);

/*
 * Map a file handle to its nodeid, creating the entry if this is the first
 * time the handle is seen, and bump the lookup count.  Call once per
 * fuse_entry_out actually delivered to the kernel; if delivery fails after
 * the call, undo with chimera_fuse_node_forget(nodeid, 1).
 */
uint64_t
chimera_fuse_node_insert(
    struct chimera_fuse_node_table *table,
    const uint8_t                  *fh,
    uint32_t                        fh_len);

/*
 * Copy out the file handle for a nodeid.  Returns 0 on success, -1 if the
 * nodeid is unknown (reply ESTALE).  fh_out must have room for
 * CHIMERA_VFS_FH_SIZE bytes.
 */
int
chimera_fuse_node_get_fh(
    struct chimera_fuse_node_table *table,
    uint64_t                        nodeid,
    uint8_t                        *fh_out,
    uint32_t                       *fh_len_out);

void
chimera_fuse_node_forget(
    struct chimera_fuse_node_table *table,
    uint64_t                        nodeid,
    uint64_t                        nlookup);
