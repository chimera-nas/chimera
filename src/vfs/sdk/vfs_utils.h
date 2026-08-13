// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <time.h>

/*
 * Compiled helpers exported by the VFS core for use by backend modules.
 * These are deliberately functions rather than header inlines so that a
 * module object incorporates no VFS code at build time; it merely calls
 * into the chimera_vfs shared library it already links against.
 */

/*
 * Canonical VFS hash for file handles, names, and readdir cookies.
 *
 * The result is masked to be non-negative when interpreted as a signed
 * 64-bit value: NFS readdir cookies are derived from this hash and the
 * Linux kernel rejects negative loff_t values in nfs_llseek_dir(), which
 * breaks seekdir()/telldir() for cookies with bit 63 set.  A backend that
 * produces its own readdir cookies must honor the same constraint.
 */
uint64_t
chimera_vfs_hash(
    const void *data,
    int         len);

/* Fill ts with the current wall-clock time.  Equivalent to
 * clock_gettime(CLOCK_REALTIME, ts) but served from the VFS core's
 * TSC-based clock, avoiding the per-call syscall/read. */
void
chimera_vfs_realtime(
    struct timespec *ts);

struct chimera_vfs_request;

/* TCP transport flavor configured for outbound (client) connections
 * (chimera_vfs_set_tcp_flavor).  Backends that open their own TCP
 * connections (the nfs and smb client modules, or any out-of-tree
 * equivalent) honor it; the vocabulary is in vfs_tcp_flavor.h. */
enum chimera_tcp_flavor
chimera_vfs_request_tcp_flavor(
    const struct chimera_vfs_request *request);
