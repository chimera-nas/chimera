// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/*
 * FUSE protocol server: exposes chimera VFS paths as kernel filesystems on
 * the local host by speaking the FUSE kernel ABI directly against /dev/fuse.
 * Linux-only; server.c registers it under #ifdef __linux__.
 */

#include "server/protocol.h"

extern struct chimera_server_protocol fuse_protocol;

/*
 * Register a FUSE mountpoint mapping a host directory to a path in chimera's
 * namespace (e.g. "/memfs_dir/subdir").  Must be called after init() and
 * before start(); the actual kernel mount happens in start().  `options` is a
 * comma-separated list: allow_other, no_default_permissions,
 * attr_timeout_ms=<n>, entry_timeout_ms=<n>.  Returns 0 on success.
 */
int
chimera_fuse_add_mount(
    void       *fuse_shared,
    const char *mountpoint,
    const char *path,
    const char *options);

/*
 * Register a mount served over a caller-supplied descriptor instead of
 * /dev/fuse -- one end of an AF_UNIX SOCK_SEQPACKET socketpair, with a test
 * harness playing the kernel on the other.  SEQPACKET is the point: it
 * preserves message boundaries, which is what makes it interchangeable with
 * /dev/fuse's one-read-one-request contract.  The server takes no notice of
 * the difference; only mount setup does.  Test-only.
 */
int
chimera_fuse_add_synthetic_mount(
    void       *fuse_shared,
    const char *path,
    int         fd);
