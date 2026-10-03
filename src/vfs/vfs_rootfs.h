// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>

struct chimera_vfs;
struct chimera_vfs_module;

/*
 * The built-in root filesystem.
 *
 * The VFS core mounts this at "/" at start, the way Linux boots with an
 * in-memory rootfs: an empty, read-only directory tree in which the core
 * creates the mount points the configured mounts need, and nothing else.  It
 * is the namespace root until a filesystem is mounted over it at "/", and
 * again once that is unmounted.  It is part of the VFS core, not a backend module: it is not
 * loadable, has no configuration, and knows nothing of the mount table -- the
 * core's ordinary mount point crossing makes its directories lead into the
 * filesystems mounted on them.
 */

extern struct chimera_vfs_module chimera_vfs_rootfs_module;

/* Insert the rootfs as the mount at "/", once, at VFS init.  The rootfs module
 * must already be registered. */
void
chimera_vfs_rootfs_mount(
    struct chimera_vfs *vfs);

/* The rootfs's own root handle. */
void
chimera_vfs_rootfs_root_fh(
    uint8_t  *fh,
    uint32_t *fh_len);

/* True when fh belongs to the rootfs. */
int
chimera_vfs_rootfs_owns(
    const void *fh,
    int         fhlen);

/*
 * Return the directory `name` in the rootfs directory `parent_fh`, creating it
 * if it does not exist.  Synchronous: the rootfs lives in memory.  Returns 0
 * with the child's handle in r_fh, or a negative errno-style chimera_vfs_error
 * (CHIMERA_VFS_ESTALE for an unknown parent, CHIMERA_VFS_ENAMETOOLONG).
 */
int
chimera_vfs_rootfs_mkdir(
    struct chimera_vfs *vfs,
    const void         *parent_fh,
    int                 parent_fh_len,
    const char         *name,
    int                 namelen,
    uint8_t            *r_fh,
    int                *r_fh_len);

/*
 * Remove the empty rootfs directory `fh` and return its parent's handle in
 * r_parent_fh and its name (up to CHIMERA_VFS_NAME_MAX bytes) in r_name.  Fails with CHIMERA_VFS_ENOTEMPTY while it holds directories,
 * CHIMERA_VFS_EBUSY for the rootfs root.  The caller checks that nothing is
 * mounted on it.
 */
int
chimera_vfs_rootfs_rmdir(
    struct chimera_vfs *vfs,
    const void         *fh,
    int                 fhlen,
    uint8_t            *r_parent_fh,
    int                *r_parent_fh_len,
    char               *r_name,
    int                *r_namelen);

/*
 * Remove the rootfs directory `fh` if it is empty, then each ancestor left
 * empty, dropping their names from the name cache: what a mount point needed
 * once its mount has gone, or a mount that failed after its mount point was
 * made.  A handle outside the rootfs is ignored.
 */
void
chimera_vfs_rootfs_prune(
    struct chimera_vfs *vfs,
    const void         *fh,
    int                 fhlen);
