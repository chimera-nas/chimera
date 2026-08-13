// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: MIT

#pragma once

/* Each module must have a unique FH_MAGIC value
 * that can never be changed.  They can be reserved
 * here.
 *
 * The 1-byte magic must be the first byte of all
 * file handles returned by the plugin to ensure
 * uniqueness across plugins.
 *
 * This header is MIT licensed, unlike the rest of chimera, so that
 * out-of-tree VFS modules under any license may modify it if desired
 * to name the VENDOR magic values differently.
 */

enum CHIMERA_FS_FH_MAGIC {
    /* Reserved for internal use by chimera */
    CHIMERA_VFS_FH_MAGIC_ROOT     = 0,
    CHIMERA_VFS_FH_MAGIC_MEMFS    = 1,
    CHIMERA_VFS_FH_MAGIC_LINUX    = 2,
    CHIMERA_VFS_FH_MAGIC_IO_URING = 3,
    CHIMERA_VFS_FH_MAGIC_CAIRN    = 4,
    CHIMERA_VFS_FH_MAGIC_DISKFS   = 5,
    CHIMERA_VFS_FH_MAGIC_NFS      = 6,
    /* KV-only backends (no filesystem; never serve a file handle, but occupy a
     * module slot so they can be selected as the default KV module). */
    CHIMERA_VFS_FH_MAGIC_MEMKV    = 7,
    CHIMERA_VFS_FH_MAGIC_SQLITE   = 8,
    CHIMERA_VFS_FH_MAGIC_SMB      = 9,

    /* The last 16 values (240-255) are reserved for proprietary
     * out-of-tree VFS modules and will never be assigned to
     * in-tree modules. */
    CHIMERA_VFS_FH_MAGIC_VENDOR0  = 240,
    CHIMERA_VFS_FH_MAGIC_VENDOR1  = 241,
    CHIMERA_VFS_FH_MAGIC_VENDOR2  = 242,
    CHIMERA_VFS_FH_MAGIC_VENDOR3  = 243,
    CHIMERA_VFS_FH_MAGIC_VENDOR4  = 244,
    CHIMERA_VFS_FH_MAGIC_VENDOR5  = 245,
    CHIMERA_VFS_FH_MAGIC_VENDOR6  = 246,
    CHIMERA_VFS_FH_MAGIC_VENDOR7  = 247,
    CHIMERA_VFS_FH_MAGIC_VENDOR8  = 248,
    CHIMERA_VFS_FH_MAGIC_VENDOR9  = 249,
    CHIMERA_VFS_FH_MAGIC_VENDOR10 = 250,
    CHIMERA_VFS_FH_MAGIC_VENDOR11 = 251,
    CHIMERA_VFS_FH_MAGIC_VENDOR12 = 252,
    CHIMERA_VFS_FH_MAGIC_VENDOR13 = 253,
    CHIMERA_VFS_FH_MAGIC_VENDOR14 = 254,
    CHIMERA_VFS_FH_MAGIC_VENDOR15 = 255
};
