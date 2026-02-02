// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once


enum chimera_vfs_error {
    CHIMERA_VFS_OK           = 0,      /* Success */
    CHIMERA_VFS_EPERM        = 1,      /* Operation not permitted */
    CHIMERA_VFS_ENOENT       = 2,      /* No such file or directory */
    CHIMERA_VFS_EIO          = 5,      /* I/O error */
    CHIMERA_VFS_ENXIO        = 6,      /* No such device or address */
    CHIMERA_VFS_EACCES       = 13,     /* Permission denied */
    CHIMERA_VFS_EFAULT       = 14,         /* Bad address */
    CHIMERA_VFS_EEXIST       = 17,     /* File exists */
    CHIMERA_VFS_EXDEV        = 18,     /* Cross-device link */
    CHIMERA_VFS_ENOTDIR      = 20,     /* Not a directory */
    CHIMERA_VFS_EISDIR       = 21,     /* Is a directory */
    CHIMERA_VFS_EINVAL       = 22,     /* Invalid argument */
    CHIMERA_VFS_EMFILE       = 24,     /* Too many open files */
    CHIMERA_VFS_EFBIG        = 27,     /* File too large */
    CHIMERA_VFS_ENOSPC       = 28,     /* No space left on device */
    CHIMERA_VFS_EROFS        = 30,     /* Read-only file system */
    CHIMERA_VFS_EMLINK       = 31,     /* Too many links */
    CHIMERA_VFS_ENAMETOOLONG = 36,     /* File name too long */
    CHIMERA_VFS_ENOTEMPTY    = 39,     /* Directory not empty */
    CHIMERA_VFS_ELOOP        = 40,    /* Too many levels of symbolic links */
    CHIMERA_VFS_EOVERFLOW    = 75,         /* Value too large for defined data type */
    CHIMERA_VFS_EBADF        = 77,     /* Bad file descriptor */
    CHIMERA_VFS_ENOTSUP      = 95,     /* Operation not supported */
    CHIMERA_VFS_EDQUOT       = 122,    /* Quota exceeded */
    CHIMERA_VFS_ESTALE       = 116,    /* Stale file handle */
    CHIMERA_VFS_EBADCOOKIE   = 200,    /* Bad readdir cookie/verifier */
    CHIMERA_VFS_UNSET        = 100000  /* Unset error code */
};