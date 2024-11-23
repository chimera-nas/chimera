#pragma once


enum chimera_vfs_error {
    CHIMERA_VFS_OK      = 0,      /* Success */
    CHIMERA_VFS_EPERM   = 1,      /* Operation not permitted */
    CHIMERA_VFS_ENOENT  = 2,      /* No such file or directory */
    CHIMERA_VFS_EIO     = 5,      /* I/O error */
    CHIMERA_VFS_ENXIO   = 6,      /* No such device or address */
    CHIMERA_VFS_EACCES  = 13,     /* Permission denied */
    CHIMERA_VFS_EEXIST  = 17,     /* File exists */
    CHIMERA_VFS_ENOTDIR = 20,     /* Not a directory */
    CHIMERA_VFS_EISDIR  = 21,     /* Is a directory */
    CHIMERA_VFS_EINVAL  = 22,     /* Invalid argument */
    CHIMERA_VFS_EFBIG   = 27,     /* File too large */
    CHIMERA_VFS_ENOSPC  = 28,     /* No space left on device */
    CHIMERA_VFS_EROFS   = 30,     /* Read-only file system */
    CHIMERA_VFS_ENOTSUP = 95,     /* Operation not supported */
    CHIMERA_VFS_UNSET   = 100000  /* Unset error code */
};