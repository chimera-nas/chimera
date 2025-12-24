// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#ifndef CHIMERA_POSIX_H
#define CHIMERA_POSIX_H

#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>

struct chimera_posix_client;
struct chimera_client_config;
struct prometheus_metrics;

struct chimera_posix_client *
chimera_posix_init(
    const struct chimera_client_config *config,
    struct prometheus_metrics          *metrics);

void
chimera_posix_shutdown(
    void);

int
chimera_posix_mount(
    const char *mount_path,
    const char *module_name,
    const char *module_path);

int
chimera_posix_umount(
    const char *mount_path);

int
chimera_posix_open(
    const char *path,
    int         flags,
    ...);

int
chimera_posix_close(
    int fd);

ssize_t
chimera_posix_read(
    int    fd,
    void  *buf,
    size_t count);

ssize_t
chimera_posix_write(
    int         fd,
    const void *buf,
    size_t      count);

int
chimera_posix_mkdir(
    const char *path,
    mode_t      mode);

int
chimera_posix_symlink(
    const char *target,
    const char *path);

int
chimera_posix_link(
    const char *oldpath,
    const char *newpath);

int
chimera_posix_unlink(
    const char *path);

int
chimera_posix_rename(
    const char *oldpath,
    const char *newpath);

ssize_t
chimera_posix_readlink(
    const char *path,
    char       *buf,
    size_t      bufsiz);

int
chimera_posix_stat(
    const char  *path,
    struct stat *st);

int
chimera_posix_fstat(
    int          fd,
    struct stat *st);

off_t
chimera_posix_lseek(
    int   fd,
    off_t offset,
    int   whence);

int64_t
chimera_posix_lseek64(
    int     fd,
    int64_t offset,
    int     whence);

ssize_t
chimera_posix_pread(
    int    fd,
    void  *buf,
    size_t count,
    off_t  offset);

ssize_t
chimera_posix_pread64(
    int     fd,
    void   *buf,
    size_t  count,
    int64_t offset);

ssize_t
chimera_posix_pwrite(
    int         fd,
    const void *buf,
    size_t      count,
    off_t       offset);

ssize_t
chimera_posix_pwrite64(
    int         fd,
    const void *buf,
    size_t      count,
    int64_t     offset);

ssize_t
chimera_posix_readv(
    int                 fd,
    const struct iovec *iov,
    int                 iovcnt);

ssize_t
chimera_posix_writev(
    int                 fd,
    const struct iovec *iov,
    int                 iovcnt);

ssize_t
chimera_posix_preadv(
    int                 fd,
    const struct iovec *iov,
    int                 iovcnt,
    off_t               offset);

ssize_t
chimera_posix_preadv64(
    int                 fd,
    const struct iovec *iov,
    int                 iovcnt,
    int64_t             offset);

ssize_t
chimera_posix_pwritev(
    int                 fd,
    const struct iovec *iov,
    int                 iovcnt,
    off_t               offset);

ssize_t
chimera_posix_pwritev64(
    int                 fd,
    const struct iovec *iov,
    int                 iovcnt,
    int64_t             offset);

ssize_t
chimera_posix_preadv2(
    int                 fd,
    const struct iovec *iov,
    int                 iovcnt,
    off_t               offset,
    int                 flags);

ssize_t
chimera_posix_preadv64v2(
    int                 fd,
    const struct iovec *iov,
    int                 iovcnt,
    int64_t             offset,
    int                 flags);

ssize_t
chimera_posix_pwritev2(
    int                 fd,
    const struct iovec *iov,
    int                 iovcnt,
    off_t               offset,
    int                 flags);

ssize_t
chimera_posix_pwritev64v2(
    int                 fd,
    const struct iovec *iov,
    int                 iovcnt,
    int64_t             offset,
    int                 flags);

#endif /* CHIMERA_POSIX_H */
