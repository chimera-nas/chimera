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

// Directory operations
struct chimera_posix_dir;
typedef struct chimera_posix_dir CHIMERA_DIR;

CHIMERA_DIR *
chimera_posix_opendir(
    const char *path);

int
chimera_posix_closedir(
    CHIMERA_DIR *dirp);

struct dirent *
chimera_posix_readdir(
    CHIMERA_DIR *dirp);

int
chimera_posix_dirfd(
    CHIMERA_DIR *dirp);

void
chimera_posix_rewinddir(
    CHIMERA_DIR *dirp);

void
chimera_posix_seekdir(
    CHIMERA_DIR *dirp,
    long         loc);

long
chimera_posix_telldir(
    CHIMERA_DIR *dirp);

int
chimera_posix_scandir(
    const char      *dirp,
    struct dirent ***namelist,
    int (*filter)(const struct dirent *),
    int (*compar)(const struct dirent **, const struct dirent **));

// FILE* operations
// CHIMERA_FILE is defined in posix_internal.h as a pointer to fd_entry
struct chimera_posix_fd_entry;
typedef struct chimera_posix_fd_entry CHIMERA_FILE;

// fpos_t equivalent for chimera
typedef struct {
    int64_t pos;
} chimera_fpos_t;

CHIMERA_FILE *
chimera_posix_fopen(
    const char *path,
    const char *mode);

CHIMERA_FILE *
chimera_posix_freopen(
    const char   *path,
    const char   *mode,
    CHIMERA_FILE *stream);

int
chimera_posix_fclose(
    CHIMERA_FILE *stream);

size_t
chimera_posix_fread(
    void         *ptr,
    size_t        size,
    size_t        nmemb,
    CHIMERA_FILE *stream);

size_t
chimera_posix_fwrite(
    const void   *ptr,
    size_t        size,
    size_t        nmemb,
    CHIMERA_FILE *stream);

int
chimera_posix_fseek(
    CHIMERA_FILE *stream,
    long          offset,
    int           whence);

int
chimera_posix_fseeko(
    CHIMERA_FILE *stream,
    off_t         offset,
    int           whence);

long
chimera_posix_ftell(
    CHIMERA_FILE *stream);

off_t
chimera_posix_ftello(
    CHIMERA_FILE *stream);

void
chimera_posix_rewind(
    CHIMERA_FILE *stream);

int
chimera_posix_fgetpos(
    CHIMERA_FILE   *stream,
    chimera_fpos_t *pos);

int
chimera_posix_fsetpos(
    CHIMERA_FILE         *stream,
    const chimera_fpos_t *pos);

int
chimera_posix_feof(
    CHIMERA_FILE *stream);

int
chimera_posix_ferror(
    CHIMERA_FILE *stream);

void
chimera_posix_clearerr(
    CHIMERA_FILE *stream);

int
chimera_posix_fileno(
    CHIMERA_FILE *stream);

int
chimera_posix_fflush(
    CHIMERA_FILE *stream);

int
chimera_posix_fgetc(
    CHIMERA_FILE *stream);

int
chimera_posix_fputc(
    int           c,
    CHIMERA_FILE *stream);

char *
chimera_posix_fgets(
    char         *s,
    int           size,
    CHIMERA_FILE *stream);

int
chimera_posix_fputs(
    const char   *s,
    CHIMERA_FILE *stream);

int
chimera_posix_ungetc(
    int           c,
    CHIMERA_FILE *stream);

// rmdir
int
chimera_posix_rmdir(
    const char *path);

// *at() functions - directory-relative operations
int
chimera_posix_openat(
    int         dirfd,
    const char *pathname,
    int         flags,
    ...);

int
chimera_posix_mkdirat(
    int         dirfd,
    const char *pathname,
    mode_t      mode);

int
chimera_posix_unlinkat(
    int         dirfd,
    const char *pathname,
    int         flags);

int
chimera_posix_renameat(
    int         olddirfd,
    const char *oldpath,
    int         newdirfd,
    const char *newpath);

int
chimera_posix_linkat(
    int         olddirfd,
    const char *oldpath,
    int         newdirfd,
    const char *newpath,
    int         flags);

int
chimera_posix_symlinkat(
    const char *target,
    int         newdirfd,
    const char *linkpath);

ssize_t
chimera_posix_readlinkat(
    int         dirfd,
    const char *pathname,
    char       *buf,
    size_t      bufsiz);

int
chimera_posix_fstatat(
    int          dirfd,
    const char  *pathname,
    struct stat *statbuf,
    int          flags);

int
chimera_posix_faccessat(
    int         dirfd,
    const char *pathname,
    int         mode,
    int         flags);

// Permission and ownership functions
int
chimera_posix_chmod(
    const char *path,
    mode_t      mode);

int
chimera_posix_fchmod(
    int    fd,
    mode_t mode);

int
chimera_posix_fchmodat(
    int         dirfd,
    const char *pathname,
    mode_t      mode,
    int         flags);

int
chimera_posix_chown(
    const char *path,
    uid_t       owner,
    gid_t       group);

int
chimera_posix_fchown(
    int   fd,
    uid_t owner,
    gid_t group);

int
chimera_posix_fchownat(
    int         dirfd,
    const char *pathname,
    uid_t       owner,
    gid_t       group,
    int         flags);

#endif /* CHIMERA_POSIX_H */
