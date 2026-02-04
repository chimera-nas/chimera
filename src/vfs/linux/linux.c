// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/sysmacros.h>
#include <sys/statvfs.h>
#include <sys/uio.h>
#include <dirent.h>
#include <fcntl.h>
#include <errno.h>
#include <uthash.h>
#include <jansson.h>
#include <linux/version.h>
#include "vfs/vfs_error.h"

// fchmodat support for AT_SYMLINK_NOFOLLOW was added in Linux 6.6
#if defined(LINUX_VERSION_CODE) && defined(KERNEL_VERSION)
#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 6, 0)
#define HAVE_FCHMODAT_AT_SYMLINK_NOFOLLOW 1
#endif /* if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 6, 0) */
#endif /* if defined(LINUX_VERSION_CODE) && defined(KERNEL_VERSION) */

#include "evpl/evpl.h"

#include "linux.h"
#include "linux_common.h"
#include "common/logging.h"
#include "common/format.h"
#include "common/misc.h"
#include "common/macros.h"

struct chimera_linux_shared {
    int readdir_verifier;
};

struct chimera_linux_thread {
    struct evpl                     *evpl;
    struct chimera_linux_mount_table mount_table;
    int                              readdir_verifier;
};

static void *
chimera_linux_init(const char *cfgfile)
{
    struct chimera_linux_shared *shared;

    shared = calloc(1, sizeof(*shared));

    if (cfgfile && cfgfile[0] != '\0') {
        json_error_t json_error;
        json_t      *cfg = json_loads(cfgfile, 0, &json_error);

        if (cfg) {
            json_t *verf = json_object_get(cfg, "readdir_verifier");

            if (verf && json_is_boolean(verf)) {
                shared->readdir_verifier = json_boolean_value(verf);
            }

            json_decref(cfg);
        }
    }

    return shared;
} /* linux_init */ /* linux_init */

static void
chimera_linux_destroy(void *private_data)
{
    free(private_data);
} /* linux_destroy */ /* linux_destroy */

static void *
chimera_linux_thread_init(
    struct evpl *evpl,
    void        *private_data)
{
    struct chimera_linux_shared *shared = private_data;
    struct chimera_linux_thread *thread =
        (struct chimera_linux_thread *) calloc(1, sizeof(*thread));

    thread->evpl             = evpl;
    thread->readdir_verifier = shared->readdir_verifier;

    return thread;
} /* linux_thread_init */ /* linux_thread_init */

static void
chimera_linux_thread_destroy(void *private_data)
{
    struct chimera_linux_thread *thread = private_data;

    linux_mount_table_destroy(&thread->mount_table);

    free(thread);
} /* linux_thread_destroy */

static inline int
chimera_linux_set_attrs(
    int                       dirfd,
    char                     *path,
    struct chimera_vfs_attrs *attr)
{
    int      rc;
    uint64_t set_mask = attr->va_set_mask;

    if (set_mask & CHIMERA_VFS_ATTR_MODE) {
#ifdef HAVE_FCHMODAT_AT_SYMLINK_NOFOLLOW
        // Use fchmodat with AT_SYMLINK_NOFOLLOW on kernels >= 6.6
        rc = fchmodat(dirfd, path, attr->va_mode, AT_SYMLINK_NOFOLLOW | AT_EMPTY_PATH);
#else  /* ifdef HAVE_FCHMODAT_AT_SYMLINK_NOFOLLOW */
        if (strlen(path) != 0) {
            rc = fchmodat(dirfd, path, attr->va_mode, 0);
        } else {
            // dirfd might be O_PATH, reopen without O_PATH via /proc/self/fd
            char procpath[64];
            snprintf(procpath, sizeof(procpath), "/proc/self/fd/%d", dirfd);
            int  reopen_fd = open(procpath, O_RDONLY);
            if (reopen_fd < 0) {
                chimera_linux_error("linux_setattr: reopen via /proc for fchmod failed: %s",
                                    strerror(errno));
                return -errno;
            }
            rc = fchmod(reopen_fd, attr->va_mode);
            close(reopen_fd);
        }
#endif /* ifdef HAVE_FCHMODAT_AT_SYMLINK_NOFOLLOW */
        if (rc) {
            chimera_linux_error("linux_setattr: fchmod(%o) failed: %s",
                                attr->va_mode, strerror(errno));

            return -errno;
        }

        attr->va_set_mask |= CHIMERA_VFS_ATTR_MODE;
    }

    if ((set_mask & (CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_GID)) ==
        (CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_GID)) {

        rc = fchownat(dirfd, path, attr->va_uid, attr->va_gid,
                      AT_SYMLINK_NOFOLLOW | AT_EMPTY_PATH);

        if (rc) {
            chimera_linux_error("linux_setattr: fchown(%u,%u) failed: %s",
                                attr->va_uid, attr->va_gid, strerror(errno));

            return -errno;
        }

        attr->va_set_mask |= CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_GID;
    } else if (set_mask & CHIMERA_VFS_ATTR_UID) {

        rc = fchownat(dirfd, path, attr->va_uid, -1,
                      AT_SYMLINK_NOFOLLOW | AT_EMPTY_PATH);

        if (rc) {
            chimera_linux_error("linux_setattr: fchown(%u,-1) failed: %s",
                                attr->va_uid,
                                strerror(errno));

            return -errno;
        }

        attr->va_set_mask |= CHIMERA_VFS_ATTR_UID;
    } else if (set_mask & CHIMERA_VFS_ATTR_GID) {

        rc = fchownat(dirfd, path, -1, attr->va_gid,
                      AT_SYMLINK_NOFOLLOW | AT_EMPTY_PATH);

        if (rc) {
            chimera_linux_error("linux_setattr: fchown(%u,-1) failed: %s",
                                attr->va_gid,
                                strerror(errno));

            return -errno;
        }

        attr->va_set_mask |= CHIMERA_VFS_ATTR_GID;
    }

    if (set_mask & CHIMERA_VFS_ATTR_SIZE) {
        // code assumes that path is empty when dirfd is valid
        rc = ftruncate(dirfd, attr->va_size);

        if (rc) {
            chimera_linux_error("linux_setattr: ftruncate(%ld) failed: %s",
                                attr->va_size,
                                strerror(errno));

            return -errno;
        }

        attr->va_set_mask |= CHIMERA_VFS_ATTR_SIZE;
    }

    if (set_mask & (CHIMERA_VFS_ATTR_ATIME | CHIMERA_VFS_ATTR_MTIME)) {
        struct timespec times[2];

        if (set_mask & CHIMERA_VFS_ATTR_ATIME) {
            if (attr->va_atime.tv_nsec == CHIMERA_VFS_TIME_NOW) {
                times[0].tv_nsec = UTIME_NOW;
            } else {
                times[0] = attr->va_atime;
            }

            attr->va_set_mask |= CHIMERA_VFS_ATTR_ATIME;
        } else {
            times[0].tv_nsec = UTIME_OMIT;
        }

        if (set_mask & CHIMERA_VFS_ATTR_MTIME) {
            if (attr->va_mtime.tv_nsec == CHIMERA_VFS_TIME_NOW) {
                times[1].tv_nsec = UTIME_NOW;
            } else {
                times[1] = attr->va_mtime;
            }

            attr->va_set_mask |= CHIMERA_VFS_ATTR_MTIME;
        } else {
            times[1].tv_nsec = UTIME_OMIT;
        }

        rc = utimensat(dirfd, path, times, AT_SYMLINK_NOFOLLOW | AT_EMPTY_PATH);

        if (rc) {
            chimera_linux_error("linux_setattr: utimensat() failed: %s",
                                strerror(errno));

            return -errno;
        }
    }

    return 0;
} /* chimera_linux_set_attrs */

static void
chimera_linux_getattr(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    int fd;

    fd = (int) request->getattr.handle->vfs_private;

    chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_LINUX,
                            &request->getattr.r_attr,
                            fd);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* linux_getattr */

static void
chimera_linux_setattr(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    int fd, rc;

    fd = request->setattr.handle->vfs_private;

    rc = chimera_linux_set_attrs(fd, "", request->setattr.set_attr);

    chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_LINUX,
                            &request->setattr.r_post_attr,
                            fd);

    request->status = chimera_linux_errno_to_status(rc);
    request->complete(request);
} /* linux_setattr */

static void
chimera_linux_mount(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    int                       mount_fd, rc;
    struct chimera_vfs_attrs *r_attr;
    char                     *scratch = (char *) request->plugin_data;

    r_attr = &request->mount.r_attr;

    TERM_STR(fullpath,
             request->mount.path,
             request->mount.pathlen,
             scratch);

    mount_fd = open(fullpath, O_DIRECTORY | O_RDONLY);

    if (mount_fd < 0) {
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }
    rc = linux_get_fh(NULL, /* mount context - compute fsid */
                      mount_fd,
                      fullpath,
                      r_attr->va_fh,
                      &r_attr->va_fh_len);

    if (rc < 0) {
        request->status = chimera_linux_errno_to_status(errno);
        close(mount_fd);
        request->complete(request);
        return;
    }

    r_attr->va_set_mask |= CHIMERA_VFS_ATTR_FH;

    chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_LINUX,
                            r_attr,
                            mount_fd);

    request->status = CHIMERA_VFS_OK;

    close(mount_fd);

    request->complete(request);
} /* chimera_linux_lookup_path */

static void
chimera_linux_umount(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    /* No action required */
    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_linux_umount */

static void
chimera_linux_lookup(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    int   parent_fd, rc;
    char *scratch = (char *) request->plugin_data;

    parent_fd = (int) request->lookup.handle->vfs_private;

    TERM_STR(fullname, request->lookup.component, request->lookup.component_len, scratch);

    rc = chimera_linux_map_child_attrs(CHIMERA_VFS_FH_MAGIC_LINUX,
                                       request,
                                       &request->lookup.r_attr,
                                       parent_fd,
                                       fullname);

    if (rc) {
        request->status = rc;
        request->complete(request);
        return;
    }

    chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_LINUX,
                            &request->lookup.r_dir_attr,
                            parent_fd);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* linux_lookup */

static void
chimera_linux_readdir(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_linux_thread *thread = private_data;
    int                          fd, dup_fd, rc;
    DIR                         *dir;
    struct dirent               *dirent;
    struct chimera_vfs_attrs     vattr;
    int                          eof = 1;

    fd = request->readdir.handle->vfs_private;

    chimera_linux_debug("linux_readdir: opening %d", fd);

    if (thread->readdir_verifier) {
        struct stat st;

        rc = fstat(fd, &st);

        if (rc == 0) {
            uint64_t mtime_verf = chimera_linux_mtime_to_verifier(&st);

            if (request->readdir.verifier &&
                request->readdir.verifier != mtime_verf) {
                request->status = CHIMERA_VFS_EBADCOOKIE;
                request->complete(request);
                return;
            }

            request->readdir.r_verifier = mtime_verf;
        }
    }

    dup_fd = openat(fd, ".", O_RDONLY | O_DIRECTORY);

    if (dup_fd < 0) {
        chimera_linux_error("linux_readdir: openat() failed: %s",
                            strerror(errno));
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }

    dir = fdopendir(dup_fd);

    if (!dir) {
        chimera_linux_error("linux_readdir: fdopendir() failed: %s",
                            strerror(errno));
        close(dup_fd);
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }

    if (request->readdir.cookie) {
        seekdir(dir, request->readdir.cookie);
    }

    vattr.va_req_mask = request->readdir.attr_mask;

    while ((dirent = readdir(dir))) {

        /* Skip . and .. unless explicitly requested */
        if (!(request->readdir.flags & CHIMERA_VFS_READDIR_EMIT_DOT)) {
            if ((dirent->d_name[0] == '.' && dirent->d_name[1] == '\0') ||
                (dirent->d_name[0] == '.' && dirent->d_name[1] == '.' &&
                 dirent->d_name[2] == '\0')) {
                continue;
            }
        }

        chimera_linux_map_child_attrs(CHIMERA_VFS_FH_MAGIC_LINUX,
                                      request,
                                      &vattr,
                                      fd,
                                      dirent->d_name);

        rc = request->readdir.callback(
            dirent->d_ino,
            dirent->d_off,
            dirent->d_name,
            strlen(dirent->d_name),
            &vattr,
            request->proto_private_data);

        if (rc) {
            eof = 0;
            break;
        }

    } /* chimera_linux_readdir */

    request->readdir.r_cookie = telldir(dir);
    request->readdir.r_eof    = eof;

    closedir(dir);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* linux_readdir */ /* linux_readdir */

static int
chimera_linux_set_open_flags(uint32_t in_flags)
{
    int flags = 0;

    if (in_flags & CHIMERA_VFS_OPEN_PATH) {
        flags |= O_PATH;
    } else {
        if (in_flags & (CHIMERA_VFS_OPEN_DIRECTORY | CHIMERA_VFS_OPEN_READ_ONLY)) {
            flags |= O_RDONLY;
        } else {
            flags |= O_RDWR;
        }
        if (in_flags & CHIMERA_VFS_OPEN_CREATE) {
            flags |= O_CREAT;
        }
        if (in_flags & CHIMERA_VFS_OPEN_EXCLUSIVE) {
            flags |= O_EXCL;
        }
    }
    if (in_flags & CHIMERA_VFS_OPEN_DIRECTORY) {
        flags |= O_DIRECTORY;
    }
    return flags;
} /* chimera_linux_set_open_flags */

static void
chimera_linux_open(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_linux_thread *thread = private_data;
    int                          flags;
    int                          fd;

    flags = chimera_linux_set_open_flags(request->open.flags);

    fd = linux_open_by_handle(&thread->mount_table,
                              request->fh,
                              request->fh_len,
                              flags);

    if (fd < 0) {
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }

    request->open.r_vfs_private = fd;

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* linux_open */

static void
chimera_linux_open_at(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    int      parent_fd, fd, flags, rc;
    uint32_t mode;
    char    *scratch = (char *) request->plugin_data;

    TERM_STR(fullname, request->open_at.name, request->open_at.namelen, scratch);

    parent_fd = request->open_at.handle->vfs_private;

    if (request->open_at.set_attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) {
        mode                                    = request->open_at.set_attr->va_mode;
        request->open_at.set_attr->va_set_mask |= CHIMERA_VFS_ATTR_MODE;
    } else {
        mode = 0600;
    }

    flags = chimera_linux_set_open_flags(request->open_at.flags);
    fd    = openat(parent_fd, fullname, flags, mode);

    if (fd < 0) {
        chimera_linux_debug("linux_open_at: openat(%d,%s,%d, 0%o) failed: %s",
                            parent_fd, fullname, flags, mode, strerror(errno));
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }

    rc = chimera_linux_set_attrs(fd, "", request->open_at.set_attr);

    if (rc < 0) {
        request->status = chimera_linux_errno_to_status(-rc);
        request->complete(request);
        return;
    }

    request->open_at.r_vfs_private = fd;

    chimera_linux_map_child_attrs(CHIMERA_VFS_FH_MAGIC_LINUX,
                                  request,
                                  &request->open_at.r_attr,
                                  parent_fd,
                                  fullname);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* linux_open_at */

static void
chimera_linux_close(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    int fd = request->close.vfs_private;

    close(fd);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_linux_close */

static void
chimera_linux_mkdir(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    int      fd, rc;
    char    *scratch = (char *) request->plugin_data;
    uint32_t mode;

    TERM_STR(fullname, request->mkdir.name, request->mkdir.name_len, scratch);

    fd = request->mkdir.handle->vfs_private;

    if (request->mkdir.set_attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) {
        mode = request->mkdir.set_attr->va_mode;
    } else {
        mode = S_IRWXU;
    }

    rc = mkdirat(fd, fullname, mode);

    chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_LINUX,
                            &request->mkdir.r_dir_post_attr,
                            fd);

    if (rc < 0) {

        if (errno == EEXIST) {
            chimera_linux_map_child_attrs(CHIMERA_VFS_FH_MAGIC_LINUX,
                                          request,
                                          &request->mkdir.r_attr,
                                          fd,
                                          fullname);
        }

        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }

    rc = chimera_linux_set_attrs(fd, fullname, request->mkdir.set_attr);

    if (rc < 0) {
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }

    chimera_linux_map_child_attrs(CHIMERA_VFS_FH_MAGIC_LINUX,
                                  request,
                                  &request->mkdir.r_attr,
                                  fd,
                                  fullname);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_linux_mkdir */

static void
chimera_linux_mknod(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    int      fd, rc;
    char    *scratch = (char *) request->plugin_data;
    uint32_t mode;
    dev_t    dev = 0;

    TERM_STR(fullname, request->mknod.name, request->mknod.name_len, scratch);

    fd = request->mknod.handle->vfs_private;

    if (request->mknod.set_attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) {
        mode = request->mknod.set_attr->va_mode;
    } else {
        mode = S_IFREG | 0644;
    }

    if (request->mknod.set_attr->va_set_mask & CHIMERA_VFS_ATTR_RDEV) {
        dev = makedev(request->mknod.set_attr->va_rdev >> 32,
                      request->mknod.set_attr->va_rdev & 0xFFFFFFFF);
    }

    chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_LINUX,
                            &request->mknod.r_dir_pre_attr,
                            fd);

    rc = mknodat(fd, fullname, mode, dev);

    chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_LINUX,
                            &request->mknod.r_dir_post_attr,
                            fd);

    if (rc < 0) {

        if (errno == EEXIST) {
            chimera_linux_map_child_attrs(CHIMERA_VFS_FH_MAGIC_LINUX,
                                          request,
                                          &request->mknod.r_attr,
                                          fd,
                                          fullname);
        }

        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }

    rc = chimera_linux_set_attrs(fd, fullname, request->mknod.set_attr);

    if (rc < 0) {
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }

    chimera_linux_map_child_attrs(CHIMERA_VFS_FH_MAGIC_LINUX,
                                  request,
                                  &request->mknod.r_attr,
                                  fd,
                                  fullname);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_linux_mknod */

static void
chimera_linux_remove(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    int   fd, rc;
    char *scratch = (char *) request->plugin_data;

    TERM_STR(fullname, request->remove.name, request->remove.namelen, scratch);

    fd = request->remove.handle->vfs_private;

    request->remove.r_removed_attr.va_req_mask = CHIMERA_VFS_ATTR_FH;

    chimera_linux_map_child_attrs(CHIMERA_VFS_FH_MAGIC_LINUX,
                                  request,
                                  &request->remove.r_removed_attr,
                                  fd,
                                  fullname);

    rc = unlinkat(fd, fullname, 0);

    if (rc == -1 && errno == EISDIR) {
        rc = unlinkat(fd, fullname, AT_REMOVEDIR);
    }

    if (rc) {
        request->status = chimera_linux_errno_to_status(errno);
    } else {
        request->status = CHIMERA_VFS_OK;
    }

    request->complete(request);
} /* chimera_linux_remove */

static void
chimera_linux_read(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_linux_thread *thread = private_data;
    struct evpl                 *evpl   = thread->evpl;
    int                          fd, i;
    ssize_t                      len, left = request->read.length;
    struct iovec                *iov;

    /* Handle 0-byte reads specially - preadv with uninitialized iov causes EFAULT */
    if (request->read.length == 0) {
        fd = (int) request->read.handle->vfs_private;
        chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_LINUX, &request->read.r_attr, fd);
        request->read.r_niov   = 0;
        request->read.r_length = 0;
        request->read.r_eof    = 1;
        request->status        = CHIMERA_VFS_OK;
        request->complete(request);
        return;
    }

    request->read.r_niov = evpl_iovec_alloc(evpl,
                                            request->read.length,
                                            4096,
                                            8,
                                            EVPL_IOVEC_FLAG_SHARED, request->read.iov);

    iov = request->plugin_data;

    for (i = 0; left && i < request->read.r_niov; i++) {

        iov[i].iov_base = request->read.iov[i].data;
        iov[i].iov_len  = request->read.iov[i].length;

        if (iov[i].iov_len > left) {
            iov[i].iov_len = left;
        }

        left -= iov[i].iov_len;
    }

    fd = (int) request->read.handle->vfs_private;

    len = preadv(fd,
                 iov,
                 request->read.r_niov,
                 request->read.offset);

    if (len < 0) {
        request->status = chimera_linux_errno_to_status(errno);

        evpl_iovecs_release(evpl, request->read.iov, request->read.r_niov);

        request->read.r_niov   = 0;
        request->read.r_length = 0;
        request->read.r_eof    = 0;
        request->complete(request);
        return;
    }

    if (len == 0) {
        evpl_iovecs_release(evpl, request->read.iov, request->read.r_niov);
        request->read.r_niov = 0;
    }

    chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_LINUX, &request->read.r_attr, fd);

    request->read.r_length = len;
    request->read.r_eof    = (len < request->read.length);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);

} /* chimera_linux_read */

static void
chimera_linux_write(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    int           fd, i, niov = 0, flags = 0;
    uint32_t      left, chunk;
    ssize_t       len;
    struct iovec *iov;

    request->write.r_sync = request->write.sync;

    iov = request->plugin_data;

    left = request->write.length;
    for (i = 0; left && i < request->write.niov; i++) {
        if (request->write.iov[i].length <= left) {
            chunk = request->write.iov[i].length;
        } else {
            chunk = left;
        }
        iov[i].iov_base = request->write.iov[i].data;
        iov[i].iov_len  = chunk;
        left           -= chunk;
        niov++;
    }

    fd = (int) request->write.handle->vfs_private;

    if (request->write.sync) {
        flags = RWF_SYNC;
    }

    len = pwritev2(fd,
                   iov,
                   niov,
                   request->write.offset,
                   flags);

    /* Note: Write iovecs are NOT released here. They were allocated on the
     * server thread and must be released there. The server's write completion
     * callback handles the release after this request completes via doorbell.
     */

    if (len < 0) {
        request->status         = chimera_linux_errno_to_status(errno);
        request->write.r_length = 0;
        request->complete(request);
        return;
    }

    request->write.r_length = len;

    chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_LINUX, &request->write.r_post_attr, fd);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);

} /* chimera_linux_write */

static void
chimera_linux_commit(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    int fd = (int) request->commit.handle->vfs_private;

    fsync(fd);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);

} /* chimera_linux_commit */

static void
chimera_linux_symlink(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    int   fd, rc;
    char *scratch = (char *) request->plugin_data;

    TERM_STR(fullname, request->symlink.name, request->symlink.namelen, scratch);
    TERM_STR(target, request->symlink.target, request->symlink.targetlen, scratch);

    fd = request->symlink.handle->vfs_private;

    rc = symlinkat(target, fd, fullname);

    if (rc < 0) {
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }

    chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_LINUX, &request->symlink.r_dir_post_attr, fd);

    linux_get_fh(request->fh, /* use parent's mount_id */
                 fd,
                 fullname,
                 request->symlink.r_attr.va_fh,
                 &request->symlink.r_attr.va_fh_len);

    chimera_linux_map_child_attrs(CHIMERA_VFS_FH_MAGIC_LINUX,
                                  request,
                                  &request->symlink.r_attr,
                                  fd,
                                  fullname);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_linux_symlink */

static void
chimera_linux_readlink(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    int fd, rc;

    fd = request->readlink.handle->vfs_private;

    rc = readlinkat(fd, "", request->readlink.r_target,
                    request->readlink.target_maxlength);

    if (rc < 0) {
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }

    request->readlink.r_target_length = rc;

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_linux_readlink */

static void
chimera_linux_rename(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_linux_thread *thread = private_data;
    int                          old_fd, new_fd, rc;
    char                        *scratch = (char *) request->plugin_data;

    TERM_STR(fullname, request->rename.name, request->rename.namelen, scratch);
    TERM_STR(full_newname, request->rename.new_name, request->rename.new_namelen, scratch);

    old_fd = linux_open_by_handle(&thread->mount_table,
                                  request->fh,
                                  request->fh_len,
                                  O_PATH | O_RDONLY | O_NOFOLLOW);

    if (old_fd < 0) {
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }

    new_fd = linux_open_by_handle(&thread->mount_table,
                                  request->rename.new_fh,
                                  request->rename.new_fhlen,
                                  O_PATH | O_RDONLY | O_NOFOLLOW);

    if (new_fd < 0) {
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        close(old_fd);
        return;
    }

    rc = renameat(old_fd, fullname, new_fd, full_newname);

    if (rc < 0) {
        request->status = chimera_linux_errno_to_status(errno);
    } else {
        request->status = CHIMERA_VFS_OK;
    }

    close(old_fd);
    close(new_fd);

    request->complete(request);
} /* chimera_linux_rename */

static void
chimera_linux_link(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_linux_thread *thread = private_data;
    int                          fd, dir_fd, rc;
    char                        *scratch = (char *) request->plugin_data;

    TERM_STR(fullname, request->link.name, request->link.namelen, scratch);

    fd = linux_open_by_handle(&thread->mount_table,
                              request->fh,
                              request->fh_len,
                              O_PATH | O_RDONLY | O_NOFOLLOW);

    if (fd < 0) {
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }

    dir_fd = linux_open_by_handle(&thread->mount_table,
                                  request->link.dir_fh,
                                  request->link.dir_fhlen,
                                  O_PATH | O_RDONLY | O_NOFOLLOW);

    if (dir_fd < 0) {
        close(fd);
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }

    rc = linkat(fd, "", dir_fd, fullname, AT_EMPTY_PATH);

    if (rc < 0) {
        request->status = chimera_linux_errno_to_status(errno);
    } else {
        request->status = CHIMERA_VFS_OK;
    }

    close(fd);
    close(dir_fd);

    request->complete(request);

} /* chimera_linux_link */
static void
chimera_linux_dispatch(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    switch (request->opcode) {
        case CHIMERA_VFS_OP_MOUNT:
            chimera_linux_mount(request, private_data);
            break;
        case CHIMERA_VFS_OP_UMOUNT:
            chimera_linux_umount(request, private_data);
            break;
        case CHIMERA_VFS_OP_LOOKUP:
            chimera_linux_lookup(request, private_data);
            break;
        case CHIMERA_VFS_OP_GETATTR:
            chimera_linux_getattr(request, private_data);
            break;
        case CHIMERA_VFS_OP_OPEN:
            chimera_linux_open(request, private_data);
            break;
        case CHIMERA_VFS_OP_OPEN_AT:
            chimera_linux_open_at(request, private_data);
            break;
        case CHIMERA_VFS_OP_CLOSE:
            chimera_linux_close(request, private_data);
            break;
        case CHIMERA_VFS_OP_MKDIR:
            chimera_linux_mkdir(request, private_data);
            break;
        case CHIMERA_VFS_OP_MKNOD:
            chimera_linux_mknod(request, private_data);
            break;
        case CHIMERA_VFS_OP_READDIR:
            chimera_linux_readdir(request, private_data);
            break;
        case CHIMERA_VFS_OP_REMOVE:
            chimera_linux_remove(request, private_data);
            break;
        case CHIMERA_VFS_OP_READ:
            chimera_linux_read(request, private_data);
            break;
        case CHIMERA_VFS_OP_WRITE:
            chimera_linux_write(request, private_data);
            break;
        case CHIMERA_VFS_OP_COMMIT:
            chimera_linux_commit(request, private_data);
            break;
        case CHIMERA_VFS_OP_SYMLINK:
            chimera_linux_symlink(request, private_data);
            break;
        case CHIMERA_VFS_OP_READLINK:
            chimera_linux_readlink(request, private_data);
            break;
        case CHIMERA_VFS_OP_RENAME:
            chimera_linux_rename(request, private_data);
            break;
        case CHIMERA_VFS_OP_LINK:
            chimera_linux_link(request, private_data);
            break;
        case CHIMERA_VFS_OP_SETATTR:
            chimera_linux_setattr(request, private_data);
            break;
        default:
            chimera_linux_error("linux_dispatch: unknown operation %d",
                                request->opcode);
            request->status = CHIMERA_VFS_ENOTSUP;
            request->complete(request);
            break;
    } /* switch */
} /* linux_dispatch */

SYMBOL_EXPORT struct chimera_vfs_module vfs_linux = {
    .name         = "linux",
    .fh_magic     = CHIMERA_VFS_FH_MAGIC_LINUX,
    .capabilities = CHIMERA_VFS_CAP_BLOCKING | CHIMERA_VFS_CAP_OPEN_PATH_REQUIRED | CHIMERA_VFS_CAP_OPEN_FILE_REQUIRED
    ,
    .init           = chimera_linux_init,
    .destroy        = chimera_linux_destroy,
    .thread_init    = chimera_linux_thread_init,
    .thread_destroy = chimera_linux_thread_destroy,
    .dispatch       = chimera_linux_dispatch,
};