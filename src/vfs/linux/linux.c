#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/statvfs.h>
#include <dirent.h>
#include <fcntl.h>
#include <errno.h>
#include "vfs/vfs_error.h"

#include "linux.h"
#include "common/logging.h"
#include "common/format.h"
#include "uthash/uthash.h"

#define chimera_linux_debug(...) chimera_debug("linux", \
                                               __FILE__, \
                                               __LINE__, \
                                               __VA_ARGS__)
#define chimera_linux_info(...)  chimera_info("linux", \
                                              __FILE__, \
                                              __LINE__, \
                                              __VA_ARGS__)
#define chimera_linux_error(...) chimera_error("linux", \
                                               __FILE__, \
                                               __LINE__, \
                                               __VA_ARGS__)
#define chimera_linux_fatal(...) chimera_fatal("linux", \
                                               __FILE__, \
                                               __LINE__, \
                                               __VA_ARGS__)
#define chimera_linux_abort(...) chimera_abort("linux", \
                                               __FILE__, \
                                               __LINE__, \
                                               __VA_ARGS__)

#define chimera_linux_fatal_if(cond, ...) \
        chimera_fatal_if(cond, "linux", __FILE__, __LINE__, __VA_ARGS__)

struct chimera_linux_mount {
    int                   mount_id;
    int                   mount_fd;
    struct UT_hash_handle hh;
};

struct chimera_linux_thread {
    struct chimera_linux_mount *mounts;
};

static inline enum chimera_vfs_error
chimera_linux_errno_to_status(int err)
{
    switch (err) {
        case 0:
            return CHIMERA_VFS_OK;
        case EPERM:
            return CHIMERA_VFS_EPERM;
        case ENOENT:
            return CHIMERA_VFS_ENOENT;
        case EIO:
            return CHIMERA_VFS_EIO;
        case ENXIO:
            return CHIMERA_VFS_ENXIO;
        case EACCES:
            return CHIMERA_VFS_EACCES;
        case EFAULT:
            return CHIMERA_VFS_EFAULT;
        case EEXIST:
            return CHIMERA_VFS_EEXIST;
        case EXDEV:
            return CHIMERA_VFS_EXDEV;
        case ENOTDIR:
            return CHIMERA_VFS_ENOTDIR;
        case EISDIR:
            return CHIMERA_VFS_EISDIR;
        case EINVAL:
            return CHIMERA_VFS_EINVAL;
        case EFBIG:
            return CHIMERA_VFS_EFBIG;
        case ENOSPC:
            return CHIMERA_VFS_ENOSPC;
        case EROFS:
            return CHIMERA_VFS_EROFS;
        case EMLINK:
            return CHIMERA_VFS_EMLINK;
        case ENAMETOOLONG:
            return CHIMERA_VFS_ENAMETOOLONG;
        case ENOTEMPTY:
            return CHIMERA_VFS_ENOTEMPTY;
        case EOVERFLOW:
            return CHIMERA_VFS_EOVERFLOW;
        case EBADF:
            return CHIMERA_VFS_EBADF;
        case ENOTSUP:
            return CHIMERA_VFS_ENOTSUP;
        case EDQUOT:
            return CHIMERA_VFS_EDQUOT;
        case ESTALE:
            return CHIMERA_VFS_ESTALE;
        default:
            return CHIMERA_VFS_UNSET;
    } /* switch */
} /* chimera_linux_errno_to_status */

static inline void
chimera_linux_stat_to_attr(
    struct chimera_vfs_attrs *attr,
    struct stat              *st)
{
    attr->va_dev   = st->st_dev;
    attr->va_ino   = st->st_ino;
    attr->va_mode  = st->st_mode;
    attr->va_nlink = st->st_nlink;
    attr->va_uid   = st->st_uid;
    attr->va_gid   = st->st_gid;
    attr->va_rdev  = st->st_rdev;
    attr->va_size  = st->st_size;
    attr->va_atime = st->st_atim;
    attr->va_mtime = st->st_mtim;
    attr->va_ctime = st->st_ctim;
} /* linux_stat_to_chimera_attr */

static inline void
chimera_linux_statvfs_to_attr(
    struct chimera_vfs_attrs *attr,
    struct statvfs           *stvfs)
{
    attr->va_space_total = stvfs->f_blocks * stvfs->f_bsize;
    attr->va_space_free  = stvfs->f_bavail * stvfs->f_bsize;
    attr->va_space_avail = attr->va_space_free;
    attr->va_space_used  = attr->va_space_total - attr->va_space_free;
} /* linux_statvfs_to_chimera_attr */

static int
open_mount_path_by_id(int mount_id)
{
    int     mi_mount_id, found;
    char    mount_path[PATH_MAX];
    char   *linep;
    FILE   *fp;
    size_t  lsize;
    ssize_t nread;

    fp = fopen("/proc/self/mountinfo", "r");

    if (fp == NULL) {
        return EIO;
    }

    found = 0;

    linep = NULL;

    while (!found) {

        nread = getline(&linep, &lsize, fp);

        if (nread == -1) {
            break;
        }

        nread = sscanf(linep, "%d %*d %*s %*s %s", &mi_mount_id, mount_path);

        if (nread != 2) {
            fprintf(stderr, "Bad sscanf()\n");
            exit(EXIT_FAILURE);
        }

        if (mi_mount_id == mount_id) {
            found = 1;
        }
    }

    free(linep);

    fclose(fp);

    if (!found) {
        errno = ENOENT;
        return -1;
    }

    return open(mount_path, O_RDONLY);
} /* open_mount_path_by_id */

static inline int
linux_get_fh(
    int          fd,
    const char  *path,
    int          pathlen,
    unsigned int flags,
    void        *fh,
    uint32_t    *fh_len)
{
    uint8_t              buf[256];
    static const uint8_t fh_magic = CHIMERA_VFS_FH_MAGIC_LINUX;
    struct file_handle  *handle   = (struct file_handle *) buf;
    int                  rc;
    int                  mount_id;
    char                 fullpath[PATH_MAX];

    memcpy(fullpath, path, pathlen);
    fullpath[pathlen] = '\0';

    handle->handle_bytes = 128;

    rc = name_to_handle_at(
        fd,
        fullpath,
        handle,
        &mount_id,
        flags);

    if (rc < 0) {
        return -1;
    }

    memcpy(fh, &fh_magic, 1);
    memcpy(fh + 1, &mount_id, 4);
    memcpy(fh + 5, handle->f_handle, handle->handle_bytes);

    *fh_len = 5 + handle->handle_bytes;

    return 0;
} /* linux_get_fh */

static inline int
linux_open_by_handle(
    struct chimera_linux_thread *thread,
    const void                  *fh,
    uint32_t                     fh_len,
    unsigned int                 flags)
{
    struct chimera_linux_mount *mount;
    uint8_t                     buf[256];
    struct file_handle         *handle   = (struct file_handle *) buf;
    int                         mount_id = *(int *) (fh + 1);

    HASH_FIND_INT(thread->mounts, &mount_id, mount);

    if (!mount) {
        chimera_linux_error("linux_open_by_handle: mount not found for id %d",
                            mount_id);

        mount           = calloc(1, sizeof(*mount));
        mount->mount_id = mount_id;
        mount->mount_fd = open_mount_path_by_id(mount_id);

        if (mount->mount_fd < 0) {
            chimera_linux_error(
                "linux_open_by_handle: open_mount_path_by_id(%d) failed: %s",
                mount_id,
                strerror(errno));
            free(mount);
            return -1;
        }

        HASH_ADD_INT(thread->mounts, mount_id, mount);
    }

    handle->handle_type  = 1;
    handle->handle_bytes = fh_len - 5;
    memcpy(handle->f_handle, fh + 5, handle->handle_bytes);

    return open_by_handle_at(mount->mount_fd, handle, flags);
} /* linux_open_by_handle */

static void *
chimera_linux_init(void)
{
    return 0;
} /* linux_init */

static void
chimera_linux_destroy(void *private_data)
{

} /* linux_destroy */

static void *
chimera_linux_thread_init(
    struct evpl *evpl,
    void        *private_data)
{
    struct chimera_linux_thread *thread =
        (struct chimera_linux_thread *) calloc(1, sizeof(*thread));

    return thread;
} /* linux_thread_init */

static void
chimera_linux_thread_destroy(void *private_data)
{
    struct chimera_linux_thread *thread = private_data;
    struct chimera_linux_mount  *mount;

    while (thread->mounts) {
        mount = thread->mounts;
        HASH_DEL(thread->mounts, mount);
        close(mount->mount_fd);
        free(mount);
    }

    free(thread);
} /* linux_thread_destroy */

static void
chimera_linux_getattr(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_linux_thread *thread = private_data;
    int                          fd, rc;
    struct stat                  st;
    struct statvfs               stvfs;

    fd = linux_open_by_handle(thread,
                              request->getattr.fh,
                              request->getattr.fh_len,
                              O_PATH | O_RDONLY);

    if (fd < 0) {
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }

    if (request->getattr.attr_mask & CHIMERA_VFS_ATTR_MASK_STAT) {
        rc = fstat(fd, &st);

        if (rc) {
            close(fd);
            request->status = chimera_linux_errno_to_status(errno);
            request->complete(request);
            return;
        }

        chimera_linux_stat_to_attr(&request->getattr.r_attr, &st);
    }

    if (request->getattr.attr_mask & CHIMERA_VFS_ATTR_MASK_STATFS) {
        rc = fstatvfs(fd, &stvfs);

        if (rc) {
            close(fd);
            request->status = chimera_linux_errno_to_status(errno);
            request->complete(request);
            return;
        }

        chimera_linux_statvfs_to_attr(&request->getattr.r_attr, &stvfs);
    }

    close(fd);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* linux_getattr */

static void
chimera_linux_lookup_path(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    int mount_fd, rc;

    chimera_linux_debug("linux_lookup_path: path=%s", request->lookup_path.path)
    ;

    mount_fd = open(request->lookup_path.path, O_DIRECTORY | O_RDONLY);

    if (mount_fd < 0) {
        chimera_linux_error("linux_lookup_path: open(%s) failed: %s",
                            request->lookup_path.path,
                            strerror(errno));
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }

    rc = linux_get_fh(mount_fd,
                      request->lookup_path.path,
                      request->lookup_path.pathlen,
                      0,
                      request->lookup_path.r_fh,
                      &request->lookup_path.r_fh_len);

    if (rc < 0) {
        chimera_linux_error("linux_lookup_path: get_fh(%s) failed: %s",
                            request->lookup_path.path,
                            strerror(errno));
        request->status = chimera_linux_errno_to_status(errno);
    } else {
        request->status = CHIMERA_VFS_OK;
    }

    close(mount_fd);

    request->complete(request);
} /* chimera_linux_lookup_path */

static void
chimera_linux_lookup(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_linux_thread *thread = private_data;
    int                          fd, rc;

    fd = linux_open_by_handle(thread,
                              request->lookup.fh,
                              request->lookup.fh_len,
                              O_PATH | O_RDONLY);
    if (fd < 0) {
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }

    rc = linux_get_fh(fd,
                      request->lookup.component,
                      request->lookup.component_len,
                      0,
                      request->lookup.r_fh,
                      &request->lookup.r_fh_len);

    close(fd);

    if (rc < 0) {
        request->status = chimera_linux_errno_to_status(errno);
    } else {
        request->status = CHIMERA_VFS_OK;
    }
    request->complete(request);
} /* linux_lookup */

static void
chimera_linux_readdir(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_linux_thread *thread = private_data;
    int                          fd, child_fd, rc;
    struct stat                  st;
    DIR                         *dir;
    struct dirent               *dirent;
    struct chimera_vfs_attrs     vattr;
    char                         fhstr[80];

    format_hex(fhstr, sizeof(fhstr), request->readdir.fh, request->readdir.
               fh_len);

    chimera_linux_debug("linux_readdir: fh=%s", fhstr);

    fd = linux_open_by_handle(thread,
                              request->readdir.fh,
                              request->readdir.fh_len,
                              O_DIRECTORY | O_RDONLY);

    if (fd < 0) {
        chimera_linux_error("linux_readdir: open_by_handle() failed: %s",
                            strerror(errno));

        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }

    dir = fdopendir(fd);

    if (!dir) {
        chimera_linux_error("linux_readdir: fdopendir() failed: %s",
                            strerror(errno));

        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }

    if (request->readdir.cookie) {
        seekdir(dir, request->readdir.cookie + 1);
    }

    while ((dirent = readdir(dir))) {

        if (strcmp(dirent->d_name, ".") == 0 ||
            strcmp(dirent->d_name, "..") == 0) {
            continue;
        }

        child_fd = openat(dirfd(dir), dirent->d_name, O_RDONLY | O_PATH |
                          O_NOFOLLOW);

        if (child_fd < 0) {
            continue;
        }

        rc = fstat(child_fd, &st);

        if (rc) {
            close(child_fd);
            continue;
        }

        rc = linux_get_fh(child_fd,
                          "",
                          0,
                          AT_EMPTY_PATH,
                          vattr.va_fh,
                          &vattr.va_fh_len);

        if (rc) {
            vattr.va_fh_len = 0;
        }

        close(child_fd);

        chimera_linux_stat_to_attr(&vattr, &st);

        rc = request->readdir.callback(
            dirent->d_off,
            dirent->d_name,
            strlen(dirent->d_name),
            &vattr,
            request->proto_private_data);

        if (rc) {
            break;
        }

    }

    closedir(dir);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* linux_readdir */

static void
chimera_linux_open_at(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_linux_thread *thread = private_data;
    int                          parent_fd;
    int                          flags;
    int                          fd;
    int                          rc;

    parent_fd = linux_open_by_handle(thread,
                                     request->open_at.parent_fh,
                                     request->open_at.parent_fh_len,
                                     O_RDONLY | O_DIRECTORY);

    if (parent_fd < 0) {
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }
    flags = 0;

    if (request->open_at.flags & CHIMERA_VFS_OPEN_RDONLY) {
        flags |= O_RDONLY;
    }

    if (request->open_at.flags & CHIMERA_VFS_OPEN_WRONLY) {
        flags |= O_WRONLY;
    }

    if (request->open_at.flags & CHIMERA_VFS_OPEN_RDWR) {
        flags |= O_RDWR;
    }

    if (request->open_at.flags & CHIMERA_VFS_OPEN_CREATE) {
        flags |= O_CREAT;
    }

    fd = openat(parent_fd,
                request->open_at.name,
                flags,
                S_IRWXU /*XXX*/);

    if (fd < 0) {
        close(parent_fd);
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }

    rc = linux_get_fh(fd, "", 0, AT_EMPTY_PATH,
                      request->open_at.fh,
                      &request->open_at.fh_len);

    if (rc) {
        close(parent_fd);
        close(fd);
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }

    request->open_at.handle.vfs_private = fd;

    close(parent_fd);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* linux_open_at */

static void
chimera_linux_close(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    int fd = request->close.handle->vfs_private;

    close(fd);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_linux_close */

static void
chimera_linux_dispatch(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    chimera_linux_debug("linux_dispatch: request=%p", request);

    switch (request->opcode) {
        case CHIMERA_VFS_OP_LOOKUP_PATH:
            chimera_linux_lookup_path(request, private_data);
            break;
        case CHIMERA_VFS_OP_LOOKUP:
            chimera_linux_lookup(request, private_data);
            break;
        case CHIMERA_VFS_OP_GETATTR:
            chimera_linux_getattr(request, private_data);
            break;
        case CHIMERA_VFS_OP_OPEN_AT:
            chimera_linux_open_at(request, private_data);
            break;
        case CHIMERA_VFS_OP_CLOSE:
            chimera_linux_close(request, private_data);
            break;
        case CHIMERA_VFS_OP_READDIR:
            chimera_linux_readdir(request, private_data);
            break;
        default:
            chimera_linux_error("linux_dispatch: unknown operation %d",
                                request->opcode);
            request->status = CHIMERA_VFS_ENOTSUP;
            request->complete(request);
            break;
    } /* switch */
} /* linux_dispatch */

struct chimera_vfs_module vfs_linux = {
    .name           = "linux",
    .fh_magic       = CHIMERA_VFS_FH_MAGIC_LINUX,
    .blocking       = 1,
    .init           = chimera_linux_init,
    .destroy        = chimera_linux_destroy,
    .thread_init    = chimera_linux_thread_init,
    .thread_destroy = chimera_linux_thread_destroy,
    .dispatch       = chimera_linux_dispatch,
};