#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/statvfs.h>
#include <sys/uio.h>
#include <dirent.h>
#include <fcntl.h>
#include <errno.h>
#include <liburing.h>

#include "uthash/utlist.h"

#include "vfs/vfs_error.h"

#include "core/evpl.h"
#include "core/event.h"
#include "core/deferral.h"

#include "io_uring.h"
#include "common/logging.h"
#include "common/format.h"
#include "common/misc.h"
#include "uthash/uthash.h"

static void
chimera_io_uring_dispatch(
    struct chimera_vfs_request *request,
    void                       *private_data);

#ifndef container_of
#define container_of(ptr, type, member) ({            \
        typeof(((type *) 0)->member) * __mptr = (ptr); \
        (type *) ((char *) __mptr - offsetof(type, member)); })
#endif // ifndef container_of

#define chimera_io_uring_debug(...)     chimera_debug("io_uring", \
                                                      __FILE__, \
                                                      __LINE__, \
                                                      __VA_ARGS__)
#define chimera_io_uring_info(...)      chimera_info("io_uring", \
                                                     __FILE__, \
                                                     __LINE__, \
                                                     __VA_ARGS__)
#define chimera_io_uring_error(...)     chimera_error("io_uring", \
                                                      __FILE__, \
                                                      __LINE__, \
                                                      __VA_ARGS__)
#define chimera_io_uring_fatal(...)     chimera_fatal("io_uring", \
                                                      __FILE__, \
                                                      __LINE__, \
                                                      __VA_ARGS__)
#define chimera_io_uring_abort(...)     chimera_abort("io_uring", \
                                                      __FILE__, \
                                                      __LINE__, \
                                                      __VA_ARGS__)

#define chimera_io_uring_fatal_if(cond, ...) \
        chimera_fatal_if(cond, "io_uring", __FILE__, __LINE__, __VA_ARGS__)

#define chimera_io_uring_abort_if(cond, ...) \
        chimera_abort_if(cond, "io_uring", __FILE__, __LINE__, __VA_ARGS__)

struct chimera_io_uring_mount {
    int                   mount_id;
    int                   mount_fd;
    struct UT_hash_handle hh;
};

struct chimera_io_uring_shared {
    struct io_uring ring;
};

struct chimera_io_uring_thread {
    struct evpl                   *evpl;
    int                            eventfd;
    struct evpl_event              event;
    struct evpl_deferral           deferral;
    struct io_uring                ring;
    uint64_t                       inflight;
    uint64_t                       max_inflight;
    struct chimera_vfs_request    *pending_requests;
    struct chimera_io_uring_mount *mounts;
};

#define TERM_STR(name, str, len, scratch) \
        char *(name) = (scratch); \
        (scratch)   += (len) + 1; \
        memcpy((name), (str), (len)); \
        (name)[(len)] = '\0';


static inline enum chimera_vfs_error
chimera_io_uring_errno_to_status(int err)
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
        case EMFILE:
            return CHIMERA_VFS_EMFILE;
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
        case ELOOP:
            return CHIMERA_VFS_ELOOP;
        default:
            chimera_io_uring_error("io_uring_errno_to_status: unknown error: %d", err)
            ;
            return CHIMERA_VFS_UNSET;
    } /* switch */
} /* chimera_io_uring_errno_to_status */

static inline void
chimera_io_uring_stat_to_attr(
    struct chimera_vfs_attrs *attr,
    struct stat              *st)
{

    attr->va_mask |= CHIMERA_VFS_ATTR_MASK_STAT;

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
} /* io_uring_stat_to_chimera_attr */

static inline void
chimera_io_uring_statx_to_attr(
    struct chimera_vfs_attrs *attr,
    struct statx             *stx)
{

    attr->va_mask |= CHIMERA_VFS_ATTR_MASK_STAT;

    attr->va_dev           = ((uint64_t) stx->stx_dev_major << 32) | stx->stx_dev_minor;
    attr->va_ino           = stx->stx_ino;
    attr->va_mode          = stx->stx_mode;
    attr->va_nlink         = stx->stx_nlink;
    attr->va_uid           = stx->stx_uid;
    attr->va_gid           = stx->stx_gid;
    attr->va_rdev          = ((uint64_t) stx->stx_rdev_major << 32) | stx->stx_rdev_minor;
    attr->va_size          = stx->stx_size;
    attr->va_atime.tv_sec  = stx->stx_atime.tv_sec;
    attr->va_atime.tv_nsec = stx->stx_atime.tv_nsec;
    attr->va_mtime.tv_sec  = stx->stx_mtime.tv_sec;
    attr->va_mtime.tv_nsec = stx->stx_mtime.tv_nsec;
    attr->va_ctime.tv_sec  = stx->stx_ctime.tv_sec;
    attr->va_ctime.tv_nsec = stx->stx_ctime.tv_nsec;
} /* io_uring_stat_to_chimera_attr */

static inline void
chimera_io_uring_statvfs_to_attr(
    struct chimera_vfs_attrs *attr,
    struct statvfs           *stvfs)
{

    attr->va_mask |= CHIMERA_VFS_ATTR_MASK_STATFS;

    attr->va_space_total = stvfs->f_blocks * stvfs->f_bsize;
    attr->va_space_free  = stvfs->f_bavail * stvfs->f_bsize;
    attr->va_space_avail = attr->va_space_free;
    attr->va_space_used  = attr->va_space_total - attr->va_space_free;

    attr->va_files_used  = stvfs->f_files;
    attr->va_files_free  = stvfs->f_ffree;
    attr->va_files_total = attr->va_files_used + attr->va_files_free;
} /* io_uring_statvfs_to_chimera_attr */

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
io_uring_get_fh(
    int          fd,
    const char  *path,
    unsigned int isdir,
    void        *fh,
    uint32_t    *fh_len)
{
    uint8_t              buf[sizeof(struct file_handle) + MAX_HANDLE_SZ];
    static const uint8_t fh_magic = CHIMERA_VFS_FH_MAGIC_IO_URING;
    struct file_handle  *handle   = (struct file_handle *) buf;
    int                  rc;
    int                  mount_id;

    handle->handle_bytes = MAX_HANDLE_SZ;

    rc = name_to_handle_at(
        fd,
        path,
        handle,
        &mount_id,
        *path ? 0 : AT_EMPTY_PATH);

    if (rc < 0) {
        return -1;
    }

    chimera_io_uring_abort_if(14 + handle->handle_bytes > CHIMERA_VFS_FH_SIZE,
                              "Returned handle exceeds CHIMERA_VFS_FH_SIZE");

    memcpy(fh, &fh_magic, 1);
    memcpy(fh + 1, &mount_id, 4);
    memcpy(fh + 5, &isdir, 1);
    memcpy(fh + 6, handle, 8 + handle->handle_bytes);

    *fh_len = 14 + handle->handle_bytes;

    return 0;
} /* io_uring_get_fh */

static inline int
io_uring_fh_is_dir(const void *fh)
{
    return *(uint8_t *) (fh + 5);
} /* io_uring_fh_is_dir */

static inline int
io_uring_open_by_handle(
    struct chimera_io_uring_thread *thread,
    const void                     *fh,
    uint32_t                        fh_len,
    unsigned int                    flags)
{
    struct chimera_io_uring_mount *mount;
    struct file_handle            *handle   = (struct file_handle *) (fh + 6);
    int                            mount_id = *(int *) (fh + 1);


    HASH_FIND_INT(thread->mounts, &mount_id, mount);

    if (!mount) {
        mount           = calloc(1, sizeof(*mount));
        mount->mount_id = mount_id;
        mount->mount_fd = open_mount_path_by_id(mount_id);

        if (mount->mount_fd < 0) {
            free(mount);
            return -1;
        }

        HASH_ADD_INT(thread->mounts, mount_id, mount);
    }

    return open_by_handle_at(mount->mount_fd, handle, flags);
} /* io_uring_open_by_handle */

static inline void
chimera_io_uring_map_attrs(
    uint64_t                  attrmask,
    struct chimera_vfs_attrs *attr,
    int                       fd,
    const void               *fh,
    int                       fhlen)
{
    int            rc;
    struct stat    st;
    struct statvfs stvfs;

    if (attrmask & (CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_FH)) {

        rc = fstat(fd, &st);

        if (rc) {
            return;
        }

        chimera_io_uring_stat_to_attr(attr, &st);
    }

    if (attrmask & CHIMERA_VFS_ATTR_FH) {
        if (fh) {
            attr->va_mask |= CHIMERA_VFS_ATTR_FH;
            memcpy(attr->va_fh, fh, fhlen);
            attr->va_fh_len = fhlen;
        } else {
            rc = io_uring_get_fh(fd, "", S_ISDIR(st.st_mode),
                                 attr->va_fh,
                                 &attr->va_fh_len);

            if (rc  == 0) {
                attr->va_mask |= CHIMERA_VFS_ATTR_FH;
            }
        }
    }

    if (attrmask & CHIMERA_VFS_ATTR_MASK_STATFS) {
        rc = fstatvfs(fd, &stvfs);

        if (rc == 0) {
            chimera_io_uring_statvfs_to_attr(attr, &stvfs);
        }
    }

} /* chimera_io_uring_map_attrs */

static void *
chimera_io_uring_init(void)
{
    struct chimera_io_uring_shared *shared;
    struct io_uring_params          params = { 0 };
    int                             rc;

    shared = calloc(1, sizeof(*shared));

    // Initialize the shared ring with default parameters
    rc = io_uring_queue_init_params(256, &shared->ring, &params);

    chimera_io_uring_abort_if(rc < 0,
                              "Failed to create shared io_uring queue: %s", strerror(-rc));

    return shared;
} /* io_uring_init */

static void
chimera_io_uring_destroy(void *private_data)
{
    struct chimera_io_uring_shared *shared = private_data;

    io_uring_queue_exit(&shared->ring);
    free(shared);
} /* io_uring_destroy */

static inline struct io_uring_sqe *
chimera_io_uring_get_sqe(
    struct chimera_io_uring_thread *thread,
    struct chimera_vfs_request     *request,
    int                             slot,
    int                             linked)
{
    struct chimera_vfs_request_handle *handle;
    struct io_uring_sqe               *sge;

    sge = io_uring_get_sqe(&thread->ring);

    chimera_io_uring_abort_if(!sge, "io_uring_get_sqe");

    thread->inflight++;

    if (linked) {
        io_uring_sqe_set_flags(sge, IOSQE_IO_HARDLINK);
    }

    handle = &request->handle[slot];

    request->handle[slot].slot = slot;

    request->token_count++;

    sge->user_data = (uint64_t) handle;

    return sge;
} /* chimera_io_uring_get_sqe */

static void
chimera_io_uring_complete(
    struct evpl       *evpl,
    struct evpl_event *event)
{
    struct chimera_io_uring_thread    *thread;
    uint64_t                           value;
    struct io_uring_cqe               *cqe;
    int                                rc, i, parent_fd;
    struct chimera_vfs_request        *request;
    struct chimera_vfs_request_handle *handle;
    struct statx                      *dir_stx, *stx;
    const char                        *name;
    struct io_uring_sqe               *sqe;

    thread = container_of(event, struct chimera_io_uring_thread, event);

    rc = read(thread->eventfd, &value, sizeof(value));

    if (rc != sizeof(value)) {
        evpl_event_mark_unreadable(&thread->event);
        return;
    }

    while (io_uring_peek_cqe(&thread->ring, &cqe) == 0) {

        thread->inflight--;

        handle = (struct chimera_vfs_request_handle *) cqe->user_data;

        request = container_of(handle, struct chimera_vfs_request, handle[handle->slot]);

        switch (request->opcode) {
            case CHIMERA_VFS_OP_LOOKUP:
                if (cqe->res >= 0) {
                    request->status = CHIMERA_VFS_OK;

                    stx = (struct statx *) request->plugin_data;

                    name = (char *) (stx + 1);
                    chimera_io_uring_statx_to_attr(&request->lookup.r_attr, stx);

                    parent_fd = request->lookup.handle->vfs_private;

                    rc = io_uring_get_fh(parent_fd, name, S_ISDIR(stx->stx_mode),
                                         request->lookup.r_attr.va_fh,
                                         &request->lookup.r_attr.va_fh_len);

                    if (rc  == 0) {
                        request->lookup.r_attr.va_mask |= CHIMERA_VFS_ATTR_FH;
                    } else {
                        request->status = chimera_io_uring_errno_to_status(errno);
                    }
                } else {
                    request->status = chimera_io_uring_errno_to_status(-cqe->res);
                }
                break;
            case CHIMERA_VFS_OP_GETATTR:
                if (cqe->res == 0) {
                    request->status = CHIMERA_VFS_OK;
                    chimera_io_uring_statx_to_attr(&request->getattr.r_attr, (struct statx *) request->plugin_data);
                }
                break;
            case CHIMERA_VFS_OP_OPEN_AT:
                if (handle->slot == 0) {
                    if (cqe->res >= 0) {
                        request->status                = CHIMERA_VFS_OK;
                        request->open_at.r_vfs_private = cqe->res;

                        dir_stx = (struct statx *) request->plugin_data;
                        stx     = (struct statx *) (dir_stx + 1);
                        name    = (char *) (stx + 1);

                        parent_fd = request->open_at.handle->vfs_private;

                        sqe = chimera_io_uring_get_sqe(thread, request, 1, 0);

                        io_uring_prep_statx(sqe, parent_fd, name, 0, AT_STATX_SYNC_AS_STAT, stx);

                        sqe = chimera_io_uring_get_sqe(thread, request, 2, 0);

                        io_uring_prep_statx(sqe, parent_fd, "", AT_EMPTY_PATH, AT_STATX_SYNC_AS_STAT, dir_stx);

                        evpl_defer(thread->evpl, &thread->deferral);

                    } else {
                        request->status = chimera_io_uring_errno_to_status(-cqe->res);
                    }
                } else if (handle->slot == 1) {
                    if (cqe->res == 0) {
                        dir_stx = (struct statx *) request->plugin_data;
                        stx     = (struct statx *) (dir_stx + 1);
                        name    = (char *) (stx + 1);

                        chimera_io_uring_statx_to_attr(&request->open_at.r_attr, stx);

                        parent_fd = request->open_at.handle->vfs_private;

                        rc = io_uring_get_fh(parent_fd, name, S_ISDIR(stx->stx_mode),
                                             request->open_at.r_attr.va_fh,
                                             &request->open_at.r_attr.va_fh_len);

                        if (rc == 0) {
                            request->open_at.r_attr.va_mask |= CHIMERA_VFS_ATTR_FH;
                        }
                    }
                } else if (handle->slot == 2) {
                    if (cqe->res == 0) {
                        dir_stx = (struct statx *) request->plugin_data;
                        chimera_io_uring_statx_to_attr(&request->open_at.r_dir_attr, dir_stx);
                    }
                }
                break;
            case CHIMERA_VFS_OP_REMOVE:
                if (cqe->res == 0) {
                    request->status = CHIMERA_VFS_OK;
                } else if (cqe->res == -EISDIR) {
                    parent_fd = request->remove.handle->vfs_private;
                    name      = request->plugin_data;
                    sqe       = chimera_io_uring_get_sqe(thread, request, 0, 0);
                    io_uring_prep_unlinkat(sqe, parent_fd, name, AT_REMOVEDIR);
                    evpl_defer(thread->evpl, &thread->deferral);
                } else {
                    request->status = chimera_io_uring_errno_to_status(-cqe->res);
                }
                break;
            case CHIMERA_VFS_OP_MKDIR:
                if (handle->slot == 0) {
                    if (cqe->res == 0) {
                        request->status = CHIMERA_VFS_OK;
                    } else {
                        request->status = chimera_io_uring_errno_to_status(-cqe->res);
                    }
                } else if (handle->slot == 1) {
                    if (cqe->res == 0) {
                        dir_stx = (struct statx *) request->plugin_data;
                        stx     = (struct statx *) (dir_stx + 1);
                        name    = (char *) (stx + 1);

                        chimera_io_uring_statx_to_attr(&request->mkdir.r_attr, dir_stx);

                        name = (char *) (stx + 1);
                        chimera_io_uring_statx_to_attr(&request->mkdir.r_attr, stx);

                        parent_fd = request->mkdir.handle->vfs_private;

                        rc = io_uring_get_fh(parent_fd, name, S_ISDIR(stx->stx_mode),
                                             request->mkdir.r_attr.va_fh,
                                             &request->mkdir.r_attr.va_fh_len);

                        if (rc == 0) {
                            request->mkdir.r_attr.va_mask |= CHIMERA_VFS_ATTR_FH;
                        }
                    }
                } else if (handle->slot == 2) {
                    if (cqe->res == 0) {
                        dir_stx = (struct statx *) request->plugin_data;
                        chimera_io_uring_statx_to_attr(&request->mkdir.r_dir_attr, dir_stx);
                    }
                }
                break;
            case CHIMERA_VFS_OP_READ:
                if (handle->slot == 0) {
                    if (cqe->res >= 0) {
                        request->status        = CHIMERA_VFS_OK;
                        request->read.r_length = cqe->res;
                        request->read.r_eof    = (cqe->res < request->read.length);
                    } else {
                        request->status = chimera_io_uring_errno_to_status(-cqe->res);

                        for (i = 0; i < request->read.r_niov; i++) {
                            evpl_iovec_release(&request->read.iov[i]);
                        }
                    }
                } else {
                    if (cqe->res == 0) {
                        chimera_io_uring_statx_to_attr(&request->read.r_attr, (struct statx *) request->plugin_data);
                    }
                }
                break;
            case CHIMERA_VFS_OP_WRITE:
                if (handle->slot == 0) {
                    if (cqe->res >= 0) {
                        request->status         = CHIMERA_VFS_OK;
                        request->write.r_length = cqe->res;
                    } else {
                        request->status = chimera_io_uring_errno_to_status(-cqe->res);
                    }
                } else {
                    if (cqe->res == 0) {
                        chimera_io_uring_statx_to_attr(&request->write.r_attr, (struct statx *) request->plugin_data);
                    }
                }
                break;
            default:
                if (cqe->res) {
                    request->status = chimera_io_uring_errno_to_status(-cqe->res);
                } else {
                    request->status = CHIMERA_VFS_OK;
                }
                break;
        } /* switch */

        --request->token_count;

        if (request->token_count == 0) {
            request->complete(request);
        }

        io_uring_cqe_seen(&thread->ring, cqe);

        while (thread->pending_requests && thread->inflight + CHIMERA_VFS_REQUEST_MAX_HANDLES < thread->max_inflight) {
            request = thread->pending_requests;
            DL_DELETE(thread->pending_requests, request);
            chimera_io_uring_dispatch(request, thread);
        }
    } /* chimera_io_uring_complete */
} /* chimera_io_uring_complete */

static void
chimera_io_uring_flush(
    struct evpl *evpl,
    void        *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;
    int                             rc;

    rc = io_uring_submit(&thread->ring);

    chimera_io_uring_abort_if(rc < 0, "io_uring_submit");
} /* chimera_io_uring_flush */

static void *
chimera_io_uring_thread_init(
    struct evpl *evpl,
    void        *private_data)
{
    struct chimera_io_uring_shared *shared = private_data;
    struct chimera_io_uring_thread *thread;
    int                             rc;
    struct io_uring_params          params = { 0 };

    thread = calloc(1, sizeof(*thread));

    thread->evpl = evpl;

    // Set up single issuer mode
    params.flags  = IORING_SETUP_SINGLE_ISSUER;
    params.flags |= IORING_SETUP_COOP_TASKRUN;

    params.flags |= IORING_SETUP_ATTACH_WQ;
    params.wq_fd  = shared->ring.ring_fd;

    thread->max_inflight = 1024;

    // Initialize io_uring with params
    rc = io_uring_queue_init_params(thread->max_inflight, &thread->ring, &params);

    chimera_io_uring_abort_if(rc < 0, "Failed to create io_uring queue: %s", strerror(-rc));

    thread->eventfd = eventfd(0, EFD_NONBLOCK);

    chimera_io_uring_abort_if(thread->eventfd < 0, "Failed to create eventfd: %s", strerror(errno));

    rc = io_uring_register_eventfd(&thread->ring, thread->eventfd);

    chimera_io_uring_abort_if(rc < 0, "Failed to register eventfd");

    thread->event.fd            = thread->eventfd;
    thread->event.read_callback = chimera_io_uring_complete;

    evpl_add_event(evpl, &thread->event);

    evpl_event_read_interest(evpl, &thread->event);

    evpl_deferral_init(&thread->deferral,
                       chimera_io_uring_flush,
                       thread);

    return thread;
} /* io_uring_thread_init */

static void
chimera_io_uring_thread_destroy(void *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;
    struct chimera_io_uring_mount  *mount;

    while (thread->mounts) {
        mount = thread->mounts;
        HASH_DEL(thread->mounts, mount);
        close(mount->mount_fd);
        free(mount);
    }

    io_uring_queue_exit(&thread->ring);
    close(thread->eventfd);

    free(thread);
} /* io_uring_thread_destroy */

static void
chimera_io_uring_getattr(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;
    int                             fd, rc;
    struct io_uring_sqe            *sqe;
    struct statx                   *stx;
    struct statvfs                  stv;
    char                           *scratch = (char *) request->plugin_data;

    request->getattr.r_attr.va_mask = 0;

    fd = (int) request->getattr.handle->vfs_private;

    sqe = chimera_io_uring_get_sqe(thread, request, 0, 0);

    stx = (struct statx *) scratch;

    io_uring_prep_statx(sqe, fd, "", AT_EMPTY_PATH | AT_SYMLINK_NOFOLLOW, AT_STATX_SYNC_AS_STAT, stx);

    if (request->getattr.attr_mask & CHIMERA_VFS_ATTR_MASK_STATFS) {
        rc = fstatvfs(fd, &stv);

        if (rc == 0) {
            chimera_io_uring_statvfs_to_attr(&request->getattr.r_attr, &stv);
        }
    }

    evpl_defer(thread->evpl, &thread->deferral);
} /* io_uring_getattr */

static void
chimera_io_uring_setattr(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;
    const struct chimera_vfs_attrs *attr   = request->setattr.attr;
    int                             fd, rc;

    fd = io_uring_open_by_handle(thread,
                                 request->fh,
                                 request->fh_len,
                                 O_PATH);

    if (fd < 0) {
        request->status = chimera_io_uring_errno_to_status(errno);
        request->complete(request);
        return;
    }

    if (attr->va_mask & CHIMERA_VFS_ATTR_MODE) {
        rc = fchmodat(fd, "", attr->va_mode, AT_SYMLINK_NOFOLLOW | AT_EMPTY_PATH
                      );

        if (rc) {
            chimera_io_uring_error("io_uring_setattr: fchmod(%o) failed: %s",
                                   attr->va_mode,
                                   strerror(errno));

            close(fd);
            request->status = chimera_io_uring_errno_to_status(errno);
            request->complete(request);
            return;
        }
    }

    if ((attr->va_mask & (CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_GID)) ==
        (CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_GID)) {

        rc = fchownat(fd, "", attr->va_uid, attr->va_gid,
                      AT_SYMLINK_NOFOLLOW | AT_EMPTY_PATH);

        if (rc) {
            chimera_io_uring_error("io_uring_setattr: fchown(%u,%u) failed: %s",
                                   attr->va_uid,
                                   attr->va_gid,
                                   strerror(errno));

            close(fd);
            request->status = chimera_io_uring_errno_to_status(errno);
            request->complete(request);
            return;
        }

    } else if (attr->va_mask & CHIMERA_VFS_ATTR_UID) {

        rc = fchownat(fd, "", attr->va_uid, -1,
                      AT_SYMLINK_NOFOLLOW | AT_EMPTY_PATH);

        if (rc) {
            chimera_io_uring_error("io_uring_setattr: fchown(%u,-1) failed: %s",
                                   attr->va_uid,
                                   strerror(errno));

            close(fd);
            request->status = chimera_io_uring_errno_to_status(errno);
            request->complete(request);
            return;
        }

    } else if (attr->va_mask & CHIMERA_VFS_ATTR_GID) {

        rc = fchownat(fd, "", -1, attr->va_gid,
                      AT_SYMLINK_NOFOLLOW | AT_EMPTY_PATH);

        if (rc) {
            chimera_io_uring_error("io_uring_setattr: fchown(%u,-1) failed: %s",
                                   attr->va_gid,
                                   strerror(errno));

            close(fd);
            request->status = chimera_io_uring_errno_to_status(errno);
            request->complete(request);
            return;
        }
    }

    if (attr->va_mask & CHIMERA_VFS_ATTR_SIZE) {
        rc = ftruncate(fd, attr->va_size);

        if (rc) {
            chimera_io_uring_error("io_uring_setattr: ftruncate(%ld) failed: %s",
                                   attr->va_size,
                                   strerror(errno));

            close(fd);
            request->status = chimera_io_uring_errno_to_status(errno);
            request->complete(request);
            return;
        }
    }

    if (attr->va_mask & (CHIMERA_VFS_ATTR_ATIME | CHIMERA_VFS_ATTR_MTIME)) {
        struct timespec times[2];

        if (attr->va_mask & CHIMERA_VFS_ATTR_ATIME) {
            if (attr->va_atime.tv_nsec == CHIMERA_VFS_TIME_NOW) {
                times[0].tv_nsec = UTIME_NOW;
            } else {
                times[0] = attr->va_atime;
            }
        } else {
            times[0].tv_nsec = UTIME_OMIT;
        }

        if (attr->va_mask & CHIMERA_VFS_ATTR_MTIME) {
            if (attr->va_mtime.tv_nsec == CHIMERA_VFS_TIME_NOW) {
                times[1].tv_nsec = UTIME_NOW;
            } else {
                times[1] = attr->va_mtime;
            }
        } else {
            times[1].tv_nsec = UTIME_OMIT;
        }

        rc = utimensat(fd, "", times, AT_SYMLINK_NOFOLLOW | AT_EMPTY_PATH);

        if (rc) {
            chimera_io_uring_error("io_uring_setattr: utimensat() failed: %s",
                                   strerror(errno));

            close(fd);
            request->status = chimera_io_uring_errno_to_status(errno);
            request->complete(request);
            return;
        }
    }

    chimera_io_uring_map_attrs(request->setattr.attr_mask,
                               &request->setattr.r_attr,
                               fd,
                               request->fh,
                               request->fh_len);

    close(fd);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* io_uring_setattr */

static void
chimera_io_uring_lookup_path(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    int                       mount_fd, rc;
    struct chimera_vfs_attrs *r_attr;
    char                     *scratch = (char *) request->plugin_data;

    r_attr = &request->lookup_path.r_attr;

    TERM_STR(fullpath,
             request->lookup_path.path,
             request->lookup_path.pathlen,
             scratch);

    mount_fd = open(fullpath, O_DIRECTORY | O_RDONLY | O_NOFOLLOW);

    if (mount_fd < 0) {
        request->status = chimera_io_uring_errno_to_status(errno);
        request->complete(request);
        return;
    }

    rc = io_uring_get_fh(mount_fd,
                         fullpath,
                         1,
                         r_attr->va_fh,
                         &r_attr->va_fh_len);

    if (rc < 0) {
        request->status = chimera_io_uring_errno_to_status(errno);
    } else {
        request->status  = CHIMERA_VFS_OK;
        r_attr->va_mask |= CHIMERA_VFS_ATTR_FH;
    }

    close(mount_fd);

    request->complete(request);
} /* chimera_io_uring_lookup_path */

static void
chimera_io_uring_lookup(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;
    struct io_uring_sqe            *sqe;
    int                             parent_fd;
    char                           *scratch = (char *) request->plugin_data;
    struct statx                   *stx;

    parent_fd = (int) request->lookup.handle->vfs_private;

    stx = (struct statx *) scratch;

    scratch += sizeof(*stx);

    TERM_STR(fullname, request->lookup.component, request->lookup.component_len, scratch);

    sqe = chimera_io_uring_get_sqe(thread, request, 0, 0);

    io_uring_prep_statx(sqe, parent_fd, fullname, AT_SYMLINK_NOFOLLOW, AT_STATX_SYNC_AS_STAT, stx);

    evpl_defer(thread->evpl, &thread->deferral);
} /* io_uring_lookup */

static void
chimera_io_uring_readdir(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;
    int                             fd, child_fd, rc;
    DIR                            *dir;
    struct dirent                  *dirent;
    struct chimera_vfs_attrs        vattr;
    int                             eof = 1;

    fd = io_uring_open_by_handle(thread,
                                 request->fh,
                                 request->fh_len,
                                 O_DIRECTORY | O_RDONLY);

    if (fd < 0) {
        chimera_io_uring_error("io_uring_readdir: open_by_handle() failed: %s",
                               strerror(errno));

        request->status = chimera_io_uring_errno_to_status(errno);
        request->complete(request);
        return;
    }

    dir = fdopendir(fd);

    if (!dir) {
        chimera_io_uring_error("io_uring_readdir: fdopendir() failed: %s",
                               strerror(errno));
        close(fd);
        request->status = chimera_io_uring_errno_to_status(errno);
        request->complete(request);
        return;
    }

    if (request->readdir.cookie) {
        seekdir(dir, request->readdir.cookie);
    }

    while ((dirent = readdir(dir))) {

        vattr.va_mask = 0;

        if (request->readdir.attrmask & (CHIMERA_VFS_ATTR_FH |
                                         CHIMERA_VFS_ATTR_MASK_STAT)) {

            child_fd = openat(dirfd(dir), dirent->d_name, O_RDONLY | O_PATH |
                              O_NOFOLLOW);

            if (child_fd >= 0) {
                chimera_io_uring_map_attrs(request->readdir.attrmask,
                                           &vattr,
                                           child_fd,
                                           NULL,
                                           0);

                close(child_fd);
            }
        }

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

    }

    request->readdir.r_cookie = telldir(dir);
    request->readdir.r_eof    = eof;

    closedir(dir);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* io_uring_readdir */

static void
chimera_io_uring_open(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;
    int                             flags  = 0;
    int                             fd;
    int                             isdir = io_uring_fh_is_dir(request->fh);

    if (request->open.flags & CHIMERA_VFS_OPEN_RDONLY) {
        flags |= O_RDONLY;
    }

    if (request->open.flags & CHIMERA_VFS_OPEN_WRONLY) {
        flags |= O_WRONLY;
    }

    if (request->open.flags & CHIMERA_VFS_OPEN_RDWR) {
        if (isdir) {
            flags |= O_RDONLY;
        } else {
            flags |= O_RDWR;
        }
    }

    fd = io_uring_open_by_handle(thread,
                                 request->fh,
                                 request->fh_len,
                                 flags);

    if (fd < 0) {
        request->status = chimera_io_uring_errno_to_status(errno);
        request->complete(request);
        return;
    }

    request->open.r_vfs_private = fd;

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* io_uring_open */

static void
chimera_io_uring_open_at(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;
    int                             parent_fd;
    int                             flags;
    char                           *scratch = (char *) request->plugin_data;
    struct io_uring_sqe            *sqe;

    scratch += 2 * sizeof(struct statx);

    TERM_STR(fullname, request->open_at.name, request->open_at.namelen, scratch);

    parent_fd = request->open_at.handle->vfs_private;

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

    sqe = chimera_io_uring_get_sqe(thread, request, 0, 0);

    io_uring_prep_openat(sqe, parent_fd, fullname, flags, S_IRWXU);

    evpl_defer(thread->evpl, &thread->deferral);
} /* io_uring_open_at */

static void
chimera_io_uring_close(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;
    struct io_uring_sqe            *sqe;
    int                             fd = request->close.vfs_private;

    sqe = chimera_io_uring_get_sqe(thread, request, 0, 0);

    io_uring_prep_close(sqe, fd);

    evpl_defer(thread->evpl, &thread->deferral);
} /* chimera_io_uring_close */

static void
chimera_io_uring_mkdir(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;
    int                             fd;
    char                           *scratch = (char *) request->plugin_data;
    struct io_uring_sqe            *sqe;
    struct statx                   *dir_stx, *stx;

    dir_stx  = (struct statx *) scratch;
    scratch += sizeof(*dir_stx);

    stx      = (struct statx *) scratch;
    scratch += sizeof(*stx);

    TERM_STR(fullname, request->mkdir.name, request->mkdir.name_len, scratch);

    fd = request->mkdir.handle->vfs_private;

    sqe = chimera_io_uring_get_sqe(thread, request, 0, 1);

    io_uring_prep_mkdirat(sqe, fd, fullname, request->mkdir.mode);

    sqe = chimera_io_uring_get_sqe(thread, request, 1, 1);

    io_uring_prep_statx(sqe, fd, fullname, 0, AT_STATX_SYNC_AS_STAT, stx);

    sqe = chimera_io_uring_get_sqe(thread, request, 2, 0);

    io_uring_prep_statx(sqe, fd, "", AT_EMPTY_PATH, AT_STATX_SYNC_AS_STAT, dir_stx);

    evpl_defer(thread->evpl, &thread->deferral);
} /* chimera_io_uring_mkdir */

static void
chimera_io_uring_remove(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;
    int                             fd;
    char                           *scratch = (char *) request->plugin_data;
    struct io_uring_sqe            *sqe;

    TERM_STR(fullname, request->remove.name, request->remove.namelen, scratch);

    fd = request->remove.handle->vfs_private;

    sqe = chimera_io_uring_get_sqe(thread, request, 0, 0);

    io_uring_prep_unlinkat(sqe, fd, fullname, 0);

    evpl_defer(thread->evpl, &thread->deferral);
} /* chimera_io_uring_remove */

static void
chimera_io_uring_access(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;
    int                             fd, rc;
    struct stat                     st;
    int                             access_mask;

    fd = io_uring_open_by_handle(thread,
                                 request->fh,
                                 request->fh_len,
                                 O_PATH | O_RDONLY | O_NOFOLLOW);

    if (fd < 0) {
        request->status = chimera_io_uring_errno_to_status(errno);
        request->complete(request);
        return;
    }

    rc = fstat(fd, &st);

    if (rc) {
        close(fd);
        request->status = chimera_io_uring_errno_to_status(errno);
        request->complete(request);
        return;
    }

    access_mask = 0;

    if (request->access.access & CHIMERA_VFS_ACCESS_READ) {
        if (st.st_mode & S_IRUSR) {
            access_mask |= CHIMERA_VFS_ACCESS_READ;
        }
    }

    if (request->access.access & CHIMERA_VFS_ACCESS_WRITE) {
        if (st.st_mode & S_IWUSR) {
            access_mask |= CHIMERA_VFS_ACCESS_WRITE;
        }
    }

    if (request->access.access & CHIMERA_VFS_ACCESS_EXECUTE) {
        if (st.st_mode & S_IXUSR) {
            access_mask |= CHIMERA_VFS_ACCESS_EXECUTE;
        }
    }

    if (request->access.attrmask & CHIMERA_VFS_ATTR_MASK_STAT) {
        chimera_io_uring_stat_to_attr(&request->access.r_attr, &st);
    }

    close(fd);

    request->status          = CHIMERA_VFS_OK;
    request->access.r_access = access_mask;
    request->complete(request);
} /* chimera_io_uring_mkdir */

static void
chimera_io_uring_read(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;
    struct io_uring_sqe            *sqe;
    struct evpl                    *evpl = thread->evpl;
    int                             fd, i;
    ssize_t                         left = request->read.length;
    struct iovec                   *iov;
    struct statx                   *stx;
    void                           *scratch = request->plugin_data;

    sqe = chimera_io_uring_get_sqe(thread, request, 0, 0);

    request->read.r_niov = evpl_iovec_alloc(evpl,
                                            request->read.length,
                                            4096,
                                            8,
                                            request->read.iov);

    stx      = (struct statx *) scratch;
    scratch += sizeof(*stx);

    iov = (struct iovec *) scratch;

    for (i = 0; left && i < request->read.r_niov; i++) {

        iov[i].iov_base = request->read.iov[i].data;
        iov[i].iov_len  = request->read.iov[i].length;

        if (iov[i].iov_len > left) {
            iov[i].iov_len = left;
        }

        left -= iov[i].iov_len;
    }

    fd = (int) request->read.handle->vfs_private;

    io_uring_prep_readv(sqe, fd, iov, request->read.r_niov, request->read.offset);

    sqe = chimera_io_uring_get_sqe(thread, request, 1, 0);

    io_uring_prep_statx(sqe, fd, "", AT_EMPTY_PATH, AT_STATX_SYNC_AS_STAT, stx);

    evpl_defer(thread->evpl, &thread->deferral);
} /* chimera_io_uring_read */

static void
chimera_io_uring_write(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;
    struct io_uring_sqe            *sge;
    int                             fd, i, niov = 0;
    uint32_t                        left, chunk;
    struct iovec                   *iov;
    int                             flags = 0;
    struct statx                   *stx;
    void                           *scratch = request->plugin_data;

    stx = (struct statx *) scratch;

    sge = chimera_io_uring_get_sqe(thread, request, 0, 1);

    request->write.r_sync = request->write.sync;

    iov = (struct iovec *) request->plugin_data;

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

    io_uring_prep_writev2(sge, fd, iov, niov, request->write.offset, flags);

    sge = chimera_io_uring_get_sqe(thread, request, 1, 0);

    io_uring_prep_statx(sge, fd, "", AT_EMPTY_PATH, AT_STATX_SYNC_AS_STAT, stx);

    evpl_defer(thread->evpl, &thread->deferral);

} /* chimera_io_uring_write */

static void
chimera_io_uring_commit(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;

    int                             fd = (int) request->commit.handle->vfs_private;
    struct io_uring_sqe            *sge;

    sge = chimera_io_uring_get_sqe(thread, request, 0, 0);

    io_uring_prep_fsync(sge, fd, 0);

    evpl_defer(thread->evpl, &thread->deferral);

} /* chimera_io_uring_commit */

static void
chimera_io_uring_symlink(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;
    int                             fd, rc;
    char                           *scratch = (char *) request->plugin_data;

    TERM_STR(fullname, request->symlink.name, request->symlink.namelen, scratch);
    TERM_STR(target, request->symlink.target, request->symlink.targetlen, scratch);

    fd = io_uring_open_by_handle(thread,
                                 request->fh,
                                 request->fh_len,
                                 O_PATH);

    if (fd < 0) {
        request->status = chimera_io_uring_errno_to_status(errno);
        request->complete(request);
        return;
    }

    rc = symlinkat(target, fd, fullname);

    if (rc < 0) {
        close(fd);
        request->status = chimera_io_uring_errno_to_status(errno);
        request->complete(request);
        return;
    }

    chimera_io_uring_map_attrs(request->symlink.attrmask,
                               &request->symlink.r_dir_attr,
                               fd,
                               request->fh,
                               request->fh_len);

    rc = io_uring_get_fh(fd,
                         fullname,
                         0,
                         request->symlink.r_attr.va_fh,
                         &request->symlink.r_attr.va_fh_len);

    close(fd);

    if (rc < 0) {
        request->status = chimera_io_uring_errno_to_status(errno);
        request->complete(request);
        return;
    }

    request->symlink.r_attr.va_mask |= CHIMERA_VFS_ATTR_FH;

    if (request->symlink.attrmask) {
        fd = io_uring_open_by_handle(thread,
                                     request->symlink.r_attr.va_fh,
                                     request->symlink.r_attr.va_fh_len,
                                     O_PATH);

        if (fd >= 0) {
            chimera_io_uring_map_attrs(request->symlink.attrmask & ~CHIMERA_VFS_ATTR_FH,
                                       &request->symlink.r_attr,
                                       fd,
                                       request->symlink.r_attr.va_fh,
                                       request->symlink.r_attr.va_fh_len);
            close(fd);
        }
    }

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_io_uring_symlink */

static void
chimera_io_uring_readlink(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;
    int                             fd, rc;

    fd = io_uring_open_by_handle(thread,
                                 request->fh,
                                 request->fh_len,
                                 O_PATH | O_RDONLY | O_NOFOLLOW);

    if (fd < 0) {
        request->status = chimera_io_uring_errno_to_status(errno);
        request->complete(request);
        return;
    }

    rc = readlinkat(fd, "", request->readlink.r_target,
                    request->readlink.target_maxlength);

    if (rc < 0) {
        close(fd);
        request->status = chimera_io_uring_errno_to_status(errno);
        request->complete(request);
        return;
    }

    request->readlink.r_target_length = rc;

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_io_uring_readlink */

static void
chimera_io_uring_rename(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;
    int                             old_fd, new_fd, rc;
    char                           *scratch = (char *) request->plugin_data;

    TERM_STR(fullname, request->rename.name, request->rename.namelen, scratch);
    TERM_STR(full_newname, request->rename.new_name, request->rename.new_namelen, scratch);

    old_fd = io_uring_open_by_handle(thread,
                                     request->fh,
                                     request->fh_len,
                                     O_PATH | O_RDONLY | O_NOFOLLOW);

    if (old_fd < 0) {
        request->status = chimera_io_uring_errno_to_status(errno);
        request->complete(request);
        return;
    }

    new_fd = io_uring_open_by_handle(thread,
                                     request->rename.new_fh,
                                     request->rename.new_fhlen,
                                     O_PATH | O_RDONLY | O_NOFOLLOW);

    if (new_fd < 0) {
        close(old_fd);
        request->status = chimera_io_uring_errno_to_status(errno);
        request->complete(request);
        return;
    }

    rc = renameat(old_fd, fullname, new_fd, full_newname);

    if (rc < 0) {
        request->status = chimera_io_uring_errno_to_status(errno);
    } else {
        request->status = CHIMERA_VFS_OK;
    }

    close(old_fd);
    close(new_fd);

    request->complete(request);
} /* chimera_io_uring_rename */

static void
chimera_io_uring_link(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;
    int                             fd, dir_fd, rc;
    char                           *scratch = (char *) request->plugin_data;

    TERM_STR(fullname, request->link.name, request->link.namelen, scratch);

    fd = io_uring_open_by_handle(thread,
                                 request->fh,
                                 request->fh_len,
                                 O_PATH | O_RDONLY | O_NOFOLLOW);

    if (fd < 0) {
        request->status = chimera_io_uring_errno_to_status(errno);
        request->complete(request);
        return;
    }

    dir_fd = io_uring_open_by_handle(thread,
                                     request->link.dir_fh,
                                     request->link.dir_fhlen,
                                     O_PATH | O_RDONLY | O_NOFOLLOW);

    if (dir_fd < 0) {
        close(fd);
        request->status = chimera_io_uring_errno_to_status(errno);
        request->complete(request);
        return;
    }

    rc = linkat(fd, "", dir_fd, fullname, AT_EMPTY_PATH);

    if (rc < 0) {
        request->status = chimera_io_uring_errno_to_status(errno);
    } else {
        request->status = CHIMERA_VFS_OK;
    }

    close(fd);
    close(dir_fd);

    request->complete(request);

} /* chimera_io_uring_link */
static void
chimera_io_uring_dispatch(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;

    if (thread->inflight + CHIMERA_VFS_REQUEST_MAX_HANDLES > thread->max_inflight) {
        /* We have given the ring too much work already, wait for completions */
        DL_APPEND(thread->pending_requests, request);
        return;
    }

    switch (request->opcode) {
        case CHIMERA_VFS_OP_LOOKUP_PATH:
            chimera_io_uring_lookup_path(request, private_data);
            break;
        case CHIMERA_VFS_OP_LOOKUP:
            chimera_io_uring_lookup(request, private_data);
            break;
        case CHIMERA_VFS_OP_GETATTR:
            chimera_io_uring_getattr(request, private_data);
            break;
        case CHIMERA_VFS_OP_OPEN:
            chimera_io_uring_open(request, private_data);
            break;
        case CHIMERA_VFS_OP_OPEN_AT:
            chimera_io_uring_open_at(request, private_data);
            break;
        case CHIMERA_VFS_OP_CLOSE:
            chimera_io_uring_close(request, private_data);
            break;
        case CHIMERA_VFS_OP_MKDIR:
            chimera_io_uring_mkdir(request, private_data);
            break;
        case CHIMERA_VFS_OP_READDIR:
            chimera_io_uring_readdir(request, private_data);
            break;
        case CHIMERA_VFS_OP_REMOVE:
            chimera_io_uring_remove(request, private_data);
            break;
        case CHIMERA_VFS_OP_ACCESS:
            chimera_io_uring_access(request, private_data);
            break;
        case CHIMERA_VFS_OP_READ:
            chimera_io_uring_read(request, private_data);
            break;
        case CHIMERA_VFS_OP_WRITE:
            chimera_io_uring_write(request, private_data);
            break;
        case CHIMERA_VFS_OP_COMMIT:
            chimera_io_uring_commit(request, private_data);
            break;
        case CHIMERA_VFS_OP_SYMLINK:
            chimera_io_uring_symlink(request, private_data);
            break;
        case CHIMERA_VFS_OP_READLINK:
            chimera_io_uring_readlink(request, private_data);
            break;
        case CHIMERA_VFS_OP_RENAME:
            chimera_io_uring_rename(request, private_data);
            break;
        case CHIMERA_VFS_OP_LINK:
            chimera_io_uring_link(request, private_data);
            break;
        case CHIMERA_VFS_OP_SETATTR:
            chimera_io_uring_setattr(request, private_data);
            break;
        default:
            chimera_io_uring_error("io_uring_dispatch: unknown operation %d",
                                   request->opcode);
            request->status = CHIMERA_VFS_ENOTSUP;
            request->complete(request);
            break;
    } /* switch */
} /* io_uring_dispatch */

struct chimera_vfs_module vfs_io_uring = {
    .name           = "io_uring",
    .fh_magic       = CHIMERA_VFS_FH_MAGIC_IO_URING,
    .blocking       = 0,
    .init           = chimera_io_uring_init,
    .destroy        = chimera_io_uring_destroy,
    .thread_init    = chimera_io_uring_thread_init,
    .thread_destroy = chimera_io_uring_thread_destroy,
    .dispatch       = chimera_io_uring_dispatch,
};