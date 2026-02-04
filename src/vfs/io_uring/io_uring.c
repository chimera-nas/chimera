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
#include <sys/sysmacros.h>
#include <sys/types.h>
#include <sys/statvfs.h>
#include <sys/eventfd.h>
#include <sys/uio.h>
#include <dirent.h>
#include <fcntl.h>
#include <errno.h>
#include <liburing.h>
#include <uthash.h>
#include <utlist.h>
#include <jansson.h>
#include <linux/version.h>

#include "vfs/vfs_error.h"

#include "evpl/evpl.h"

#include "io_uring.h"
#include "../linux/linux_common.h"
#include "common/logging.h"
#include "common/macros.h"

// fchmodat support for AT_SYMLINK_NOFOLLOW was added in Linux 6.6
#if defined(LINUX_VERSION_CODE) && defined(KERNEL_VERSION)
    #if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 6, 0)
        #define HAVE_FCHMODAT_AT_SYMLINK_NOFOLLOW 1
    #endif /* if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 6, 0) */
#endif /* if defined(LINUX_VERSION_CODE) && defined(KERNEL_VERSION) */

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

struct chimera_io_uring_shared {
    struct io_uring ring;
    int             readdir_verifier;
};

struct chimera_io_uring_thread {
    struct evpl                     *evpl;
    struct evpl_doorbell             doorbell;
    struct evpl_deferral             deferral;
    struct io_uring                  ring;
    uint64_t                         inflight;
    uint64_t                         max_inflight;
    struct chimera_vfs_request      *pending_requests;
    struct chimera_linux_mount_table mount_table;
    int                              readdir_verifier;
};

static void *
chimera_io_uring_init(const char *cfgfile)
{
    struct chimera_io_uring_shared *shared;
    struct io_uring_params          params = { 0 };
    int                             rc;

    shared = calloc(1, sizeof(*shared));

    // Initialize the shared ring with default parameters
    rc = io_uring_queue_init_params(256, &shared->ring, &params);

    if (rc < 0) {
        chimera_io_uring_error("Failed to create shared io_uring queue, io_uring disabled: %s", strerror(-rc));
        free(shared);
        return NULL;
    }

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
} /* io_uring_init */ /* io_uring_init */

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
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell)
{
    struct chimera_io_uring_thread    *thread;
    struct io_uring_cqe               *cqe;
    int                                parent_fd;
    struct chimera_vfs_request        *request;
    struct chimera_vfs_request_handle *handle;
    struct statx                      *dir_stx, *stx;
    const char                        *name;
    struct io_uring_sqe               *sqe;
    void                              *scratch;

    thread = container_of(doorbell, struct chimera_io_uring_thread, doorbell);

    while (io_uring_peek_cqe(&thread->ring, &cqe) == 0) {

        handle = (struct chimera_vfs_request_handle *) cqe->user_data;

        request = container_of(handle, struct chimera_vfs_request, handle[handle->slot]);

        switch (request->opcode) {
            case CHIMERA_VFS_OP_LOOKUP:
                if (cqe->res >= 0) {
                    request->status = CHIMERA_VFS_OK;

                    stx = (struct statx *) request->plugin_data;

                    name = (char *) (stx + 1);

                    parent_fd = request->lookup.handle->vfs_private;

                    chimera_linux_map_child_attrs_statx(CHIMERA_VFS_FH_MAGIC_IO_URING,
                                                        request,
                                                        &request->lookup.r_attr,
                                                        parent_fd,
                                                        name,
                                                        stx);

                } else {
                    request->status = chimera_linux_errno_to_status(-cqe->res);
                }
                break;
            case CHIMERA_VFS_OP_GETATTR:
                if (cqe->res == 0) {
                    struct statx *stx = (struct statx *) request->plugin_data;
                    request->status = CHIMERA_VFS_OK;
                    chimera_linux_map_attrs_statx(CHIMERA_VFS_FH_MAGIC_IO_URING,
                                                  &request->getattr.r_attr,
                                                  request->getattr.handle->vfs_private,
                                                  stx);
                }
                break;
            case CHIMERA_VFS_OP_OPEN_AT:
                if (handle->slot == 0) {
                    if (cqe->res >= 0) {
                        request->status                = CHIMERA_VFS_OK;
                        request->open_at.r_vfs_private = cqe->res;
                        dir_stx                        = (struct statx *) request->plugin_data;
                        stx                            = (struct statx *) (dir_stx + 1);
                        name                           = (char *) (stx + 1);

                        parent_fd = request->open_at.handle->vfs_private;

                        sqe = chimera_io_uring_get_sqe(thread, request, 1, 0);

                        io_uring_prep_statx(sqe, parent_fd, name, 0, AT_STATX_SYNC_AS_STAT, stx);

                        sqe = chimera_io_uring_get_sqe(thread, request, 2, 0);

                        io_uring_prep_statx(sqe, parent_fd, "", AT_EMPTY_PATH, AT_STATX_SYNC_AS_STAT, dir_stx);

                        evpl_defer(thread->evpl, &thread->deferral);

                    } else {
                        request->status = chimera_linux_errno_to_status(-cqe->res);
                    }
                } else if (handle->slot == 1) {
                    if (cqe->res == 0) {
                        dir_stx = (struct statx *) request->plugin_data;
                        stx     = (struct statx *) (dir_stx + 1);
                        name    = (char *) (stx + 1);

                        parent_fd = request->open_at.handle->vfs_private;

                        chimera_linux_map_child_attrs_statx(CHIMERA_VFS_FH_MAGIC_IO_URING,
                                                            request,
                                                            &request->open_at.r_attr,
                                                            parent_fd,
                                                            name,
                                                            stx);
                    }

                } else if (handle->slot == 2) {
                    if (cqe->res == 0) {
                        dir_stx   = (struct statx *) request->plugin_data;
                        parent_fd = request->open_at.handle->vfs_private;
                        chimera_linux_map_attrs_statx(CHIMERA_VFS_FH_MAGIC_IO_URING,
                                                      &request->open_at.r_dir_post_attr,
                                                      parent_fd,
                                                      dir_stx);
                    }
                }
                break;
            case CHIMERA_VFS_OP_REMOVE:
                /* Remove is now synchronous, so this should never be reached */
                chimera_io_uring_abort("io_uring completion for synchronous remove operation");
                break;
            case CHIMERA_VFS_OP_MKDIR:
                if (handle->slot == 0) {
                    if (cqe->res == 0) {
                        request->status = CHIMERA_VFS_OK;
                    } else {
                        request->status = chimera_linux_errno_to_status(-cqe->res);
                    }

                    scratch = (char *) request->plugin_data;

                    dir_stx  = (struct statx *) scratch;
                    scratch += sizeof(*dir_stx);

                    stx      = (struct statx *) scratch;
                    scratch += sizeof(*stx);

                    TERM_STR(fullname, request->mkdir.name, request->mkdir.name_len, scratch);

                    parent_fd = request->mkdir.handle->vfs_private;

                    sqe = chimera_io_uring_get_sqe(thread, request, 1, 0);

                    io_uring_prep_statx(sqe, parent_fd, fullname, 0, AT_STATX_SYNC_AS_STAT, stx);

                    sqe = chimera_io_uring_get_sqe(thread, request, 2, 0);

                    io_uring_prep_statx(sqe, parent_fd, "", AT_EMPTY_PATH, AT_STATX_SYNC_AS_STAT, dir_stx);

                    evpl_defer(thread->evpl, &thread->deferral);
                } else if (handle->slot == 1) {
                    if (cqe->res == 0) {
                        dir_stx   = (struct statx *) request->plugin_data;
                        stx       = (struct statx *) (dir_stx + 1);
                        name      = (char *) (stx + 1);
                        parent_fd = request->mkdir.handle->vfs_private;

                        chimera_linux_map_child_attrs_statx(CHIMERA_VFS_FH_MAGIC_IO_URING,
                                                            request,
                                                            &request->mkdir.r_attr,
                                                            parent_fd,
                                                            name,
                                                            stx);
                    }
                } else if (handle->slot == 2) {
                    if (cqe->res == 0) {
                        dir_stx = (struct statx *) request->plugin_data;
                        chimera_linux_statx_to_attr(&request->mkdir.r_dir_post_attr, dir_stx);
                    }
                }
                break;
            case CHIMERA_VFS_OP_READ:
                if (handle->slot == 0) {
                    if (cqe->res >= 0) {
                        request->status        = CHIMERA_VFS_OK;
                        request->read.r_length = cqe->res;
                        request->read.r_eof    = (cqe->res < request->read.length);

                        if (cqe->res == 0) {
                            evpl_iovecs_release(evpl, request->read.iov, request->read.r_niov);
                            request->read.r_niov = 0;
                        }

                    } else {
                        request->status = chimera_linux_errno_to_status(-cqe->res);

                        evpl_iovecs_release(evpl, request->read.iov, request->read.r_niov);
                    }
                } else {
                    if (cqe->res == 0) {
                        chimera_linux_statx_to_attr(&request->read.r_attr, (struct statx *) request->plugin_data);
                    }
                }
                break;
            case CHIMERA_VFS_OP_WRITE:
                if (cqe->res >= 0) {
                    request->status         = CHIMERA_VFS_OK;
                    request->write.r_length = cqe->res;
                } else {
                    request->status         = chimera_linux_errno_to_status(-cqe->res);
                    request->write.r_length = 0;
                }
                /* Note: Write iovecs are NOT released here. They were allocated on the
                 * server thread and must be released there. The server's write completion
                 * callback handles the release after this request completes via doorbell.
                 */
                break;

            default:
                if (cqe->res) {
                    request->status = chimera_linux_errno_to_status(-cqe->res);
                } else {
                    request->status = CHIMERA_VFS_OK;
                }
                break;
        } /* switch */

        --request->token_count;

        if (request->token_count == 0) {
            thread->inflight--;
            request->complete(request);
        }

        io_uring_cqe_seen(&thread->ring, cqe);

    } /* chimera_io_uring_complete */

    while (thread->pending_requests && thread->inflight < thread->max_inflight) {
        request = thread->pending_requests;
        DL_DELETE(thread->pending_requests, request);
        chimera_io_uring_dispatch(request, thread);
    }
} /* chimera_io_uring_complete */ /* chimera_io_uring_complete */ /* chimera_io_uring_complete */

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

    thread->evpl             = evpl;
    thread->readdir_verifier = shared->readdir_verifier;

    // Set up single issuer mode
    params.flags  = IORING_SETUP_SINGLE_ISSUER;
    params.flags |= IORING_SETUP_COOP_TASKRUN;

    params.flags |= IORING_SETUP_ATTACH_WQ;
    params.wq_fd  = shared->ring.ring_fd;

    thread->max_inflight = 1024;

    // Initialize io_uring with params
    rc = io_uring_queue_init_params(4 * thread->max_inflight, &thread->ring, &params);

    chimera_io_uring_abort_if(rc < 0, "Failed to create io_uring queue: %s", strerror(-rc));

    evpl_add_doorbell(evpl, &thread->doorbell, chimera_io_uring_complete);

    rc = io_uring_register_eventfd(&thread->ring, evpl_doorbell_fd(&thread->doorbell));

    chimera_io_uring_abort_if(rc < 0, "Failed to register eventfd");

    evpl_deferral_init(&thread->deferral,
                       chimera_io_uring_flush,
                       thread);

    return thread;
} /* io_uring_thread_init */ /* io_uring_thread_init */

static void
chimera_io_uring_thread_destroy(void *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;

    linux_mount_table_destroy(&thread->mount_table);

    io_uring_queue_exit(&thread->ring);
    evpl_remove_doorbell(thread->evpl, &thread->doorbell);

    free(thread);
} /* io_uring_thread_destroy */

static void
chimera_io_uring_getattr(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;
    int                             fd;
    struct io_uring_sqe            *sqe;
    struct statx                   *stx;
    char                           *scratch = (char *) request->plugin_data;

    fd = (int) request->getattr.handle->vfs_private;

    sqe = chimera_io_uring_get_sqe(thread, request, 0, 0);

    stx = (struct statx *) scratch;

    io_uring_prep_statx(sqe, fd, "", AT_EMPTY_PATH | AT_SYMLINK_NOFOLLOW, AT_STATX_SYNC_AS_STAT, stx);

    evpl_defer(thread->evpl, &thread->deferral);
} /* io_uring_getattr */

static void
chimera_io_uring_setattr(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;
    int                             fd, rc;

    --thread->inflight;

    fd = request->setattr.handle->vfs_private;


    if (request->setattr.set_attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) {
#ifdef HAVE_FCHMODAT_AT_SYMLINK_NOFOLLOW
        // Use fchmodat with AT_SYMLINK_NOFOLLOW on kernels >= 6.6
        rc = fchmodat(fd, "", request->setattr.set_attr->va_mode,
                      AT_SYMLINK_NOFOLLOW | AT_EMPTY_PATH);
#else  /* ifdef HAVE_FCHMODAT_AT_SYMLINK_NOFOLLOW */
        // Fall back to fchmod on older kernels (AT_SYMLINK_NOFOLLOW not supported)
        rc = fchmod(fd, attr->va_mode);
#endif /* ifdef HAVE_FCHMODAT_AT_SYMLINK_NOFOLLOW */

        if (rc) {
            chimera_io_uring_error("io_uring_setattr: fchmod(%o) failed: %s",
                                   request->setattr.set_attr->va_mode,
                                   strerror(errno));

            request->status = chimera_linux_errno_to_status(errno);
            request->complete(request);
            return;
        }

        request->setattr.set_attr->va_set_mask |= CHIMERA_VFS_ATTR_MODE;
    }

    if ((request->setattr.set_attr->va_set_mask & (CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_GID)) ==
        (CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_GID)) {
        /* Both UID and GID are being set */
        rc = fchownat(fd, "", request->setattr.set_attr->va_uid, request->setattr.set_attr->va_gid,
                      AT_SYMLINK_NOFOLLOW | AT_EMPTY_PATH);

        if (rc) {
            chimera_io_uring_error("io_uring_setattr: fchown(%u,%u) failed: %s",
                                   request->setattr.set_attr->va_uid,
                                   request->setattr.set_attr->va_gid,
                                   strerror(errno));

            request->status = chimera_linux_errno_to_status(errno);
            request->complete(request);
            return;
        }

        request->setattr.set_attr->va_set_mask |= CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_GID;
    } else if (request->setattr.set_attr->va_set_mask & CHIMERA_VFS_ATTR_UID) {
        /* Only UID is being set */
        rc = fchownat(fd, "", request->setattr.set_attr->va_uid, -1,
                      AT_SYMLINK_NOFOLLOW | AT_EMPTY_PATH);

        if (rc) {
            chimera_io_uring_error("io_uring_setattr: fchown(%u,-1) failed: %s",
                                   request->setattr.set_attr->va_uid,
                                   strerror(errno));

            request->status = chimera_linux_errno_to_status(errno);
            request->complete(request);
            return;
        }

        request->setattr.set_attr->va_set_mask |= CHIMERA_VFS_ATTR_UID;
    } else if (request->setattr.set_attr->va_set_mask & CHIMERA_VFS_ATTR_GID) {
        /* Only GID is being set */
        rc = fchownat(fd, "", -1, request->setattr.set_attr->va_gid,
                      AT_SYMLINK_NOFOLLOW | AT_EMPTY_PATH);

        if (rc) {
            chimera_io_uring_error("io_uring_setattr: fchown(-1,%u) failed: %s",
                                   request->setattr.set_attr->va_gid,
                                   strerror(errno));

            request->status = chimera_linux_errno_to_status(errno);
            request->complete(request);
            return;
        }

        request->setattr.set_attr->va_set_mask |= CHIMERA_VFS_ATTR_GID;
    }

    if (request->setattr.set_attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE) {
        rc = ftruncate(fd, request->setattr.set_attr->va_size);

        if (rc) {
            chimera_io_uring_error("io_uring_setattr: ftruncate(%ld) failed: %s",
                                   request->setattr.set_attr->va_size,
                                   strerror(errno));

            request->status = chimera_linux_errno_to_status(errno);
            request->complete(request);
            return;
        }

        request->setattr.set_attr->va_set_mask |= CHIMERA_VFS_ATTR_SIZE;
    }

    if (request->setattr.set_attr->va_set_mask & (CHIMERA_VFS_ATTR_ATIME | CHIMERA_VFS_ATTR_MTIME)) {
        struct timespec times[2];

        if (request->setattr.set_attr->va_set_mask & CHIMERA_VFS_ATTR_ATIME) {
            if (request->setattr.set_attr->va_atime.tv_nsec == CHIMERA_VFS_TIME_NOW) {
                times[0].tv_nsec = UTIME_NOW;
            } else {
                times[0] = request->setattr.set_attr->va_atime;
            }

            request->setattr.set_attr->va_set_mask |= CHIMERA_VFS_ATTR_ATIME;
        } else {
            times[0].tv_nsec = UTIME_OMIT;
        }

        if (request->setattr.set_attr->va_set_mask & CHIMERA_VFS_ATTR_MTIME) {
            if (request->setattr.set_attr->va_mtime.tv_nsec == CHIMERA_VFS_TIME_NOW) {
                times[1].tv_nsec = UTIME_NOW;
            } else {
                times[1] = request->setattr.set_attr->va_mtime;
            }

            request->setattr.set_attr->va_set_mask |= CHIMERA_VFS_ATTR_MTIME;
        } else {
            times[1].tv_nsec = UTIME_OMIT;
        }

        rc = utimensat(fd, "", times, AT_SYMLINK_NOFOLLOW | AT_EMPTY_PATH);

        if (rc) {
            chimera_io_uring_error("io_uring_setattr: utimensat() failed: %s",
                                   strerror(errno));

            request->status = chimera_linux_errno_to_status(errno);
            request->complete(request);
            return;
        }
    }

    chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_IO_URING,
                            &request->setattr.r_post_attr,
                            fd);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* io_uring_setattr */

static void
chimera_io_uring_mount(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    int   mount_fd, rc;
    char *scratch = (char *) request->plugin_data;

    TERM_STR(fullpath,
             request->mount.path,
             request->mount.pathlen,
             scratch);

    mount_fd = open(fullpath, O_DIRECTORY | O_RDONLY | O_NOFOLLOW);

    if (mount_fd < 0) {
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }

    rc = linux_get_fh(NULL, /* mount context - compute fsid */
                      mount_fd,
                      fullpath,
                      request->mount.r_attr.va_fh,
                      &request->mount.r_attr.va_fh_len);

    if (rc < 0) {
        request->status = chimera_linux_errno_to_status(errno);
        close(mount_fd);
        request->complete(request);
        return;
    }

    request->mount.r_attr.va_set_mask |= CHIMERA_VFS_ATTR_FH;

    chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_IO_URING,
                            &request->mount.r_attr,
                            mount_fd);

    close(mount_fd);

    request->status = CHIMERA_VFS_OK;

    request->complete(request);
} /* chimera_io_uring_getrootfh */

static void
chimera_io_uring_umount(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    /* No action required */
    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_io_uring_umount */

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
    int                             fd, dup_fd, rc;
    DIR                            *dir;
    struct dirent                  *dirent;
    struct chimera_vfs_attrs        vattr;
    int                             eof = 1;

    --thread->inflight;


    fd = request->readdir.handle->vfs_private;

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
        chimera_io_uring_error("io_uring_readdir: openat() failed: %s",
                               strerror(errno));
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }

    dir = fdopendir(dup_fd);

    if (!dir) {
        chimera_io_uring_error("io_uring_readdir: fdopendir() failed: %s",
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

        chimera_linux_map_child_attrs(CHIMERA_VFS_FH_MAGIC_IO_URING,
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

    }

    request->readdir.r_cookie = telldir(dir);
    request->readdir.r_eof    = eof;

    closedir(dir);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* io_uring_readdir */ /* io_uring_readdir */

static void
chimera_io_uring_open(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;
    int                             flags  = 0;
    int                             fd;

    --thread->inflight;


    if (request->open.flags & CHIMERA_VFS_OPEN_PATH) {
        flags |= O_PATH;
    }

    if (request->open.flags & CHIMERA_VFS_OPEN_DIRECTORY) {
        flags |= O_DIRECTORY;
    }
    if (request->open.flags & (CHIMERA_VFS_OPEN_READ_ONLY | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY)) {
        flags |= O_RDONLY;
    } else {
        flags |= O_RDWR;
    }

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
} /* io_uring_open */

static void
chimera_io_uring_open_at(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;
    int                             parent_fd;
    int                             flags;
    uint32_t                        mode;
    char                           *scratch = (char *) request->plugin_data;
    struct io_uring_sqe            *sqe;

    scratch += 2 * sizeof(struct statx);

    TERM_STR(fullname, request->open_at.name, request->open_at.namelen, scratch);

    parent_fd = request->open_at.handle->vfs_private;

    flags = 0;

    if (request->open_at.flags & (CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY)) {
        flags |= O_RDONLY;
    } else {
        flags |= O_RDWR;
    }

    if (request->open_at.flags & CHIMERA_VFS_OPEN_PATH) {
        flags |= O_PATH;
    }

    if (request->open_at.flags & CHIMERA_VFS_OPEN_DIRECTORY) {
        flags |= O_DIRECTORY;
    }

    if (request->open_at.flags & CHIMERA_VFS_OPEN_CREATE) {
        flags |= O_CREAT;
    }

    if (request->open_at.flags & CHIMERA_VFS_OPEN_EXCLUSIVE) {
        flags |= O_EXCL;
    }

    sqe = chimera_io_uring_get_sqe(thread, request, 0, 0);

    if (request->open_at.set_attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) {
        mode = request->open_at.set_attr->va_mode;

        request->open_at.set_attr->va_set_mask |= CHIMERA_VFS_ATTR_MODE;
    } else {
        mode = 0600;
    }

    io_uring_prep_openat(sqe, parent_fd, fullname, flags, mode);

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
    uint32_t                        mode;
    char                           *scratch = (char *) request->plugin_data;
    struct io_uring_sqe            *sqe;

    scratch += sizeof(struct statx) * 2;

    TERM_STR(fullname, request->mkdir.name, request->mkdir.name_len, scratch);

    fd = request->mkdir.handle->vfs_private;

    sqe = chimera_io_uring_get_sqe(thread, request, 0, 0);

    if (request->mkdir.set_attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) {
        mode = request->mkdir.set_attr->va_mode;
    } else {
        mode = S_IRWXU;
    }

    io_uring_prep_mkdirat(sqe, fd, fullname, mode);

    evpl_defer(thread->evpl, &thread->deferral);
} /* chimera_io_uring_mkdir */

static void
chimera_io_uring_mknod(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;
    int                             fd, rc;
    char                           *scratch = (char *) request->plugin_data;
    uint32_t                        mode;
    dev_t                           dev = 0;

    --thread->inflight;

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

    chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_IO_URING,
                            &request->mknod.r_dir_pre_attr,
                            fd);

    rc = mknodat(fd, fullname, mode, dev);

    chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_IO_URING,
                            &request->mknod.r_dir_post_attr,
                            fd);

    if (rc < 0) {

        if (errno == EEXIST) {
            chimera_linux_map_child_attrs(CHIMERA_VFS_FH_MAGIC_IO_URING,
                                          request,
                                          &request->mknod.r_attr,
                                          fd,
                                          fullname);
        }

        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }

    chimera_linux_map_child_attrs(CHIMERA_VFS_FH_MAGIC_IO_URING,
                                  request,
                                  &request->mknod.r_attr,
                                  fd,
                                  fullname);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_io_uring_mknod */

static void
chimera_io_uring_remove(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;
    int                             fd, rc;
    char                           *scratch = (char *) request->plugin_data;

    --thread->inflight;

    TERM_STR(fullname, request->remove.name, request->remove.namelen, scratch);

    fd = request->remove.handle->vfs_private;

    /* Get the file handle before removing, so VFS can invalidate attribute cache */
    request->remove.r_removed_attr.va_req_mask = CHIMERA_VFS_ATTR_FH;

    chimera_linux_map_child_attrs(CHIMERA_VFS_FH_MAGIC_IO_URING,
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
} /* chimera_io_uring_remove */ /* chimera_io_uring_remove */

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

    /* Handle 0-byte reads specially - readv with uninitialized iov causes EFAULT */
    if (request->read.length == 0) {
        fd  = (int) request->read.handle->vfs_private;
        stx = (struct statx *) scratch;
        /* Pre-fill result fields since we won't submit readv */
        request->status        = CHIMERA_VFS_OK;
        request->read.r_niov   = 0;
        request->read.r_length = 0;
        request->read.r_eof    = 1;
        sqe                    = chimera_io_uring_get_sqe(thread, request, 1, 0);
        io_uring_prep_statx(sqe, fd, "", AT_EMPTY_PATH, AT_STATX_SYNC_AS_STAT, stx);
        evpl_defer(thread->evpl, &thread->deferral);
        return;
    }

    sqe = chimera_io_uring_get_sqe(thread, request, 0, 0);

    request->read.r_niov = evpl_iovec_alloc(evpl,
                                            request->read.length,
                                            4096,
                                            8,
                                            EVPL_IOVEC_FLAG_SHARED, request->read.iov);

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
    int                             flags   = 0;
    void                           *scratch = request->plugin_data;

    sge = chimera_io_uring_get_sqe(thread, request, 0, 0);

    request->write.r_sync = request->write.sync;

    iov = (struct iovec *) scratch;

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

    /* Don't return post-write stat info - the linked statx may see stale
     * metadata before the write's effects are fully visible. Let the VFS
     * make an explicit getattr call when needed. */
    request->write.r_post_attr.va_set_mask = 0;

    evpl_defer(thread->evpl, &thread->deferral);

} /* chimera_io_uring_write */ /* chimera_io_uring_write */

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

    --thread->inflight;


    TERM_STR(fullname, request->symlink.name, request->symlink.namelen, scratch);
    TERM_STR(target, request->symlink.target, request->symlink.targetlen, scratch);

    fd = request->mkdir.handle->vfs_private;

    rc = symlinkat(target, fd, fullname);

    if (rc < 0) {
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }

    chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_IO_URING,
                            &request->symlink.r_dir_post_attr,
                            fd);

    chimera_linux_map_child_attrs(CHIMERA_VFS_FH_MAGIC_IO_URING,
                                  request,
                                  &request->symlink.r_attr,
                                  fd,
                                  fullname);

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

    --thread->inflight;


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
} /* chimera_io_uring_readlink */

static void
chimera_io_uring_rename(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;
    int                             old_fd, new_fd, rc;
    char                           *scratch = (char *) request->plugin_data;

    --thread->inflight;


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
        close(old_fd);
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
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
} /* chimera_io_uring_rename */

static void
chimera_io_uring_link(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;
    int                             fd, dir_fd, rc;
    char                           *scratch = (char *) request->plugin_data;

    --thread->inflight;

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

} /* chimera_io_uring_link */

static void
chimera_io_uring_dispatch(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;

    if (thread->inflight >= thread->max_inflight) {
        /* We have given the ring too much work already, wait for completions */
        DL_APPEND(thread->pending_requests, request);
        return;
    }

    thread->inflight++;

    switch (request->opcode) {
        case CHIMERA_VFS_OP_MOUNT:
            chimera_io_uring_mount(request, private_data);
            break;
        case CHIMERA_VFS_OP_UMOUNT:
            chimera_io_uring_umount(request, private_data);
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
        case CHIMERA_VFS_OP_MKNOD:
            chimera_io_uring_mknod(request, private_data);
            break;
        case CHIMERA_VFS_OP_READDIR:
            chimera_io_uring_readdir(request, private_data);
            break;
        case CHIMERA_VFS_OP_REMOVE:
            chimera_io_uring_remove(request, private_data);
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
            --thread->inflight;
            request->status = CHIMERA_VFS_ENOTSUP;
            request->complete(request);
            break;
    } /* switch */
} /* io_uring_dispatch */

SYMBOL_EXPORT struct chimera_vfs_module vfs_io_uring = {
    .name           = "io_uring",
    .fh_magic       = CHIMERA_VFS_FH_MAGIC_IO_URING,
    .capabilities   = CHIMERA_VFS_CAP_OPEN_PATH_REQUIRED | CHIMERA_VFS_CAP_OPEN_FILE_REQUIRED,
    .init           = chimera_io_uring_init,
    .destroy        = chimera_io_uring_destroy,
    .thread_init    = chimera_io_uring_thread_init,
    .thread_destroy = chimera_io_uring_thread_destroy,
    .dispatch       = chimera_io_uring_dispatch,
};