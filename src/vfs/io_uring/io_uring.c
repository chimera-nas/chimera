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
#include <sys/ioctl.h>
#include <sys/xattr.h>
#include <linux/fs.h>
#include <liburing.h>
#include <uthash.h>
#include <utlist.h>
#include <jansson.h>
#include <linux/version.h>

#include "vfs/vfs_error.h"
#include "vfs/vfs_internal.h"

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

#define CHIMERA_IO_URING_STATX_MASK        STATX_BASIC_STATS

struct chimera_io_uring_shared {
    struct io_uring ring;
    int             readdir_verifier;
};

/*
 * Per-thread cache of io_uring registered personalities, keyed by credential
 * hash.  A personality snapshots a client identity in the kernel so an async
 * openat/mkdirat SQE can carry it (sqe->personality) instead of impersonating
 * the whole thread across the submit-to-completion window -- which would be
 * wrong under batched submission, where many in-flight SQEs would otherwise run
 * under whichever identity was set last.  Registration is amortised across
 * requests from the same caller; the least-recently-used entry is evicted (and
 * unregistered) when the table is full.
 */
#define CHIMERA_IO_URING_MAX_PERSONALITIES 64

struct chimera_io_uring_personality {
    uint64_t cred_hash;
    uint64_t lru;
    int      id;
    int      valid;
};

struct chimera_io_uring_thread {
    struct evpl                        *evpl;
    struct evpl_doorbell                doorbell;
    struct evpl_poll                   *poll;
    struct evpl_deferral                deferral;
    struct io_uring                     ring;
    uint64_t                            inflight;
    uint64_t                            max_inflight;
    struct chimera_vfs_request         *pending_requests;
    struct chimera_linux_mount_table    mount_table;
    int                                 readdir_verifier;
    int                                 personality_supported;
    uint64_t                            personality_lru_clock;
    struct chimera_io_uring_personality personalities[CHIMERA_IO_URING_MAX_PERSONALITIES];
};

/*
 * Return a registered personality id for `cred`'s identity, or 0 to use the
 * thread's own (server) credentials, or -1 if personalities are unavailable so
 * the caller falls back to per-thread impersonation.  Server-matching creds
 * need no personality.  A miss briefly impersonates the cred on this thread to
 * register a personality capturing it, then restores.
 */
static int
chimera_io_uring_get_personality(
    struct chimera_io_uring_thread *thread,
    const struct chimera_vfs_cred  *cred)
{
    const struct chimera_vfs_cred *sc = chimera_vfs_get_server_cred();
    uint64_t                       hash;
    int                            i, slot, free_slot = -1, lru_slot = 0, id, rc;

    if (!thread->personality_supported ||
        cred->flavor != CHIMERA_VFS_AUTH_UNIX ||
        (cred->uid == sc->uid && cred->gid == sc->gid)) {
        return 0;
    }

    hash = chimera_vfs_cred_hash(cred);

    for (i = 0; i < CHIMERA_IO_URING_MAX_PERSONALITIES; i++) {
        if (!thread->personalities[i].valid) {
            if (free_slot < 0) {
                free_slot = i;
            }
            continue;
        }
        if (thread->personalities[i].cred_hash == hash) {
            thread->personalities[i].lru = ++thread->personality_lru_clock;
            return thread->personalities[i].id;
        }
        if (thread->personalities[i].lru < thread->personalities[lru_slot].lru) {
            lru_slot = i;
        }
    }

    /* Miss: register a personality capturing this identity. */
    rc = chimera_setup_credential(cred, NULL);
    if (rc != 0) {
        return -1;
    }
    id = io_uring_register_personality(&thread->ring);
    chimera_restore_privilege(cred);

    if (id < 0) {
        return -1;
    }

    slot = (free_slot >= 0) ? free_slot : lru_slot;
    if (thread->personalities[slot].valid) {
        io_uring_unregister_personality(&thread->ring,
                                        thread->personalities[slot].id);
    }
    thread->personalities[slot].cred_hash = hash;
    thread->personalities[slot].id        = id;
    thread->personalities[slot].lru       = ++thread->personality_lru_clock;
    thread->personalities[slot].valid     = 1;
    return id;
} /* chimera_io_uring_get_personality */

static void *
chimera_io_uring_init(
    const char                *cfgdata,
    struct prometheus_metrics *metrics)
{
    (void) metrics;
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

    if (cfgdata && cfgdata[0] != '\0') {
        json_error_t json_error;
        json_t      *cfg = json_loads(cfgdata, 0, &json_error);

        if (cfg) {
            json_t *verf = json_object_get(cfg, "readdir_verifier");

            if (json_is_boolean(verf)) {
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

static int
chimera_io_uring_set_open_attrs(
    int                       fd,
    struct chimera_vfs_attrs *attr)
{
    uint64_t set_mask = attr->va_set_mask;

    /* Apply ownership requested via the attribute set.  For an AUTH_ATTR
     * credential, chimera_setup_credential() injects the caller's UID/GID here
     * (rather than impersonating with setfsuid), so without this fchown a newly
     * created file would be owned by the server identity instead of the client
     * -- which then fails the owner access check on a root-run server.  Mirrors
     * the linux backend's chimera_linux_set_attrs(). */
    if ((set_mask & (CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_GID))) {
        uid_t uid = (set_mask & CHIMERA_VFS_ATTR_UID) ? (uid_t) attr->va_uid : (uid_t) -1;
        gid_t gid = (set_mask & CHIMERA_VFS_ATTR_GID) ? (gid_t) attr->va_gid : (gid_t) -1;

        if (fchown(fd, uid, gid) < 0) {
            return errno;
        }
    }

    if (set_mask & CHIMERA_VFS_ATTR_SIZE) {
        if (ftruncate(fd, attr->va_size) < 0) {
            return errno;
        }
    }

    if (set_mask & (CHIMERA_VFS_ATTR_ATIME | CHIMERA_VFS_ATTR_MTIME)) {
        struct timespec times[2];
        int             have_any = 0;

        if (set_mask & CHIMERA_VFS_ATTR_ATIME) {
            if (attr->va_atime.tv_nsec == CHIMERA_VFS_TIME_NOW) {
                times[0].tv_nsec = UTIME_NOW;
                have_any         = 1;
            } else if (attr->va_atime.tv_nsec == CHIMERA_VFS_TIME_OMIT) {
                times[0].tv_nsec = UTIME_OMIT;
            } else {
                times[0] = attr->va_atime;
                have_any = 1;
            }
        } else {
            times[0].tv_nsec = UTIME_OMIT;
        }

        if (set_mask & CHIMERA_VFS_ATTR_MTIME) {
            if (attr->va_mtime.tv_nsec == CHIMERA_VFS_TIME_NOW) {
                times[1].tv_nsec = UTIME_NOW;
                have_any         = 1;
            } else if (attr->va_mtime.tv_nsec == CHIMERA_VFS_TIME_OMIT) {
                times[1].tv_nsec = UTIME_OMIT;
            } else {
                times[1] = attr->va_mtime;
                have_any = 1;
            }
        } else {
            times[1].tv_nsec = UTIME_OMIT;
        }

        if (have_any && futimens(fd, times) < 0) {
            return errno;
        }
    }

    return 0;
} /* chimera_io_uring_set_open_attrs */

static void
chimera_io_uring_reap(
    struct evpl                    *evpl,
    struct chimera_io_uring_thread *thread)
{
    struct io_uring_cqe               *cqe;
    int                                rc;
    int                                parent_fd;
    struct chimera_vfs_request        *request;
    struct chimera_vfs_request_handle *handle;
    struct statx                      *dir_stx, *stx;
    const char                        *name;
    struct io_uring_sqe               *sqe;
    void                              *scratch;

    while (io_uring_peek_cqe(&thread->ring, &cqe) == 0) {

        handle = (struct chimera_vfs_request_handle *) cqe->user_data;

        request = container_of(handle, struct chimera_vfs_request, handle[handle->slot]);

        switch (request->opcode) {
            case CHIMERA_VFS_OP_LOOKUP_AT:
                if (cqe->res >= 0) {
                    request->status = CHIMERA_VFS_OK;

                    stx = (struct statx *) request->plugin_data;

                    name = (char *) (stx + 1);

                    parent_fd = request->lookup_at.handle->vfs_private;

                    chimera_linux_map_child_attrs_statx(CHIMERA_VFS_FH_MAGIC_IO_URING,
                                                        request,
                                                        &request->lookup_at.r_attr,
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

                        rc = chimera_io_uring_set_open_attrs(
                            cqe->res, request->open_at.set_attr);
                        if (rc != 0) {
                            request->status = chimera_linux_errno_to_status(rc);
                            break;
                        }

                        sqe = chimera_io_uring_get_sqe(thread, request, 1, 0);

                        if (request->open_at.flags & CHIMERA_VFS_OPEN_NOFOLLOW) {
                            io_uring_prep_statx(sqe, cqe->res, "",
                                                AT_EMPTY_PATH | AT_SYMLINK_NOFOLLOW | AT_STATX_SYNC_AS_STAT,
                                                CHIMERA_IO_URING_STATX_MASK, stx);
                        } else {
                            io_uring_prep_statx(sqe, parent_fd, name, AT_STATX_SYNC_AS_STAT,
                                                CHIMERA_IO_URING_STATX_MASK, stx);
                        }

                        sqe = chimera_io_uring_get_sqe(thread, request, 2, 0);

                        io_uring_prep_statx(sqe, parent_fd, "", AT_EMPTY_PATH | AT_STATX_SYNC_AS_STAT,
                                            CHIMERA_IO_URING_STATX_MASK, dir_stx);

                        evpl_defer(thread->evpl, &thread->deferral);

                    } else {
                        request->status = chimera_linux_errno_to_status(-cqe->res);
                    }
                } else if (handle->slot == 1) {
                    if (cqe->res == 0) {
                        dir_stx = (struct statx *) request->plugin_data;
                        stx     = (struct statx *) (dir_stx + 1);
                        name    = (char *) (stx + 1);

                        if (request->open_at.flags & CHIMERA_VFS_OPEN_NOFOLLOW) {
                            chimera_linux_map_child_attrs_statx(CHIMERA_VFS_FH_MAGIC_IO_URING,
                                                                request,
                                                                &request->open_at.r_attr,
                                                                request->open_at.r_vfs_private,
                                                                "",
                                                                stx);
                        } else {
                            parent_fd = request->open_at.handle->vfs_private;

                            chimera_linux_map_child_attrs_statx(CHIMERA_VFS_FH_MAGIC_IO_URING,
                                                                request,
                                                                &request->open_at.r_attr,
                                                                parent_fd,
                                                                name,
                                                                stx);
                        }
                    } else {
                        request->status = chimera_linux_errno_to_status(-cqe->res);
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
            case CHIMERA_VFS_OP_REMOVE_AT:
                /* Remove is now synchronous, so this should never be reached */
                chimera_io_uring_abort("io_uring completion for synchronous remove operation");
                break;
            case CHIMERA_VFS_OP_MKDIR_AT:
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

                    TERM_STR(fullname, request->mkdir_at.name, request->mkdir_at.name_len, scratch);

                    parent_fd = request->mkdir_at.handle->vfs_private;

                    /* SMB (AUTH_ATTR) does not impersonate the caller, so the
                     * mkdir runs as the server identity and the new directory
                     * is owned by the server -- which then fails the owner-
                     * default access check on any subsequent open by the
                     * client.  chimera_setup_credential injected the caller's
                     * UID/GID into set_attr above; apply them now so the
                     * directory ends up owned by the requesting principal,
                     * matching what the linux backend does via
                     * chimera_linux_set_attrs.  fchownat is a fast metadata
                     * call (no I/O) and is gated on AUTH_ATTR injection. */
                    if (request->status == CHIMERA_VFS_OK) {
                        uint64_t mask = request->mkdir_at.set_attr->va_set_mask;
                        if (mask & (CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_GID)) {
                            uid_t uid = (mask & CHIMERA_VFS_ATTR_UID)
                                ? (uid_t) request->mkdir_at.set_attr->va_uid : (uid_t) -1;
                            gid_t gid = (mask & CHIMERA_VFS_ATTR_GID)
                                ? (gid_t) request->mkdir_at.set_attr->va_gid : (gid_t) -1;
                            if (fchownat(parent_fd, fullname, uid, gid, 0) < 0) {
                                request->status = chimera_linux_errno_to_status(errno);
                            }
                        }
                    }

                    sqe = chimera_io_uring_get_sqe(thread, request, 1, 0);

                    io_uring_prep_statx(sqe, parent_fd, fullname, AT_STATX_SYNC_AS_STAT,
                                        CHIMERA_IO_URING_STATX_MASK, stx);

                    sqe = chimera_io_uring_get_sqe(thread, request, 2, 0);

                    io_uring_prep_statx(sqe, parent_fd, "", AT_EMPTY_PATH | AT_STATX_SYNC_AS_STAT,
                                        CHIMERA_IO_URING_STATX_MASK, dir_stx);

                    evpl_defer(thread->evpl, &thread->deferral);
                } else if (handle->slot == 1) {
                    if (cqe->res == 0) {
                        dir_stx   = (struct statx *) request->plugin_data;
                        stx       = (struct statx *) (dir_stx + 1);
                        name      = (char *) (stx + 1);
                        parent_fd = request->mkdir_at.handle->vfs_private;

                        chimera_linux_map_child_attrs_statx(CHIMERA_VFS_FH_MAGIC_IO_URING,
                                                            request,
                                                            &request->mkdir_at.r_attr,
                                                            parent_fd,
                                                            name,
                                                            stx);
                    }
                } else if (handle->slot == 2) {
                    if (cqe->res == 0) {
                        dir_stx = (struct statx *) request->plugin_data;
                        chimera_linux_statx_to_attr(&request->mkdir_at.r_dir_post_attr, dir_stx);
                    }
                }
                break;
            case CHIMERA_VFS_OP_READ:
                if (handle->slot == 0) {
                    /* The VFS core owns request->read.iov (allocated on the
                     * connection thread); it trims/releases the buffers on
                     * completion, so io_uring only reports the outcome here. */
                    if (cqe->res >= 0) {
                        request->status        = CHIMERA_VFS_OK;
                        request->read.r_length = cqe->res;
                        request->read.r_eof    = (cqe->res < request->read.length);
                    } else {
                        request->status = chimera_linux_errno_to_status(-cqe->res);
                    }
                } else {
                    if (cqe->res == 0) {
                        stx = (struct statx *) request->plugin_data;

                        if (request->read.r_attr.va_req_mask & CHIMERA_VFS_ATTR_MASK_STAT) {
                            chimera_linux_statx_to_attr(&request->read.r_attr, stx);
                        }

                        if (request->status == CHIMERA_VFS_OK) {
                            request->read.r_eof =
                                (request->read.length > 0 &&
                                 request->read.offset + request->read.r_length >= stx->stx_size);
                        } else if (request->status == CHIMERA_VFS_EINVAL &&
                                   request->read.offset >= stx->stx_size) {
                            request->status        = CHIMERA_VFS_OK;
                            request->read.r_length = 0;
                            request->read.r_niov   = 0;
                            request->read.r_eof    = 1;
                        }
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
            if (request->opcode == CHIMERA_VFS_OP_OPEN_AT ||
                request->opcode == CHIMERA_VFS_OP_MKDIR_AT) {
                chimera_restore_privilege(request->cred);
            }
            thread->inflight--;
            request->complete(request);
        }

        io_uring_cqe_seen(&thread->ring, cqe);

    } /* while peek_cqe */

    while (thread->pending_requests && thread->inflight < thread->max_inflight) {
        request = thread->pending_requests;
        DL_DELETE(thread->pending_requests, request);
        chimera_io_uring_dispatch(request, thread);
    }
} /* chimera_io_uring_reap */

/*
 * Doorbell callback: used while the evpl loop is sleeping in epoll.  The ring's
 * eventfd is registered in that state, so a completion wakes the loop here.
 */
static void
chimera_io_uring_complete(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell)
{
    struct chimera_io_uring_thread *thread =
        container_of(doorbell, struct chimera_io_uring_thread, doorbell);

    chimera_io_uring_reap(evpl, thread);
} /* chimera_io_uring_complete */

/*
 * Poll callbacks: while the evpl loop is in busy-poll (spin) mode it calls
 * chimera_io_uring_poll every iteration to drain the CQ with no syscall.  The
 * enter/exit callbacks unregister/re-register the ring eventfd so the kernel
 * does not bother signaling it during the spin phase (mirrors libevpl's own
 * io_uring framework in ext/libevpl/src/core/io_uring/io_uring.c).
 */
static void
chimera_io_uring_poll(
    struct evpl *evpl,
    void        *private_data)
{
    chimera_io_uring_reap(evpl, private_data);
} /* chimera_io_uring_poll */

static void
chimera_io_uring_poll_enter(
    struct evpl *evpl,
    void        *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;

    io_uring_unregister_eventfd(&thread->ring);
    chimera_io_uring_reap(evpl, thread);
} /* chimera_io_uring_poll_enter */

static void
chimera_io_uring_poll_exit(
    struct evpl *evpl,
    void        *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;

    io_uring_register_eventfd(&thread->ring, evpl_doorbell_fd(&thread->doorbell));
    chimera_io_uring_reap(evpl, thread);
} /* chimera_io_uring_poll_exit */

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

    /* Probe registered-personality support (kernel >= 5.18): register the
     * server's own creds, and if that succeeds personalities are available --
     * unregister the probe and remember the capability.  Otherwise we fall back
     * to per-thread impersonation around each async open/mkdir. */
    rc = io_uring_register_personality(&thread->ring);
    if (rc >= 0) {
        thread->personality_supported = 1;
        io_uring_unregister_personality(&thread->ring, rc);
    }

    evpl_deferral_init(&thread->deferral,
                       chimera_io_uring_flush,
                       thread);

    thread->poll = evpl_add_poll(evpl,
                                 chimera_io_uring_poll_enter,
                                 chimera_io_uring_poll_exit,
                                 chimera_io_uring_poll,
                                 thread);

    return thread;
} /* io_uring_thread_init */ /* io_uring_thread_init */

static void
chimera_io_uring_thread_destroy(void *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;
    int                             i;

    linux_mount_table_destroy(&thread->mount_table);

    for (i = 0; i < CHIMERA_IO_URING_MAX_PERSONALITIES; i++) {
        if (thread->personalities[i].valid) {
            io_uring_unregister_personality(&thread->ring,
                                            thread->personalities[i].id);
        }
    }

    evpl_remove_poll(thread->evpl, thread->poll);
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

    io_uring_prep_statx(sqe, fd, "",
                        AT_EMPTY_PATH | AT_SYMLINK_NOFOLLOW | AT_STATX_SYNC_AS_STAT,
                        CHIMERA_IO_URING_STATX_MASK, stx);

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

    rc = chimera_setup_credential(request->cred, request->setattr.set_attr);
    if (rc != 0) {
        request->status = chimera_linux_errno_to_status(rc);
        request->complete(request);
        return;
    }

    if (request->setattr.set_attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) {
#ifdef HAVE_FCHMODAT_AT_SYMLINK_NOFOLLOW
        // Use fchmodat with AT_SYMLINK_NOFOLLOW on kernels >= 6.6
        rc = fchmodat(fd, "", request->setattr.set_attr->va_mode,
                      AT_SYMLINK_NOFOLLOW | AT_EMPTY_PATH);
#else  /* ifdef HAVE_FCHMODAT_AT_SYMLINK_NOFOLLOW */
        // Fall back to chmod via /proc/self/fd on older kernels
        // (fchmod doesn't work on O_PATH file descriptors)
        {
            char procpath[64];
            snprintf(procpath, sizeof(procpath), "/proc/self/fd/%d", fd);
            rc = chmod(procpath, request->setattr.set_attr->va_mode);
        }
#endif /* ifdef HAVE_FCHMODAT_AT_SYMLINK_NOFOLLOW */

        if (rc) {
            chimera_io_uring_error("io_uring_setattr: fchmod(%o) failed: %s",
                                   request->setattr.set_attr->va_mode,
                                   strerror(errno));

            chimera_restore_privilege(request->cred);
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

            chimera_restore_privilege(request->cred);
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

            chimera_restore_privilege(request->cred);
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

            chimera_restore_privilege(request->cred);
            request->status = chimera_linux_errno_to_status(errno);
            request->complete(request);
            return;
        }

        request->setattr.set_attr->va_set_mask |= CHIMERA_VFS_ATTR_GID;
    }

    if (request->setattr.set_attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE) {
        // A size that does not fit in a signed off_t cannot be set on any
        // backing filesystem (truncate would see it as negative and fail
        // EINVAL).  Report it as "file too large" so NFS returns FBIG rather
        // than INVAL.
        if (request->setattr.set_attr->va_size > (uint64_t) INT64_MAX) {
            chimera_restore_privilege(request->cred);
            request->status = CHIMERA_VFS_EFBIG;
            request->complete(request);
            return;
        }

        // fd might be O_PATH which doesn't support ftruncate directly,
        // so use truncate() on /proc/self/fd/N path which follows the symlink
        char procpath[64];
        snprintf(procpath, sizeof(procpath), "/proc/self/fd/%d", fd);
        rc = truncate(procpath, request->setattr.set_attr->va_size);

        if (rc) {
            chimera_io_uring_error("io_uring_setattr: truncate(%ld) failed: %s",
                                   request->setattr.set_attr->va_size,
                                   strerror(errno));

            chimera_restore_privilege(request->cred);
            request->status = chimera_linux_errno_to_status(errno);
            request->complete(request);
            return;
        }

        request->setattr.set_attr->va_set_mask |= CHIMERA_VFS_ATTR_SIZE;
    }

    if (request->setattr.set_attr->va_set_mask & (CHIMERA_VFS_ATTR_ATIME | CHIMERA_VFS_ATTR_MTIME)) {
        struct timespec times[2];
        int             have_any = 0;

        if (request->setattr.set_attr->va_set_mask & CHIMERA_VFS_ATTR_ATIME) {
            if (request->setattr.set_attr->va_atime.tv_nsec == CHIMERA_VFS_TIME_NOW) {
                times[0].tv_nsec = UTIME_NOW;
                have_any         = 1;
            } else if (request->setattr.set_attr->va_atime.tv_nsec == CHIMERA_VFS_TIME_OMIT) {
                times[0].tv_nsec = UTIME_OMIT;
            } else {
                times[0] = request->setattr.set_attr->va_atime;
                have_any = 1;
            }

            request->setattr.set_attr->va_set_mask |= CHIMERA_VFS_ATTR_ATIME;
        } else {
            times[0].tv_nsec = UTIME_OMIT;
        }

        if (request->setattr.set_attr->va_set_mask & CHIMERA_VFS_ATTR_MTIME) {
            if (request->setattr.set_attr->va_mtime.tv_nsec == CHIMERA_VFS_TIME_NOW) {
                times[1].tv_nsec = UTIME_NOW;
                have_any         = 1;
            } else if (request->setattr.set_attr->va_mtime.tv_nsec == CHIMERA_VFS_TIME_OMIT) {
                times[1].tv_nsec = UTIME_OMIT;
            } else {
                times[1] = request->setattr.set_attr->va_mtime;
                have_any = 1;
            }

            request->setattr.set_attr->va_set_mask |= CHIMERA_VFS_ATTR_MTIME;
        } else {
            times[1].tv_nsec = UTIME_OMIT;
        }

        if (have_any) {
            rc = utimensat(fd, "", times, AT_SYMLINK_NOFOLLOW | AT_EMPTY_PATH);

            if (rc) {
                chimera_io_uring_error("io_uring_setattr: utimensat() failed: %s",
                                       strerror(errno));

                chimera_restore_privilege(request->cred);
                request->status = chimera_linux_errno_to_status(errno);
                request->complete(request);
                return;
            }
        }
    }

    chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_IO_URING,
                            &request->setattr.r_post_attr,
                            fd);

    chimera_restore_privilege(request->cred);
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
chimera_io_uring_lookup_at(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;
    struct io_uring_sqe            *sqe;
    int                             parent_fd;
    char                           *scratch = (char *) request->plugin_data;
    struct statx                   *stx;

    parent_fd = (int) request->lookup_at.handle->vfs_private;

    stx = (struct statx *) scratch;

    scratch += sizeof(*stx);

    TERM_STR(fullname, request->lookup_at.component, request->lookup_at.component_len, scratch);

    sqe = chimera_io_uring_get_sqe(thread, request, 0, 0);

    io_uring_prep_statx(sqe, parent_fd, fullname, AT_SYMLINK_NOFOLLOW | AT_STATX_SYNC_AS_STAT,
                        CHIMERA_IO_URING_STATX_MASK, stx);

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

    rc = chimera_setup_credential(request->cred, NULL);
    if (rc != 0) {
        request->status = chimera_linux_errno_to_status(rc);
        request->complete(request);
        return;
    }

    if (thread->readdir_verifier) {
        struct stat st;

        rc = fstat(fd, &st);

        if (rc == 0) {
            uint64_t mtime_verf = chimera_linux_mtime_to_verifier(&st);

            if (request->readdir.verifier &&
                request->readdir.verifier != mtime_verf) {
                chimera_restore_privilege(request->cred);
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
        chimera_restore_privilege(request->cred);
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }

    dir = fdopendir(dup_fd);

    if (!dir) {
        chimera_io_uring_error("io_uring_readdir: fdopendir() failed: %s",
                               strerror(errno));
        close(dup_fd);
        chimera_restore_privilege(request->cred);
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
    chimera_restore_privilege(request->cred);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* io_uring_readdir */ /* io_uring_readdir */

static void
chimera_io_uring_open_fh(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;
    int                             flags  = 0;
    int                             fd;

    --thread->inflight;


    if (request->open_fh.flags & CHIMERA_VFS_OPEN_PATH) {
        flags |= O_PATH;
    }

    if (request->open_fh.flags & CHIMERA_VFS_OPEN_DIRECTORY) {
        flags |= O_DIRECTORY;
    }
    if (request->open_fh.flags & (CHIMERA_VFS_OPEN_READ_ONLY | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY)) {
        flags |= O_RDONLY;
    } else {
        flags |= O_RDWR;
    }

    fd = linux_open_by_handle(&thread->mount_table,
                              request->fh,
                              request->fh_len,
                              flags);

    if (fd < 0) {
        if (errno == ENOTDIR && (request->open_fh.flags & CHIMERA_VFS_OPEN_DIRECTORY)) {
            int         probe_fd = linux_open_by_handle(&thread->mount_table,
                                                        request->fh,
                                                        request->fh_len,
                                                        O_PATH | O_NOFOLLOW);
            struct stat st;

            if (probe_fd >= 0) {
                if (fstat(probe_fd, &st) == 0 && S_ISLNK(st.st_mode)) {
                    request->status = CHIMERA_VFS_ESYMLINK;
                } else {
                    request->status = CHIMERA_VFS_ENOTDIR;
                }
                close(probe_fd);
            } else {
                request->status = CHIMERA_VFS_ENOTDIR;
            }
        } else {
            request->status = chimera_linux_errno_to_status(errno);
        }
        request->complete(request);
        return;
    }

    request->open_fh.r_vfs_private = fd;

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
    int                             flags, rc, personality;
    uint32_t                        mode;
    char                           *scratch = (char *) request->plugin_data;
    struct io_uring_sqe            *sqe;

    scratch += 2 * sizeof(struct statx);

    TERM_STR(fullname, request->open_at.name, request->open_at.namelen, scratch);

    parent_fd = request->open_at.handle->vfs_private;

    flags = 0;

    if (request->open_at.flags & (CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY)) {
        flags |= O_RDONLY;
    } else if ((request->open_at.flags & CHIMERA_VFS_OPEN_READ_ONLY) &&
               !(request->open_at.set_attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE)) {
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

    if (request->open_at.flags & CHIMERA_VFS_OPEN_NOFOLLOW) {
        flags = O_PATH | O_NOFOLLOW;
    }

    if (request->open_at.flags & CHIMERA_VFS_OPEN_EXCLUSIVE) {
        flags |= O_EXCL;
    }

    /* Carry the caller's identity on the SQE via a registered personality so it
     * is applied per-op in the kernel; only fall back to impersonating this
     * thread (server creds, AUTH_ATTR injection, or kernels without
     * personalities) when no personality is used. */
    personality = chimera_io_uring_get_personality(thread, request->cred);
    if (personality <= 0) {
        rc = chimera_setup_credential(request->cred, request->open_at.set_attr);
        if (rc != 0) {
            --thread->inflight;
            request->status = chimera_linux_errno_to_status(rc);
            request->complete(request);
            return;
        }
    }

    /* chimera_setup_credential injects the AUTH_ATTR caller's UID/GID into
     * set_attr; ownership is only meaningful when this open creates the object.
     * Drop the injected UID/GID for a non-creating open so set_open_attrs does
     * not fchown an already-existing file (which would also fail on the O_PATH
     * metadata-only handle used for some opens). */
    if (!(request->open_at.flags & CHIMERA_VFS_OPEN_CREATE)) {
        request->open_at.set_attr->va_set_mask &=
            ~(CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_GID);
    }

    sqe = chimera_io_uring_get_sqe(thread, request, 0, 0);

    if (request->open_at.set_attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) {
        mode                                    = request->open_at.set_attr->va_mode;
        request->open_at.set_attr->va_set_mask &= ~CHIMERA_VFS_ATTR_MODE;
    } else {
        mode = 0600;
    }

    io_uring_prep_openat(sqe, parent_fd, fullname, flags, mode);

    /* prep_openat zeroes sqe->personality, so set it after. */
    if (personality > 0) {
        sqe->personality = personality;
    }

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
chimera_io_uring_mkdir_at(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;
    int                             fd, rc, personality;
    uint32_t                        mode;
    char                           *scratch  = (char *) request->plugin_data;
    struct chimera_vfs_attrs       *set_attr = request->mkdir_at.set_attr;
    struct io_uring_sqe            *sqe;

    scratch += sizeof(struct statx) * 2;

    TERM_STR(fullname, request->mkdir_at.name, request->mkdir_at.name_len, scratch);

    fd = request->mkdir_at.handle->vfs_private;

    personality = chimera_io_uring_get_personality(thread, request->cred);
    if (personality <= 0) {
        rc = chimera_setup_credential(request->cred, set_attr);
        if (rc != 0) {
            --thread->inflight;
            request->status = chimera_linux_errno_to_status(rc);
            request->complete(request);
            return;
        }
    }

    sqe = chimera_io_uring_get_sqe(thread, request, 0, 0);

    if (set_attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) {
        mode = set_attr->va_mode;
    } else {
        mode = S_IRWXU;
    }

    io_uring_prep_mkdirat(sqe, fd, fullname, mode);

    /* prep_mkdirat zeroes sqe->personality, so set it after. */
    if (personality > 0) {
        sqe->personality = personality;
    }

    evpl_defer(thread->evpl, &thread->deferral);
} /* chimera_io_uring_mkdir_at */

static void
chimera_io_uring_mknod_at(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;
    int                             fd, rc;
    char                           *scratch = (char *) request->plugin_data;
    uint32_t                        mode;
    dev_t                           dev = 0;

    --thread->inflight;

    TERM_STR(fullname, request->mknod_at.name, request->mknod_at.name_len, scratch);

    fd = request->mknod_at.handle->vfs_private;

    if (request->mknod_at.set_attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) {
        mode = request->mknod_at.set_attr->va_mode;
    } else {
        mode = S_IFREG | 0644;
    }

    if (request->mknod_at.set_attr->va_set_mask & CHIMERA_VFS_ATTR_RDEV) {
        dev = makedev(request->mknod_at.set_attr->va_rdev >> 32,
                      request->mknod_at.set_attr->va_rdev & 0xFFFFFFFF);
    }

    chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_IO_URING,
                            &request->mknod_at.r_dir_pre_attr,
                            fd);

    rc = chimera_setup_credential(request->cred, request->mknod_at.set_attr);
    if (rc != 0) {
        request->status = chimera_linux_errno_to_status(rc);
        request->complete(request);
        return;
    }

    rc = mknodat(fd, fullname, mode, dev);

    int mknodat_errno = errno;

    chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_IO_URING,
                            &request->mknod_at.r_dir_post_attr,
                            fd);

    if (rc < 0) {
        chimera_restore_privilege(request->cred);

        if (mknodat_errno == EEXIST) {
            chimera_linux_map_child_attrs(CHIMERA_VFS_FH_MAGIC_IO_URING,
                                          request,
                                          &request->mknod_at.r_attr,
                                          fd,
                                          fullname);
        }

        request->status = chimera_linux_errno_to_status(mknodat_errno);
        request->complete(request);
        return;
    }

    /* SMB (AUTH_ATTR) does not impersonate the caller, so mknodat ran as
     * the server identity; apply the caller's UID/GID that
     * chimera_setup_credential injected.  Mirrors symlink_at's fchownat. */
    {
        uint64_t mask = request->mknod_at.set_attr->va_set_mask;
        if (mask & (CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_GID)) {
            uid_t uid = (mask & CHIMERA_VFS_ATTR_UID)
                ? (uid_t) request->mknod_at.set_attr->va_uid : (uid_t) -1;
            gid_t gid = (mask & CHIMERA_VFS_ATTR_GID)
                ? (gid_t) request->mknod_at.set_attr->va_gid : (gid_t) -1;
            if (fchownat(fd, fullname, uid, gid, AT_SYMLINK_NOFOLLOW) < 0) {
                chimera_restore_privilege(request->cred);
                request->status = chimera_linux_errno_to_status(errno);
                request->complete(request);
                return;
            }
        }
    }

    chimera_linux_map_child_attrs(CHIMERA_VFS_FH_MAGIC_IO_URING,
                                  request,
                                  &request->mknod_at.r_attr,
                                  fd,
                                  fullname);

    chimera_restore_privilege(request->cred);
    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_io_uring_mknod_at */

static void
chimera_io_uring_remove_at(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;
    int                             fd, rc;
    char                           *scratch = (char *) request->plugin_data;

    --thread->inflight;

    TERM_STR(fullname, request->remove_at.name, request->remove_at.namelen, scratch);

    fd = request->remove_at.handle->vfs_private;

    /* Get the file handle before removing, so VFS can invalidate attribute cache */
    request->remove_at.r_removed_attr.va_req_mask = CHIMERA_VFS_ATTR_FH;

    chimera_linux_map_child_attrs(CHIMERA_VFS_FH_MAGIC_IO_URING,
                                  request,
                                  &request->remove_at.r_removed_attr,
                                  fd,
                                  fullname);

    rc = chimera_setup_credential(request->cred, NULL);
    if (rc != 0) {
        request->status = chimera_linux_errno_to_status(rc);
        request->complete(request);
        return;
    }

    rc = unlinkat(fd, fullname, 0);

    if (rc == -1 && errno == EISDIR) {
        rc = unlinkat(fd, fullname, AT_REMOVEDIR);
    }

    int unlinkat_errno = errno;
    chimera_restore_privilege(request->cred);

    if (rc) {
        request->status = chimera_linux_errno_to_status(unlinkat_errno);
    } else {
        request->status = CHIMERA_VFS_OK;
    }

    request->complete(request);
} /* chimera_io_uring_remove_at */ /* chimera_io_uring_remove_at */

static void
chimera_io_uring_read(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;
    struct io_uring_sqe            *sqe;
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
        request->read.r_eof    = 0;
        if (request->read.r_attr.va_req_mask & CHIMERA_VFS_ATTR_MASK_STAT) {
            sqe = chimera_io_uring_get_sqe(thread, request, 1, 0);
            io_uring_prep_statx(sqe, fd, "", AT_EMPTY_PATH | AT_STATX_SYNC_AS_STAT,
                                CHIMERA_IO_URING_STATX_MASK, stx);
            evpl_defer(thread->evpl, &thread->deferral);
        } else {
            /* No attrs requested and no readv to submit: complete inline. */
            --thread->inflight;
            request->complete(request);
        }
        return;
    }

    sqe = chimera_io_uring_get_sqe(thread, request, 0, 0);

    /* The VFS core allocated the read buffers on the connection thread
     * (io_uring does not advertise CAP_READ_PROVIDES_BUFFERS) and placed them
     * in request->read.iov, padded to a 4 KiB boundary on both sides.  Build
     * the readv vector from them: offset the first buffer by aligned_prefix so
     * file offset `offset` lands where the VFS core trims to on completion, and
     * cap the vector at the requested length.  The VFS core owns the buffers --
     * io_uring neither allocates nor releases them. */
    chimera_io_uring_abort_if(request->read.buffers_provided == 0,
                              "io_uring read dispatched without VFS-provided buffers");

    stx      = (struct statx *) scratch;
    scratch += sizeof(*stx);

    iov = (struct iovec *) scratch;

    for (i = 0; left && i < request->read.buffers_provided; i++) {

        iov[i].iov_base = request->read.iov[i].data;
        iov[i].iov_len  = request->read.iov[i].length;

        if (i == 0) {
            iov[i].iov_base = (char *) iov[i].iov_base + request->read.aligned_prefix;
            iov[i].iov_len -= request->read.aligned_prefix;
        }

        if (iov[i].iov_len > (size_t) left) {
            iov[i].iov_len = left;
        }

        left -= iov[i].iov_len;
    }

    fd = (int) request->read.handle->vfs_private;

    io_uring_prep_readv(sqe, fd, iov, i, request->read.offset);

    sqe = chimera_io_uring_get_sqe(thread, request, 1, 0);
    io_uring_prep_statx(sqe, fd, "", AT_EMPTY_PATH | AT_STATX_SYNC_AS_STAT,
                        CHIMERA_IO_URING_STATX_MASK, stx);

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
chimera_io_uring_allocate(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    int fd   = (int) request->allocate.handle->vfs_private;
    int mode = 0;
    int rc;

    if (request->allocate.flags & CHIMERA_VFS_ALLOCATE_DEALLOCATE) {
        mode = FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE;
    }

    rc = fallocate(fd, mode, request->allocate.offset, request->allocate.length);

    if (rc < 0) {
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }

    chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_IO_URING, &request->allocate.r_post_attr, fd);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);

} /* chimera_io_uring_allocate */

static void
chimera_io_uring_copy_range(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;
    int                             src_fd, dst_fd;
    loff_t                          src_off, dst_off;
    uint64_t                        remaining;
    uint64_t                        copied = 0;
    ssize_t                         rc;

    --thread->inflight;

    if (request->copy_range.src_handle->vfs_module !=
        request->copy_range.dst_handle->vfs_module) {
        request->status = CHIMERA_VFS_ENOTSUP;
        request->complete(request);
        return;
    }

    src_fd    = (int) request->copy_range.src_handle->vfs_private;
    dst_fd    = (int) request->copy_range.dst_handle->vfs_private;
    src_off   = (loff_t) request->copy_range.src_offset;
    dst_off   = (loff_t) request->copy_range.dst_offset;
    remaining = request->copy_range.length;

    while (remaining > 0) {
        rc = copy_file_range(src_fd, &src_off, dst_fd, &dst_off, remaining, 0);

        if (rc < 0) {
            if (errno == EINTR) {
                continue;
            }
            request->status = chimera_linux_errno_to_status(errno);
            request->complete(request);
            return;
        }

        if (rc == 0) {
            break;
        }

        copied    += (uint64_t) rc;
        remaining -= (uint64_t) rc;
    }

    chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_IO_URING,
                            &request->copy_range.r_post_attr, dst_fd);

    request->copy_range.r_length = copied;
    request->status              = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_io_uring_copy_range */

static void
chimera_io_uring_clone_range(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;
    int                             src_fd, dst_fd;
    struct file_clone_range         args;
    int                             rc;

    --thread->inflight;

    if (request->clone_range.src_handle->vfs_module !=
        request->clone_range.dst_handle->vfs_module) {
        request->status = CHIMERA_VFS_ENOTSUP;
        request->complete(request);
        return;
    }

    src_fd = (int) request->clone_range.src_handle->vfs_private;
    dst_fd = (int) request->clone_range.dst_handle->vfs_private;

    args.src_fd      = src_fd;
    args.src_offset  = request->clone_range.src_offset;
    args.src_length  = request->clone_range.length;
    args.dest_offset = request->clone_range.dst_offset;

    rc = ioctl(dst_fd, FICLONERANGE, &args);

    if (rc < 0) {
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }

    chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_IO_URING,
                            &request->clone_range.r_post_attr, dst_fd);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_io_uring_clone_range */

static void
chimera_io_uring_seek(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    int   fd = (int) request->seek.handle->vfs_private;
    int   whence;
    off_t result;

    if (request->seek.what == 0) {
        whence = SEEK_DATA;
    } else {
        whence = SEEK_HOLE;
    }

    result = lseek(fd, request->seek.offset, whence);

    if (result < 0) {
        if (errno == ENXIO) {
            request->seek.r_eof    = 1;
            request->seek.r_offset = 0;
            request->status        = CHIMERA_VFS_OK;
        } else {
            request->status = chimera_linux_errno_to_status(errno);
        }
        request->complete(request);
        return;
    }

    request->seek.r_eof    = 0;
    request->seek.r_offset = result;
    request->status        = CHIMERA_VFS_OK;
    request->complete(request);

} /* chimera_io_uring_seek */

static void
chimera_io_uring_symlink_at(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;
    int                             fd, rc;
    char                           *scratch  = (char *) request->plugin_data;
    struct chimera_vfs_attrs       *set_attr = request->symlink_at.set_attr;

    --thread->inflight;

    if (request->symlink_at.namelen + request->symlink_at.targetlen + 2 >
        CHIMERA_VFS_PLUGIN_DATA_SIZE) {
        request->status = CHIMERA_VFS_ENAMETOOLONG;
        request->complete(request);
        return;
    }

    TERM_STR(fullname, request->symlink_at.name, request->symlink_at.namelen, scratch);
    TERM_STR(target, request->symlink_at.target, request->symlink_at.targetlen, scratch);

    fd = request->symlink_at.handle->vfs_private;

    /* symlinks do not support chmod, remove mode from attr set mask */
    set_attr->va_set_mask &= ~CHIMERA_VFS_ATTR_MODE;

    rc = chimera_setup_credential(request->cred, set_attr);
    if (rc != 0) {
        request->status = chimera_linux_errno_to_status(rc);
        request->complete(request);
        return;
    }

    rc = symlinkat(target, fd, fullname);

    if (rc < 0) {
        chimera_restore_privilege(request->cred);
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }

    /* Set ownership on the symlink if requested */
    if (set_attr->va_set_mask & (CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_GID)) {
        uid_t uid = (set_attr->va_set_mask & CHIMERA_VFS_ATTR_UID) ? (uid_t) set_attr->va_uid : (uid_t) -1;
        gid_t gid = (set_attr->va_set_mask & CHIMERA_VFS_ATTR_GID) ? (gid_t) set_attr->va_gid : (gid_t) -1;
        rc = fchownat(fd, fullname, uid, gid, AT_SYMLINK_NOFOLLOW);
        if (rc < 0) {
            chimera_restore_privilege(request->cred);
            request->status = chimera_linux_errno_to_status(errno);
            request->complete(request);
            return;
        }
    }
    chimera_restore_privilege(request->cred);

    chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_IO_URING,
                            &request->symlink_at.r_dir_post_attr,
                            fd);

    chimera_linux_map_child_attrs(CHIMERA_VFS_FH_MAGIC_IO_URING,
                                  request,
                                  &request->symlink_at.r_attr,
                                  fd,
                                  fullname);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_io_uring_symlink_at */

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

    chimera_linux_map_attrs(CHIMERA_VFS_FH_MAGIC_IO_URING, &request->readlink.r_attr, fd);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_io_uring_readlink */

static void
chimera_io_uring_rename_at(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;
    int                             old_fd, new_fd, rc;
    char                           *scratch = (char *) request->plugin_data;

    --thread->inflight;


    TERM_STR(fullname, request->rename_at.name, request->rename_at.namelen, scratch);
    TERM_STR(full_newname, request->rename_at.new_name, request->rename_at.new_namelen, scratch);

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
                                  request->rename_at.new_fh,
                                  request->rename_at.new_fhlen,
                                  O_PATH | O_RDONLY | O_NOFOLLOW);

    if (new_fd < 0) {
        close(old_fd);
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }

    rc = chimera_setup_credential(request->cred, NULL);
    if (rc != 0) {
        close(old_fd);
        close(new_fd);
        request->status = chimera_linux_errno_to_status(rc);
        request->complete(request);
        return;
    }

    rc = renameat(old_fd, fullname, new_fd, full_newname);

    if (rc < 0) {
        request->status = chimera_linux_errno_to_status(errno);
    } else {
        request->status = CHIMERA_VFS_OK;
    }

    chimera_restore_privilege(request->cred);
    close(old_fd);
    close(new_fd);

    request->complete(request);
} /* chimera_io_uring_rename_at */

static void
chimera_io_uring_link_at(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;
    int                             fd, dir_fd, rc;
    char                           *scratch = (char *) request->plugin_data;

    --thread->inflight;

    TERM_STR(fullname, request->link_at.name, request->link_at.namelen, scratch);

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
                                  request->link_at.dir_fh,
                                  request->link_at.dir_fhlen,
                                  O_PATH | O_RDONLY | O_NOFOLLOW);

    if (dir_fd < 0) {
        close(fd);
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }

    rc = linkat(fd, "", dir_fd, fullname, AT_EMPTY_PATH);

    if (rc < 0) {
        if (errno == EPERM) {
            struct stat st;

            if (fstat(fd, &st) == 0 && S_ISDIR(st.st_mode)) {
                request->status = CHIMERA_VFS_EISDIR;
            } else {
                request->status = CHIMERA_VFS_EPERM;
            }
        } else {
            request->status = chimera_linux_errno_to_status(errno);
        }
    } else {
        request->status = CHIMERA_VFS_OK;
    }

    close(fd);
    close(dir_fd);

    request->complete(request);

} /* chimera_io_uring_link_at */

static void
chimera_io_uring_lock(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;
    int                             fd     = (int) request->lock.handle->vfs_private;
    int                             cmd;
    struct flock                    fl = { 0 };
    int                             rc;

    --thread->inflight;

    switch (request->lock.lock_type) {
        case CHIMERA_VFS_LOCK_READ:
            fl.l_type = F_RDLCK;
            break;
        case CHIMERA_VFS_LOCK_WRITE:
            fl.l_type = F_WRLCK;
            break;
        case CHIMERA_VFS_LOCK_UNLOCK:
            fl.l_type = F_UNLCK;
            break;
        default:
            request->status = chimera_linux_errno_to_status(EINVAL);
            request->complete(request);
            return;
    } /* switch */

    fl.l_whence = request->lock.whence;
    if (request->lock.whence == SEEK_END) {
        fl.l_start = (off_t) (int64_t) request->lock.offset;
        fl.l_len   = (off_t) (int64_t) request->lock.length;
    } else {
        fl.l_start = (off_t) request->lock.offset;
        fl.l_len   = (off_t) request->lock.length; /* 0 = to EOF */
    }
    fl.l_pid = 0;

    if (request->lock.flags & CHIMERA_VFS_LOCK_TEST) {
        cmd = F_GETLK;
    } else if (request->lock.flags & CHIMERA_VFS_LOCK_WAIT) {
        /*
         * NOTE: F_SETLKW is a blocking syscall.  Unlike lseek or fallocate,
         * which complete quickly, this call can block indefinitely until the
         * contending lock is released.  While blocked here, this io_uring
         * thread cannot process any other requests.  A proper async fix would
         * offload the wait to a dedicated thread and deliver the completion
         * via the evpl doorbell.
         */
        cmd = F_SETLKW;
    } else {
        cmd = F_SETLK;
    }

    rc = fcntl(fd, cmd, &fl);

    if (rc < 0) {
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }

    if (request->lock.flags & CHIMERA_VFS_LOCK_TEST) {
        if (fl.l_type == F_UNLCK) {
            request->lock.r_conflict_type   = CHIMERA_VFS_LOCK_UNLOCK;
            request->lock.r_conflict_offset = 0;
            request->lock.r_conflict_length = 0;
            request->lock.r_conflict_pid    = 0;
        } else {
            request->lock.r_conflict_type = (fl.l_type == F_RDLCK)
                ? CHIMERA_VFS_LOCK_READ
                : CHIMERA_VFS_LOCK_WRITE;
            request->lock.r_conflict_offset = fl.l_start;
            request->lock.r_conflict_length = fl.l_len;
            request->lock.r_conflict_pid    = fl.l_pid;
        }
    }

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_io_uring_lock */

static void
chimera_io_uring_get_xattr(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;
    int                             fd     = (int) request->get_xattr.handle->vfs_private;
    char                           *name;
    ssize_t                         rc;

    --thread->inflight;

    name = malloc(request->get_xattr.namelen + 1);
    memcpy(name, request->get_xattr.name, request->get_xattr.namelen);
    name[request->get_xattr.namelen] = '\0';

    rc = fgetxattr(fd, name, request->get_xattr.value,
                   request->get_xattr.value_maxlen);
    free(name);

    if (rc < 0) {
        request->status = chimera_linux_errno_to_status(errno);
    } else {
        request->get_xattr.r_value_len = rc;
        request->status                = CHIMERA_VFS_OK;
    }

    request->complete(request);
} /* chimera_io_uring_get_xattr */

static void
chimera_io_uring_set_xattr(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;
    int                             fd     = (int) request->set_xattr.handle->vfs_private;
    char                           *name;
    int                             flags = 0;
    int                             rc;
    struct stat                     st;

    --thread->inflight;

    if (request->set_xattr.option == CHIMERA_VFS_XATTR_CREATE) {
        flags = XATTR_CREATE;
    } else if (request->set_xattr.option == CHIMERA_VFS_XATTR_REPLACE) {
        flags = XATTR_REPLACE;
    }

    if (fstat(fd, &st) < 0) {
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }
    chimera_linux_stat_to_attr(&request->set_xattr.r_pre_attr, &st);

    name = malloc(request->set_xattr.namelen + 1);
    memcpy(name, request->set_xattr.name, request->set_xattr.namelen);
    name[request->set_xattr.namelen] = '\0';

    rc = fsetxattr(fd, name, request->set_xattr.value,
                   request->set_xattr.value_len, flags);
    free(name);

    if (rc < 0) {
        request->status = chimera_linux_errno_to_status(errno);
    } else if (fstat(fd, &st) < 0) {
        request->status = chimera_linux_errno_to_status(errno);
    } else {
        chimera_linux_stat_to_attr(&request->set_xattr.r_post_attr, &st);
        request->status = CHIMERA_VFS_OK;
    }

    request->complete(request);
} /* chimera_io_uring_set_xattr */

static void
chimera_io_uring_list_xattrs(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;
    int                             fd     = (int) request->list_xattrs.handle->vfs_private;
    ssize_t                         rc;
    char                           *p, *end;

    --thread->inflight;

    rc = flistxattr(fd, request->list_xattrs.buffer,
                    request->list_xattrs.max_bytes);
    if (rc < 0) {
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }
    if (rc > request->list_xattrs.max_bytes) {
        request->status = CHIMERA_VFS_ERANGE;
        request->complete(request);
        return;
    }

    request->list_xattrs.r_len    = rc;
    request->list_xattrs.r_eof    = 1;
    request->list_xattrs.r_cookie = 0;

    p   = request->list_xattrs.buffer;
    end = p + rc;
    while (p < end) {
        request->list_xattrs.r_count++;
        p += strlen(p) + 1;
    }

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_io_uring_list_xattrs */

static void
chimera_io_uring_remove_xattr(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_io_uring_thread *thread = private_data;
    int                             fd     = (int) request->remove_xattr.handle->vfs_private;
    char                           *name;
    int                             rc;
    struct stat                     st;

    --thread->inflight;

    if (fstat(fd, &st) < 0) {
        request->status = chimera_linux_errno_to_status(errno);
        request->complete(request);
        return;
    }
    chimera_linux_stat_to_attr(&request->remove_xattr.r_pre_attr, &st);

    name = malloc(request->remove_xattr.namelen + 1);
    memcpy(name, request->remove_xattr.name, request->remove_xattr.namelen);
    name[request->remove_xattr.namelen] = '\0';

    rc = fremovexattr(fd, name);
    free(name);

    if (rc < 0) {
        request->status = chimera_linux_errno_to_status(errno);
    } else if (fstat(fd, &st) < 0) {
        request->status = chimera_linux_errno_to_status(errno);
    } else {
        chimera_linux_stat_to_attr(&request->remove_xattr.r_post_attr, &st);
        request->status = CHIMERA_VFS_OK;
    }

    request->complete(request);
} /* chimera_io_uring_remove_xattr */

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
        case CHIMERA_VFS_OP_LOOKUP_AT:
            chimera_io_uring_lookup_at(request, private_data);
            break;
        case CHIMERA_VFS_OP_GETATTR:
            chimera_io_uring_getattr(request, private_data);
            break;
        case CHIMERA_VFS_OP_OPEN_FH:
            chimera_io_uring_open_fh(request, private_data);
            break;
        case CHIMERA_VFS_OP_OPEN_AT:
            chimera_io_uring_open_at(request, private_data);
            break;
        case CHIMERA_VFS_OP_CLOSE:
            chimera_io_uring_close(request, private_data);
            break;
        case CHIMERA_VFS_OP_MKDIR_AT:
            chimera_io_uring_mkdir_at(request, private_data);
            break;
        case CHIMERA_VFS_OP_MKNOD_AT:
            chimera_io_uring_mknod_at(request, private_data);
            break;
        case CHIMERA_VFS_OP_READDIR:
            chimera_io_uring_readdir(request, private_data);
            break;
        case CHIMERA_VFS_OP_REMOVE_AT:
            chimera_io_uring_remove_at(request, private_data);
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
        case CHIMERA_VFS_OP_SYMLINK_AT:
            chimera_io_uring_symlink_at(request, private_data);
            break;
        case CHIMERA_VFS_OP_READLINK:
            chimera_io_uring_readlink(request, private_data);
            break;
        case CHIMERA_VFS_OP_RENAME_AT:
            chimera_io_uring_rename_at(request, private_data);
            break;
        case CHIMERA_VFS_OP_LINK_AT:
            chimera_io_uring_link_at(request, private_data);
            break;
        case CHIMERA_VFS_OP_SETATTR:
            chimera_io_uring_setattr(request, private_data);
            break;
        case CHIMERA_VFS_OP_ALLOCATE:
            chimera_io_uring_allocate(request, private_data);
            break;
        case CHIMERA_VFS_OP_COPY_RANGE:
            chimera_io_uring_copy_range(request, private_data);
            break;
        case CHIMERA_VFS_OP_CLONE_RANGE:
            chimera_io_uring_clone_range(request, private_data);
            break;
        case CHIMERA_VFS_OP_SEEK:
            chimera_io_uring_seek(request, private_data);
            break;
        case CHIMERA_VFS_OP_LOCK:
            chimera_io_uring_lock(request, private_data);
            break;
        case CHIMERA_VFS_OP_GET_XATTR:
            chimera_io_uring_get_xattr(request, private_data);
            break;
        case CHIMERA_VFS_OP_SET_XATTR:
            chimera_io_uring_set_xattr(request, private_data);
            break;
        case CHIMERA_VFS_OP_LIST_XATTRS:
            chimera_io_uring_list_xattrs(request, private_data);
            break;
        case CHIMERA_VFS_OP_REMOVE_XATTR:
            chimera_io_uring_remove_xattr(request, private_data);
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
    .name         = "io_uring",
    .fh_magic     = CHIMERA_VFS_FH_MAGIC_IO_URING,
    .capabilities = CHIMERA_VFS_CAP_OPEN_PATH_REQUIRED | CHIMERA_VFS_CAP_OPEN_FILE_REQUIRED | CHIMERA_VFS_CAP_FS |
        CHIMERA_VFS_CAP_FS_PATH_OP | CHIMERA_VFS_CAP_FS_LOCK |
        CHIMERA_VFS_CAP_COPY_RANGE | CHIMERA_VFS_CAP_CLONE_RANGE |
        CHIMERA_VFS_CAP_DELEGATES_DAC | CHIMERA_VFS_CAP_XATTR,
    .init           = chimera_io_uring_init,
    .destroy        = chimera_io_uring_destroy,
    .thread_init    = chimera_io_uring_thread_init,
    .thread_destroy = chimera_io_uring_thread_destroy,
    .dispatch       = chimera_io_uring_dispatch,
};
