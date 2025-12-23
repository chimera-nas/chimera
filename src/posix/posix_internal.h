// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#ifndef CHIMERA_POSIX_INTERNAL_H
#define CHIMERA_POSIX_INTERNAL_H

#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>

#include <utlist.h>
#include "common/macros.h"
#include "../client/client.h"
#include "../client/client_internal.h"
#include "vfs/vfs.h"
#include "posix.h"

struct chimera_posix_worker;
struct chimera_posix_request;

typedef void (*chimera_posix_request_callback)(
    struct chimera_posix_worker  *worker,
    struct chimera_posix_request *request);

struct chimera_posix_request {
    pthread_mutex_t                  lock;
    pthread_cond_t                   cond;
    int                              done;
    enum chimera_vfs_error           status;
    ssize_t                          result;
    struct chimera_vfs_open_handle  *handle;
    struct chimera_stat              st;
    int                              target_len;
    chimera_posix_request_callback   callback;
    struct chimera_posix_request    *next;
    union {
        struct {
            const char *path;
            int         flags;
        } open;
        struct {
            struct chimera_vfs_open_handle *handle;
        } close;
        struct {
            struct chimera_vfs_open_handle *handle;
            uint64_t                        offset;
            size_t                          length;
            void                           *buf;
        } read;
        struct {
            struct chimera_vfs_open_handle *handle;
            uint64_t                        offset;
            size_t                          length;
            const void                     *buf;
            struct evpl_iovec               iov[CHIMERA_CLIENT_IOV_MAX];
            int                             niov;
        } write;
        struct {
            const char *path;
        } mkdir;
        struct {
            const char *path;
            const char *target;
        } symlink;
        struct {
            const char *oldpath;
            const char *newpath;
        } link;
        struct {
            const char *path;
        } remove;
        struct {
            const char *oldpath;
            const char *newpath;
        } rename;
        struct {
            const char *path;
            char       *buf;
            size_t      buflen;
        } readlink;
        struct {
            const char *path;
        } stat;
        struct {
            const char *mount_path;
            const char *module_name;
            const char *module_path;
        } mount;
        struct {
            const char *mount_path;
        } umount;
    } u;
};

struct chimera_posix_fd_entry {
    struct chimera_vfs_open_handle *handle;
    uint64_t                        offset;
    int                             in_use;
};

struct chimera_posix_worker {
    pthread_mutex_t               lock;
    struct chimera_posix_request *head;
    struct chimera_posix_request *tail;
    struct chimera_posix_request *free_requests;
    struct evpl_doorbell          doorbell;
    struct chimera_client_thread *client_thread;
    struct chimera_posix_client  *parent;
    int                           index;
    struct evpl                  *evpl;
};

struct chimera_posix_client {
    struct chimera_client         *client;
    struct evpl_threadpool        *pool;
    struct chimera_posix_worker   *workers;
    int                            nworkers;
    atomic_uint                    next_worker;
    pthread_mutex_t                fd_lock;
    struct chimera_posix_fd_entry *fds;
    int                            fd_cap;
    int                            next_fd;
    atomic_int                     init_cursor;
    int                            owns_config;
};

extern struct chimera_posix_client *chimera_posix_global;

static FORCE_INLINE struct chimera_posix_client *
chimera_posix_get_global(void)
{
    return chimera_posix_global;
}

static FORCE_INLINE int
chimera_posix_errno_from_status(enum chimera_vfs_error status)
{
    if (status == CHIMERA_VFS_OK) {
        return 0;
    }

    return (int) status;
}

static FORCE_INLINE void
chimera_posix_request_finish(struct chimera_posix_request *req, enum chimera_vfs_error status)
{
    pthread_mutex_lock(&req->lock);
    req->status = status;
    req->done   = 1;
    pthread_cond_signal(&req->cond);
    pthread_mutex_unlock(&req->lock);
}

static FORCE_INLINE struct chimera_posix_request *
chimera_posix_request_create(struct chimera_posix_worker *worker)
{
    struct chimera_posix_request *req;

    if (worker->free_requests) {
        req = worker->free_requests;
        LL_DELETE(worker->free_requests, req);
        req->done   = 0;
        req->status = CHIMERA_VFS_OK;
        req->result = 0;
        req->handle = NULL;
        req->next   = NULL;
    } else {
        req = calloc(1, sizeof(*req));
        pthread_mutex_init(&req->lock, NULL);
        pthread_cond_init(&req->cond, NULL);
    }

    return req;
}

static FORCE_INLINE void
chimera_posix_request_release(
    struct chimera_posix_worker  *worker,
    struct chimera_posix_request *req)
{
    if (!req) {
        return;
    }

    LL_PREPEND(worker->free_requests, req);
}

static FORCE_INLINE void
chimera_posix_worker_enqueue(
    struct chimera_posix_worker    *worker,
    struct chimera_posix_request   *request,
    chimera_posix_request_callback  callback)
{
    request->callback = callback;

    pthread_mutex_lock(&worker->lock);
    if (worker->tail) {
        worker->tail->next = request;
        worker->tail       = request;
    } else {
        worker->head = worker->tail = request;
    }
    pthread_mutex_unlock(&worker->lock);

    evpl_ring_doorbell(&worker->doorbell);
}

static FORCE_INLINE struct chimera_posix_worker *
chimera_posix_choose_worker(struct chimera_posix_client *posix)
{
    unsigned int idx = atomic_fetch_add(&posix->next_worker, 1);

    return &posix->workers[idx % (unsigned int) posix->nworkers];
}

static FORCE_INLINE int
chimera_posix_wait(struct chimera_posix_request *req)
{
    pthread_mutex_lock(&req->lock);
    while (!req->done) {
        pthread_cond_wait(&req->cond, &req->lock);
    }
    pthread_mutex_unlock(&req->lock);

    return chimera_posix_errno_from_status(req->status);
}

static FORCE_INLINE void
chimera_posix_fill_stat(struct stat *dst, const struct chimera_stat *src)
{
    dst->st_dev   = src->st_dev;
    dst->st_ino   = src->st_ino;
    dst->st_mode  = src->st_mode;
    dst->st_nlink = src->st_nlink;
    dst->st_uid   = src->st_uid;
    dst->st_gid   = src->st_gid;
    dst->st_rdev  = src->st_rdev;
    dst->st_size  = src->st_size;
    dst->st_atim  = src->st_atim;
    dst->st_mtim  = src->st_mtim;
    dst->st_ctim  = src->st_ctim;
}

static FORCE_INLINE void
chimera_posix_iovec_memcpy(
    struct evpl_iovec *iov,
    const void        *buf,
    size_t             len)
{
    size_t      copied = 0;
    const char *p      = buf;

    for (int i = 0; copied < len; i++) {
        size_t chunk = iov[i].length;

        if (chunk > len - copied) {
            chunk = len - copied;
        }

        memcpy(iov[i].data, p + copied, chunk);
        copied += chunk;
    }
}

static FORCE_INLINE unsigned int
chimera_posix_to_chimera_flags(int flags)
{
    unsigned int out = 0;

    if (flags & O_CREAT) {
        out |= CHIMERA_VFS_OPEN_CREATE;
    }

    if (flags & O_DIRECTORY) {
        out |= CHIMERA_VFS_OPEN_DIRECTORY;
    }

    if ((flags & O_ACCMODE) == O_RDONLY) {
        out |= CHIMERA_VFS_OPEN_READ_ONLY;
    }

    return out;
}

static FORCE_INLINE struct chimera_posix_fd_entry *
chimera_posix_fd_get(struct chimera_posix_client *posix, int fd)
{
    if (fd < 0 || fd >= posix->fd_cap) {
        return NULL;
    }

    if (!posix->fds[fd].in_use) {
        return NULL;
    }

    return &posix->fds[fd];
}

static FORCE_INLINE int
chimera_posix_fd_put(struct chimera_posix_client *posix, struct chimera_vfs_open_handle *handle)
{
    int fd;

    pthread_mutex_lock(&posix->fd_lock);

    fd = -1;

    for (int i = 0; i < posix->fd_cap; i++) {
        int idx = (posix->next_fd + i) % (posix->fd_cap ? posix->fd_cap : 1);

        if (idx < 3) {
            continue;
        }

        if (!posix->fds[idx].in_use) {
            fd = idx;
            break;
        }
    }

    if (fd == -1) {
        int oldcap = posix->fd_cap ? posix->fd_cap : 0;
        int newcap = oldcap ? oldcap * 2 : 64;
        struct chimera_posix_fd_entry *newfds = realloc(posix->fds, sizeof(*newfds) * newcap);

        if (!newfds) {
            pthread_mutex_unlock(&posix->fd_lock);
            return -1;
        }

        memset(newfds + oldcap, 0, sizeof(*newfds) * (newcap - oldcap));
        posix->fds    = newfds;
        posix->fd_cap = newcap;
        fd            = oldcap ? oldcap : 3;
    }

    posix->next_fd        = fd + 1;
    posix->fds[fd].handle = handle;
    posix->fds[fd].offset = 0;
    posix->fds[fd].in_use = 1;

    pthread_mutex_unlock(&posix->fd_lock);

    return fd;
}

static FORCE_INLINE void
chimera_posix_fd_clear(struct chimera_posix_client *posix, int fd)
{
    pthread_mutex_lock(&posix->fd_lock);
    if (fd >= 0 && fd < posix->fd_cap) {
        posix->fds[fd].handle = NULL;
        posix->fds[fd].offset = 0;
        posix->fds[fd].in_use = 0;
    }
    pthread_mutex_unlock(&posix->fd_lock);
}

void * chimera_posix_worker_init(
    struct evpl *evpl,
    void        *private_data);
void chimera_posix_worker_shutdown(
    struct evpl *evpl,
    void        *private_data);
void chimera_posix_worker_doorbell(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell);

void chimera_posix_exec_open(
    struct chimera_posix_worker  *worker,
    struct chimera_posix_request *request);
void chimera_posix_exec_close(
    struct chimera_posix_worker  *worker,
    struct chimera_posix_request *request);
void chimera_posix_exec_read(
    struct chimera_posix_worker  *worker,
    struct chimera_posix_request *request);
void chimera_posix_exec_write(
    struct chimera_posix_worker  *worker,
    struct chimera_posix_request *request);
void chimera_posix_exec_mkdir(
    struct chimera_posix_worker  *worker,
    struct chimera_posix_request *request);
void chimera_posix_exec_symlink(
    struct chimera_posix_worker  *worker,
    struct chimera_posix_request *request);
void chimera_posix_exec_link(
    struct chimera_posix_worker  *worker,
    struct chimera_posix_request *request);
void chimera_posix_exec_remove(
    struct chimera_posix_worker  *worker,
    struct chimera_posix_request *request);
void chimera_posix_exec_rename(
    struct chimera_posix_worker  *worker,
    struct chimera_posix_request *request);
void chimera_posix_exec_readlink(
    struct chimera_posix_worker  *worker,
    struct chimera_posix_request *request);
void chimera_posix_exec_stat(
    struct chimera_posix_worker  *worker,
    struct chimera_posix_request *request);
void chimera_posix_exec_mount(
    struct chimera_posix_worker  *worker,
    struct chimera_posix_request *request);
void chimera_posix_exec_umount(
    struct chimera_posix_worker  *worker,
    struct chimera_posix_request *request);

#endif /* CHIMERA_POSIX_INTERNAL_H */
