// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdlib.h>
#include <string.h>

#include "posix_internal.h"

struct chimera_posix_client *chimera_posix_global;

struct chimera_posix_client *
chimera_posix_get_global(void)
{
    return chimera_posix_global;
}

int
chimera_posix_errno_from_status(enum chimera_vfs_error status)
{
    if (status == CHIMERA_VFS_OK) {
        return 0;
    }

    return (int) status;
}

void
chimera_posix_request_finish(struct chimera_posix_request *req, enum chimera_vfs_error status)
{
    pthread_mutex_lock(&req->lock);
    req->status = status;
    req->done   = 1;
    pthread_cond_signal(&req->cond);
    pthread_mutex_unlock(&req->lock);
}

struct chimera_posix_request *
chimera_posix_request_create(enum chimera_posix_request_type type)
{
    struct chimera_posix_request *req = calloc(1, sizeof(*req));

    req->type = type;
    pthread_mutex_init(&req->lock, NULL);
    pthread_cond_init(&req->cond, NULL);

    return req;
}

void
chimera_posix_request_destroy(struct chimera_posix_request *req)
{
    if (!req) {
        return;
    }

    pthread_mutex_destroy(&req->lock);
    pthread_cond_destroy(&req->cond);
    free(req);
}

void
chimera_posix_worker_enqueue(struct chimera_posix_worker *worker, struct chimera_posix_request *request)
{
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

struct chimera_posix_worker *
chimera_posix_choose_worker(struct chimera_posix_client *posix)
{
    unsigned int idx = atomic_fetch_add(&posix->next_worker, 1);

    return &posix->workers[idx % (unsigned int) posix->nworkers];
}

int
chimera_posix_wait(struct chimera_posix_request *req)
{
    pthread_mutex_lock(&req->lock);
    while (!req->done) {
        pthread_cond_wait(&req->cond, &req->lock);
    }
    pthread_mutex_unlock(&req->lock);

    return chimera_posix_errno_from_status(req->status);
}

void
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

void
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

unsigned int
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

struct chimera_posix_fd_entry *
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

int
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

void
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

void *
chimera_posix_worker_init(
    struct evpl *evpl,
    void        *private_data)
{
    struct chimera_posix_client *posix  = private_data;
    int                           idx   = atomic_fetch_add(&posix->init_cursor, 1);
    struct chimera_posix_worker  *worker = &posix->workers[idx];

    worker->parent = posix;
    worker->index  = idx;
    worker->evpl   = evpl;

    pthread_mutex_init(&worker->lock, NULL);
    evpl_add_doorbell(evpl, &worker->doorbell, chimera_posix_worker_doorbell);

    worker->client_thread = chimera_client_thread_init(evpl, posix->client);

    return worker;
}

void
chimera_posix_worker_shutdown(
    struct evpl *evpl,
    void        *private_data)
{
    struct chimera_posix_worker *worker = private_data;

    if (worker->client_thread) {
        chimera_client_thread_shutdown(evpl, worker->client_thread);
    }

    evpl_remove_doorbell(evpl, &worker->doorbell);
    pthread_mutex_destroy(&worker->lock);
}

void
chimera_posix_worker_doorbell(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell)
{
    struct chimera_posix_worker *worker = container_of(doorbell, struct chimera_posix_worker, doorbell);

    for (;;) {
        struct chimera_posix_request *request;

        pthread_mutex_lock(&worker->lock);
        request = worker->head;
        if (request) {
            worker->head = request->next;
            if (worker->head == NULL) {
                worker->tail = NULL;
            }
        }
        pthread_mutex_unlock(&worker->lock);

        if (!request) {
            break;
        }

        chimera_posix_dispatch_request(worker, request);
    }
}

