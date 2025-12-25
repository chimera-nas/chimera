// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#ifndef CHIMERA_POSIX_INTERNAL_H
#define CHIMERA_POSIX_INTERNAL_H

#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>

#include <dirent.h>
#include <utlist.h>
#include "common/macros.h"
#include "../client/client.h"
#include "../client/client_internal.h"
#include "vfs/vfs.h"
#include "posix.h"

// Directory stream for opendir/readdir/closedir
struct chimera_posix_dir {
    int                   fd;        // File descriptor for the directory
    uint64_t              cookie;    // Current position cookie
    int                   eof;       // End of directory reached
    int                   buf_valid; // Whether buf contains valid entry
    struct chimera_dirent buf;       // Buffer for current entry
    struct dirent         entry;     // POSIX dirent to return
};

struct chimera_posix_completion {
    pthread_mutex_t                mutex;
    pthread_cond_t                 cond;
    struct chimera_client_request *request;
    enum chimera_vfs_error status;
    int                            done;
};

#define CHIMERA_POSIX_FD_IO_ACTIVE 0x01
#define CHIMERA_POSIX_FD_CLOSING   0x02
#define CHIMERA_POSIX_FD_CLOSED    0x04

struct chimera_posix_fd_entry {
    pthread_mutex_t                 lock;
    pthread_cond_t                  cond;
    struct chimera_vfs_open_handle *handle;
    struct chimera_posix_fd_entry  *next;
    uint64_t                        offset;
    unsigned int                    flags;
    int                             refcnt;
    int                             io_waiters;
    int                             pending_close;
    int                             close_waiters;
    int                             eof_flag;    // For FILE* feof() support
    int                             error_flag;  // For FILE* ferror() support
    int                             ungetc_char; // For FILE* ungetc() support (-1 = none)
} __attribute__((aligned(64)));

// CHIMERA_FILE is a pointer to an fd_entry for FILE* operations
typedef struct chimera_posix_fd_entry CHIMERA_FILE;

struct chimera_posix_worker {
    pthread_mutex_t                lock;
    struct chimera_client_request *pending_requests;
    struct evpl_doorbell           doorbell;
    struct chimera_client_thread  *client_thread;
    struct chimera_posix_client   *parent;
    int                            index;
    struct evpl                   *evpl;
} __attribute__((aligned(64)));

struct chimera_posix_client {
    struct chimera_client         *client;
    struct evpl_threadpool        *pool;
    struct chimera_posix_worker   *workers;
    int                            nworkers;
    atomic_uint                    next_worker;
    pthread_mutex_t                fd_lock;
    struct chimera_posix_fd_entry *fds;
    struct chimera_posix_fd_entry *free_list;
    int                            max_fds;
    atomic_int                     init_cursor;
    int                            owns_config;
} __attribute__((aligned(64)));

extern struct chimera_posix_client *chimera_posix_global;

static FORCE_INLINE struct chimera_posix_client *
chimera_posix_get_global(void)
{
    return chimera_posix_global;
} // chimera_posix_get_global

static FORCE_INLINE int
chimera_posix_errno_from_status(enum chimera_vfs_error status)
{
    if (status == CHIMERA_VFS_OK) {
        return 0;
    }

    return (int) status;
} // chimera_posix_errno_from_status

static FORCE_INLINE void
chimera_posix_complete(
    struct chimera_posix_completion *comp,
    enum chimera_vfs_error           status)
{
    pthread_mutex_lock(&comp->mutex);
    comp->status = status;
    comp->done   = 1;
    pthread_cond_signal(&comp->cond);
    pthread_mutex_unlock(&comp->mutex);
} // chimera_posix_complete

static FORCE_INLINE void
chimera_posix_completion_init(
    struct chimera_posix_completion *comp,
    struct chimera_client_request   *req)
{
    pthread_mutex_init(&comp->mutex, NULL);
    pthread_cond_init(&comp->cond, NULL);
    comp->request = req;
    comp->status  = CHIMERA_VFS_OK;
    comp->done    = 0;

    req->heap_allocated = 0;
} // chimera_posix_completion_init

static FORCE_INLINE void
chimera_posix_completion_destroy(struct chimera_posix_completion *comp)
{
    pthread_mutex_destroy(&comp->mutex);
    pthread_cond_destroy(&comp->cond);
} // chimera_posix_completion_destroy

static FORCE_INLINE void
chimera_posix_worker_enqueue(
    struct chimera_posix_worker    *worker,
    struct chimera_client_request  *request,
    chimera_client_request_callback callback)
{
    request->sync_callback = callback;

    pthread_mutex_lock(&worker->lock);
    DL_APPEND(worker->pending_requests, request);
    pthread_mutex_unlock(&worker->lock);

    evpl_ring_doorbell(&worker->doorbell);
} // chimera_posix_worker_enqueue

static FORCE_INLINE struct chimera_posix_worker *
chimera_posix_choose_worker(struct chimera_posix_client *posix)
{
    unsigned int idx = atomic_fetch_add(&posix->next_worker, 1);

    return &posix->workers[idx % (unsigned int) posix->nworkers];
} // chimera_posix_choose_worker

static FORCE_INLINE int
chimera_posix_wait(struct chimera_posix_completion *comp)
{
    pthread_mutex_lock(&comp->mutex);
    while (!comp->done) {
        pthread_cond_wait(&comp->cond, &comp->mutex);
    }
    pthread_mutex_unlock(&comp->mutex);

    return chimera_posix_errno_from_status(comp->status);
} // chimera_posix_wait

static FORCE_INLINE void
chimera_posix_fill_stat(
    struct stat               *dst,
    const struct chimera_stat *src)
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
} // chimera_posix_fill_stat

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
} // chimera_posix_iovec_memcpy

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
} // chimera_posix_to_chimera_flags

static FORCE_INLINE int
chimera_posix_fd_alloc(
    struct chimera_posix_client    *posix,
    struct chimera_vfs_open_handle *handle)
{
    struct chimera_posix_fd_entry *entry;
    int                            fd;

    pthread_mutex_lock(&posix->fd_lock);

    entry = posix->free_list;

    if (!entry) {
        pthread_mutex_unlock(&posix->fd_lock);
        return -1;
    }

    posix->free_list = entry->next;
    entry->next      = NULL;

    pthread_mutex_unlock(&posix->fd_lock);

    fd = (int) (entry - posix->fds);

    entry->handle        = handle;
    entry->offset        = 0;
    entry->flags         = 0;
    entry->refcnt        = 0;
    entry->io_waiters    = 0;
    entry->pending_close = 0;
    entry->close_waiters = 0;
    entry->eof_flag      = 0;
    entry->error_flag    = 0;
    entry->ungetc_char   = -1;

    return fd;
} // chimera_posix_fd_alloc

static FORCE_INLINE void
chimera_posix_fd_free(
    struct chimera_posix_client *posix,
    int                          fd)
{
    struct chimera_posix_fd_entry *entry;

    if (fd < 0 || fd >= posix->max_fds) {
        return;
    }

    entry = &posix->fds[fd];

    entry->handle      = NULL;
    entry->offset      = 0;
    entry->eof_flag    = 0;
    entry->error_flag  = 0;
    entry->ungetc_char = -1;

    pthread_mutex_lock(&posix->fd_lock);
    entry->next      = posix->free_list;
    posix->free_list = entry;
    pthread_mutex_unlock(&posix->fd_lock);
} // chimera_posix_fd_free

static FORCE_INLINE struct chimera_posix_fd_entry *
chimera_posix_fd_acquire(
    struct chimera_posix_client *posix,
    int                          fd,
    unsigned int                 flags_to_set)
{
    struct chimera_posix_fd_entry *entry;

    if (fd < 0 || fd >= posix->max_fds) {
        errno = EBADF;
        return NULL;
    }

    entry = &posix->fds[fd];

    pthread_mutex_lock(&entry->lock);

    // If CLOSED, return error
    if (entry->flags & CHIMERA_POSIX_FD_CLOSED) {
        pthread_mutex_unlock(&entry->lock);
        errno = EBADF;
        return NULL;
    }

    // If caller wants IO_ACTIVE flag (read/write operations)
    if (flags_to_set & CHIMERA_POSIX_FD_IO_ACTIVE) {
        // Wait for existing IO to complete
        while (entry->flags & CHIMERA_POSIX_FD_IO_ACTIVE) {
            entry->io_waiters++;
            pthread_cond_wait(&entry->cond, &entry->lock);
            entry->io_waiters--;
        }

        // Check if fd was closed or is closing
        if (entry->flags & (CHIMERA_POSIX_FD_CLOSED | CHIMERA_POSIX_FD_CLOSING)) {
            pthread_mutex_unlock(&entry->lock);
            errno = EBADF;
            return NULL;
        }

        entry->flags |= CHIMERA_POSIX_FD_IO_ACTIVE;
    }

    // If caller wants CLOSING flag (close operation)
    if (flags_to_set & CHIMERA_POSIX_FD_CLOSING) {
        // If CLOSING is already set by another thread, wait for it to complete
        if (entry->flags & CHIMERA_POSIX_FD_CLOSING) {
            entry->close_waiters++;
            while (!(entry->flags & CHIMERA_POSIX_FD_CLOSED)) {
                pthread_cond_wait(&entry->cond, &entry->lock);
            }
            entry->close_waiters--;
            pthread_mutex_unlock(&entry->lock);
            errno = EBADF;
            return NULL;
        }

        // Set CLOSING and pending_close
        entry->flags        |= CHIMERA_POSIX_FD_CLOSING;
        entry->pending_close = 1;

        // Wait for existing operations to complete
        while (entry->refcnt > 0) {
            pthread_cond_wait(&entry->cond, &entry->lock);
        }
    }

    entry->refcnt++;
    pthread_mutex_unlock(&entry->lock);

    return entry;
} // chimera_posix_fd_acquire

static FORCE_INLINE void
chimera_posix_fd_release(
    struct chimera_posix_fd_entry *entry,
    unsigned int                   flags_to_clear)
{
    pthread_mutex_lock(&entry->lock);

    // If completing an IO operation
    if (flags_to_clear & CHIMERA_POSIX_FD_IO_ACTIVE) {
        entry->flags &= ~CHIMERA_POSIX_FD_IO_ACTIVE;
        if (entry->io_waiters > 0) {
            pthread_cond_signal(&entry->cond);
        }
    }

    // If completing a close operation
    if (flags_to_clear & CHIMERA_POSIX_FD_CLOSING) {
        entry->flags        &= ~CHIMERA_POSIX_FD_CLOSING;
        entry->flags        |= CHIMERA_POSIX_FD_CLOSED;
        entry->pending_close = 0;
        if (entry->close_waiters > 0) {
            pthread_cond_broadcast(&entry->cond);
        }
    }

    entry->refcnt--;

    // Signal if refcnt is zero and a close is pending
    if (entry->refcnt == 0 && entry->pending_close) {
        pthread_cond_signal(&entry->cond);
    }

    pthread_mutex_unlock(&entry->lock);
} // chimera_posix_fd_release

static FORCE_INLINE off_t
chimera_posix_fd_lseek(
    struct chimera_posix_client *posix,
    int                          fd,
    off_t                        offset,
    int                          whence,
    off_t                        file_size)
{
    struct chimera_posix_fd_entry *entry;
    off_t                          new_offset;

    if (fd < 0 || fd >= posix->max_fds) {
        errno = EBADF;
        return -1;
    }

    entry = &posix->fds[fd];

    pthread_mutex_lock(&entry->lock);

    // If CLOSED, return error
    if (entry->flags & CHIMERA_POSIX_FD_CLOSED) {
        pthread_mutex_unlock(&entry->lock);
        errno = EBADF;
        return -1;
    }

    // Wait for any IO to complete
    while (entry->flags & CHIMERA_POSIX_FD_IO_ACTIVE) {
        entry->io_waiters++;
        pthread_cond_wait(&entry->cond, &entry->lock);
        entry->io_waiters--;
    }

    // Check again if fd was closed while waiting
    if (entry->flags & (CHIMERA_POSIX_FD_CLOSED | CHIMERA_POSIX_FD_CLOSING)) {
        pthread_mutex_unlock(&entry->lock);
        errno = EBADF;
        return -1;
    }

    // Calculate new offset based on whence
    switch (whence) {
        case SEEK_SET:
            new_offset = offset;
            break;
        case SEEK_CUR:
            new_offset = (off_t) entry->offset + offset;
            break;
        case SEEK_END:
            new_offset = file_size + offset;
            break;
        default:
            pthread_mutex_unlock(&entry->lock);
            errno = EINVAL;
            return -1;
    } // switch

    // Validate new offset
    if (new_offset < 0) {
        pthread_mutex_unlock(&entry->lock);
        errno = EINVAL;
        return -1;
    }

    entry->offset = (uint64_t) new_offset;

    pthread_mutex_unlock(&entry->lock);

    return new_offset;
} // chimera_posix_fd_lseek

void * chimera_posix_worker_init(
    struct evpl *evpl,
    void        *private_data);
void chimera_posix_worker_shutdown(
    struct evpl *evpl,
    void        *private_data);
void chimera_posix_worker_doorbell(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell);

// FILE* helper functions
static FORCE_INLINE int
chimera_posix_file_to_fd(
    struct chimera_posix_client *posix,
    CHIMERA_FILE                *file)
{
    if (!file) {
        return -1;
    }

    return (int) (file - posix->fds);
} // chimera_posix_file_to_fd

static FORCE_INLINE CHIMERA_FILE *
chimera_posix_fd_to_file(
    struct chimera_posix_client *posix,
    int                          fd)
{
    if (fd < 0 || fd >= posix->max_fds) {
        return NULL;
    }

    return &posix->fds[fd];
} // chimera_posix_fd_to_file

#endif /* CHIMERA_POSIX_INTERNAL_H */
