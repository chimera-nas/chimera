// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>
#include "posix_internal.h"
#include "../client/client_dup.h"

static int
chimera_posix_dup2_locked(
    int oldfd,
    int newfd)
{
    struct chimera_posix_client    *posix  = chimera_posix_get_global();
    struct chimera_posix_worker    *worker = chimera_posix_choose_worker(posix);
    struct chimera_posix_fd_entry  *old_entry, *new_entry;
    struct chimera_vfs_open_handle *handle, *replaced;

    if (newfd < 0 || newfd >= posix->max_fds) {
        errno = EBADF;
        return -1;
    }
    old_entry = chimera_posix_fd_acquire(posix, oldfd, 0);
    if (!old_entry) {
        return -1;
    }
    if (oldfd == newfd) {
        chimera_posix_fd_release(old_entry, 0);
        return newfd;
    }
    handle    = old_entry->handle;
    new_entry = &posix->fds[newfd];

    /* Reserve the target atomically with free-list allocation. CLOSING blocks
     * new users while prior operations drain, including pending SETLKW calls.
     * Never hold the table mutex across worker/backend completion. */
    for (;;) {
        pthread_mutex_lock(&posix->fd_lock);
        pthread_mutex_lock(&new_entry->lock);
        if (!(new_entry->flags & CHIMERA_POSIX_FD_CLOSING)) {
            break;
        }
        pthread_mutex_unlock(&posix->fd_lock);
        while (new_entry->flags & CHIMERA_POSIX_FD_CLOSING) {
            pthread_cond_wait(&new_entry->cond, &new_entry->lock);
        }
        pthread_mutex_unlock(&new_entry->lock);
    }
    replaced = (new_entry->flags & CHIMERA_POSIX_FD_CLOSED) ? NULL : new_entry->handle;
    struct chimera_posix_fd_entry **link = &posix->free_list;
    while (*link && *link != new_entry) {
        link = &(*link)->next;
    }
    if (*link) {
        *link           = new_entry->next;
        new_entry->next = NULL;
    }
    new_entry->flags        |= CHIMERA_POSIX_FD_CLOSING;
    new_entry->pending_close = 1;
    pthread_mutex_unlock(&new_entry->lock);
    pthread_mutex_unlock(&posix->fd_lock);

    if (replaced) {
        chimera_posix_locks_retire_file(posix, replaced);
        pthread_mutex_lock(&new_entry->lock);
        while (new_entry->refcnt) {
            pthread_cond_wait(&new_entry->cond, &new_entry->lock);
        }
        pthread_mutex_unlock(&new_entry->lock);
        (void) chimera_posix_locks_release_file(posix, replaced);
        chimera_posix_close_on_worker(worker, replaced);
    }
    chimera_dup_handle(worker->client_thread, handle);

    pthread_mutex_lock(&posix->fd_lock);
    pthread_mutex_lock(&new_entry->lock);
    chimera_posix_ofd_release_locked(new_entry);
    new_entry->ofd = old_entry->ofd;
    new_entry->ofd->refcnt++;
    new_entry->handle        = handle;
    new_entry->flags         = 0;
    new_entry->refcnt        = 0;
    new_entry->pending_close = 0;
    new_entry->eof_flag      = 0;
    new_entry->error_flag    = 0;
    new_entry->ungetc_char   = -1;
    new_entry->generation++;
    pthread_cond_broadcast(&new_entry->cond);
    pthread_mutex_unlock(&new_entry->lock);
    pthread_mutex_unlock(&posix->fd_lock);
    chimera_posix_fd_release(old_entry, 0);
    return newfd;
} /* chimera_posix_dup2_locked */

/* Serialize replacement before pinning either source. Crossed dup2(a,b) and
 * dup2(b,a) must not each retain the source reference the other is draining. */
SYMBOL_EXPORT int
chimera_posix_dup2(
    int oldfd,
    int newfd)
{
    struct chimera_posix_client *posix = chimera_posix_get_global();

    pthread_mutex_lock(&posix->dup_lock);
    int                          result = chimera_posix_dup2_locked(oldfd, newfd);
    pthread_mutex_unlock(&posix->dup_lock);
    return result;
} /* chimera_posix_dup2 */
