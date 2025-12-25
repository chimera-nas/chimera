// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <errno.h>

#include "posix_internal.h"
#include "../client/client_dup.h"

/* Helper to remove an entry from the free list if it's there */
static void
chimera_posix_remove_from_free_list(
    struct chimera_posix_client   *posix,
    struct chimera_posix_fd_entry *target)
{
    struct chimera_posix_fd_entry **pp;

    pthread_mutex_lock(&posix->fd_lock);

    pp = &posix->free_list;

    while (*pp) {
        if (*pp == target) {
            *pp          = target->next;
            target->next = NULL;
            break;
        }
        pp = &(*pp)->next;
    }

    pthread_mutex_unlock(&posix->fd_lock);
} /* chimera_posix_remove_from_free_list */

SYMBOL_EXPORT int
chimera_posix_dup2(
    int oldfd,
    int newfd)
{
    struct chimera_posix_client    *posix  = chimera_posix_get_global();
    struct chimera_posix_worker    *worker = chimera_posix_choose_worker(posix);
    struct chimera_posix_fd_entry  *old_entry;
    struct chimera_posix_fd_entry  *new_entry;
    struct chimera_vfs_open_handle *handle;

    /* Validate newfd range */
    if (newfd < 0 || newfd >= posix->max_fds) {
        errno = EBADF;
        return -1;
    }

    /* Acquire the source fd */
    old_entry = chimera_posix_fd_acquire(posix, oldfd, 0);

    if (!old_entry) {
        errno = EBADF;
        return -1;
    }

    /* If oldfd == newfd and valid, just return newfd (POSIX requirement) */
    if (oldfd == newfd) {
        chimera_posix_fd_release(old_entry, 0);
        return newfd;
    }

    handle = old_entry->handle;

    /* Get the destination fd entry */
    new_entry = &posix->fds[newfd];

    /* Lock the new entry to check if it's in use */
    pthread_mutex_lock(&new_entry->lock);

    /* If newfd is open, we need to close it silently */
    if (new_entry->handle && !(new_entry->flags & CHIMERA_POSIX_FD_CLOSED)) {
        struct chimera_vfs_open_handle *old_handle = new_entry->handle;

        /* Mark as closed */
        new_entry->flags |= CHIMERA_POSIX_FD_CLOSED;
        pthread_mutex_unlock(&new_entry->lock);

        /* Close the old handle */
        chimera_close(worker->client_thread, old_handle);
    } else {
        pthread_mutex_unlock(&new_entry->lock);

        /* If the entry is not open, it might be in the free list - remove it */
        chimera_posix_remove_from_free_list(posix, new_entry);
    }

    /* Increment the opencnt on the source handle */
    chimera_dup_handle(worker->client_thread, handle);

    /* Set up the new fd entry */
    pthread_mutex_lock(&new_entry->lock);
    new_entry->handle      = handle;
    new_entry->offset      = 0;
    new_entry->flags       = 0;
    new_entry->refcnt      = 0;
    new_entry->eof_flag    = 0;
    new_entry->error_flag  = 0;
    new_entry->ungetc_char = -1;
    pthread_mutex_unlock(&new_entry->lock);

    chimera_posix_fd_release(old_entry, 0);

    return newfd;
} /* chimera_posix_dup2 */
