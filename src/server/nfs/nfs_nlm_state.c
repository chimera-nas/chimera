// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>
#include <time.h>

/*
 * Recursively create path components, equivalent to mkdir -p.
 * Returns 0 on success, -1 on error (errno set).  EEXIST is not an error.
 */
static int
mkdir_p(
    const char *path,
    mode_t      mode)
{
    size_t plen = strlen(path) + 1;
    char  *tmp  = malloc(plen);
    char  *p;
    size_t len;

    if (!tmp) {
        return -1;
    }
    memcpy(tmp, path, plen);
    len = plen - 1;
    if (len > 0 && tmp[len - 1] == '/') {
        tmp[len - 1] = '\0';
    }
    for (p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = '\0';
            if (mkdir(tmp, mode) != 0 && errno != EEXIST) {
                free(tmp);
                return -1;
            }
            *p = '/';
        }
    }
    if (mkdir(tmp, mode) != 0 && errno != EEXIST) {
        free(tmp);
        return -1;
    }
    free(tmp);
    return 0;
} /* mkdir_p */

#include "nfs_nlm_state.h"
#include "vfs/vfs_release.h"
#include "nfs_internal.h"

/* state_dir + '/' + sanitized hostname (up to LM_MAXSTRLEN) + ".nlm" + '\0' */
#define NLM_CLIENT_PATH_MAX (sizeof(((struct nlm_state *) 0)->state_dir) + LM_MAXSTRLEN + 6)

/*
 * Replace characters unsafe for filenames with '_'.
 * dest must be at least len+1 bytes.
 */
static void
sanitize_hostname(
    char       *dest,
    const char *src,
    size_t      len)
{
    size_t i;

    for (i = 0; i < len && src[i] != '\0'; i++) {
        char c = src[i];
        if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
            (c >= '0' && c <= '9') || c == '.' || c == '-') {
            dest[i] = c;
        } else {
            dest[i] = '_';
        }
    }
    dest[i] = '\0';
} /* sanitize_hostname */

static void
nlm_state_client_file_path(
    struct nlm_state *state,
    const char       *hostname,
    char             *path,
    size_t            path_size)
{
    char safe[LM_MAXSTRLEN + 1];

    sanitize_hostname(safe, hostname, sizeof(safe) - 1);
    snprintf(path, path_size, "%s/%s.nlm", state->state_dir, safe);
} /* nlm_state_client_file_path */

void
nlm_state_init(
    struct nlm_state *state,
    const char       *state_dir)
{
    pthread_mutex_init(&state->mutex, NULL);
    state->clients   = NULL;
    state->in_grace  = 0;
    state->grace_end = 0;
    snprintf(state->state_dir, sizeof(state->state_dir), "%s", state_dir);

    chimera_nfs_debug("NLM state init: state_dir=%s", state->state_dir);
} /* nlm_state_init */

void
nlm_state_destroy(struct nlm_state *state)
{
    struct nlm_client     *client, *tmp_client;
    struct nlm_lock_entry *entry, *tmp_entry;

#ifndef __clang_analyzer__

    /* HASH_DEL blows clangs mind so we disable this block under analyzer */

    HASH_ITER(hh, state->clients, client, tmp_client)
    {
        HASH_DEL(state->clients, client);
        DL_FOREACH_SAFE(client->locks, entry, tmp_entry)
        {
            DL_DELETE(client->locks, entry);
            nlm_lock_entry_free(entry);
        }
        free(client);
    }

#endif /* ifndef __clang_analyzer__ */

    pthread_mutex_destroy(&state->mutex);
} /* nlm_state_destroy */

void
nlm_state_load(struct nlm_state *state)
{
    /* Persistence is intentionally out of scope in this pass.  All lease
    * state lives in memory and is lost on server restart.  We still
    * remove any stale .nlm files from the legacy state directory so
    * they don't accumulate, but we do not honor a grace period based
    * on them (the in-memory state is empty, so there is nothing to
    * reclaim).  See plan: "all the lease related metadata should be
    * tracked only in memory even when its supposed to be persistent". */
    DIR           *dir;
    struct dirent *ent;
    struct stat    sb;
    char           path[NLM_CLIENT_PATH_MAX];

    if (stat(state->state_dir, &sb) != 0) {
        return;
    }

    dir = opendir(state->state_dir);
    if (!dir) {
        return;
    }

    while ((ent = readdir(dir)) != NULL) {
        size_t nlen = strlen(ent->d_name);
        if (nlen > 4 && strcmp(ent->d_name + nlen - 4, ".nlm") == 0) {
            snprintf(path, sizeof(path), "%s/%s",
                     state->state_dir, ent->d_name);
            unlink(path);
        }
    }
    closedir(dir);
} /* nlm_state_load */

struct nlm_client *
nlm_client_lookup_or_create(
    struct nlm_state *state,
    const char       *hostname)
{
    struct nlm_client *client;

    HASH_FIND_STR(state->clients, hostname, client);
    if (client) {
        return client;
    }

    chimera_nfs_debug("NLM: creating new client state for '%s'", hostname);

    client = calloc(1, sizeof(*client));
    if (!client) {
        return NULL;
    }
    client->magic = NLM_CLIENT_MAGIC;
    snprintf(client->hostname, sizeof(client->hostname), "%s", hostname);
    HASH_ADD_STR(state->clients, hostname, client);
    return client;
} /* nlm_client_lookup_or_create */

/* In-memory only: persistence is deliberately deferred.  These functions
 * remain as no-op stubs so existing call sites do not need to be touched. */
void
nlm_state_persist_client_disabled(
    struct nlm_state  *state,
    struct nlm_client *client);

void
nlm_state_persist_client(
    struct nlm_state  *state,
    struct nlm_client *client)
{
    (void) state;
    (void) client;
} /* nlm_state_persist_client */

#if 0
static void
nlm_state_persist_client_disabled(
    struct nlm_state  *state,
    struct nlm_client *client)
{
    char                   path[NLM_CLIENT_PATH_MAX];
    char                   tmp_path[NLM_CLIENT_PATH_MAX + 4]; /* + ".tmp" */
    char                   hostname[LM_MAXSTRLEN + 1];
    FILE                  *fp;
    struct nlm_file_hdr    hdr;
    struct nlm_file_entry *snapshot = NULL;
    struct nlm_lock_entry *entry;
    uint32_t               count = 0;
    uint32_t               i;

    /* Phase 1: snapshot the client's lock list under the mutex so that
     * the file I/O in Phase 2 does not hold the mutex. */
    pthread_mutex_lock(&state->mutex);

    snprintf(hostname, sizeof(hostname), "%s", client->hostname);

    DL_FOREACH(client->locks, entry)
    {
        /* Only persist confirmed (non-pending) locks */
        if (!entry->pending) {
            count++;
        }
    }

    if (count == 0) {
        pthread_mutex_unlock(&state->mutex);
        nlm_state_client_file_path(state, hostname, path, sizeof(path));
        chimera_nfs_debug("NLM persist: '%s' has no confirmed locks, removing state file",
                          hostname);
        unlink(path);
        return;
    }

    snapshot = calloc(count, sizeof(*snapshot));
    if (!snapshot) {
        pthread_mutex_unlock(&state->mutex);
        chimera_nfs_debug("NLM persist: out of memory for snapshot of '%s'", hostname);
        return;
    }

    i = 0;
    DL_FOREACH(client->locks, entry)
    {
        if (entry->pending) {
            continue;
        }
        snapshot[i].fh_len = entry->fh_len;
        memcpy(snapshot[i].fh, entry->fh, entry->fh_len);
        snapshot[i].oh_len = entry->oh_len;
        memcpy(snapshot[i].oh, entry->oh, entry->oh_len);
        snapshot[i].svid      = entry->svid;
        snapshot[i].exclusive = entry->exclusive ? 1 : 0;
        snapshot[i].offset    = entry->offset;
        snapshot[i].length    = entry->length;
        i++;
    }

    pthread_mutex_unlock(&state->mutex);

    /* Phase 2: write snapshot to disk without holding the mutex. */
    nlm_state_client_file_path(state, hostname, path, sizeof(path));
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", path);

    chimera_nfs_debug("NLM persist: '%s' has %u lock(s), writing %s", hostname, count, path);

    fp = fopen(tmp_path, "wb");
    if (!fp) {
        chimera_nfs_debug("NLM: failed to open state file %s for writing: %s",
                          tmp_path, strerror(errno));
        free(snapshot);
        return;
    }

    hdr.magic       = NLM_FILE_MAGIC;
    hdr.version     = NLM_FILE_VERSION;
    hdr.num_entries = count;
    hdr.pad         = 0;

    if (fwrite(&hdr, sizeof(hdr), 1, fp) != 1) {
        fclose(fp);
        unlink(tmp_path);
        free(snapshot);
        return;
    }

    for (i = 0; i < count; i++) {
        if (fwrite(&snapshot[i], sizeof(snapshot[i]), 1, fp) != 1) {
            fclose(fp);
            unlink(tmp_path);
            free(snapshot);
            return;
        }
    }

    fclose(fp);
    free(snapshot);

    if (rename(tmp_path, path) != 0) {
        chimera_nfs_debug("NLM: failed to rename state file %s -> %s: %s",
                          tmp_path, path, strerror(errno));
        unlink(tmp_path);
    }
} /* nlm_state_persist_client_disabled */
#endif /* if 0 */

void
nlm_state_remove_client_file(
    struct nlm_state *state,
    const char       *hostname)
{
    /* No-op in this pass — persistence deferred (see nlm_state_load). */
    (void) state;
    (void) hostname;
} /* nlm_state_remove_client_file */

/*
 * Release every lock held (or being acquired) by `client`.
 *
 * IMPORTANT: the caller must NOT hold state->mutex.  This function takes it
 * internally for the detach phase and DROPS it before releasing leases.  That
 * matters because chimera_vfs_lease_release() pumps the file's pending queue,
 * which can synchronously fire another waiter's NLM acquire callback -- and
 * that callback itself takes state->mutex.  Releasing leases under the mutex
 * would therefore self-deadlock.
 */
void
nlm_client_release_all_locks(
    struct nlm_state          *state,
    struct nlm_client         *client,
    struct chimera_vfs_thread *vfs_thread,
    struct chimera_vfs_state  *vfs_state,
    struct chimera_vfs_cred   *cred)
{
    struct nlm_lock_entry *entry, *tmp;
    struct nlm_lock_entry *reap  = NULL;  /* detached entries this call owns */
    int                    count = 0;

    (void) cred;
#ifdef __clang_analyzer__
    /* The list-walk bodies below are elided under the analyzer (utlist macro
     * false positive); keep these from tripping unused-variable diagnostics. */
    (void) entry;
    (void) tmp;
    (void) reap;
    (void) count;
    (void) vfs_thread;
    (void) vfs_state;
#endif /* __clang_analyzer__ */

    /* Phase 1 (under the lock): cancel queued blocking tickets and detach every
     * entry we own onto a private `reap` list.  Entries whose acquire callback
     * is firing concurrently (cancel lost the race) are left on client->locks
     * for that callback to remove and free. */
    pthread_mutex_lock(&state->mutex);

    DL_FOREACH(client->locks, entry)
    {
        count++;
    }
    chimera_nfs_debug("NLM: releasing all %d lock(s) for client '%s'", count,
                      client->hostname);

    /* The DL_DELETE/DL_APPEND utlist macros trip a scan-build null-deref false
     * positive on the list head's prev field; guarded the same way as
     * nlm_state_destroy in this file. */
#ifndef __clang_analyzer__
    DL_FOREACH_SAFE(client->locks, entry, tmp)
    {
        /* A still-pending entry whose blocking acquire is queued in vfs_state
         * (an NLM blocking LOCK not yet granted) must have its ticket cancelled
         * before we free it -- otherwise the pending pump would later fire the
         * acquire callback on freed memory.  chimera_vfs_lease_acquire_cancel is
         * the atomic arbiter: true exactly once if it dequeued the ticket before
         * the pump claimed it.
         *
         *   - cancel == true : the callback will NOT fire; WE own the entry.
         *     Its original LOCK RPC already received NLM4_BLOCKED, so no reply is
         *     owed; free the acquire ctx the callback would have freed.
         *   - cancel == false: the acquire callback is firing concurrently and
         *     will DL_DELETE + free this entry itself -- leave it linked, skip. */
        if (entry->pending && !entry->lease_inserted && entry->file_state) {
            if (!chimera_vfs_lease_acquire_cancel(vfs_state, &entry->ticket)) {
                continue;
            }
            free(entry->ticket.private_data);
            entry->ticket.private_data = NULL;
            entry->pending             = false;
        }

        DL_DELETE(client->locks, entry);
        entry->next = NULL;
        entry->prev = NULL;
        DL_APPEND(reap, entry);
    }
#endif /* ifndef __clang_analyzer__ */

    pthread_mutex_unlock(&state->mutex);

    /* Phase 2 (no lock held): release leases (which may pump and fire other
     * waiters' callbacks), drop file_state refs, close handles, free. */
#ifndef __clang_analyzer__
    DL_FOREACH_SAFE(reap, entry, tmp)
    {
        if (entry->lease_inserted) {
            chimera_vfs_lease_release(vfs_state, entry->file_state, &entry->lease);
            entry->lease_inserted = false;
        }
        if (entry->file_state) {
            chimera_vfs_state_put(vfs_state, entry->file_state);
            entry->file_state = NULL;
        }
        if (entry->handle) {
            chimera_vfs_release(vfs_thread, entry->handle);
        }
        DL_DELETE(reap, entry);
        nlm_lock_entry_free(entry);
    }
#endif /* ifndef __clang_analyzer__ */
} /* nlm_client_release_all_locks */
