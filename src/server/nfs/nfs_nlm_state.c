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
    DIR           *dir;
    struct dirent *ent;
    int            found = 0;
    struct stat    sb;
    char           path[NLM_CLIENT_PATH_MAX];

    /* Ensure state directory exists */
    if (stat(state->state_dir, &sb) != 0) {
        if (errno == ENOENT) {
            if (mkdir_p(state->state_dir, 0700) != 0) {
                chimera_nfs_debug("NLM: failed to create state directory %s: %s",
                                  state->state_dir, strerror(errno));
            }
        }
        return;
    }

    dir = opendir(state->state_dir);
    if (!dir) {
        return;
    }

    while ((ent = readdir(dir)) != NULL) {
        size_t nlen = strlen(ent->d_name);
        if (nlen > 4 && strcmp(ent->d_name + nlen - 4, ".nlm") == 0) {
            found++;
        }
    }
    closedir(dir);

    if (found == 0) {
        chimera_nfs_debug("NLM: no state files found, no grace period");
    }

    if (found > 0) {
        state->in_grace  = 1;
        state->grace_end = time(NULL) + NLM_GRACE_PERIOD_SECS;
        chimera_nfs_debug("NLM: found %d state file(s), grace period active for %d seconds",
                          found, NLM_GRACE_PERIOD_SECS);

        /* Scan again and delete stale files from a previous crash.
         * On restart the VFS locks are gone; clients must reclaim them.
         * We keep no in-memory state for them -- the grace period is enough. */
        dir = opendir(state->state_dir);
        if (dir) {
            while ((ent = readdir(dir)) != NULL) {
                size_t nlen = strlen(ent->d_name);
                if (nlen > 4 && strcmp(ent->d_name + nlen - 4, ".nlm") == 0) {
                    snprintf(path, sizeof(path), "%s/%s",
                             state->state_dir, ent->d_name);
                    unlink(path);
                }
            }
            closedir(dir);
        }
    }
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

void
nlm_state_persist_client(
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
} /* nlm_state_persist_client */

void
nlm_state_remove_client_file(
    struct nlm_state *state,
    const char       *hostname)
{
    char path[NLM_CLIENT_PATH_MAX];

    nlm_state_client_file_path(state, hostname, path, sizeof(path));
    chimera_nfs_debug("NLM: removing state file for '%s'", hostname);
    unlink(path);
} /* nlm_state_remove_client_file */

void
nlm_client_release_all_locks(
    struct nlm_state          *state,
    struct nlm_client         *client,
    struct chimera_vfs_thread *vfs_thread,
    struct chimera_vfs_cred   *cred)
{
    struct nlm_lock_entry *entry, *tmp;
    int                    count = 0;

    DL_FOREACH(client->locks, entry)
    {
        count++;
    }

    chimera_nfs_debug("NLM: releasing all %d lock(s) for client '%s'", count, client->hostname);

    DL_FOREACH_SAFE(client->locks, entry, tmp)
    {
        /* Releasing the handle closes the underlying FD, which in turn
         * releases all POSIX advisory locks held by this process on that file.
         * Pending entries (handle == NULL) are in-flight VFS operations;
         * skip releasing them here -- the VFS callback will clean up. */
        if (entry->handle) {
            chimera_vfs_release(vfs_thread, entry->handle);
        }
        nlm_lock_entry_free(entry);
    }
    client->locks = NULL;

    /* NOTE: the on-disk state file is NOT removed here.  Callers must call
     * nlm_state_remove_client_file() after releasing the mutex so that
     * blocking file I/O does not occur while the mutex is held.  The client
     * object itself is kept in the hash table; it is freed in
     * nlm_state_destroy() when the server shuts down. */
} /* nlm_client_release_all_locks */
