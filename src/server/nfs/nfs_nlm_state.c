// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "common/thread.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include "common/dirent.h"
#include <sys/stat.h>
#ifdef _WIN32
#include "common/platform.h"
#endif /* ifdef _WIN32 */
#ifdef _WIN32
#include "common/platform.h"
#else  /* ifdef _WIN32 */
#include <unistd.h>
#endif /* ifdef _WIN32 */
#include <time.h>

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
    struct nlm_state   *state,
    struct chimera_vfs *vfs,
    const char         *state_dir)
{
    evpl_mutex_init(&state->mutex, NULL);
    state->clients   = NULL;
    state->vfs       = vfs;
    state->stopping  = false;
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
        chimera_vfs_lock_domain_destroy(client->domain);
        free(client);
    }

#endif /* ifndef __clang_analyzer__ */

    evpl_mutex_destroy(&state->mutex);
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
    client->domain = chimera_vfs_lock_domain_create(state->vfs);
    if (!client->domain) {
        free(client);
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

void
nlm_state_remove_client_file(
    struct nlm_state *state,
    const char       *hostname)
{
    /* No-op in this pass — persistence deferred (see nlm_state_load). */
    (void) state;
    (void) hostname;
} /* nlm_state_remove_client_file */

/* Client recovery invalidates admission before releasing handle anchors.
 * Pending entries always belong to their compound, even before OPEN returns.
 * Domain retirement only schedules worker completions, never calls NLM inline. */
void
nlm_client_release_all_locks(
    struct nlm_state          *state,
    struct nlm_client         *client,
    struct chimera_vfs_thread *vfs_thread,
    struct chimera_vfs_state  *vfs_state,
    struct chimera_vfs_cred   *cred)
{
    struct nlm_lock_entry *entry, *next, *reap = NULL;

    (void) vfs_state;
    (void) cred;
    evpl_mutex_lock(&state->mutex);
    chimera_vfs_lock_domain_retire_all(client->domain);
    entry         = client->locks;
    client->locks = NULL;
    while (entry) {
        next = entry->next;
        if (entry->pending) {
            entry->reaped = true;
            DL_APPEND(client->locks, entry);
        } else {
            entry->next = reap;
            reap        = entry;
        }
        entry = next;
    }
    evpl_mutex_unlock(&state->mutex);
    while (reap) {
        entry = reap;
        reap  = entry->next;
        chimera_vfs_release(vfs_thread, entry->handle);
        nlm_lock_entry_free(entry);
    }
} /* nlm_client_release_all_locks */

void
nlm_state_shutdown(struct nlm_state *state)
{
    struct nlm_client     *client, *next;
    struct nlm_lock_entry *entry;

    evpl_mutex_lock(&state->mutex);
    state->stopping = true;
    HASH_ITER(hh, state->clients, client, next)
    {
        chimera_vfs_lock_domain_shutdown(NULL, client->domain);
        DL_FOREACH(client->locks, entry)
        {
            if (entry->pending) {
                entry->reaped       = true;
                entry->disconnected = true;
            }
        }
    }
    evpl_mutex_unlock(&state->mutex);
} /* nlm_state_shutdown */

/* Connection pointers may be recycled as soon as notify returns. Mark every
 * pending request before that, including UNLOCKs on a connection that never
 * acquired locks and therefore has no nlm_client private_data. */
void
nlm_state_disconnect(
    struct nlm_state      *state,
    struct evpl_rpc2_conn *conn)
{
    struct nlm_client     *client, *next;
    struct nlm_lock_entry *entry;

    evpl_mutex_lock(&state->mutex);
    HASH_ITER(hh, state->clients, client, next)
    {
        DL_FOREACH(client->locks, entry)
        {
            if (entry->pending && entry->conn == conn) {
                entry->disconnected = true;
            }
        }
    }
    evpl_mutex_unlock(&state->mutex);
} /* nlm_state_disconnect */
