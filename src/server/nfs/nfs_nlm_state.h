// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <stdbool.h>
#include <time.h>
#include "common/thread.h"
#include <uthash.h>
#include <utlist.h>

#include "nfs4_xdr.h"
#include "nlm4_xdr.h"
#include "vfs/vfs.h"
#include "vfs/vfs_lock.h"
#include "vfs/sdk/vfs_cred.h"

/* Magic number stored in nlm_client.magic to distinguish it from nfs4_session *
 * when reading connection private_data in the disconnect handler. */
#define NLM_CLIENT_MAGIC      0x4E4C4D43U /* "NLMC" */

/* Grace period: time after server restart during which only reclaim locks
* are accepted. This matches the grace period used by most NFS servers. */
#define NLM_GRACE_PERIOD_SECS 90

/*
 * One pending request or accepted owner/file handle anchor.
 * The open handle is kept alive for the duration of the lock --
 * POSIX advisory locks are tied to the open file description.
 */
struct nlm_lock_entry {
    uint8_t                         fh[NFS4_FHSIZE];
    uint32_t                        fh_len;
    uint8_t                         vfh[CHIMERA_VFS_FH_SIZE];    /* canonical owner/file anchor */
    uint32_t                        vfh_len;
    uint8_t                         oh[LM_MAXSTRLEN];    /* owner handle (opaque) */
    uint32_t                        oh_len;
    int32_t                         svid;      /* client-side PID */
    uint64_t                        offset;
    uint64_t                        length;    /* 0 == to EOF (POSIX) */
    bool                            exclusive;
    bool                            pending;    /* true while VFS open/acquire in flight */
    struct chimera_vfs_open_handle *handle;    /* kept open while lock held */
    /* Pending compound owned by the submitting worker. The registry mutex
     * protects cancellation against terminal completion/free, including OPEN. */
    struct chimera_vfs_compound    *compound;
    int                             lock_index;
    bool                            reaped;
    bool                            disconnected;
    struct evpl_rpc2_conn          *conn;  /* identity only, guarded by state mutex */
    bool                            unlock;
    struct nlm_lock_entry          *next;
    struct nlm_lock_entry          *prev;
};

/*
 * Per-client NLM state, keyed by caller_name (hostname).
 * magic must be first to allow safe type checking via conn private_data.
 */
struct nlm_client {
    uint32_t                        magic;  /* NLM_CLIENT_MAGIC */
    uint32_t                        conn_count; /* # active conns with private_data set */
    char                            hostname[LM_MAXSTRLEN + 1];
    struct nlm_lock_entry          *locks;  /* pending requests and one handle anchor per owner/file */
    struct chimera_vfs_lock_domain *domain; /* canonical accepted intervals */
    UT_hash_handle                  hh;     /* keyed by hostname */
};

/*
 * Global NLM lock state shared across all server threads.
 * All mutations are protected by mutex.
 */
struct nlm_state {
    evpl_mutex_t        mutex;
    struct nlm_client  *clients;            /* uthash table, keyed by hostname */
    struct chimera_vfs *vfs;
    bool                stopping;
    int                 in_grace;           /* non-zero during grace period */
    time_t              grace_end;          /* time_t when grace period expires */
    char                state_dir[256];    /* directory for lock state files */
};

/* -------------------------------------------------------------------------
 * Inline helpers
 * ---------------------------------------------------------------------- */

/*
 * Check if the server is currently in its post-restart grace period.
 * Uses lazy expiry: clears in_grace when the deadline passes.
 *
 * Caller MUST hold state->mutex: this function writes state->in_grace.
 */
static inline int
nlm_state_in_grace(struct nlm_state *state)
{
    if (!state->in_grace) {
        return 0;
    }
    if (time(NULL) >= state->grace_end) {
        state->in_grace = 0;
        return 0;
    }
    return 1;
} /* nlm_state_in_grace */

/*
 * Open the post-restart grace window: only reclaim locks are accepted until it
 * lazily expires (nlm_state_in_grace) NLM_GRACE_PERIOD_SECS from now.  Called
 * from single-threaded server init, or otherwise with state->mutex held.
 */
static inline void
nlm_state_begin_grace(struct nlm_state *state)
{
    state->in_grace  = 1;
    state->grace_end = time(NULL) + NLM_GRACE_PERIOD_SECS;
} /* nlm_state_begin_grace */

/*
 * Close the grace window early (nothing left to reclaim).  Called from
 * single-threaded server init, or otherwise with state->mutex held.
 */
static inline void
nlm_state_end_grace(struct nlm_state *state)
{
    state->in_grace = 0;
} /* nlm_state_end_grace */

/*
 * Allocate and zero a new lock entry.
 */
static inline struct nlm_lock_entry *
nlm_lock_entry_alloc(void)
{
    return calloc(1, sizeof(struct nlm_lock_entry));
} /* nlm_lock_entry_alloc */

/*
 * Free a lock entry (does NOT close the handle; caller must close first).
 */
static inline void
nlm_lock_entry_free(struct nlm_lock_entry *entry)
{
    free(entry);
} /* nlm_lock_entry_free */

void
nlm_state_init(
    struct nlm_state   *state,
    struct chimera_vfs *vfs,
    const char         *state_dir);

void nlm_state_shutdown(
    struct nlm_state *state);

void nlm_state_disconnect(
    struct nlm_state      *state,
    struct evpl_rpc2_conn *conn);

void
nlm_state_destroy(
    struct nlm_state *state);

void
nlm_state_load(
    struct nlm_state *state);

struct nlm_client *
nlm_client_lookup_or_create(
    struct nlm_state *state,
    const char       *hostname);

void
nlm_state_persist_client(
    struct nlm_state  *state,
    struct nlm_client *client);

void
nlm_state_remove_client_file(
    struct nlm_state *state,
    const char       *hostname);

void
nlm_client_release_all_locks(
    struct nlm_state          *state,
    struct nlm_client         *client,
    struct chimera_vfs_thread *vfs_thread,
    struct chimera_vfs_state  *vfs_state,
    struct chimera_vfs_cred   *cred);
