// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <stdbool.h>
#include <time.h>
#include <pthread.h>
#include <uthash.h>
#include <utlist.h>

#include "nfs4_xdr.h"
#include "nlm4_xdr.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_cred.h"

/* Magic number stored in nlm_client.magic to distinguish it from nfs4_session *
 * when reading connection private_data in the disconnect handler. */
#define NLM_CLIENT_MAGIC      0x4E4C4D43U /* "NLMC" */

/* Grace period: time after server restart during which only reclaim locks
* are accepted. This matches the grace period used by most NFS servers. */
#define NLM_GRACE_PERIOD_SECS 90

/* On-disk state file format */
#define NLM_FILE_MAGIC        0x4E4C4D53U /* "NLMS" */
#define NLM_FILE_VERSION      1

/*
 * One lock held on behalf of an NLM client.
 * The open handle is kept alive for the duration of the lock --
 * POSIX advisory locks are tied to the open file description.
 */
struct nlm_lock_entry {
    uint8_t                         fh[NFS4_FHSIZE];
    uint32_t                        fh_len;
    uint8_t                         oh[LM_MAXSTRLEN]; /* owner handle (opaque) */
    uint32_t                        oh_len;
    int32_t                         svid;   /* client-side PID */
    uint64_t                        offset;
    uint64_t                        length; /* 0 == to EOF (POSIX) */
    bool                            exclusive;
    bool                            pending; /* true while VFS open/lock in flight */
    struct chimera_vfs_open_handle *handle; /* kept open while lock held */
    struct nlm_lock_entry          *next;
    struct nlm_lock_entry          *prev;
};

/*
 * Per-client NLM state, keyed by caller_name (hostname).
 * magic must be first to allow safe type checking via conn private_data.
 */
struct nlm_client {
    uint32_t               magic;           /* NLM_CLIENT_MAGIC */
    uint32_t               conn_count;      /* # active conns with private_data set */
    char                   hostname[LM_MAXSTRLEN + 1];
    struct nlm_lock_entry *locks;           /* DL_LIST of active locks */
    UT_hash_handle         hh;              /* keyed by hostname */
};

/*
 * Global NLM lock state shared across all server threads.
 * All mutations are protected by mutex.
 */
struct nlm_state {
    pthread_mutex_t    mutex;
    struct nlm_client *clients;             /* uthash table, keyed by hostname */
    int                in_grace;            /* non-zero during grace period */
    time_t             grace_end;           /* time_t when grace period expires */
    char               state_dir[256];     /* directory for lock state files */
};

/*
 * On-disk lock entry layout.
 * Written as header + array of nlm_file_entry.
 */
struct nlm_file_hdr {
    uint32_t magic;
    uint32_t version;
    uint32_t num_entries;
    uint32_t pad;
};

struct nlm_file_entry {
    uint32_t fh_len;
    uint8_t  fh[NFS4_FHSIZE];
    uint32_t oh_len;
    uint8_t  oh[LM_MAXSTRLEN];
    int32_t  svid;
    uint32_t exclusive;
    uint64_t offset;
    uint64_t length;
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

/*
 * Check whether two byte ranges overlap.
 * length == 0 means "to EOF" (POSIX convention used in nlm_lock_entry).
 *
 * Additions are guarded against uint64_t overflow: a client-supplied
 * offset+length pair like (0xFFFFFFFFFFFFFF00, 0x200) would wrap to a
 * small value and bypass conflict detection without the guards below.
 */

/* Returns non-zero if a + b > c, overflow-safe for uint64_t. */
static inline int
u64_sum_gt(
    uint64_t a,
    uint64_t b,
    uint64_t c)
{
    return b > UINT64_MAX - a || a + b > c;
} /* u64_sum_gt */

static inline int
nlm_ranges_overlap(
    uint64_t a_off,
    uint64_t a_len,
    uint64_t b_off,
    uint64_t b_len)
{
    if (a_len == 0 && b_len == 0) {
        return 1;
    }
    if (a_len == 0) {
        /* a covers [a_off, EOF): overlaps b if b's end exceeds a's start */
        return u64_sum_gt(b_off, b_len, a_off);
    }
    if (b_len == 0) {
        /* b covers [b_off, EOF): overlaps a if a's end exceeds b's start */
        return u64_sum_gt(a_off, a_len, b_off);
    }
    /* Both finite: [a_off, a_off+a_len) overlaps [b_off, b_off+b_len)
     * iff each range's end exceeds the other's start. */
    return u64_sum_gt(b_off, b_len, a_off) && u64_sum_gt(a_off, a_len, b_off);
} /* nlm_ranges_overlap */

/*
 * Check the in-memory NLM state for a lock that conflicts with the
 * requested (fh/offset/length/exclusive) from owner (req_hostname/req_oh).
 * Returns the first conflicting entry found, or NULL if none.
 *
 * The caller must hold state->mutex.
 */
static inline struct nlm_lock_entry *
nlm_state_find_conflict(
    struct nlm_state *state,
    const uint8_t    *fh,
    uint32_t          fh_len,
    const char       *req_hostname,
    const uint8_t    *req_oh,
    uint32_t          req_oh_len,
    int32_t           req_svid,
    uint64_t          req_offset,
    uint64_t          req_length,
    bool              req_exclusive)
{
    struct nlm_client     *client, *tmp_client;
    struct nlm_lock_entry *entry;

    HASH_ITER(hh, state->clients, client, tmp_client)
    {
        DL_FOREACH(client->locks, entry)
        {
            /* Skip entries for a different file */
            if (entry->fh_len != fh_len || memcmp(entry->fh, fh, fh_len) != 0) {
                continue;
            }

            /* No conflict if ranges don't overlap */
            if (!nlm_ranges_overlap(entry->offset, entry->length,
                                    req_offset, req_length)) {
                continue;
            }

            /* No conflict if both are shared (read) locks */
            if (!entry->exclusive && !req_exclusive) {
                continue;
            }

            /* No conflict with ourselves (same hostname + oh + svid).
             * Note: pending entries from other clients ARE treated as
             * conflicts intentionally -- the pending sentinel prevents
             * TOCTOU races between in-flight VFS calls. */
            if (strcmp(client->hostname, req_hostname) == 0 &&
                entry->oh_len == req_oh_len &&
                entry->svid == req_svid &&
                memcmp(entry->oh, req_oh, req_oh_len) == 0) {
                continue;
            }

            return entry;
        }
    }
    return NULL;
} /* nlm_state_find_conflict */

/*
 * Find a lock entry matching owner handle + svid + file handle + byte range.
 * Per RFC 1813 sec. A.6.4, UNLOCK identifies by owner + fh + exact offset/length.
 * A client holding two non-overlapping locks on the same file (same owner)
 * requires range matching to release the correct one.
 */
static inline struct nlm_lock_entry *
nlm_client_find_lock(
    struct nlm_client *client,
    const uint8_t     *oh,
    uint32_t           oh_len,
    int32_t            svid,
    const uint8_t     *fh,
    uint32_t           fh_len,
    uint64_t           offset,
    uint64_t           length)
{
    struct nlm_lock_entry *entry;

    DL_FOREACH(client->locks, entry)
    {
        /* Skip entries whose VFS open/lock is still in flight; the handle is
        * NULL until the open callback fires, so using it would crash.  An
        * UNLOCK that races with an in-flight LOCK is treated the same as
        * "lock not found" -- RFC 1813 sec. A.6.4 says reply NLM4_GRANTED. */
        if (entry->pending) {
            continue;
        }

        if (entry->oh_len == oh_len &&
            entry->svid == svid &&
            entry->fh_len == fh_len &&
            entry->offset == offset &&
            entry->length == length &&
            memcmp(entry->oh, oh, oh_len) == 0 &&
            memcmp(entry->fh, fh, fh_len) == 0) {
            return entry;
        }
    }
    return NULL;
} /* nlm_client_find_lock */

/* -------------------------------------------------------------------------
 * Functions implemented in nfs_nlm_state.c
 * ---------------------------------------------------------------------- */

void
nlm_state_init(
    struct nlm_state *state,
    const char       *state_dir);

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
    struct chimera_vfs_cred   *cred);
