// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "vfs_state.h"
#include "vfs_internal.h"
#include "common/macros.h"

/*
 * Default lease-break deadline.  Matches the SMB2 client expectation of
 * "the server will wait roughly 30s for the break ack before forcing
 * revocation," and is well inside the NFSv4 lease (typically 60-90s).
 */
#define CHIMERA_VFS_STATE_DEFAULT_BREAK_DEADLINE_MS 30000

/* Upper bound on caching leases broken / revoked in a single batched pass.  A
 * file with more concurrent caching holders than this is pathological; any
 * excess is handled on the next pass. */
#define CHIMERA_VFS_STATE_MAX_BREAK_BATCH           64

/*
 * Implicit I/O leases held by chimera on behalf of leaseless actors are
 * dropped once they have been idle (no in-flight I/O) for this long.  Keeps
 * the resident per-file state bounded for write-once / read-once workloads
 * while still letting bursts of I/O on the same file reuse a held lease.
 */
#define CHIMERA_VFS_STATE_DEFAULT_IMPLICIT_IDLE_MS  10000

/*
 * NFSv4 delegation recall deadline.  A delegation being recalled keeps
 * conflicting acquirers waiting (NFS4ERR_DELAY); if the holder does not return
 * it within this window the server revokes it so other access can proceed.
 * Real clients return in milliseconds; this is a generous upper bound.  It is
 * deliberately longer than a typical client's own DELAY-retry window so that a
 * client that intends to return the delegation (rather than abandon it) gets
 * the chance to, instead of having a competing open silently granted under it.
 */
#define CHIMERA_VFS_NFS_DELEG_RECALL_MS             15000

/*
 * Recall deadline for a delegation broken by a namespace/metadata operation
 * (REMOVE/RENAME/LINK).  The operation itself blocks (NFS4ERR_DELAY) while the
 * recall is outstanding, so this is shorter than the conflicting-open recall
 * window above -- the holder either returns promptly or is revoked so the
 * operation can complete within a client's retry budget.
 */
#define CHIMERA_VFS_NFS_DELEG_METAOP_MS             5000

/* Retry every I/O request parked waiting for the implicit lease on `file`.
 * Defined below; forward-declared so the break ack/revoke/remove paths can
 * unblock parked I/O alongside protocol-lease acquires. */
static void
chimera_vfs_state_pump_io(
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file);

/* Milliseconds elapsed from `then` to `now` (both stopwatch ticks).  Clamps
 * to 0 if `now` precedes `then`. */
static inline uint64_t
chimera_vfs_elapsed_ms(
    uint64_t then,
    uint64_t now)
{
    return now > then ? chimera_vfs_ticks_to_ns(now - then) / 1000000ULL : 0;
} /* chimera_vfs_elapsed_ms */

/* True if `lease`'s break deadline has elapsed (stopwatch ticks). */
static inline bool
chimera_vfs_break_deadline_passed(const struct chimera_vfs_lease *lease)
{
    return chimera_vfs_now_ticks() >= lease->break_deadline;
} /* chimera_vfs_break_deadline_passed */

/* -------------------------------------------------------------------- */
/* Lifecycle                                                            */
/* -------------------------------------------------------------------- */

SYMBOL_EXPORT struct chimera_vfs_state *
chimera_vfs_state_init(void)
{
    struct chimera_vfs_state *state;
    int                       i;

    state = calloc(1, sizeof(*state));
    if (!state) {
        return NULL;
    }

    for (i = 0; i < CHIMERA_VFS_STATE_NUM_BUCKETS; i++) {
        pthread_mutex_init(&state->buckets[i].lock, NULL);
        state->buckets[i].files = NULL;
    }

    state->default_break_deadline_ms = CHIMERA_VFS_STATE_DEFAULT_BREAK_DEADLINE_MS;
    state->implicit_idle_ms          = CHIMERA_VFS_STATE_DEFAULT_IMPLICIT_IDLE_MS;

    return state;
} /* chimera_vfs_state_init */

SYMBOL_EXPORT void
chimera_vfs_state_destroy(struct chimera_vfs_state *state)
{
    struct chimera_vfs_file_state *file, *next;
    int                            i;

    if (!state) {
        return;
    }

    for (i = 0; i < CHIMERA_VFS_STATE_NUM_BUCKETS; i++) {
        file = state->buckets[i].files;
        while (file) {
            next = file->bucket_next;
            /* In Stage A there is no protocol caller, so we should never
             * leak per-file state.  Asserts here would be too strict for
             * test code that exits without releasing; instead we just
             * tear down whatever remains. */
            pthread_mutex_destroy(&file->lock);
            free(file);
            file = next;
        }
        pthread_mutex_destroy(&state->buckets[i].lock);
    }

    free(state);
} /* chimera_vfs_state_destroy */

/* -------------------------------------------------------------------- */
/* Per-file state lookup                                                */
/* -------------------------------------------------------------------- */

static inline struct chimera_vfs_state_bucket *
chimera_vfs_state_bucket_for(
    struct chimera_vfs_state *state,
    uint64_t                  fh_hash)
{
    return &state->buckets[fh_hash & (CHIMERA_VFS_STATE_NUM_BUCKETS - 1)];
} /* chimera_vfs_state_bucket_for */

static inline int
chimera_vfs_file_state_match(
    const struct chimera_vfs_file_state *file,
    const uint8_t                       *fh,
    uint8_t                              fh_len,
    uint64_t                             fh_hash)
{
    return file->fh_hash == fh_hash &&
           file->fh_len == fh_len &&
           memcmp(file->fh, fh, fh_len) == 0;
} /* chimera_vfs_file_state_match */

SYMBOL_EXPORT struct chimera_vfs_file_state *
chimera_vfs_state_get(
    struct chimera_vfs_state *state,
    const uint8_t            *fh,
    uint8_t                   fh_len,
    uint64_t                  fh_hash,
    bool                      create)
{
    struct chimera_vfs_state_bucket *bucket;
    struct chimera_vfs_file_state   *file;

    bucket = chimera_vfs_state_bucket_for(state, fh_hash);

    pthread_mutex_lock(&bucket->lock);

    for (file = bucket->files; file; file = file->bucket_next) {
        if (chimera_vfs_file_state_match(file, fh, fh_len, fh_hash)) {
            file->refcount++;
            pthread_mutex_unlock(&bucket->lock);
            return file;
        }
    }

    if (!create) {
        pthread_mutex_unlock(&bucket->lock);
        return NULL;
    }

    file = calloc(1, sizeof(*file));
    if (!file) {
        pthread_mutex_unlock(&bucket->lock);
        return NULL;
    }

    memcpy(file->fh, fh, fh_len);
    file->fh_len   = fh_len;
    file->fh_hash  = fh_hash;
    file->refcount = 1;
    file->state    = state;
    pthread_mutex_init(&file->lock, NULL);

    file->bucket_next = bucket->files;
    bucket->files     = file;

    pthread_mutex_unlock(&bucket->lock);
    return file;
} /* chimera_vfs_state_get */

SYMBOL_EXPORT void
chimera_vfs_state_put(
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file)
{
    struct chimera_vfs_state_bucket *bucket;
    struct chimera_vfs_file_state  **link;
    bool                             empty;

    if (!file) {
        return;
    }

    bucket = chimera_vfs_state_bucket_for(state, file->fh_hash);

    pthread_mutex_lock(&bucket->lock);

    chimera_vfs_abort_if(file->refcount == 0, "double put on vfs_state file");

    file->refcount--;
    if (file->refcount != 0) {
        pthread_mutex_unlock(&bucket->lock);
        return;
    }

    /* Refcount reached zero.  Tear down only if no leases remain — a
     * lease holder is effectively a reference too (held implicitly via
     * the caller that owns the lease), but if any caller forgot to put
     * after acquiring a lease we'd otherwise free state with live
     * leases.  Detect this and keep the entry around; a future put()
     * will retry. */
    empty = (file->range_locks == NULL &&
             file->share_resvs == NULL &&
             file->caching_leases == NULL);

    if (!empty) {
        /* Restore the implicit reference held by the lease(s). */
        file->refcount = 1;
        pthread_mutex_unlock(&bucket->lock);
        return;
    }

    /* Unlink from bucket and free. */
    for (link = &bucket->files; *link; link = &(*link)->bucket_next) {
        if (*link == file) {
            *link = file->bucket_next;
            break;
        }
    }

    pthread_mutex_unlock(&bucket->lock);

    pthread_mutex_destroy(&file->lock);
    free(file);
} /* chimera_vfs_state_put */

SYMBOL_EXPORT void
chimera_vfs_file_state_release(struct chimera_vfs_file_state *file)
{
    if (file) {
        chimera_vfs_state_put(file->state, file);
    }
} /* chimera_vfs_file_state_release */

/* -------------------------------------------------------------------- */
/* Conflict matrix                                                      */
/* -------------------------------------------------------------------- */

/* Same-owner coalescing: two leases share an owner iff protocol +
 * client_key + owner_lo + owner_hi all match.  For SMB this is the
 * (client_guid, lease_key) tuple that lets one client open the same
 * file multiple times without breaking its own lease.  For NFSv4 it
 * is (clientid, lock_owner4).  For NLM it is (hostname-hash, svid). */
static inline bool
chimera_vfs_lease_owner_equal(
    const struct chimera_vfs_lease_owner *a,
    const struct chimera_vfs_lease_owner *b)
{
    return a->protocol == b->protocol &&
           a->client_key == b->client_key &&
           a->owner_lo == b->owner_lo &&
           a->owner_hi == b->owner_hi;
} /* chimera_vfs_lease_owner_equal */

/* Same-lease-key test for SMB2 caching leases, IGNORING client_key.
 *
 * MS-SMB2 identifies a lease by (ClientGuid, LeaseKey); chimera's owner key
 * encodes ClientGuid in client_key and LeaseKey in owner_lo/owner_hi.  When a
 * conflicting open or a write presents the SAME LeaseKey as an existing caching
 * holder, the object store shares the cache rather than breaking it -- so the
 * holder must be exempted from the break.  Windows applies this on the lease key
 * alone (two opens with the same lease key, even from different connections with
 * distinct ClientGuids, do not break one another: WPTS Leasing_FileLeasing*_
 * SameLeaseKey).  This mirrors the SHARE-acquire side's has_break_skip_key,
 * which is keyed on the lease key only (see chimera_vfs_state_would_conflict).
 *
 * Restricted to SMB2 caching leases on both sides: NFSv4 delegations and the
 * internal implicit lease never share an SMB lease key, and owner_equal already
 * covers a holder's own same-client/same-key re-open. */
static inline bool
chimera_vfs_lease_smb2_same_key(
    const struct chimera_vfs_lease_owner *holder,
    const struct chimera_vfs_lease_owner *opener)
{
    return holder->protocol == CHIMERA_VFS_LEASE_PROTO_SMB2 &&
           opener->protocol == CHIMERA_VFS_LEASE_PROTO_SMB2 &&
           holder->owner_lo == opener->owner_lo &&
           holder->owner_hi == opener->owner_hi;
} /* chimera_vfs_lease_smb2_same_key */

/* Byte-range overlap test for half-open intervals [off, off+len).
 *
 * length==0 is a genuine zero-byte range (SMB2 zero-length lock): [off, off)
 * overlaps another range only when off lies strictly inside it.  "To EOF" is
 * represented as a length whose end saturates at UINT64_MAX -- callers that
 * mean to-EOF pass UINT64_MAX (or any length that overflows the end), and the
 * NLM/NFSv4 boundaries translate their wire to-EOF sentinel accordingly.  The
 * end is computed overflow-safe: a wrapping off+len saturates to UINT64_MAX
 * rather than aliasing a small value (which would bypass conflict detection). */
static inline bool
chimera_vfs_range_overlap(
    uint64_t a_off,
    uint64_t a_len,
    uint64_t b_off,
    uint64_t b_len)
{
    /* Compute exclusive ends in 128-bit so a range touching the last byte of
     * the 64-bit space (e.g. offset=2^64-1, length=1 -> end=2^64) is not aliased
     * to a zero-length range.  UINT64_MAX as a length is the to-EOF sentinel and
     * extends to the end of the address space. */
    __uint128_t a_end = (a_len == UINT64_MAX)
        ? ((__uint128_t) 1 << 64) : (__uint128_t) a_off + a_len;
    __uint128_t b_end = (b_len == UINT64_MAX)
        ? ((__uint128_t) 1 << 64) : (__uint128_t) b_off + b_len;

    return a_off < b_end && b_off < a_end;
} /* chimera_vfs_range_overlap */

/* Conflict between two range leases.  Classical fcntl rule: only
 * conflict if ranges overlap AND at least one side is exclusive (W). */
static inline bool
chimera_vfs_range_conflict(
    const struct chimera_vfs_lease *a,
    const struct chimera_vfs_lease *b)
{
    /* A revoked holder grants nothing; it conflicts with no one.  (The
     * write-probe term below is independent of `a`'s mode, so a zeroed-out
     * revoked lease would otherwise still appear to conflict with a write
     * acquire until the protocol layer removes it.) */
    if (a->break_state == CHIMERA_VFS_BREAK_REVOKED) {
        return false;
    }

    if (!chimera_vfs_range_overlap(a->offset, a->length,
                                   b->offset, b->length)) {
        return false;
    }

    return (a->mode.granted & CHIMERA_VFS_LEASE_MODE_W) ||
           (b->mode.granted & CHIMERA_VFS_LEASE_MODE_W);
} /* chimera_vfs_range_conflict */

/* Conflict between two share reservations.  Lifted from
 * chimera_smb_sharemode_check_conflict() at smb_sharemode.c:110-150 —
 * the same predicate expressed in RWH-mask terms.  An access bit on one
 * side conflicts if the other side denies it. */
static inline bool
chimera_vfs_share_conflict(
    const struct chimera_vfs_lease *existing,
    const struct chimera_vfs_lease *probe)
{
    if (existing->mode.granted & probe->mode.denied) {
        return true;
    }

    if (probe->mode.granted & existing->mode.denied) {
        return true;
    }

    return false;
} /* chimera_vfs_share_conflict */

/* Conflict between two caching leases.  An RWH triple on a holder
 * conflicts with another holder if their granted bits overlap AND the
 * combination is mutually exclusive — both can't hold W, etc.  In
 * practice, since caching leases use the SMB lease bits directly, the
 * rule simplifies to: any bit shared in `granted` between two different
 * owners is a conflict.  W and H are exclusive across clients; R is
 * shareable (multiple clients can each hold an R-lease — that's how
 * SMB2 Level2 oplocks generalize). */
/* The mode a holder effectively grants for conflict purposes.  While an SMB
 * oplock/lease is mid-break (notification sent, awaiting the client's ack), the
 * client is obligated to comply, so the holder is treated as already at its
 * retained (post-break) level -- this lets a coexisting acquirer (e.g. a 2nd
 * open settling at LEVEL_II) proceed immediately instead of blocking on the
 * ack.  NFSv4 delegations keep their full granted mode while RECALLING, so a
 * conflicting open waits for the DELEGRETURN (or recall-timeout revoke). */
static inline uint8_t
chimera_vfs_lease_effective_granted(const struct chimera_vfs_lease *l)
{
    if (l->break_state == CHIMERA_VFS_BREAK_BREAKING &&
        l->owner.protocol == CHIMERA_VFS_LEASE_PROTO_SMB2) {
        return l->break_needed_mode;
    }
    return l->mode.granted;
} /* chimera_vfs_lease_effective_granted */

static inline bool
chimera_vfs_caching_conflict(
    const struct chimera_vfs_lease *existing,
    const struct chimera_vfs_lease *probe)
{
    uint8_t e = chimera_vfs_lease_effective_granted(existing);
    uint8_t p = probe->mode.granted;

    /* W (write-cache) is exclusive — only one holder may have W on a
     * given file, even across owners.  Two readers (R-only) coexist. */
    if ((e & CHIMERA_VFS_LEASE_MODE_W) && (p & (CHIMERA_VFS_LEASE_MODE_R |
                                                CHIMERA_VFS_LEASE_MODE_W))) {
        return true;
    }
    if ((p & CHIMERA_VFS_LEASE_MODE_W) && (e & (CHIMERA_VFS_LEASE_MODE_R |
                                                CHIMERA_VFS_LEASE_MODE_W))) {
        return true;
    }

    /* H (handle-cache) is exclusive only ACROSS CLIENTS — a different client
     * caching the open handle conflicts (a batch oplock is sole-handle), but two
     * lease keys of the SAME client may each hold handle caching (MS-SMB2 lease
     * semantics: a client's RH lease is not broken by another RH lease of its own
     * under a different key -- smb2.lease.break expects two RH leases to coexist).
     * Write caching above is the only mode exclusive even within one client. */
    if ((e & CHIMERA_VFS_LEASE_MODE_H) && (p & CHIMERA_VFS_LEASE_MODE_H) &&
        existing->owner.client_key != probe->owner.client_key) {
        return true;
    }

    return false;
} /* chimera_vfs_caching_conflict */

/* Evaluate a conflicting holder that the acquirer may be able to recall.
 * A holder with a break_cb (e.g. chimera's own implicit I/O lease, an SMB
 * oplock, an NFSv4 delegation) is recalled rather than failing the acquire:
 * an IDLE holder is selected as the break victim; a holder already BREAKING
 * keeps the acquirer waiting until its recall deadline elapses, after which
 * it becomes revocable.  Returns true if the holder is NOT breakable, in
 * which case the caller must hard-deny. */
static inline bool
chimera_vfs_eval_breakable(
    struct chimera_vfs_lease  *cur,
    bool                      *has_breakable,
    struct chimera_vfs_lease **idle_break,
    struct chimera_vfs_lease **expired_break)
{
    if (!cur->owner.break_cb) {
        return true;
    }

    if (cur->break_state == CHIMERA_VFS_BREAK_IDLE) {
        *has_breakable = true;
        if (!*idle_break) {
            *idle_break = cur;
        }
    } else if (cur->break_state == CHIMERA_VFS_BREAK_BREAKING) {
        *has_breakable = true;
        if (!*expired_break && chimera_vfs_break_deadline_passed(cur)) {
            *expired_break = cur;
        }
    }
    return false;
} /* chimera_vfs_eval_breakable */

/* When a hard share-mode conflict comes from an SMB open that also holds a
 * batch (handle-caching) oplock/lease, that holder may close in response to a
 * break and so free the share conflict.  MS-FSA breaks such a batch oplock
 * BEFORE the share-access check: the conflicting open parks until the holder
 * either closes (then the open is granted) or merely acks while keeping the
 * handle (then the share conflict stands and the open is denied).  Find that
 * still-breakable handle-caching lease -- matched to the share holder by their
 * common owning open (cb_private, set identically on an SMB open's share
 * reservation and caching lease) -- so a SHARE acquire can park on its break
 * instead of denying outright.  Returns NULL when the holder has no
 * still-breakable handle-caching lease, i.e. a genuine hard conflict. */
static inline struct chimera_vfs_lease *
chimera_vfs_share_batch_escape(
    const struct chimera_vfs_file_state *file,
    const struct chimera_vfs_lease      *share_holder)
{
    struct chimera_vfs_lease *cur;

    if (share_holder->owner.protocol != CHIMERA_VFS_LEASE_PROTO_SMB2 ||
        !share_holder->owner.cb_private) {
        return NULL;
    }

    for (cur = file->caching_leases; cur; cur = cur->next) {
        if (cur->owner.cb_private != share_holder->owner.cb_private ||
            !cur->owner.break_cb ||
            !(cur->mode.granted & CHIMERA_VFS_LEASE_MODE_H)) {
            continue;
        }
        if (cur->break_state == CHIMERA_VFS_BREAK_IDLE ||
            cur->break_state == CHIMERA_VFS_BREAK_BREAKING) {
            return cur;
        }
    }
    return NULL;
} /* chimera_vfs_share_batch_escape */

/* A lease whose owning client/session is no longer considered alive (e.g. an
 * NFSv4 client past its lease in courtesy state).  Such a holder retains its
 * lease only "courteously": a conflicting acquire reclaims it on demand rather
 * than being denied.  is_alive_cb is optional; absent ⇒ always alive. */
static inline bool
chimera_vfs_lease_holder_reclaimable(
    const struct chimera_vfs_lease *holder,
    const struct chimera_vfs_lease *probe)
{
    /* A holder is reclaimable only when its owning client/session is no longer
     * alive (e.g. an NFSv4 client past its lease, in courtesy state) AND it
     * belongs to a *different* client than the acquirer.  A client never
     * reclaims its own held leases on a conflicting acquire: it revives its
     * lease and the conflict stands (e.g. two lock-owners of one client still
     * conflict, NFS4ERR_DENIED).  is_alive_cb is optional; absent ⇒ alive. */
    return holder->owner.is_alive_cb &&
           holder->owner.client_key != probe->owner.client_key &&
           !holder->owner.is_alive_cb(holder, holder->owner.cb_private);
} /* chimera_vfs_lease_holder_reclaimable */

SYMBOL_EXPORT enum chimera_vfs_lease_result
chimera_vfs_state_would_conflict(
    const struct chimera_vfs_file_state *file,
    const struct chimera_vfs_lease      *probe,
    struct chimera_vfs_lease           **conflict_out)
{
    struct chimera_vfs_lease *cur;
    bool has_breakable_conflict = false;

    if (conflict_out) {
        *conflict_out = NULL;
    }

    switch (probe->kind) {
        case CHIMERA_VFS_LEASE_RANGE:
            for (cur = file->range_locks; cur; cur = cur->next) {
                if (chimera_vfs_lease_owner_equal(&cur->owner, &probe->owner)) {
                    /* Same owner: coalesce / upgrade rather than conflict. */
                    continue;
                }
                if (chimera_vfs_range_conflict(cur, probe)) {
                    if (conflict_out) {
                        *conflict_out = cur;
                    }
                    return CHIMERA_VFS_LEASE_DENIED;
                }
            }
            /* A range lock may also be blocked by a caching W-lease on
             * another client (the other client believes it has exclusive
             * write access and a range lock here would invalidate that
             * cache).  This is the "I/O breaks caching" rule applied
             * conservatively to range locks too. */
            for (cur = file->caching_leases; cur; cur = cur->next) {
                if (chimera_vfs_lease_owner_equal(&cur->owner, &probe->owner)) {
                    continue;
                }
                /* A byte-range lock and a caching lease held by the SAME open do
                 * not break each other: an SMB open's caching lease is keyed by
                 * its lease key while its range locks are keyed by the file id,
                 * so owner_equal does not catch them, but cb_private points at
                 * the common owning open on both. */
                if (probe->owner.cb_private &&
                    cur->owner.cb_private == probe->owner.cb_private) {
                    continue;
                }
                /* A read range lock conflicts with a W cache on another
                 * client (their writes may be cached); a write range lock
                 * conflicts with any R/W cache on another client. */
                if (probe->mode.granted & CHIMERA_VFS_LEASE_MODE_W) {
                    if (cur->mode.granted & (CHIMERA_VFS_LEASE_MODE_R |
                                             CHIMERA_VFS_LEASE_MODE_W)) {
                        if (cur->owner.break_cb &&
                            cur->break_state == CHIMERA_VFS_BREAK_IDLE) {
                            has_breakable_conflict = true;
                            if (conflict_out && !*conflict_out) {
                                *conflict_out = cur;
                            }
                        } else {
                            if (conflict_out) {
                                *conflict_out = cur;
                            }
                            return CHIMERA_VFS_LEASE_DENIED;
                        }
                    }
                } else if (cur->mode.granted & CHIMERA_VFS_LEASE_MODE_W) {
                    if (cur->owner.break_cb &&
                        cur->break_state == CHIMERA_VFS_BREAK_IDLE) {
                        has_breakable_conflict = true;
                        if (conflict_out && !*conflict_out) {
                            *conflict_out = cur;
                        }
                    } else {
                        if (conflict_out) {
                            *conflict_out = cur;
                        }
                        return CHIMERA_VFS_LEASE_DENIED;
                    }
                }
            }
            break;

        case CHIMERA_VFS_LEASE_SHARE:
        {
            struct chimera_vfs_lease *idle_break    = NULL;
            struct chimera_vfs_lease *expired_break = NULL;

            /* An inert (granted=0, denied=0) SHARE entry is an attribute-only
             * open registered purely for visibility (sole-access / delete-
             * pending / stat-open queries).  It can neither conflict with nor
             * break any holder, so grant it with no side effects. */
            if (probe->mode.granted == 0 && probe->mode.denied == 0) {
                return CHIMERA_VFS_LEASE_GRANTED;
            }

            for (cur = file->share_resvs; cur; cur = cur->next) {
                if (chimera_vfs_lease_owner_equal(&cur->owner, &probe->owner)) {
                    continue;
                }
                if (!chimera_vfs_share_conflict(cur, probe)) {
                    continue;
                }
                /* A breakable share holder is chimera's own implicit I/O
                 * lease: recall it so a real client open can take the file.
                 * A non-breakable holder is an ordinary client open and is a
                 * hard share conflict -- UNLESS it is an SMB open holding a
                 * batch (handle-caching) oplock, in which case the holder may
                 * close on a break and free the conflict, so park on that
                 * break rather than denying (MS-FSA batch-before-share). */
                if (chimera_vfs_eval_breakable(cur, &has_breakable_conflict,
                                               &idle_break, &expired_break)) {
                    struct chimera_vfs_lease *batch =
                        chimera_vfs_share_batch_escape(file, cur);

                    if (!batch) {
                        if (conflict_out) {
                            *conflict_out = cur;
                        }
                        return CHIMERA_VFS_LEASE_DENIED;
                    }

                    has_breakable_conflict = true;
                    if (batch->break_state == CHIMERA_VFS_BREAK_IDLE) {
                        if (!idle_break) {
                            idle_break = batch;
                        }
                    } else if (!expired_break &&
                               chimera_vfs_break_deadline_passed(batch)) {
                        expired_break = batch;
                    }
                }
            }
            /* A new SHARE acquire may also need to break a caching lease on
             * another owner.  Two cases:
             *   - H (handle-caching) leases (SMB) are invalidated whenever a
             *     different client opens the file.
             *   - NFSv4 delegations (CACHING leases owned by the NFSv4 server)
             *     are recalled when a conflicting open arrives: a read
             *     delegation (R) by a writer or a deny-read; a write
             *     delegation (W) by any other open.  Limiting the R/W rule to
             *     NFSv4-owned leases keeps SMB oplock/lease break semantics
             *     unchanged (those run through the CACHING-vs-CACHING path). */
            for (cur = file->caching_leases; cur; cur = cur->next) {
                bool want_break_h   = false;
                bool want_break_nfs = false;

                if (chimera_vfs_lease_owner_equal(&cur->owner, &probe->owner)) {
                    continue;
                }
                /* Same client + same lease key: the requester's own lease
                 * (a second open under one lease key). It coalesces; do
                 * not break it. */
                if (probe->has_break_skip_key &&
                    cur->owner.owner_lo == probe->break_skip_lo &&
                    cur->owner.owner_hi == probe->break_skip_hi) {
                    continue;
                }
                if (!cur->owner.break_cb) {
                    continue;
                }

                /* A handle-caching holder is broken by a conflicting open's share
                 * reservation ONLY if it is a legacy SMB oplock (batch): the holder
                 * closes its deferred handle so the open can proceed.  An SMB2 RqLs
                 * lease shares handle caching across owners and is NOT broken by
                 * another open -- its (exclusive) write cache is handled by the
                 * caching-contention path instead. */
                if ((cur->mode.granted & CHIMERA_VFS_LEASE_MODE_H) &&
                    cur->grant && cur->grant->is_oplock) {
                    want_break_h = true;
                }

                if (cur->owner.protocol == CHIMERA_VFS_LEASE_PROTO_NFSV4 &&
                    cur->owner.client_key != probe->owner.client_key) {
                    /* A client never recalls its own delegation; recall is
                     * for conflicting access by *other* clients. */
                    uint8_t g  = cur->mode.granted;
                    uint8_t pg = probe->mode.granted;
                    uint8_t pd = probe->mode.denied;

                    if ((g & CHIMERA_VFS_LEASE_MODE_W) &&
                        (pg & (CHIMERA_VFS_LEASE_MODE_R | CHIMERA_VFS_LEASE_MODE_W))) {
                        want_break_nfs = true; /* write deleg vs any open */
                    }
                    if ((g & CHIMERA_VFS_LEASE_MODE_R) &&
                        (pg & CHIMERA_VFS_LEASE_MODE_W)) {
                        want_break_nfs = true; /* read deleg vs writer */
                    }
                    if (pd & g) {
                        want_break_nfs = true; /* deny clashes with cache */
                    }
                }

                /* SMB handle-cache: break only an IDLE holder (existing
                * optimistic-after-break semantics retained for SMB). */
                if (want_break_h &&
                    cur->break_state == CHIMERA_VFS_BREAK_IDLE) {
                    has_breakable_conflict = true;
                    if (!idle_break) {
                        idle_break = cur;
                    }
                }

                /* NFSv4 delegation: a holder that is still IDLE must be
                 * recalled; one already BREAKING keeps the acquirer waiting
                 * (do NOT grant conflicting access mid-recall) until it
                 * returns the delegation or its recall deadline elapses, at
                 * which point it becomes revocable. */
                if (want_break_nfs) {
                    if (cur->break_state == CHIMERA_VFS_BREAK_IDLE) {
                        has_breakable_conflict = true;
                        if (!idle_break) {
                            idle_break = cur;
                        }
                    } else if (cur->break_state == CHIMERA_VFS_BREAK_BREAKING) {
                        has_breakable_conflict = true;
                        if (!expired_break &&
                            chimera_vfs_break_deadline_passed(cur)) {
                            expired_break = cur;
                        }
                    }
                }
            }

            if (conflict_out && !*conflict_out) {
                *conflict_out = idle_break ? idle_break : expired_break;
            }
        }
        break;

        case CHIMERA_VFS_LEASE_CACHING:
            for (cur = file->caching_leases; cur; cur = cur->next) {
                if (chimera_vfs_lease_owner_equal(&cur->owner, &probe->owner)) {
                    continue;
                }
                if (chimera_vfs_caching_conflict(cur, probe)) {
                    /* A conflicting caching holder that can still be recalled (has a
                     * break callback and is not already mid-break) is broken down to
                     * the conflict-clearing floor (chimera_vfs_break_retain_for): the
                     * acquirer parks until it acks.  Because that floor drops exactly
                     * the contended bits, the break genuinely resolves the conflict
                     * (no livelock), so a W acquirer recalls a peer's read cache
                     * rather than capping itself.  A holder with no break path -- or
                     * one already past IDLE whose effective mode still conflicts -- is
                     * a hard denial. */
                    if (cur->owner.break_cb &&
                        cur->break_state == CHIMERA_VFS_BREAK_IDLE) {
                        has_breakable_conflict = true;
                        if (conflict_out && !*conflict_out) {
                            *conflict_out = cur;
                        }
                    } else {
                        if (conflict_out) {
                            *conflict_out = cur;
                        }
                        return CHIMERA_VFS_LEASE_DENIED;
                    }
                }
            }

            /* An SMB exclusive (W) or batch (W|H) oplock may only be granted to
             * the SOLE opener of a file: if another client already has it open
             * (a share reservation with a different client key), the request is
             * capped to a shared read cache (LEVEL_II).  Signal that by denying
             * the W/H grant -- the SMB create path retries for R-only, which
             * coexists.  (Pure attribute-only opens take no share reservation,
             * so they do not preclude an oplock.) */
            if (probe->owner.protocol == CHIMERA_VFS_LEASE_PROTO_SMB2 &&
                (probe->mode.granted & (CHIMERA_VFS_LEASE_MODE_W |
                                        CHIMERA_VFS_LEASE_MODE_H))) {
                for (cur = file->share_resvs; cur; cur = cur->next) {
                    if (cur->owner.client_key == probe->owner.client_key) {
                        continue; /* the requesting client's own open(s) */
                    }
                    /* An inert (0,0) attribute-only registration is not a real
                     * data opener and does not preclude an exclusive oplock --
                     * matching the prior behavior where stat-opens took no share
                     * reservation at all. */
                    if (cur->mode.granted == 0 && cur->mode.denied == 0) {
                        continue;
                    }
                    if (conflict_out) {
                        *conflict_out = cur;
                    }
                    return CHIMERA_VFS_LEASE_DENIED;
                }
            }
            /* An NFSv4 delegation must not be granted when another client
             * already has the file open in a conflicting mode: a read
             * delegation is denied by another client's write open; a write
             * delegation by any other client's open (RFC 7530 §10.2 / §10.4).
             * (SMB caching leases use the caching-vs-caching path above, so
             * this NFSv4-only check leaves them unaffected.) */
            if (probe->owner.protocol == CHIMERA_VFS_LEASE_PROTO_NFSV4) {
                struct chimera_vfs_lease *idle_break    = NULL;
                struct chimera_vfs_lease *expired_break = NULL;

                for (cur = file->share_resvs; cur; cur = cur->next) {
                    uint8_t sg = cur->mode.granted;
                    bool conflict;

                    if (cur->owner.client_key == probe->owner.client_key) {
                        continue; /* the requesting client's own open */
                    }
                    if (probe->mode.granted & CHIMERA_VFS_LEASE_MODE_W) {
                        conflict = (sg & (CHIMERA_VFS_LEASE_MODE_R |
                                          CHIMERA_VFS_LEASE_MODE_W)) != 0;
                    } else {
                        conflict = (sg & CHIMERA_VFS_LEASE_MODE_W) != 0;
                    }
                    if (!conflict) {
                        continue;
                    }
                    /* A breakable share holder (chimera's implicit I/O lease)
                     * is recalled so the delegation can be granted once the
                     * in-flight I/O drains; a real client open is a hard
                     * conflict that denies the delegation. */
                    if (chimera_vfs_eval_breakable(cur, &has_breakable_conflict,
                                                   &idle_break, &expired_break)) {
                        if (conflict_out) {
                            *conflict_out = cur;
                        }
                        return CHIMERA_VFS_LEASE_DENIED;
                    }
                }

                if (conflict_out && !*conflict_out) {
                    *conflict_out = idle_break ? idle_break : expired_break;
                }
            }
            break;

        case CHIMERA_VFS_LEASE_KIND_MAX:
            break;
    } /* switch */

    return has_breakable_conflict ? CHIMERA_VFS_LEASE_BREAKING
                                  : CHIMERA_VFS_LEASE_GRANTED;
} /* chimera_vfs_state_would_conflict */

/* Insert a lease onto the appropriate per-kind list.  Caller holds
 * file->lock. */
static inline void
chimera_vfs_file_state_insert_lease(
    struct chimera_vfs_file_state *file,
    struct chimera_vfs_lease      *lease)
{
    struct chimera_vfs_lease **head;

    switch (lease->kind) {
        case CHIMERA_VFS_LEASE_RANGE:
            head = &file->range_locks;
            break;
        case CHIMERA_VFS_LEASE_SHARE:
            head = &file->share_resvs;
            break;
        case CHIMERA_VFS_LEASE_CACHING:
            head = &file->caching_leases;
            break;
        case CHIMERA_VFS_LEASE_KIND_MAX:
        default:
            return;
    } /* switch */

    lease->file = file;
    lease->prev = NULL;
    lease->next = *head;
    if (*head) {
        (*head)->prev = lease;
    }
    *head = lease;
} /* chimera_vfs_file_state_insert_lease */

static inline void
chimera_vfs_file_state_remove_lease(
    struct chimera_vfs_file_state *file,
    struct chimera_vfs_lease      *lease)
{
    struct chimera_vfs_lease **head;

    switch (lease->kind) {
        case CHIMERA_VFS_LEASE_RANGE:
            head = &file->range_locks;
            break;
        case CHIMERA_VFS_LEASE_SHARE:
            head = &file->share_resvs;
            break;
        case CHIMERA_VFS_LEASE_CACHING:
            head = &file->caching_leases;
            break;
        case CHIMERA_VFS_LEASE_KIND_MAX:
        default:
            return;
    } /* switch */

    if (lease->prev) {
        lease->prev->next = lease->next;
    } else if (*head == lease) {
        *head = lease->next;
    }

    if (lease->next) {
        lease->next->prev = lease->prev;
    }

    lease->prev = NULL;
    lease->next = NULL;
    lease->file = NULL;
} /* chimera_vfs_file_state_remove_lease */

/* The mode a conflicting holder is allowed to RETAIN when `acquirer` forces it to
* break -- i.e. the break floor handed to begin_break.  The floor must be the
* holder's granted mode with exactly the bits that CONFLICT with the acquirer
* cleared (mirroring chimera_vfs_caching_conflict): only then is the floor
* strictly below `granted`, so the break actually resolves the contention.  A
* fixed retain (e.g. always keep R|H) livelocks -- the holder keeps the very bit
* the acquirer needs gone, and would_conflict keeps returning BREAKING -- and a
* floor of the acquirer's own mode never reduces a same-mode holder at all (an
* NFSv4 W recall against a W holder would no-op and spin).
*
* - acquirer has W (write cache / write open): the holder's cached R and W are
*   both stale, so it drops R|W and keeps only H/D (handle + delete caching),
*   which survive a peer's write.
* - acquirer has R (read cache) but not W: only the holder's exclusive W is
*   incompatible; it drops W and keeps R|H|D.
* - acquirer has H (handle cache) from a DIFFERENT client: handle caching is
*   sole-across-clients, so the holder drops H.
* Bits the acquirer does not contend (notably D) are always retained, and a
* same-client H acquirer leaves the holder's H intact (SMB lease keys coexist). */
static inline uint8_t
chimera_vfs_break_retain_for(
    const struct chimera_vfs_lease *acquirer,
    const struct chimera_vfs_lease *holder)
{
    uint8_t a    = acquirer->mode.granted;
    uint8_t keep = holder->mode.granted;

    if (a & CHIMERA_VFS_LEASE_MODE_W) {
        keep &= ~(CHIMERA_VFS_LEASE_MODE_R | CHIMERA_VFS_LEASE_MODE_W);
    }
    if (a & CHIMERA_VFS_LEASE_MODE_R) {
        keep &= ~CHIMERA_VFS_LEASE_MODE_W;
    }
    if ((a & CHIMERA_VFS_LEASE_MODE_H) &&
        acquirer->owner.client_key != holder->owner.client_key) {
        keep &= ~CHIMERA_VFS_LEASE_MODE_H;
    }
    return keep;
} /* chimera_vfs_break_retain_for */

SYMBOL_EXPORT enum chimera_vfs_lease_result
chimera_vfs_state_try_insert(
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file,
    struct chimera_vfs_lease      *lease,
    struct chimera_vfs_lease     **conflict_out)
{
    enum chimera_vfs_lease_result result;
    struct chimera_vfs_lease     *conflict;

    if (conflict_out) {
        *conflict_out = NULL;
    }

    /* Outer loop: re-probe after reclaiming courtesy (dead-holder) leases,
    * which may turn a conflict into a grant without the caller waiting. */
    for ( ; ;) {
        bool began_live_break = false;

        conflict = NULL;

        pthread_mutex_lock(&file->lock);
        result = chimera_vfs_state_would_conflict(file, lease, &conflict);

        if (result == CHIMERA_VFS_LEASE_GRANTED) {
            chimera_vfs_file_state_insert_lease(file, lease);
            pthread_mutex_unlock(&file->lock);
            if (conflict_out) {
                *conflict_out = NULL;
            }
            return CHIMERA_VFS_LEASE_GRANTED;
        }
        pthread_mutex_unlock(&file->lock);

        if (conflict_out) {
            *conflict_out = conflict;
        }

        if (result == CHIMERA_VFS_LEASE_DENIED) {
            /* A hard conflict against a holder whose owning client has lapsed
             * (courtesy state) is reclaimable: revoke it and re-probe.  Its
             * lease lingers REVOKED until the protocol layer removes it, but
             * would_conflict skips REVOKED leases, so this terminates. */
            if (conflict &&
                chimera_vfs_lease_holder_reclaimable(conflict, lease)) {
                chimera_vfs_lease_revoke(conflict);
                continue;
            }
            return CHIMERA_VFS_LEASE_DENIED;
        }

        /* result == BREAKING.  Kick off a break on EVERY breakable conflicting
         * holder, not just the first.  A single conflicting open must recall
         * all conflicting read delegations (NFSv4 width) / oplocks at once;
         * otherwise the acquirer would retry once per holder.  A holder whose
         * client has lapsed is revoked outright (no point recalling a client
         * that is gone).  begin_break flips a live holder to BREAKING (and is
         * idempotent), so re-running the probe returns the next still-IDLE
         * conflict until none remain. */
        bool revoked_any = false;

        while (conflict) {
            if (chimera_vfs_lease_holder_reclaimable(conflict, lease)) {
                chimera_vfs_lease_revoke(conflict);
                revoked_any = true;
            } else if (conflict->break_state == CHIMERA_VFS_BREAK_IDLE) {
                /* Start the recall.  NFSv4 delegations get a bounded recall
                 * deadline so an unresponsive holder can be revoked; other
                 * holders (SMB) keep the default deadline. */
                uint32_t deadline_ms =
                    (conflict->owner.protocol == CHIMERA_VFS_LEASE_PROTO_NFSV4)
                    ? CHIMERA_VFS_NFS_DELEG_RECALL_MS : 0;
                chimera_vfs_lease_begin_break(state, conflict,
                                              chimera_vfs_break_retain_for(lease, conflict),
                                              deadline_ms);
                began_live_break = true;
            } else {
                /* Surfaced because its recall deadline elapsed -- the holder
                 * never returned the delegation.  Revoke it so this acquirer
                 * can proceed. */
                chimera_vfs_lease_revoke(conflict);
                revoked_any = true;
            }
            conflict = NULL;
            pthread_mutex_lock(&file->lock);
            if (chimera_vfs_state_would_conflict(file, lease, &conflict) !=
                CHIMERA_VFS_LEASE_BREAKING) {
                conflict = NULL;
            }
            pthread_mutex_unlock(&file->lock);
        }

        if (began_live_break) {
            /* A live holder is mid-break; the caller waits for its ack. */
            return CHIMERA_VFS_LEASE_BREAKING;
        }

        if (revoked_any) {
            /* Only dead/unresponsive holders were breakable and they are now
             * revoked; re-probe from the top -- the acquire likely succeeds. */
            continue;
        }

        /* A breakable holder is mid-break with its deadline still pending and
         * nothing was actionable this pass; the caller waits for the ack. */
        return CHIMERA_VFS_LEASE_BREAKING;
    }
} /* chimera_vfs_state_try_insert */

/* -------------------------------------------------------------------- */
/* Pending-acquire queue                                                */
/* -------------------------------------------------------------------- */

/* All three helpers expect file->lock held by caller. */

static inline void
chimera_vfs_pending_enqueue_locked(
    struct chimera_vfs_file_state      *file,
    struct chimera_vfs_pending_acquire *ticket)
{
    ticket->file   = file;
    ticket->queued = true;
    ticket->next   = NULL;
    ticket->prev   = file->pending_tail;

    if (file->pending_tail) {
        file->pending_tail->next = ticket;
    } else {
        file->pending_head = ticket;
    }
    file->pending_tail = ticket;
} /* chimera_vfs_pending_enqueue_locked */

static inline void
chimera_vfs_pending_dequeue_locked(
    struct chimera_vfs_file_state      *file,
    struct chimera_vfs_pending_acquire *ticket)
{
    if (ticket->prev) {
        ticket->prev->next = ticket->next;
    } else if (file->pending_head == ticket) {
        file->pending_head = ticket->next;
    }

    if (ticket->next) {
        ticket->next->prev = ticket->prev;
    } else if (file->pending_tail == ticket) {
        file->pending_tail = ticket->prev;
    }

    ticket->prev   = NULL;
    ticket->next   = NULL;
    ticket->queued = false;
} /* chimera_vfs_pending_dequeue_locked */

static inline struct chimera_vfs_pending_acquire *
chimera_vfs_pending_drain_locked(struct chimera_vfs_file_state *file)
{
    struct chimera_vfs_pending_acquire *head = file->pending_head;
    struct chimera_vfs_pending_acquire *t;

    file->pending_head = NULL;
    file->pending_tail = NULL;
    for (t = head; t; t = t->next) {
        t->queued = false;
    }
    return head;
} /* chimera_vfs_pending_drain_locked */

/* Retry every queued acquire on `file`.  Called after any state mutation
 * that could clear a conflict (remove, ack with downgrade, revoke).  This
 * MUST be called with file->lock NOT held; it takes and releases the lock
 * internally. */
static void
chimera_vfs_state_pump_pending(
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file)
{
    struct chimera_vfs_pending_acquire *head, *t, *next;
    enum chimera_vfs_lease_result       result;
    struct chimera_vfs_lease           *conflict;

    pthread_mutex_lock(&file->lock);
    head = chimera_vfs_pending_drain_locked(file);
    pthread_mutex_unlock(&file->lock);

    for (t = head; t; t = next) {
        next     = t->next;
        t->prev  = NULL;
        t->next  = NULL;
        conflict = NULL;

        result = chimera_vfs_state_try_insert(state, file, t->lease, &conflict);

        if (result == CHIMERA_VFS_LEASE_BREAKING) {
            /* Re-queue and wait for the next ack/revoke.  begin_break
             * has already been invoked on the conflicting holder by
             * try_insert. */
            pthread_mutex_lock(&file->lock);
            chimera_vfs_pending_enqueue_locked(file, t);
            pthread_mutex_unlock(&file->lock);
            continue;
        }

        t->cb(result,
              result == CHIMERA_VFS_LEASE_GRANTED ? t->lease : NULL,
              conflict,
              t->private_data);
    }
} /* chimera_vfs_state_pump_pending */

SYMBOL_EXPORT void
chimera_vfs_state_remove(
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file,
    struct chimera_vfs_lease      *lease)
{
    (void) state;

    pthread_mutex_lock(&file->lock);
    chimera_vfs_file_state_remove_lease(file, lease);
    pthread_mutex_unlock(&file->lock);

    /* Removing a lease may unblock a pending acquire or a parked I/O. */
    chimera_vfs_state_pump_pending(state, file);
    chimera_vfs_state_pump_io(state, file);
} /* chimera_vfs_state_remove */

/* -------------------------------------------------------------------- */
/* Async acquire / release / test                                       */
/* -------------------------------------------------------------------- */

SYMBOL_EXPORT void
chimera_vfs_lease_acquire(
    struct chimera_vfs_state           *state,
    struct chimera_vfs_file_state      *file,
    struct chimera_vfs_lease           *lease,
    struct chimera_vfs_pending_acquire *ticket,
    bool                                wait,
    chimera_vfs_lease_acquire_cb_t      cb,
    void                               *private_data)
{
    enum chimera_vfs_lease_result result;
    struct chimera_vfs_lease     *conflict = NULL;

    ticket->lease        = lease;
    ticket->cb           = cb;
    ticket->private_data = private_data;
    ticket->file         = file;
    ticket->queued       = false;
    ticket->prev         = NULL;
    ticket->next         = NULL;

    result = chimera_vfs_state_try_insert(state, file, lease, &conflict);

    if (result == CHIMERA_VFS_LEASE_BREAKING && wait) {
        /* Conflict is breakable and caller said to wait.  Queue the
         * ticket; it will fire when the break completes (via pump). */
        pthread_mutex_lock(&file->lock);
        chimera_vfs_pending_enqueue_locked(file, ticket);
        pthread_mutex_unlock(&file->lock);
        return;
    }

    /* Synchronous outcome (GRANTED, DENIED, or wait==false BREAKING). */
    cb(result,
       result == CHIMERA_VFS_LEASE_GRANTED ? lease : NULL,
       conflict,
       private_data);
} /* chimera_vfs_lease_acquire */

SYMBOL_EXPORT void
chimera_vfs_lease_release(
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file,
    struct chimera_vfs_lease      *lease)
{
    chimera_vfs_state_remove(state, file, lease);
} /* chimera_vfs_lease_release */

/* -------------------------------------------------------------------- */
/* Caching grant — VFS-owned shared caching lease                       */
/* -------------------------------------------------------------------- */

/* Find an existing caching grant on `file` matching `owner`.  Caller holds
 * file->lock. */
static struct chimera_vfs_caching_grant *
chimera_vfs_caching_grant_find_locked(
    struct chimera_vfs_file_state        *file,
    const struct chimera_vfs_lease_owner *owner)
{
    struct chimera_vfs_caching_grant *g;

    for (g = file->caching_grants; g; g = g->grant_next) {
        if (chimera_vfs_lease_owner_equal(&g->lease.owner, owner)) {
            return g;
        }
        /* MS-SMB2 same-lease-key: an open that presents the same LeaseKey as an
         * existing SMB2 RqLs lease grant joins that grant (shares its cache
         * state), even when it arrives on a different ClientGuid/connection.
         * Windows keys the underlying oplock by lease key, so two opens with one
         * lease key share a single lease rather than breaking one another (WPTS
         * Leasing_FileLeasing{V1,V2}_SameLeaseKey).  Matched here so the second
         * open is granted the existing state (e.g. RWH) and no break fires.
         * Restricted to lease grants (!is_oplock): a legacy oplock is keyed by a
         * per-open file id, never a shareable lease key. */
        if (!g->is_oplock &&
            chimera_vfs_lease_smb2_same_key(&g->lease.owner, owner)) {
            return g;
        }
    }
    return NULL;
} /* chimera_vfs_caching_grant_find_locked */

SYMBOL_EXPORT struct chimera_vfs_caching_grant *
chimera_vfs_caching_grant_coalesce(
    struct chimera_vfs_file_state        *file,
    const struct chimera_vfs_lease_owner *owner,
    struct chimera_vfs_lease_mode         want,
    int                                   upgrade_ok)
{
    struct chimera_vfs_caching_grant *grant;

    pthread_mutex_lock(&file->lock);
    grant = chimera_vfs_caching_grant_find_locked(file, owner);
    if (grant) {
        grant->refcount++;

        /* A re-open while the lease is mid-break must NOT upgrade it: MS-SMB2
         * 3.3.5.9.11 has the re-open succeed at the lease's CURRENT (downgrading)
         * state and report SMB2_LEASE_FLAG_BREAK_IN_PROGRESS instead of granting
         * back the bits being broken away (smb2.lease.breaking3).  Only an IDLE
         * lease can be upgraded. */
        if (upgrade_ok &&
            grant->lease.break_state == CHIMERA_VFS_BREAK_IDLE) {
            uint8_t cur = grant->lease.mode.granted;

            /* MS-SMB2 3.3.5.9.11: a lease is upgraded to the REQUESTED state only
             * when that state is a (strict) superset of the currently held state;
             * a request that is not a superset (a downgrade, or a lateral move such
             * as RH -> RW) leaves the lease unchanged.  This is not a bitwise union:
             * RH + a request for RW stays RH, it does not become RWH. */
            if (want.granted != cur &&
                (want.granted & cur) == cur) {
                struct chimera_vfs_lease  probe    = grant->lease;
                struct chimera_vfs_lease *conflict = NULL;

                /* And only if the larger mode is grantable without breaking another
                 * owner's holder (would_conflict coalesces our own same-owner
                 * lease).  Never break another lease for an upgrade -- keep the
                 * current mode otherwise. */
                probe.mode.granted = want.granted;
                if (chimera_vfs_state_would_conflict(file, &probe, &conflict) ==
                    CHIMERA_VFS_LEASE_GRANTED) {
                    grant->lease.mode.granted = want.granted;
                    grant->epoch++;
                }
            }
        }
    }
    pthread_mutex_unlock(&file->lock);
    return grant;
} /* chimera_vfs_caching_grant_coalesce */

SYMBOL_EXPORT void
chimera_vfs_caching_grant_link(
    struct chimera_vfs_file_state    *file,
    struct chimera_vfs_caching_grant *grant)
{
    pthread_mutex_lock(&file->lock);
    grant->grant_next    = file->caching_grants;
    file->caching_grants = grant;
    pthread_mutex_unlock(&file->lock);
} /* chimera_vfs_caching_grant_link */

SYMBOL_EXPORT enum chimera_vfs_lease_result
chimera_vfs_caching_grant_acquire(
    struct chimera_vfs_state             *state,
    struct chimera_vfs_file_state        *file,
    const struct chimera_vfs_lease_owner *owner,
    struct chimera_vfs_lease_mode         want,
    int                                   upgrade_ok,
    struct chimera_vfs_caching_grant    **grant_out,
    struct chimera_vfs_lease            **conflict_out)
{
    struct chimera_vfs_caching_grant *grant, *existing;
    enum chimera_vfs_lease_result     result;

    *grant_out = NULL;
    if (conflict_out) {
        *conflict_out = NULL;
    }

    /* Coalesce onto an existing same-owner grant. */
    grant = chimera_vfs_caching_grant_coalesce(file, owner, want, upgrade_ok);
    if (grant) {
        *grant_out = grant;
        return CHIMERA_VFS_LEASE_GRANTED;
    }

    /* No existing grant: allocate one and arbitrate against other owners. */
    grant = calloc(1, sizeof(*grant));
    if (!grant) {
        return CHIMERA_VFS_LEASE_DENIED;
    }
    grant->lease.kind        = CHIMERA_VFS_LEASE_CACHING;
    grant->lease.mode        = want;
    grant->lease.owner       = *owner;
    grant->lease.grant       = grant;
    grant->lease.break_state = CHIMERA_VFS_BREAK_IDLE;
    grant->file              = file;
    grant->refcount          = 1;
    grant->epoch             = 1;

    result = chimera_vfs_state_try_insert(state, file, &grant->lease, conflict_out);
    if (result != CHIMERA_VFS_LEASE_GRANTED) {
        free(grant);
        return result;
    }

    /* Link onto the per-file grant index.  Re-check under the lock that no peer
     * created the same-owner grant while try_insert ran (would_conflict
     * coalesces same-owner, so two concurrent first-acquires could each insert a
     * lease for one owner -- collapse onto the one that linked first). */
    pthread_mutex_lock(&file->lock);
    existing = chimera_vfs_caching_grant_find_locked(file, owner);
    if (existing) {
        existing->refcount++;
        chimera_vfs_file_state_remove_lease(file, &grant->lease);
        pthread_mutex_unlock(&file->lock);
        free(grant);
        *grant_out = existing;
        return CHIMERA_VFS_LEASE_GRANTED;
    }
    grant->grant_next    = file->caching_grants;
    file->caching_grants = grant;
    pthread_mutex_unlock(&file->lock);

    *grant_out = grant;
    return CHIMERA_VFS_LEASE_GRANTED;
} /* chimera_vfs_caching_grant_acquire */

SYMBOL_EXPORT void
chimera_vfs_caching_grant_release(
    struct chimera_vfs_state         *state,
    struct chimera_vfs_caching_grant *grant,
    bool                              pump)
{
    struct chimera_vfs_file_state     *file = grant->file;
    struct chimera_vfs_caching_grant **pp;
    bool                               last;

    pthread_mutex_lock(&file->lock);

    chimera_vfs_abort_if(grant->refcount == 0,
                         "double release of caching grant");
    grant->refcount--;
    last = (grant->refcount == 0);

    if (last) {
        /* Unlink from the owner index and from the caching_leases list. */
        for (pp = &file->caching_grants; *pp; pp = &(*pp)->grant_next) {
            if (*pp == grant) {
                *pp = grant->grant_next;
                break;
            }
        }
        chimera_vfs_file_state_remove_lease(file, &grant->lease);
    }

    pthread_mutex_unlock(&file->lock);

    if (last) {
        /* Removing the lease may unblock a pending acquire or parked I/O -- but
         * only pump when a live connection still exists to answer the woken
         * waiter (skipped at thread shutdown; see chimera_smb_durable_drain_all). */
        if (pump) {
            chimera_vfs_state_pump_pending(state, file);
            chimera_vfs_state_pump_io(state, file);
        }
        free(grant);
    }
} /* chimera_vfs_caching_grant_release */

SYMBOL_EXPORT void
chimera_vfs_state_revoke_breaks(
    struct chimera_vfs_state               *state,
    const uint8_t                          *fh,
    uint8_t                                 fh_len,
    uint64_t                                fh_hash,
    const struct chimera_vfs_caching_grant *except)
{
    struct chimera_vfs_file_state *file;
    struct chimera_vfs_lease      *to_revoke[CHIMERA_VFS_STATE_MAX_BREAK_BATCH];
    struct chimera_vfs_lease      *cur;
    int                            n = 0;
    int                            i;

    if (!state || fh_len == 0) {
        return;
    }

    file = chimera_vfs_state_get(state, fh, fh_len, fh_hash, false);
    if (!file) {
        return;
    }

    pthread_mutex_lock(&file->lock);
    for (cur = file->caching_leases; cur; cur = cur->next) {
        if (cur->break_state == CHIMERA_VFS_BREAK_BREAKING &&
            cur->grant != except &&
            n < CHIMERA_VFS_STATE_MAX_BREAK_BATCH) {
            to_revoke[n++] = cur;
        }
    }
    pthread_mutex_unlock(&file->lock);

    for (i = 0; i < n; i++) {
        chimera_vfs_lease_revoke(to_revoke[i]);
    }

    chimera_vfs_state_put(state, file);
} /* chimera_vfs_state_revoke_breaks */

SYMBOL_EXPORT bool
chimera_vfs_state_caching_breaking(
    struct chimera_vfs_state               *state,
    const uint8_t                          *fh,
    uint8_t                                 fh_len,
    uint64_t                                fh_hash,
    const struct chimera_vfs_caching_grant *except)
{
    struct chimera_vfs_file_state *file;
    struct chimera_vfs_lease      *cur;
    bool                           breaking = false;

    if (!state || fh_len == 0) {
        return false;
    }

    file = chimera_vfs_state_get(state, fh, fh_len, fh_hash, false);
    if (!file) {
        return false;
    }

    pthread_mutex_lock(&file->lock);
    for (cur = file->caching_leases; cur; cur = cur->next) {
        /* Only an RqLs LEASE break holds the conflicting open pending (MS-SMB2
         * 3.3.5.9).  A legacy oplock keeps chimera's optimistic post-break
         * behavior (the open proceeds without waiting), so exclude is_oplock
         * grants here. */
        if (cur->break_state == CHIMERA_VFS_BREAK_BREAKING &&
            cur->grant && cur->grant->break_ack_required &&
            !cur->grant->is_oplock &&
            cur->grant != except) {
            breaking = true;
            break;
        }
    }
    pthread_mutex_unlock(&file->lock);

    chimera_vfs_state_put(state, file);
    return breaking;
} /* chimera_vfs_state_caching_breaking */

SYMBOL_EXPORT bool
chimera_vfs_lease_acquire_cancel(
    struct chimera_vfs_state           *state,
    struct chimera_vfs_pending_acquire *ticket)
{
    struct chimera_vfs_file_state *file = ticket->file;
    bool                           was_queued;

    (void) state;

    if (!file) {
        return false;
    }

    pthread_mutex_lock(&file->lock);
    was_queued = ticket->queued;
    if (was_queued) {
        chimera_vfs_pending_dequeue_locked(file, ticket);
    }
    pthread_mutex_unlock(&file->lock);

    return was_queued;
} /* chimera_vfs_lease_acquire_cancel */

SYMBOL_EXPORT enum chimera_vfs_lease_result
chimera_vfs_lease_test(
    struct chimera_vfs_file_state  *file,
    const struct chimera_vfs_lease *probe,
    struct chimera_vfs_lease      **conflict_out)
{
    enum chimera_vfs_lease_result result;

    pthread_mutex_lock(&file->lock);
    result = chimera_vfs_state_would_conflict(file, probe, conflict_out);
    pthread_mutex_unlock(&file->lock);
    return result;
} /* chimera_vfs_lease_test */

SYMBOL_EXPORT bool
chimera_vfs_state_range_io_conflict(
    struct chimera_vfs_state             *state,
    const uint8_t                        *fh,
    uint8_t                               fh_len,
    uint64_t                              fh_hash,
    uint64_t                              offset,
    uint64_t                              length,
    bool                                  is_write,
    const struct chimera_vfs_lease_owner *owner)
{
    struct chimera_vfs_file_state *file;
    struct chimera_vfs_lease      *cur;
    bool                           conflict = false;

    /* Fast path: with no per-file state there are no byte-range locks. */
    file = chimera_vfs_state_get(state, fh, fh_len, fh_hash, false);
    if (!file) {
        return false;
    }

    pthread_mutex_lock(&file->lock);
    for (cur = file->range_locks; cur; cur = cur->next) {
        if (!chimera_vfs_range_overlap(cur->offset, cur->length, offset, length)) {
            continue;
        }
        if (cur->mode.granted & CHIMERA_VFS_LEASE_MODE_W) {
            /* Exclusive lock: blocks all I/O from other owners; the lock
             * owner may still read and write within its own range. */
            if (!chimera_vfs_lease_owner_equal(&cur->owner, owner)) {
                conflict = true;
                break;
            }
        } else {
            /* Shared lock: reads are permitted for everyone, but writes are
             * denied for everyone — including the lock owner (MS-FSA). */
            if (is_write) {
                conflict = true;
                break;
            }
        }
    }
    pthread_mutex_unlock(&file->lock);

    chimera_vfs_state_put(state, file);
    return conflict;
} /* chimera_vfs_state_range_io_conflict */

/* -------------------------------------------------------------------- */
/* Break orchestration                                                  */
/* -------------------------------------------------------------------- */

/* Compute the next single-bit break step: the holder's granted mode with the
 * ONE highest-priority bit that is still above `floor` removed.  Priority is
 * W (write cache, flush first) > H (handle cache) > D > R (read cache, the most
 * shareable, dropped last).  A lease cascades one bit per ack toward its floor
 * (RWH -> RH -> R -> NONE for floor 0), which is what the MS-SMB2 lease-break
 * sequence the smbtorture breaking* tests assert requires.  Returns `granted`
 * unchanged once it is already at or below the floor. */
/* True for a lease that breaks one caching bit at a time (an SMB2 RqLs lease).
 * A legacy SMB oplock breaks as one indivisible level, and an NFSv4 delegation
 * is recalled whole; both jump straight to the floor instead of cascading. */
static inline bool
chimera_vfs_lease_cascades(const struct chimera_vfs_lease *lease)
{
    return lease->grant != NULL && !lease->grant->is_oplock;
} /* chimera_vfs_lease_cascades */

static inline uint8_t
chimera_vfs_lease_break_step(
    uint8_t granted,
    uint8_t floor)
{
    uint8_t excess = granted & ~floor;

    if (excess & CHIMERA_VFS_LEASE_MODE_W) {
        return granted & ~CHIMERA_VFS_LEASE_MODE_W;
    }
    if (excess & CHIMERA_VFS_LEASE_MODE_H) {
        return granted & ~CHIMERA_VFS_LEASE_MODE_H;
    }
    if (excess & CHIMERA_VFS_LEASE_MODE_D) {
        return granted & ~CHIMERA_VFS_LEASE_MODE_D;
    }
    if (excess & CHIMERA_VFS_LEASE_MODE_R) {
        return granted & ~CHIMERA_VFS_LEASE_MODE_R;
    }
    return granted;
} /* chimera_vfs_lease_break_step */

/* `floor` is the ultimate target of the break (the maximal mode the holder may
 * keep given the conflict that triggered it) -- NOT the immediate downgrade.
 * The first notification asks the holder to drop a single bit toward that floor;
 * chimera_vfs_lease_ack fires the remaining steps one per ack.  When a break is
 * already in flight, a new conflict with a lower floor only deepens the target
 * (the in-flight step still stands and the cascade carries on to the new floor);
 * it does not re-notify. */
SYMBOL_EXPORT void
chimera_vfs_lease_begin_break(
    struct chimera_vfs_state *state,
    struct chimera_vfs_lease *lease,
    uint8_t                   floor,
    uint32_t                  deadline_ms)
{
    struct chimera_vfs_file_state *file = lease->file;
    chimera_vfs_lease_break_cb_t   cb;
    void                          *cb_priv;
    uint8_t                        step = 0;
    bool                           should_invoke;

    if (deadline_ms == 0) {
        deadline_ms = state->default_break_deadline_ms;
    }

    if (file) {
        pthread_mutex_lock(&file->lock);
    }

    if (lease->break_state == CHIMERA_VFS_BREAK_BREAKING) {
        /* Cascade already running: deepen the floor (a writer arriving behind a
         * reader, an overwrite behind a plain open) so it continues lower, but
         * leave the outstanding notification alone. */
        lease->break_floor &= floor;
        should_invoke       = false;
    } else {
        /* Only an SMB2 RqLs lease cascades one bit per ack toward the floor.  A
         * legacy SMB oplock (one indivisible level: exclusive/batch -> II ->
         * none) and an NFSv4 delegation (CB_RECALL returns the whole delegation)
         * both break in a single shot, so they jump straight to the floor. */
        step = chimera_vfs_lease_cascades(lease)
            ? chimera_vfs_lease_break_step(lease->mode.granted, floor)
            : floor;
        should_invoke = (step != lease->mode.granted) &&
            (lease->owner.break_cb != NULL);
    }

    if (should_invoke) {
        lease->break_state       = CHIMERA_VFS_BREAK_BREAKING;
        lease->break_floor       = floor;
        lease->break_needed_mode = step;
        lease->break_deadline    = chimera_vfs_now_ticks() +
            chimera_vfs_ns_to_ticks((uint64_t) deadline_ms * 1000000ULL);
        cb      = lease->owner.break_cb;
        cb_priv = lease->owner.cb_private;
    } else {
        cb      = NULL;
        cb_priv = NULL;
    }

    if (file) {
        pthread_mutex_unlock(&file->lock);
    }

    /* Invoke the break callback outside the file lock — the protocol
     * server may need to send packets, take other locks, etc. */
    if (cb) {
        cb(lease, step, cb_priv);
    }
} /* chimera_vfs_lease_begin_break */

SYMBOL_EXPORT void
chimera_vfs_lease_ack(
    struct chimera_vfs_lease     *lease,
    struct chimera_vfs_lease_mode resulting)
{
    struct chimera_vfs_file_state *file    = lease->file;
    bool                           mutated = false;
    chimera_vfs_lease_break_cb_t   cb      = NULL;
    void                          *cb_priv = NULL;
    uint8_t                        step    = 0;

    if (file) {
        pthread_mutex_lock(&file->lock);
    }

    if (lease->break_state == CHIMERA_VFS_BREAK_BREAKING) {
        lease->mode = resulting;

        /* Cascade: the conflict that started this break may need the holder
         * lower than the bit it just acked (a plain RW open drives RWH -> RH ->
         * R -> NONE).  If the acked mode is still above the floor, fire the next
         * single-bit break immediately and stay BREAKING -- the waiter parked on
         * this break keeps waiting and a same-key re-open still sees the break in
         * progress.  Only when the holder reaches the floor does the break
         * finish: re-arm to IDLE if it kept any mode (a later conflict can break
         * it again) or go inert (ACKED) at NONE until the holder closes. */
        step = chimera_vfs_lease_cascades(lease)
            ? chimera_vfs_lease_break_step(resulting.granted, lease->break_floor)
            : resulting.granted;

        if (step != resulting.granted && lease->owner.break_cb) {
            uint32_t deadline_ms = (file && file->state)
                ? file->state->default_break_deadline_ms
                : CHIMERA_VFS_STATE_DEFAULT_BREAK_DEADLINE_MS;

            lease->break_needed_mode = step;
            lease->break_deadline    = chimera_vfs_now_ticks() +
                chimera_vfs_ns_to_ticks((uint64_t) deadline_ms * 1000000ULL);
            cb      = lease->owner.break_cb;
            cb_priv = lease->owner.cb_private;
        } else {
            lease->break_state = resulting.granted ? CHIMERA_VFS_BREAK_IDLE
                                                   : CHIMERA_VFS_BREAK_ACKED;
            mutated = true;
        }
    }

    if (file) {
        pthread_mutex_unlock(&file->lock);
    }

    /* Fire the next cascade step (outside the lock — the SMB cb sends a packet). */
    if (cb) {
        cb(lease, step, cb_priv);
        return;
    }

    /* Acking the final step may unblock pending acquires or parked I/O. */
    if (mutated && file && file->state) {
        chimera_vfs_state_pump_pending(file->state, file);
        chimera_vfs_state_pump_io(file->state, file);
    }
} /* chimera_vfs_lease_ack */

SYMBOL_EXPORT void
chimera_vfs_lease_revoke(struct chimera_vfs_lease *lease)
{
    struct chimera_vfs_file_state *file = lease->file;
    chimera_vfs_lease_revoked_cb_t revoked_cb;
    void                          *cb_private;
    bool                           newly_revoked;

    if (file) {
        pthread_mutex_lock(&file->lock);
    }

    newly_revoked       = (lease->break_state != CHIMERA_VFS_BREAK_REVOKED);
    lease->mode.granted = 0;
    lease->mode.denied  = 0;
    lease->break_state  = CHIMERA_VFS_BREAK_REVOKED;
    revoked_cb          = lease->owner.revoked_cb;
    cb_private          = lease->owner.cb_private;

    if (file) {
        pthread_mutex_unlock(&file->lock);
    }

    /* Notify the protocol layer (once) that this lease was force-revoked. */
    if (newly_revoked && revoked_cb) {
        revoked_cb(lease, cb_private);
    }

    /* Revoking a lease may unblock pending acquires or parked I/O. */
    if (file && file->state) {
        chimera_vfs_state_pump_pending(file->state, file);
        chimera_vfs_state_pump_io(file->state, file);
    }
} /* chimera_vfs_lease_revoke */

/* Recall every caching lease on `file`, regardless of owner.  Caller holds a
 * reference on `file`.  Returns true if any caching holder still retains a
 * granted mode (the recall is in flight; caller should wait/retry). */
static bool
chimera_vfs_break_caching_file(
    struct chimera_vfs_state             *state,
    struct chimera_vfs_file_state        *file,
    const struct chimera_vfs_open_handle *skip_handle,
    bool                                  flush_only)
{
    bool had;

    /* Drive each caching holder forward: recall a still-IDLE holder, and
     * revoke a holder whose recall deadline has elapsed (never returned).
     * begin_break flips IDLE->BREAKING and revoke clears the granted mode, so
     * neither is re-selected; the loop terminates once only not-yet-expired
     * BREAKING holders remain. */
    for ( ; ; ) {
        struct chimera_vfs_lease *cur;
        struct chimera_vfs_lease *to_break  = NULL;
        struct chimera_vfs_lease *to_revoke = NULL;

        pthread_mutex_lock(&file->lock);
        for (cur = file->caching_leases; cur; cur = cur->next) {
            if (cur->mode.granted == 0 || !cur->owner.break_cb) {
                continue;
            }
            /* The mutation is issued through this very lease's open handle: the
             * holder is coherent with its own change, so do not recall it. */
            if (skip_handle && cur->owner.op_handle == skip_handle) {
                continue;
            }
            /* Flush recall: only a write-caching (W) holder has dirty data to
             * flush; a read-only / handle-only holder keeps its cache untouched
             * (a data setattr does not stale a pure read cache -- the client
             * revalidates on the resulting mtime/size change).  This is what lets
             * a same-client metadata storm stop churning: once a holder has
             * dropped W it is no longer recalled.
             *
             * This filter is an SMB read-cache optimization only.  An NFSv4
             * delegation is not just a data cache: it guarantees attribute
             * stability and that no foreign change occurs without a recall (RFC
             * 7530 10.4), so a metadata-only setattr (e.g. a server-side chmod,
             * pynfs DELEG20) MUST recall a read delegation held by another party.
             * The mutating handle's own lease is already spared by skip_handle
             * above, so this only ever recalls foreign delegations. */
            if (flush_only && !(cur->mode.granted & CHIMERA_VFS_LEASE_MODE_W) &&
                cur->owner.protocol != CHIMERA_VFS_LEASE_PROTO_NFSV4) {
                continue;
            }
            if (cur->break_state == CHIMERA_VFS_BREAK_IDLE) {
                to_break = cur;
                break;
            }
            /* A holder that already acked an earlier break but still retains a
             * mode (an SMB lease downgraded to LEVEL_II, granted=R, ACKED) must
             * be re-armed and broken again — a namespace/metadata mutation
             * needs the cache gone entirely.  Reset to IDLE so begin_break
             * re-fires it to NONE.  (NFSv4 delegations never sit ACKED with a
             * retained mode — they return to NONE or are revoked — so this
             * only affects SMB read caches.) */
            if (cur->break_state == CHIMERA_VFS_BREAK_ACKED) {
                cur->break_state = CHIMERA_VFS_BREAK_IDLE;
                to_break         = cur;
                break;
            }
            if (cur->break_state == CHIMERA_VFS_BREAK_BREAKING &&
                chimera_vfs_break_deadline_passed(cur)) {
                to_revoke = cur;
                break;
            }
        }
        pthread_mutex_unlock(&file->lock);

        if (to_break) {
            /* Break to NONE (needed_mode 0): the holder writes back dirty data
             * and drops its cache entirely.  This holds for both recall flavors.
             *
             * A flush recall would ideally retain the holder's read cache (break
             * only W, keep R|H) to spare a metadata-storm client a full re-lease
             * per op.  But retaining R is only coherent if the changed region of
             * that read cache is also invalidated, which chimera has no mechanism
             * for today -- retaining R after a flush serves stale bytes (fsx
             * MAPREAD corruption).  So a flush still revokes its W holders to
             * NONE; what flush_only buys is the *filter* above -- read-only /
             * handle-only holders, which an attribute-only setattr does not
             * stale, are left untouched rather than needlessly recalled.  Fully
             * decoupling the flush from the read-cache drop is a follow-up.
             *
             * Passing the granted mode would let the SMB break_cb retain a read
             * cache and ack non-zero, so this loop would see granted != 0 forever
             * and park permanently.  NFSv4 delegations recall fully regardless. */
            chimera_vfs_lease_begin_break(state, to_break, 0,
                                          CHIMERA_VFS_NFS_DELEG_METAOP_MS);
        } else if (to_revoke) {
            chimera_vfs_lease_revoke(to_revoke);
        } else {
            break;
        }
    }

    /* Still blocked iff some caching holder retains an *effective* granted mode.
     * A holder mid-break is treated by its effective level: an SMB oplock/lease
     * that has been notified (BREAKING) is obligated to comply, so it no longer
     * blocks -- the metadata op proceeds and the client's real ack arrives
     * asynchronously (this matches the pre-real-break optimistic behaviour and
     * is what chimera_vfs_lease_effective_granted() encodes).  An NFSv4
     * delegation keeps its full mode while RECALLING, so it still blocks until
     * the client returns it or its recall deadline elapses (revoked above). */
    pthread_mutex_lock(&file->lock);
    had = false;
    {
        struct chimera_vfs_lease *cur;
        /* A flush recall only needs the write cache gone (W flushed); the holder
         * may keep R|H.  So it blocks only while some holder retains an effective
         * W -- a notified (BREAKING) SMB holder has effective == its retained
         * R|H (no W), so the op proceeds at once and the client's flush ack
         * arrives asynchronously (the client serialises break->writeback->reply
         * on its own connection, so a later read by it is still coherent).  A
         * namespace recall blocks while any effective mode remains. */
        uint8_t                   block_mask = flush_only ? CHIMERA_VFS_LEASE_MODE_W : 0xFF;
        for (cur = file->caching_leases; cur; cur = cur->next) {
            if (skip_handle && cur->owner.op_handle == skip_handle) {
                continue;
            }
            if (chimera_vfs_lease_effective_granted(cur) & block_mask) {
                had = true;
                break;
            }
        }
    }
    pthread_mutex_unlock(&file->lock);

    return had;
} /* chimera_vfs_break_caching_file */

SYMBOL_EXPORT bool
chimera_vfs_state_break_caching(
    struct chimera_vfs_state *state,
    const uint8_t            *fh,
    uint8_t                   fh_len,
    uint64_t                  fh_hash)
{
    struct chimera_vfs_file_state *file;
    bool                           had;

    file = chimera_vfs_state_get(state, fh, fh_len, fh_hash, false);
    if (!file) {
        return false;
    }

    /* No skip handle: this whole-file recall (NFSv4 REMOVE/RENAME) breaks every
     * caching holder, including the operating client's own delegation.  It is a
     * namespace mutation, so revoke fully (flush_only == false). */
    had = chimera_vfs_break_caching_file(state, file, NULL, false);

    chimera_vfs_state_put(state, file);
    return had;
} /* chimera_vfs_state_break_caching */

/* -------------------------------------------------------------------- */
/* Break-on-write                                                       */
/* -------------------------------------------------------------------- */

/* A write invalidates every read-caching (R) lease on the file: those
 * holders can no longer trust their cached data, so each must break down
 * to NONE.  The exception is the WRITER'S OWN lease (matched by owner key):
 * the writing handle is coherent with its own change, so its lease is not
 * recalled against itself -- whether that lease holds the write cache (an
 * exclusive/batch holder writing through its own cache) or only read+handle
 * caching (smb2.lease.complex1: a write by an RH holder breaks the OTHER
 * client's read lease, never its own RH).  A read cache held by a DIFFERENT
 * owner of the same client (a second lease key) is still staled and breaks.
 *
 * Breaks are dispatched via begin_break with needed_mode==0, which the
 * SMB break callback interprets as "break to NONE" (the open-conflict
 * path always passes a non-zero retained mask, so 0 is an unambiguous
 * write-invalidation signal).  Inner form: caller holds a reference on
 * `file`. */
static void
chimera_vfs_break_reads_for_write(
    struct chimera_vfs_state             *state,
    struct chimera_vfs_file_state        *file,
    const struct chimera_vfs_lease_owner *writer)
{
    struct chimera_vfs_lease *cur;
    struct chimera_vfs_lease *to_break[CHIMERA_VFS_STATE_MAX_BREAK_BATCH];
    int                       n = 0;
    int                       i;

    pthread_mutex_lock(&file->lock);

    for (cur = file->caching_leases; cur; cur = cur->next) {
        if ((cur->mode.granted & CHIMERA_VFS_LEASE_MODE_R) == 0) {
            continue;
        }
        if (chimera_vfs_lease_owner_equal(&cur->owner, writer)) {
            continue;
        }
        /* A holder sharing the writer's SMB2 lease key (even under a different
         * ClientGuid) shares the cache and is coherent with this write: do not
         * break it (MS-SMB2 same-lease-key; WPTS Leasing_*_SameLeaseKey writes
         * from both clients without breaking the peer's lease). */
        if (chimera_vfs_lease_smb2_same_key(&cur->owner, writer)) {
            continue;
        }
        if (n < CHIMERA_VFS_STATE_MAX_BREAK_BATCH) {
            to_break[n++] = cur;
        }
    }

    pthread_mutex_unlock(&file->lock);

    for (i = 0; i < n; i++) {
        chimera_vfs_lease_begin_break(state, to_break[i], 0, 0);
    }
} /* chimera_vfs_break_reads_for_write */

SYMBOL_EXPORT void
chimera_vfs_state_break_on_write(
    struct chimera_vfs_state             *state,
    const uint8_t                        *fh,
    uint8_t                               fh_len,
    uint64_t                              fh_hash,
    const struct chimera_vfs_lease_owner *writer)
{
    struct chimera_vfs_file_state *file;

    file = chimera_vfs_state_get(state, fh, fh_len, fh_hash, false);
    if (!file) {
        return;
    }

    chimera_vfs_break_reads_for_write(state, file, writer);

    chimera_vfs_state_put(state, file);
} /* chimera_vfs_state_break_on_write */

/* Break-on-open: a conflicting open by a *different* owner breaks caching
 * holders.  `trigger_bits` selects which holders break -- only a lease whose
 * granted mode intersects `trigger_bits` is affected -- and `retain_mode` is
 * the mask the holder keeps (R to downgrade an exclusive/batch oplock to
 * LEVEL_II, 0 to break all the way to NONE).  Holders the opener owns are
 * skipped.  The break is optimistic from vfs_state's point of view (it is
 * queued; the opener proceeds), but the SMB layer may choose to wait. */
SYMBOL_EXPORT void
chimera_vfs_state_break_caching_for_open(
    struct chimera_vfs_state             *state,
    const uint8_t                        *fh,
    uint8_t                               fh_len,
    uint64_t                              fh_hash,
    const struct chimera_vfs_lease_owner *opener,
    uint8_t                               trigger_bits,
    uint8_t                               retain_mode)
{
    struct chimera_vfs_file_state *file;
    struct chimera_vfs_lease      *cur;
    struct chimera_vfs_lease      *to_break[CHIMERA_VFS_STATE_MAX_BREAK_BATCH];
    int                            n = 0;
    int                            i;

    file = chimera_vfs_state_get(state, fh, fh_len, fh_hash, false);
    if (!file) {
        return;
    }

    pthread_mutex_lock(&file->lock);

    for (cur = file->caching_leases; cur; cur = cur->next) {
        /* Only holders that actually hold one of the trigger bits beyond what
         * they get to retain need to break. */
        if ((cur->mode.granted & trigger_bits & ~retain_mode) == 0) {
            continue;
        }
        if (chimera_vfs_lease_owner_equal(&cur->owner, opener)) {
            continue;
        }
        /* Same SMB2 lease key (even from a different ClientGuid): the opens share
         * one logical lease and the holder is NOT broken (MS-SMB2 same-lease-key
         * caching; mirrors the SHARE-side has_break_skip_key). */
        if (chimera_vfs_lease_smb2_same_key(&cur->owner, opener)) {
            continue;
        }
        /* A *non-lease* open by the SAME client must not recall that client's own
         * RqLs lease.  The opening client is internally coherent with its own
         * (non-data) open, so a server-driven break here is spurious -- and the
         * Linux SMB client cannot ack a break for a lease its new plain handle
         * does not own, so the break goes unacked, the lease is stuck BREAKING,
         * and every such open then stalls the full break-deadline (cthon special
         * hang).  Coherence on an actual write is preserved by the untouched
         * chimera_vfs_break_on_write path -- so this suppresses ONLY the open
         * break, never the write break (the over-broad earlier suppression of the
         * write break served stale reads and was reverted).  A *lease* opener with
         * a different key (2nd LeaseKey, same client) still breaks here, as
         * MS-SMB2 Leasing_*_SameLeaseKey / smb2.lease.complex1 require. */
        if (opener->protocol == CHIMERA_VFS_LEASE_PROTO_SMB2 &&
            cur->owner.protocol == CHIMERA_VFS_LEASE_PROTO_SMB2 &&
            !opener->is_lease &&
            opener->client_key == cur->owner.client_key &&
            cur->grant && !cur->grant->is_oplock) {
            continue;
        }
        /* A pure handle-trigger break (the pre-share-check pass) is a legacy SMB
         * oplock behavior: a batch oplock's handle is recalled before the share
         * decision so the holder can close.  An SMB2 RqLs LEASE keeps its handle
         * cache on a conflicting open (only its write cache is exclusive), so do
         * not handle-break a lease here -- its write cache is invalidated by the
         * separate W-trigger pass / caching-contention break instead. */
        if (trigger_bits == CHIMERA_VFS_LEASE_MODE_H &&
            cur->grant && !cur->grant->is_oplock) {
            continue;
        }
        if (n < CHIMERA_VFS_STATE_MAX_BREAK_BATCH) {
            to_break[n++] = cur;
        }
    }

    pthread_mutex_unlock(&file->lock);

    for (i = 0; i < n; i++) {
        chimera_vfs_lease_begin_break(state, to_break[i], retain_mode, 0);
    }

    chimera_vfs_state_put(state, file);
} /* chimera_vfs_state_break_caching_for_open */

/* -------------------------------------------------------------------- */
/* Implicit I/O lease                                                   */
/* -------------------------------------------------------------------- */

/*
 * The VFS core holds an implicit lease on behalf of actors that perform
 * I/O without requesting a protocol lease (NFSv3, S3, NFSv4 data I/O).
 * The lease is a deny-nothing SHARE registered on the file's share list
 * under a chimera-internal owner.  It is *cached*: kept across operations
 * and dropped only when it goes idle (chimera_vfs_state_reap_idle) or when
 * another holder recalls it (chimera_vfs_implicit_break_cb).  Each in-flight
 * I/O pins it (implicit_inflight); a recall waits for the pin count to drain
 * before the lease is dropped.  Modeling it as a deny=0 SHARE means two
 * independent leaseless writers never serialize against each other, yet a
 * write-share still recalls another client's conflicting delegation/oplock
 * and a real client open can recall the implicit lease in turn.
 */

static void
chimera_vfs_implicit_break_cb(
    struct chimera_vfs_lease *lease,
    uint8_t                   needed_mode,
    void                     *private_data);

static void
chimera_vfs_implicit_finish_drain(
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file);

static void
chimera_vfs_io_resume(
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file,
    struct chimera_vfs_request    *request);

/* Deterministic per-file owner for chimera's implicit lease.  protocol==
 * INTERNAL and client_key==0 distinguish it from every real client owner,
 * so it never coalesces with (and thus always recalls / is recalled by) a
 * client's delegation, oplock, or deny-mode open. */
static void
chimera_vfs_implicit_owner(
    struct chimera_vfs_file_state  *file,
    struct chimera_vfs_lease_owner *out)
{
    memset(out, 0, sizeof(*out));
    out->protocol   = CHIMERA_VFS_LEASE_PROTO_INTERNAL;
    out->client_key = 0;
    out->owner_lo   = file->fh_hash;
    out->owner_hi   = 0;
    out->break_cb   = chimera_vfs_implicit_break_cb;
    out->cb_private = file->state;
} /* chimera_vfs_implicit_owner */

/* Recall callback for the implicit lease: a conflicting holder needs the
 * file.  Stop admitting new I/O (implicit_draining) and, once the in-flight
 * pins drain, drop the lease.  Invoked outside file->lock by begin_break. */
static void
chimera_vfs_implicit_break_cb(
    struct chimera_vfs_lease *lease,
    uint8_t                   needed_mode,
    void                     *private_data)
{
    struct chimera_vfs_state      *state = private_data;
    struct chimera_vfs_file_state *file  = lease->file;
    bool                           drop_now;

    (void) needed_mode;

    if (!file) {
        return;
    }

    pthread_mutex_lock(&file->lock);
    file->implicit_draining = 1;
    drop_now                = (file->implicit_inflight == 0);
    pthread_mutex_unlock(&file->lock);

    if (drop_now) {
        chimera_vfs_implicit_finish_drain(state, file);
    }
    /* Otherwise the last chimera_vfs_io_lease_release() finishes the drain. */
} /* chimera_vfs_implicit_break_cb */

/* Remove the (drained) implicit lease and release the reference it held on
 * the file state.  Idempotent across the racing release / break_cb callers
 * via the implicit_active guard.  Pumps both wait queues so a recaller and
 * any parked I/O re-evaluate. */
static void
chimera_vfs_implicit_finish_drain(
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file)
{
    bool removed = false;

    pthread_mutex_lock(&file->lock);
    if (file->implicit_active) {
        chimera_vfs_file_state_remove_lease(file, &file->implicit_lease);
        file->implicit_active            = 0;
        file->implicit_lease.break_state = CHIMERA_VFS_BREAK_IDLE;
        removed                          = true;
    }
    file->implicit_draining = 0;
    pthread_mutex_unlock(&file->lock);

    if (removed) {
        chimera_vfs_state_pump_pending(state, file);
        chimera_vfs_state_pump_io(state, file);
        /* Drop the reference the lease itself held (taken at activation). */
        chimera_vfs_state_put(state, file);
    }
} /* chimera_vfs_implicit_finish_drain */

/* Drive begin_break / revoke on every breakable holder conflicting with
 * `probe` until none remain IDLE, mirroring the loop in
 * chimera_vfs_state_try_insert().  Returns the final would_conflict result:
 * GRANTED once all conflicts have cleared, DENIED on a non-breakable
 * holder, or BREAKING if holders are still mid-recall (caller parks).
 * Caller must NOT hold file->lock. */
static enum chimera_vfs_lease_result
chimera_vfs_state_drive_breaks(
    struct chimera_vfs_state       *state,
    struct chimera_vfs_file_state  *file,
    const struct chimera_vfs_lease *probe)
{
    struct chimera_vfs_lease     *conflict = NULL;
    enum chimera_vfs_lease_result result;

    pthread_mutex_lock(&file->lock);
    result = chimera_vfs_state_would_conflict(file, probe, &conflict);
    pthread_mutex_unlock(&file->lock);

    while (result == CHIMERA_VFS_LEASE_BREAKING && conflict) {
        if (conflict->break_state == CHIMERA_VFS_BREAK_IDLE) {
            uint32_t deadline_ms =
                (conflict->owner.protocol == CHIMERA_VFS_LEASE_PROTO_NFSV4)
                ? CHIMERA_VFS_NFS_DELEG_RECALL_MS : 0;
            chimera_vfs_lease_begin_break(state, conflict,
                                          chimera_vfs_break_retain_for(probe, conflict),
                                          deadline_ms);
        } else {
            chimera_vfs_lease_revoke(conflict);
        }
        conflict = NULL;
        pthread_mutex_lock(&file->lock);
        result = chimera_vfs_state_would_conflict(file, probe, &conflict);
        pthread_mutex_unlock(&file->lock);
    }

    return result;
} /* chimera_vfs_state_drive_breaks */

/* Park `request` on the file's I/O wait queue.  Caller holds file->lock. */
static void
chimera_vfs_io_park_locked(
    struct chimera_vfs_file_state *file,
    struct chimera_vfs_request    *request)
{
    struct chimera_vfs_pending_acquire *ticket = &request->io_lease_ticket;

    ticket->lease        = NULL;
    ticket->cb           = NULL;
    ticket->private_data = request;
    ticket->file         = file;
    ticket->queued       = true;
    ticket->next         = NULL;
    ticket->prev         = file->io_wait_tail;

    if (file->io_wait_tail) {
        file->io_wait_tail->next = ticket;
    } else {
        file->io_wait_head = ticket;
    }
    file->io_wait_tail = ticket;
} /* chimera_vfs_io_park_locked */

/* Forward decl: chimera_vfs_state_io_resume() and io_try's synchronous
 * conflict-clear path both reach io_try, which is defined below. */
static void
chimera_vfs_io_try(
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file,
    struct chimera_vfs_request    *request);

/* Retry every parked I/O request.  The pump can run on any thread (lease
 * release, break ack/revoke, or the idle reaper), so each request is
 * marshaled back to its owning thread (chimera_vfs_io_resume_post) rather than
 * resumed inline: its dispatch and reply must run on request->thread, whose
 * connection iovecs are thread-local.  chimera_vfs_state_io_resume() then
 * re-runs io_try on the owning thread. */
static void
chimera_vfs_state_pump_io(
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file)
{
    struct chimera_vfs_pending_acquire *head, *t, *next;

    (void) state;

    pthread_mutex_lock(&file->lock);
    head               = file->io_wait_head;
    file->io_wait_head = NULL;
    file->io_wait_tail = NULL;
    for (t = head; t; t = t->next) {
        t->queued = false;
    }
    pthread_mutex_unlock(&file->lock);

    for (t = head; t; t = next) {
        struct chimera_vfs_request *request = t->private_data;

        next    = t->next;
        t->prev = NULL;
        t->next = NULL;

        chimera_vfs_io_resume_post(request);
    }
} /* chimera_vfs_state_pump_io */

/* Resume a parked I/O request on its owning thread (called from that thread's
 * doorbell drain).  The request carries the pinned file in io_lease_file. */
SYMBOL_EXPORT void
chimera_vfs_state_io_resume(struct chimera_vfs_request *request)
{
    struct chimera_vfs_file_state *file = request->io_lease_file;

    if (!file) {
        return;
    }

    chimera_vfs_io_try(request->thread->vfs->vfs_state, file, request);
} /* chimera_vfs_state_io_resume */

/* Acquire / upgrade the implicit lease for `request` and either proceed
 * (io_next), park (to be retried by pump_io), or fail.  `file` carries the
 * request's in-flight reference (taken at acquire entry, released by
 * chimera_vfs_io_lease_release on the proceed/park paths, or dropped here on
 * the fail path).  Parking happens under the same lock that observes the
 * conflict, so a concurrent ack / finish_drain that pumps the queue cannot
 * lose the wakeup. */
static void
chimera_vfs_io_try(
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file,
    struct chimera_vfs_request    *request)
{
    struct chimera_vfs_lease      probe;
    struct chimera_vfs_lease     *conflict = NULL;
    enum chimera_vfs_lease_result result;
    uint8_t                       need;
    uint8_t                       target;
    bool                          was_active;
    bool                          activated = false;

    /* Namespace/metadata mutation: recall every caching lease on the target
     * file (regardless of owner — RFC 7530 §10.4.5 requires recalling even
     * the operating client's own delegation), then proceed.  No lease is held
     * on chimera's behalf.  The periodic maintenance pass re-pumps parked
     * waiters so an unresponsive holder is revoked at its recall deadline. */
    if (request->io_recall_all) {
        if (chimera_vfs_break_caching_file(state, file, request->io_handle,
                                           request->io_recall_flush_only)) {
            pthread_mutex_lock(&file->lock);
            chimera_vfs_io_park_locked(file, request);
            request->io_lease_file = file;
            pthread_mutex_unlock(&file->lock);
            return;
        }
        request->io_lease_file = NULL;
        chimera_vfs_state_put(state, file);
        request->io_next(request);
        return;
    }

    need = (request->opcode == CHIMERA_VFS_OP_WRITE)
        ? CHIMERA_VFS_LEASE_MODE_W : CHIMERA_VFS_LEASE_MODE_R;

    pthread_mutex_lock(&file->lock);

    if (file->implicit_draining) {
        /* A recall of our own lease is draining; wait for it to finish, then
         * finish_drain will pump us to re-acquire fresh. */
        chimera_vfs_io_park_locked(file, request);
        request->io_lease_file = file;
        pthread_mutex_unlock(&file->lock);
        return;
    }

    was_active = file->implicit_active;
    target     = was_active
        ? (uint8_t) (file->implicit_lease.mode.granted | need)
        : need;

    memset(&probe, 0, sizeof(probe));
    probe.kind         = CHIMERA_VFS_LEASE_SHARE;
    probe.mode.granted = target;
    probe.mode.denied  = 0;
    chimera_vfs_implicit_owner(file, &probe.owner);

    result = chimera_vfs_state_would_conflict(file, &probe, &conflict);

    if (result == CHIMERA_VFS_LEASE_GRANTED) {
        if (!was_active) {
            file->implicit_lease             = probe;
            file->implicit_lease.break_state = CHIMERA_VFS_BREAK_IDLE;
            chimera_vfs_file_state_insert_lease(file, &file->implicit_lease);
            file->implicit_active = 1;
            activated             = true;
        } else {
            file->implicit_lease.mode.granted = target;
        }
        file->implicit_inflight++;
        file->implicit_last_used = chimera_vfs_now_ticks();
        pthread_mutex_unlock(&file->lock);

        if (activated) {
            /* Reference for the lease itself, dropped by finish_drain. */
            chimera_vfs_state_get(state, file->fh, file->fh_len,
                                  file->fh_hash, false);
        }

        request->io_lease_file = file;
        if (need == CHIMERA_VFS_LEASE_MODE_W) {
            struct chimera_vfs_lease_owner iowner;
            chimera_vfs_implicit_owner(file, &iowner);
            chimera_vfs_break_reads_for_write(state, file, &iowner);
        }
        request->io_next(request);
        return;
    }

    if (result == CHIMERA_VFS_LEASE_DENIED) {
        pthread_mutex_unlock(&file->lock);
        request->io_lease_file = NULL;
        /* Only drop the reference if this request owns it; on the pinned-handle
         * fast path the handle owns the long-lived ref. */
        if (request->io_owns_lease_ref) {
            chimera_vfs_state_put(state, file);
        }
        request->status = CHIMERA_VFS_EACCES;
        request->complete(request);
        return;
    }

    /* BREAKING: park atomically with the conflict observation, then drive the
     * recalls outside the lock. */
    chimera_vfs_io_park_locked(file, request);
    request->io_lease_file = file;
    pthread_mutex_unlock(&file->lock);

    if (chimera_vfs_state_drive_breaks(state, file, &probe) !=
        CHIMERA_VFS_LEASE_BREAKING) {
        /* The conflict cleared synchronously (the breakable holder was
         * removed/revoked rather than going to an async recall); retry the
         * parked waiters now.  Otherwise an async recall is in flight and its
         * ack/revoke will pump us. */
        chimera_vfs_state_pump_io(state, file);
    }
} /* chimera_vfs_io_try */

SYMBOL_EXPORT void
chimera_vfs_io_lease_acquire(
    struct chimera_vfs_request           *request,
    const struct chimera_vfs_lease_owner *owner,
    void (                               *next )(struct chimera_vfs_request *request))
{
    struct chimera_vfs_state      *state = request->thread->vfs->vfs_state;
    struct chimera_vfs_file_state *file;

    request->io_next       = next;
    request->io_lease_file = NULL;

    /* A lease-holding client (NFSv4 delegation, SMB oplock) supplies its own
     * owner: it already registered its lease at open time, so per-I/O we
     * only invalidate other holders' read caches on a write.  No implicit
     * lease is held on its behalf. */
    if (owner) {
        if (request->opcode == CHIMERA_VFS_OP_WRITE) {
            chimera_vfs_state_break_on_write(state, request->fh,
                                             request->fh_len, request->fh_hash,
                                             owner);
        }
        next(request);
        return;
    }

    if (!state) {
        next(request);
        return;
    }

    /* Fast path: a cached open handle anchors the per-file state for its whole
     * lifetime, so we attach it once (lazily, racing other first-I/O threads via
     * an acquire/release CAS) and thereafter borrow the handle's reference per
     * I/O -- skipping the bucket-locked state_get/put entirely.  The handle's
     * long-lived ref plus opencnt>0 for the duration of this op (the protocol
     * drops opencnt only after the read/write callback returns, strictly after
     * io_lease_release) keep `file` alive.  io_owns_lease_ref stays 0 so release
     * does not state_put.  Synthetic/transient handles are not cached, so they
     * fall through to the legacy per-I/O path. */
    if (request->io_handle &&
        request->io_handle->cache_id != CHIMERA_VFS_OPEN_ID_SYNTHETIC) {
        struct chimera_vfs_open_handle *handle = request->io_handle;

        file = __atomic_load_n(&handle->file_state, __ATOMIC_ACQUIRE);
        if (!file) {
            struct chimera_vfs_file_state *expected = NULL;

            file = chimera_vfs_state_get(state, request->fh, request->fh_len,
                                         request->fh_hash, true);
            if (file &&
                !__atomic_compare_exchange_n(&handle->file_state, &expected, file,
                                             false, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
                /* Lost the attach race: another thread installed it first.  Drop
                 * our ref and borrow the winner's (the handle owns that one). */
                chimera_vfs_state_put(state, file);
                file = expected;
            }
        }

        if (file) {
            request->io_owns_lease_ref = 0;
            chimera_vfs_io_try(state, file, request);
            return;
        }
        /* state_get failed; fall through to the legacy path. */
    }

    /* Legacy per-I/O path (synthetic handle, or no handle): chimera holds an
     * implicit lease on its behalf.  The reference taken here is owned by the
     * request and dropped by io_lease_release. */
    file = chimera_vfs_state_get(state, request->fh, request->fh_len,
                                 request->fh_hash, true);
    if (!file) {
        next(request);
        return;
    }

    request->io_owns_lease_ref = 1;
    chimera_vfs_io_try(state, file, request);
} /* chimera_vfs_io_lease_acquire */

SYMBOL_EXPORT void
chimera_vfs_io_recall(
    struct chimera_vfs_request *request,
    const uint8_t              *fh,
    uint8_t                     fh_len,
    uint64_t                    fh_hash,
    int                         flush_only,
    void (                     *next )(struct chimera_vfs_request *request))
{
    struct chimera_vfs_state      *state = request->thread->vfs->vfs_state;
    struct chimera_vfs_file_state *file;

    request->io_next              = next;
    request->io_lease_file        = NULL;
    request->io_recall_all        = 1;
    request->io_recall_flush_only = flush_only ? 1 : 0;
    request->io_owns_lease_ref    = 1;

    /* Fast path: no per-file state means no caching lease to recall. */
    if (!state || fh_len == 0) {
        next(request);
        return;
    }

    file = chimera_vfs_state_get(state, fh, fh_len, fh_hash, false);
    if (!file) {
        next(request);
        return;
    }

    chimera_vfs_io_try(state, file, request);
} /* chimera_vfs_io_recall */

SYMBOL_EXPORT void
chimera_vfs_io_lease_release(struct chimera_vfs_request *request)
{
    struct chimera_vfs_state      *state;
    struct chimera_vfs_file_state *file = request->io_lease_file;
    bool                           finish;

    if (!file) {
        return; /* fast path / lease-holding client: nothing pinned */
    }

    request->io_lease_file = NULL;
    state                  = request->thread->vfs->vfs_state;

    pthread_mutex_lock(&file->lock);
    if (file->implicit_inflight > 0) {
        file->implicit_inflight--;
    }
    finish = (file->implicit_inflight == 0 &&
              file->implicit_draining &&
              file->implicit_active);
    pthread_mutex_unlock(&file->lock);

    if (finish) {
        chimera_vfs_implicit_finish_drain(state, file);
    }

    /* On the pinned-handle fast path the handle owns the long-lived reference,
     * so only the inflight pin (decremented above) was this op's; the ref is
     * released at handle teardown via chimera_vfs_file_state_release(). */
    if (request->io_owns_lease_ref) {
        chimera_vfs_state_put(state, file);
    }
} /* chimera_vfs_io_lease_release */

/* Revoke every caching lease on `file` that has been BREAKING past its break
 * deadline (the holder never acknowledged).  Returns true if any were revoked.
 * Caller must NOT hold file->lock; revoke takes it internally.  Collect under the
 * lock, then revoke (which re-locks + pumps), so the list walk is not disturbed
 * mid-revoke. */
static bool
chimera_vfs_state_revoke_expired_breaks(
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file)
{
    struct chimera_vfs_lease *expired[CHIMERA_VFS_STATE_MAX_BREAK_BATCH];
    struct chimera_vfs_lease *cur;
    int                       n = 0;
    int                       i;

    (void) state;

    pthread_mutex_lock(&file->lock);
    for (cur = file->caching_leases; cur; cur = cur->next) {
        if (cur->break_state == CHIMERA_VFS_BREAK_BREAKING &&
            chimera_vfs_break_deadline_passed(cur) &&
            n < CHIMERA_VFS_STATE_MAX_BREAK_BATCH) {
            expired[n++] = cur;
        }
    }
    pthread_mutex_unlock(&file->lock);

    for (i = 0; i < n; i++) {
        chimera_vfs_lease_revoke(expired[i]);
    }
    return n > 0;
} /* chimera_vfs_state_revoke_expired_breaks */

SYMBOL_EXPORT void
chimera_vfs_state_reap_idle(
    struct chimera_vfs_state *state,
    uint64_t                  idle_ms)
{
#define CHIMERA_VFS_STATE_REAP_BATCH 64
    uint64_t now = chimera_vfs_now_ticks();
    int      b;

    for (b = 0; b < CHIMERA_VFS_STATE_NUM_BUCKETS; b++) {
        struct chimera_vfs_state_bucket *bucket = &state->buckets[b];
        struct chimera_vfs_file_state   *cand[CHIMERA_VFS_STATE_REAP_BATCH];
        struct chimera_vfs_file_state   *file;
        int                              n = 0;
        int                              i;

        pthread_mutex_lock(&bucket->lock);
        for (file = bucket->files;
             file && n < CHIMERA_VFS_STATE_REAP_BATCH;
             file = file->bucket_next) {
            /* Candidates: files holding an implicit lease (reapable when idle)
             * or with parked I/O waiters (re-pumped so an unresponsive holder
             * is revoked at its recall deadline and the waiter makes progress). */
            if (file->implicit_active || file->io_wait_head) {
                file->refcount++;
                cand[n++] = file;
            }
        }
        pthread_mutex_unlock(&bucket->lock);

        for (i = 0; i < n; i++) {
            bool reapable;
            bool has_waiters;

            file = cand[i];

            pthread_mutex_lock(&file->lock);
            reapable = file->implicit_active &&
                !file->implicit_draining &&
                file->implicit_inflight == 0 &&
                chimera_vfs_elapsed_ms(file->implicit_last_used, now) >= idle_ms;
            if (reapable) {
                file->implicit_draining = 1;
            }
            has_waiters = (file->io_wait_head != NULL);
            pthread_mutex_unlock(&file->lock);

            if (reapable) {
                /* finish_drain pumps the wait queue itself. */
                chimera_vfs_implicit_finish_drain(state, file);
            } else if (has_waiters) {
                /* A parked I/O / namespace op is waiting on a caching holder to
                 * break.  If that holder never acknowledged and its break
                 * deadline has elapsed, forcibly revoke it (MS-SMB2 / NFSv4
                 * recall timeout) so the waiter can proceed, then re-pump. */
                chimera_vfs_state_revoke_expired_breaks(state, file);
                chimera_vfs_state_pump_io(state, file);
            }

            chimera_vfs_state_put(state, file);
        }
    }
#undef CHIMERA_VFS_STATE_REAP_BATCH
} /* chimera_vfs_state_reap_idle */
