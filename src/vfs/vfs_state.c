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

/* Milliseconds elapsed from `then` to `now` (both CLOCK_MONOTONIC).  Clamps
 * to 0 if `now` precedes `then`. */
static inline uint64_t
chimera_vfs_elapsed_ms(
    const struct timespec *then,
    const struct timespec *now)
{
    if (now->tv_sec < then->tv_sec ||
        (now->tv_sec == then->tv_sec && now->tv_nsec < then->tv_nsec)) {
        return 0;
    }
    return (uint64_t) (now->tv_sec - then->tv_sec) * 1000ULL +
           (uint64_t) (now->tv_nsec - then->tv_nsec) / 1000000ULL;
} /* chimera_vfs_elapsed_ms */

/* True if `lease`'s break deadline has elapsed (CLOCK_MONOTONIC). */
static inline bool
chimera_vfs_break_deadline_passed(const struct chimera_vfs_lease *lease)
{
    struct timespec now;

    clock_gettime(CLOCK_MONOTONIC, &now);
    if (now.tv_sec != lease->break_deadline.tv_sec) {
        return now.tv_sec > lease->break_deadline.tv_sec;
    }
    return now.tv_nsec >= lease->break_deadline.tv_nsec;
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

/* Byte-range overlap test.  length==0 means "to EOF" (NLM/SMB
 * convention); represent EOF as UINT64_MAX for the math. */
static inline bool
chimera_vfs_range_overlap(
    uint64_t a_off,
    uint64_t a_len,
    uint64_t b_off,
    uint64_t b_len)
{
    uint64_t a_end = (a_len == 0) ? UINT64_MAX : a_off + a_len;
    uint64_t b_end = (b_len == 0) ? UINT64_MAX : b_off + b_len;

    return a_off < b_end && b_off < a_end;
} /* chimera_vfs_range_overlap */

/* Conflict between two range leases.  Classical fcntl rule: only
 * conflict if ranges overlap AND at least one side is exclusive (W). */
static inline bool
chimera_vfs_range_conflict(
    const struct chimera_vfs_lease *a,
    const struct chimera_vfs_lease *b)
{
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
static inline bool
chimera_vfs_caching_conflict(
    const struct chimera_vfs_lease *existing,
    const struct chimera_vfs_lease *probe)
{
    uint8_t e = existing->mode.granted;
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

    /* H (handle-cache) is exclusive across owners — only one client may
     * cache the open handle. */
    if ((e & CHIMERA_VFS_LEASE_MODE_H) && (p & CHIMERA_VFS_LEASE_MODE_H)) {
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
                 * hard share conflict. */
                if (chimera_vfs_eval_breakable(cur, &has_breakable_conflict,
                                               &idle_break, &expired_break)) {
                    if (conflict_out) {
                        *conflict_out = cur;
                    }
                    return CHIMERA_VFS_LEASE_DENIED;
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

                if (cur->mode.granted & CHIMERA_VFS_LEASE_MODE_H) {
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

SYMBOL_EXPORT enum chimera_vfs_lease_result
chimera_vfs_state_try_insert(
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file,
    struct chimera_vfs_lease      *lease,
    struct chimera_vfs_lease     **conflict_out)
{
    enum chimera_vfs_lease_result result;
    struct chimera_vfs_lease     *conflict = NULL;

    pthread_mutex_lock(&file->lock);

    result = chimera_vfs_state_would_conflict(file, lease, &conflict);

    if (result == CHIMERA_VFS_LEASE_GRANTED) {
        chimera_vfs_file_state_insert_lease(file, lease);
        pthread_mutex_unlock(&file->lock);
        return CHIMERA_VFS_LEASE_GRANTED;
    }

    if (conflict_out) {
        *conflict_out = conflict;
    }

    if (result == CHIMERA_VFS_LEASE_BREAKING && conflict) {
        /* Kick off a break on EVERY breakable conflicting holder, not just
         * the first.  A single conflicting open must recall all conflicting
         * read delegations (NFSv4 width) / oplocks at once; otherwise the
         * acquirer would have to retry once per holder.  begin_break flips
         * each holder to BREAKING (and is idempotent), so re-running the
         * conflict probe returns the next still-IDLE conflict until none
         * remain.  Caller retries once the breaks complete. */
        pthread_mutex_unlock(&file->lock);

        while (conflict) {
            if (conflict->break_state == CHIMERA_VFS_BREAK_IDLE) {
                /* Start the recall.  NFSv4 delegations get a bounded recall
                 * deadline so an unresponsive holder can be revoked; other
                 * holders (SMB) keep the default deadline. */
                uint32_t deadline_ms =
                    (conflict->owner.protocol == CHIMERA_VFS_LEASE_PROTO_NFSV4)
                    ? CHIMERA_VFS_NFS_DELEG_RECALL_MS : 0;
                chimera_vfs_lease_begin_break(state, conflict,
                                              lease->mode.granted, deadline_ms);
            } else {
                /* Surfaced because its recall deadline elapsed -- the holder
                 * never returned the delegation.  Revoke it so this acquirer
                 * (on a subsequent retry) can proceed. */
                chimera_vfs_lease_revoke(conflict);
            }
            conflict = NULL;
            pthread_mutex_lock(&file->lock);
            if (chimera_vfs_state_would_conflict(file, lease, &conflict) !=
                CHIMERA_VFS_LEASE_BREAKING) {
                conflict = NULL;
            }
            pthread_mutex_unlock(&file->lock);
        }
        return CHIMERA_VFS_LEASE_BREAKING;
    }

    pthread_mutex_unlock(&file->lock);
    return result;
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

SYMBOL_EXPORT void
chimera_vfs_lease_begin_break(
    struct chimera_vfs_state *state,
    struct chimera_vfs_lease *lease,
    uint8_t                   needed_mode,
    uint32_t                  deadline_ms)
{
    struct chimera_vfs_file_state *file = lease->file;
    struct timespec                now;
    chimera_vfs_lease_break_cb_t   cb;
    void                          *cb_priv;
    bool                           should_invoke;

    if (deadline_ms == 0) {
        deadline_ms = state->default_break_deadline_ms;
    }

    if (file) {
        pthread_mutex_lock(&file->lock);
    }

    should_invoke = (lease->break_state == CHIMERA_VFS_BREAK_IDLE) &&
        (lease->owner.break_cb != NULL);

    if (should_invoke) {
        lease->break_state       = CHIMERA_VFS_BREAK_BREAKING;
        lease->break_needed_mode = needed_mode;
        clock_gettime(CLOCK_MONOTONIC, &now);
        now.tv_sec  += deadline_ms / 1000;
        now.tv_nsec += (long) (deadline_ms % 1000) * 1000000L;
        if (now.tv_nsec >= 1000000000L) {
            now.tv_sec  += 1;
            now.tv_nsec -= 1000000000L;
        }
        lease->break_deadline = now;
        cb                    = lease->owner.break_cb;
        cb_priv               = lease->owner.cb_private;
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
        cb(lease, needed_mode, cb_priv);
    }
} /* chimera_vfs_lease_begin_break */

SYMBOL_EXPORT void
chimera_vfs_lease_ack(
    struct chimera_vfs_lease     *lease,
    struct chimera_vfs_lease_mode resulting)
{
    struct chimera_vfs_file_state *file    = lease->file;
    bool                           mutated = false;

    if (file) {
        pthread_mutex_lock(&file->lock);
    }

    if (lease->break_state == CHIMERA_VFS_BREAK_BREAKING) {
        lease->mode        = resulting;
        lease->break_state = CHIMERA_VFS_BREAK_ACKED;
        mutated            = true;
    }

    if (file) {
        pthread_mutex_unlock(&file->lock);
    }

    /* Acking a break may unblock pending acquires or parked I/O. */
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
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file)
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
            /* Break to NONE (needed_mode 0), not to the currently-granted
             * mode: a namespace/metadata mutation invalidates the holder's
             * cache entirely.  Passing the granted mode would let the SMB
             * lease break_cb retain its read cache (LEVEL_II) and ack with a
             * non-zero mode, so this loop would see granted != 0 forever and
             * the caller would park permanently.  NFSv4 delegations recall
             * fully regardless of needed_mode, so 0 is correct for them too. */
            chimera_vfs_lease_begin_break(state, to_break, 0,
                                          CHIMERA_VFS_NFS_DELEG_METAOP_MS);
        } else if (to_revoke) {
            chimera_vfs_lease_revoke(to_revoke);
        } else {
            break;
        }
    }

    /* Still blocked iff some caching holder retains a granted mode. */
    pthread_mutex_lock(&file->lock);
    had = false;
    {
        struct chimera_vfs_lease *cur;
        for (cur = file->caching_leases; cur; cur = cur->next) {
            if (cur->mode.granted != 0) {
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

    had = chimera_vfs_break_caching_file(state, file);

    chimera_vfs_state_put(state, file);
    return had;
} /* chimera_vfs_state_break_caching */

/* -------------------------------------------------------------------- */
/* Break-on-write                                                       */
/* -------------------------------------------------------------------- */

/* Upper bound on caching leases broken by a single write.  A file with
 * more concurrent caching holders than this is pathological; any excess
 * is left for a subsequent write to break. */
#define CHIMERA_VFS_STATE_MAX_BREAK_BATCH 64

/* A write invalidates every read-caching (R) lease on the file: those
 * holders can no longer trust their cached data, so each must break down
 * to NONE.  The one exception is the writer's own write-caching (W) lease
 * — an exclusive/batch holder is allowed to write through its own cache
 * without breaking it.  A read-only (LEVEL_II) lease held by the writer
 * itself DOES break (a II oplock caches reads only, so writing through it
 * stales that cache) — this is the SMB2 "self break to none".
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
        if ((cur->mode.granted & CHIMERA_VFS_LEASE_MODE_W) &&
            chimera_vfs_lease_owner_equal(&cur->owner, writer)) {
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
                                          probe->mode.granted, deadline_ms);
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

/* Forward decl: io_try and pump_io are mutually recursive (a synchronous
 * conflict-clear in io_try pumps the queue, which re-runs io_try). */
static void
chimera_vfs_io_try(
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file,
    struct chimera_vfs_request    *request);

/* Retry every parked I/O request. */
static void
chimera_vfs_state_pump_io(
    struct chimera_vfs_state      *state,
    struct chimera_vfs_file_state *file)
{
    struct chimera_vfs_pending_acquire *head, *t, *next;

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

        chimera_vfs_io_try(state, file, request);
    }
} /* chimera_vfs_state_pump_io */

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
        if (chimera_vfs_break_caching_file(state, file)) {
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
        clock_gettime(CLOCK_MONOTONIC, &file->implicit_last_used);
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
        chimera_vfs_state_put(state, file);
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

    /* Leaseless actor: chimera holds an implicit lease on its behalf.  The
     * reference taken here is carried by the request until release. */
    file = chimera_vfs_state_get(state, request->fh, request->fh_len,
                                 request->fh_hash, true);
    if (!file) {
        next(request);
        return;
    }

    chimera_vfs_io_try(state, file, request);
} /* chimera_vfs_io_lease_acquire */

SYMBOL_EXPORT void
chimera_vfs_io_recall(
    struct chimera_vfs_request *request,
    const uint8_t              *fh,
    uint8_t                     fh_len,
    uint64_t                    fh_hash,
    void (                     *next )(struct chimera_vfs_request *request))
{
    struct chimera_vfs_state      *state = request->thread->vfs->vfs_state;
    struct chimera_vfs_file_state *file;

    request->io_next       = next;
    request->io_lease_file = NULL;
    request->io_recall_all = 1;

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

    chimera_vfs_state_put(state, file);
} /* chimera_vfs_io_lease_release */

SYMBOL_EXPORT void
chimera_vfs_state_reap_idle(
    struct chimera_vfs_state *state,
    uint64_t                  idle_ms)
{
#define CHIMERA_VFS_STATE_REAP_BATCH 64
    struct timespec now;
    int             b;

    clock_gettime(CLOCK_MONOTONIC, &now);

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
                chimera_vfs_elapsed_ms(&file->implicit_last_used, &now) >= idle_ms;
            if (reapable) {
                file->implicit_draining = 1;
            }
            has_waiters = (file->io_wait_head != NULL);
            pthread_mutex_unlock(&file->lock);

            if (reapable) {
                /* finish_drain pumps the wait queue itself. */
                chimera_vfs_implicit_finish_drain(state, file);
            } else if (has_waiters) {
                chimera_vfs_state_pump_io(state, file);
            }

            chimera_vfs_state_put(state, file);
        }
    }
#undef CHIMERA_VFS_STATE_REAP_BATCH
} /* chimera_vfs_state_reap_idle */
