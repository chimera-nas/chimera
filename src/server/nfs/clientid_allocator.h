// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/*
 * NFSv4 client ID allocator: the locked public API.
 *
 * The NFS client registry decides *whether* SETCLIENTID or EXCHANGE_ID needs a
 * new client ID; the allocator only issues a fresh opaque uint64_t in its
 * advertised safety scope.  It never sees owners, verifiers, principals,
 * XIDs, leases or sessions, and nothing outside the allocator and the
 * diagnostic decoder may interpret the bits it returns (clientid_format.h).
 *
 * Configuration is one JSON object (locked schema, unknown keys rejected at
 * every level):
 *
 *   {
 *     "schema":  1,
 *     "backend": "local",            "local" (Type 1); "test" in test builds
 *     "type":    1,                  see the Type registry in clientid_format.h
 *     "kwargs":  { ... },            owned and validated by the backend + Type
 *     "common":  {                   owned and validated by the wrapper
 *       "max_pending":            4096,
 *       "defer_retry_limit":      5,
 *       "defer_retry_backoff_ms": 10
 *     }
 *   }
 *
 * Completion contract for clientid_allocator_allocate_async():
 *   - return 0: accepted; `done` runs exactly once, never inline, on the
 *     `home` event loop, with no allocator lock held;
 *   - return an errno: rejected; `done` never runs;
 *   - callers submit from the thread that owns `home` (a call for a known
 *     `home` from another thread is EINVAL); a backend may finish on any
 *     thread and the wrapper hands the result back to `home`;
 *   - at most common.max_pending accepted requests are outstanding;
 *   - an ID that is allocated but never published is abandoned, never reused;
 *   - there is no cancellation: the caller keeps `arg` alive until `done`.
 *
 * Statuses form a closed set, each with a fixed class:
 *
 *   0          allocated
 *   EAGAIN     transient: max_pending reached, backend not ready, refill in
 *              progress, store timeout/unreachable, queueing failed before
 *              acceptance
 *   ENOMEM     transient: no memory for the request
 *   ESHUTDOWN  transient: the allocator is being destroyed
 *   ENOSPC     permanent: this allocator's namespace is exhausted
 *   EIO        permanent: backend failure needing an operator, or any
 *              backend error outside this set
 *   EINVAL     permanent: caller bug (NULL argument, or a call made off the
 *              thread that owns `home`)
 *
 * The registry maps transient statuses to NFS4ERR_DELAY and permanent ones to
 * NFS4ERR_SERVERFAULT (never a permanent condition to DELAY: the Linux client
 * retries NFS4ERR_DELAY without limit while establishing its lease).
 */

#include <errno.h>
#include <stdint.h>
#include <jansson.h>

#include "common/export.h"

/* The Windows C runtime has no ESHUTDOWN; 2003 follows the other Chimera errno
 * extensions in common/windows.h (EDQUOT 2001, ESTALE 2002). */
#ifndef ESHUTDOWN
#define ESHUTDOWN 2003
#endif /* ifndef ESHUTDOWN */

struct evpl;
struct clientid_allocator;

struct clientid_allocator_config {
    const json_t *json;   /* the resolved configuration object */
};

typedef void (*clientid_alloc_cb)(
    void    *arg,
    int      status,
    uint64_t clientid);

/* Configuration preflight.  0 = supported; EINVAL = malformed or out of
 * range (including any unknown key); ENOTSUP = valid but this build cannot run
 * it (unknown backend, a Type the backend does not support, a reserved
 * Version).  It cannot certify that an external namespace grant is unique. */
SYMBOL_EXPORT int
clientid_allocator_supported(
    const struct clientid_allocator_config *config);

/* Validates again (even if supported() already ran), logs the redacted
 * configuration once, and builds the allocator.  Performs no network I/O. */
SYMBOL_EXPORT int
clientid_allocator_create(
    const struct clientid_allocator_config *config,
    struct clientid_allocator             **out);

SYMBOL_EXPORT int
clientid_allocator_allocate_async(
    struct clientid_allocator *allocator,
    struct evpl               *home,
    clientid_alloc_cb          done,
    void                      *arg);

/*
 * Shutdown, in this order:
 *
 *   1. clientid_allocator_shutdown(): stop admissions.  Every later
 *      allocate_async() returns ESHUTDOWN; accepted requests still complete.
 *   2. clientid_allocator_detach(home), on each home loop's own thread before
 *      that loop is destroyed: runs the loop until every request accepted on
 *      it has been called back, then retires its delivery state.
 *   3. clientid_allocator_destroy(), once every home loop is detached or
 *      destroyed: tears down the backend, then the delivery state.  If any
 *      request is still pending it logs and leaks rather than free memory a
 *      callback could still reach.
 */
SYMBOL_EXPORT void
clientid_allocator_shutdown(
    struct clientid_allocator *allocator);

SYMBOL_EXPORT void
clientid_allocator_detach(
    struct clientid_allocator *allocator,
    struct evpl               *home);

SYMBOL_EXPORT void
clientid_allocator_destroy(
    struct clientid_allocator *allocator);

/* The closed set of non-zero statuses, as an index for per-status counters. */
enum clientid_status_index {
    CLIENTID_STATUS_EAGAIN = 0,
    CLIENTID_STATUS_ENOMEM,
    CLIENTID_STATUS_ESHUTDOWN,
    CLIENTID_STATUS_ENOSPC,
    CLIENTID_STATUS_EIO,
    CLIENTID_STATUS_EINVAL,
    CLIENTID_STATUS_COUNT
};

/* Index of a non-zero closed-set status, or -1 for 0 or anything else. */
static inline int
clientid_allocator_status_index(int status)
{
    switch (status) {
        case EAGAIN:    return CLIENTID_STATUS_EAGAIN;
        case ENOMEM:    return CLIENTID_STATUS_ENOMEM;
        case ESHUTDOWN: return CLIENTID_STATUS_ESHUTDOWN;
        case ENOSPC:    return CLIENTID_STATUS_ENOSPC;
        case EIO:       return CLIENTID_STATUS_EIO;
        case EINVAL:    return CLIENTID_STATUS_EINVAL;
        default:        return -1;
    } // switch
} /* clientid_allocator_status_index */

/* Lower-case name for an index (a metrics label value). */
static inline const char *
clientid_allocator_status_index_name(int index)
{
    static const char *const names[CLIENTID_STATUS_COUNT] = {
        "eagain", "enomem", "eshutdown", "enospc", "eio", "einval"
    };

    return (index >= 0 && index < CLIENTID_STATUS_COUNT) ? names[index] : "unknown";
} /* clientid_allocator_status_index_name */

struct clientid_allocator_stats {
    uint64_t issued;          /* IDs handed to callers */
    uint64_t remaining;       /* IDs left in the current namespace */
    uint64_t pending;         /* accepted, callback not yet run */
    uint64_t failed;          /* callbacks with a non-zero status (all causes) */
    uint64_t unknown_status;  /* backend statuses outside the closed set (as EIO) */
    /* Refused by allocate_async, no callback, by status. */
    uint64_t rejected[CLIENTID_STATUS_COUNT];
    /* Accepted and called back with a non-zero status, by status. */
    uint64_t failed_by_status[CLIENTID_STATUS_COUNT];
};

/* A consistent snapshot of the counters (exported as metrics by the NFS
 * server). */
SYMBOL_EXPORT void
clientid_allocator_get_stats(
    struct clientid_allocator       *allocator,
    struct clientid_allocator_stats *stats);

/* 1 if `status` is transient (retry later: NFS4ERR_DELAY), 0 if it is OK or
 * permanent. */
static inline int
clientid_allocator_status_is_transient(int status)
{
    return status == EAGAIN || status == ENOMEM || status == ESHUTDOWN;
} /* clientid_allocator_status_is_transient */
