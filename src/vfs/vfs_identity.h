// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "common/export.h"

/*
 * Identity resolver: the asynchronous front-end to the user cache (the single
 * identity authority).  A lookup that hits the cache returns synchronously on
 * the calling thread (a lock-free RCU read).  A miss is dispatched to a pool of
 * worker threads which run the registered, potentially-blocking miss handlers
 * (NSS, winbind) off the event loop, populate the cache, and wake the caller on
 * its own evpl thread via the doorbell -- the same park-and-resume pattern the
 * VFS delegation threads use.  This keeps name/SID resolution out of the
 * protocol fast paths and gives every subsystem one consistent answer.
 */

#include <stdint.h>

#include "vfs_user_cache.h"

struct chimera_vfs;
struct chimera_vfs_thread;
struct chimera_vfs_identity;

/* enum chimera_vfs_identity_key lives in vfs_user_cache.h. */

/* How long an authoritative miss is cached. */
#define CHIMERA_VFS_IDENTITY_NEGATIVE_TTL 60 /* seconds */

/*
 * A resolved identity.  An identity is either a user or a group, and the two
 * are not interchangeable -- a SID in particular is ambiguous until resolved,
 * so the resolver tags which kind came back rather than forcing a group through
 * a user-shaped record.
 */
struct chimera_vfs_identity_result {
    int                      is_group;
    struct chimera_vfs_user  user;  /* valid when !is_group */
    struct chimera_vfs_group group; /* valid when  is_group */
};

/*
 * Outcome of a resolution.  NOT_FOUND is an authoritative negative (the
 * identity provider answered and there is no such identity); UNAVAILABLE means
 * the provider could not be asked or could not answer (winbind down, no handler
 * able to try), so the caller must not treat it as proof the identity does not
 * exist.  PASS is for handlers only and is never delivered as a result.
 */
enum chimera_vfs_identity_status {
    CHIMERA_VFS_IDENTITY_OK          = 0,
    CHIMERA_VFS_IDENTITY_NOT_FOUND   = 1, /* authoritative negative          */
    CHIMERA_VFS_IDENTITY_UNAVAILABLE = 2, /* infrastructure failure          */
    CHIMERA_VFS_IDENTITY_PASS        = 3, /* handler cannot answer this key;
                                           * handler-only, never a result    */
};

/*
 * Delivered to the resolve callback.  `result` is non-NULL exactly when status
 * is OK, and then points to a transient copy valid only for the duration of the
 * callback (copy out what you need).
 */
typedef void (*chimera_vfs_identity_callback)(
    enum chimera_vfs_identity_status          status,
    const struct chimera_vfs_identity_result *result,
    void                                     *private_data);

/*
 * A miss handler performs a (possibly blocking) lookup on a resolver worker
 * thread and fills `*out`: either out->user (uid/gid/ngids/gids/username/sid)
 * or out->group (gid/groupname/sid) with out->is_group set.  Returns OK on
 * success, NOT_FOUND if it authoritatively knows no such identity, UNAVAILABLE
 * if it failed to ask its provider, or PASS if it cannot answer this kind of
 * key at all.  Handlers are tried in registration order; the overall status is
 * aggregated from their answers (see chimera_vfs_identity_run_handlers).
 */
typedef enum chimera_vfs_identity_status (*chimera_vfs_identity_handler)(
    enum chimera_vfs_identity_key key,
    uint32_t id,
    const char *name,
    struct chimera_vfs_identity_result *out,
    void *private_data);

SYMBOL_EXPORT struct chimera_vfs_identity *
chimera_vfs_identity_create(
    struct chimera_vfs *vfs,
    int                 num_workers);

SYMBOL_EXPORT void
chimera_vfs_identity_destroy(
    struct chimera_vfs_identity *identity);

/* Register a miss handler (e.g. the SMB server registers a winbind handler). */
SYMBOL_EXPORT void
chimera_vfs_identity_register_handler(
    struct chimera_vfs          *vfs,
    chimera_vfs_identity_handler handler,
    void                        *private_data);

/*
 * Resolve an identity.  On a cache hit the callback fires inline before this
 * returns; on a miss it fires later on `thread`'s evpl loop.  `id` is used for
 * BY_UID/BY_GID; `name` (a NUL-terminated username or SID string) for
 * BY_NAME/BY_SID.
 */
SYMBOL_EXPORT void
chimera_vfs_identity_resolve(
    struct chimera_vfs_thread    *thread,
    enum chimera_vfs_identity_key key,
    uint32_t                      id,
    const char                   *name,
    chimera_vfs_identity_callback callback,
    void                         *private_data);

/*
 * Ask the resolver to fetch and cache an identity, with nobody waiting on the
 * answer.  A cached identity or cached authoritative miss makes this a no-op;
 * otherwise a worker resolves it and caches the outcome as a resolve would,
 * then frees the job.  No VFS thread is recorded or touched, so a warm may
 * outlive the thread that issued it.  Use this rather than a resolve with an
 * empty callback: a resolve's job is handed back to its thread, which must
 * then still exist.
 */
SYMBOL_EXPORT void
chimera_vfs_identity_warm(
    struct chimera_vfs           *vfs,
    enum chimera_vfs_identity_key key,
    uint32_t                      id,
    const char                   *name);

/* Drain this thread's completed resolve jobs (called from its doorbell). */
SYMBOL_EXPORT void
chimera_vfs_identity_thread_complete(
    struct chimera_vfs_thread *thread);

/*
 * Non-zero if the identity is already in the cache (a synchronous RCU probe, no
 * resolution).  Lets a caller decide whether a resolve would block before
 * committing to the async park path.
 */
SYMBOL_EXPORT int
chimera_vfs_identity_cached(
    struct chimera_vfs           *vfs,
    enum chimera_vfs_identity_key key,
    uint32_t                      id,
    const char                   *name);
