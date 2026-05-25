// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

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

struct chimera_vfs;
struct chimera_vfs_thread;
struct chimera_vfs_user;
struct chimera_vfs_identity;

enum chimera_vfs_identity_key {
    CHIMERA_VFS_IDENTITY_BY_UID,
    CHIMERA_VFS_IDENTITY_BY_GID,
    CHIMERA_VFS_IDENTITY_BY_NAME,
    CHIMERA_VFS_IDENTITY_BY_SID,
};

/*
 * Delivered to the resolve callback.  `user` is NULL when the identity could
 * not be resolved; otherwise it points to a transient copy valid only for the
 * duration of the callback (copy out what you need).
 */
typedef void (*chimera_vfs_identity_callback)(
    const struct chimera_vfs_user *user,
    void                          *private_data);

/*
 * A miss handler performs a (possibly blocking) lookup on a resolver worker
 * thread and fills `*out` (uid/gid/ngids/gids/username/sid).  Returns 0 on
 * success, -1 if it cannot resolve the key.  Handlers are tried in registration
 * order until one succeeds.
 */
typedef int (*chimera_vfs_identity_handler)(
    enum chimera_vfs_identity_key key,
    uint32_t                      id,
    const char                   *name,
    struct chimera_vfs_user      *out,
    void                         *private_data);

struct chimera_vfs_identity *
chimera_vfs_identity_create(
    struct chimera_vfs *vfs,
    int                 num_workers);

void
chimera_vfs_identity_destroy(
    struct chimera_vfs_identity *identity);

/* Register a miss handler (e.g. the SMB server registers a winbind handler). */
void
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
void
chimera_vfs_identity_resolve(
    struct chimera_vfs_thread    *thread,
    enum chimera_vfs_identity_key key,
    uint32_t                      id,
    const char                   *name,
    chimera_vfs_identity_callback callback,
    void                         *private_data);

/* Drain this thread's completed resolve jobs (called from its doorbell). */
void
chimera_vfs_identity_thread_complete(
    struct chimera_vfs_thread *thread);

/*
 * Non-zero if the identity is already in the cache (a synchronous RCU probe, no
 * resolution).  Lets a caller decide whether a resolve would block before
 * committing to the async park path.
 */
int
chimera_vfs_identity_cached(
    struct chimera_vfs           *vfs,
    enum chimera_vfs_identity_key key,
    uint32_t                      id,
    const char                   *name);
