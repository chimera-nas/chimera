// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/*
 * Identity engine: the asynchronous front-end to the user cache (the single
 * identity authority) and the host of the identity modules behind it.  A
 * lookup that hits the cache returns synchronously on the calling thread (a
 * lock-free RCU read).  A miss is dispatched to a pool of worker threads which
 * walk the registered modules (sdk/vfs_identity_module.h) -- NSS, winbind, an
 * out-of-tree backend -- off the event loop, populate the cache, and wake the
 * caller on its own evpl thread via the doorbell -- the same park-and-resume
 * pattern the VFS delegation threads use.  This keeps name/SID resolution out
 * of the protocol fast paths and gives every subsystem one consistent answer.
 */

#include <stdint.h>

#include "sdk/vfs_identity_module.h"

struct chimera_vfs;
struct chimera_vfs_thread;
struct chimera_vfs_identity;
struct chimera_vfs_module_cfg;

/*
 * Delivered to the resolve callback.  `result` is NULL when the identity could
 * not be resolved; otherwise it points to a transient copy valid only for the
 * duration of the callback (copy out what you need).
 */
typedef void (*chimera_vfs_identity_callback)(
    const struct chimera_vfs_identity_result *result,
    void                                     *private_data);

struct chimera_vfs_identity *
chimera_vfs_identity_create(
    struct chimera_vfs *vfs,
    int                 num_workers);

void
chimera_vfs_identity_destroy(
    struct chimera_vfs_identity *identity);

/*
 * Load and register the configured identity modules, in order.  Each entry
 * names a module (symbol identity_<module_name>); a module_path is dlopen'ed
 * first for an out-of-tree module, otherwise the symbol must already be linked
 * in.  Configuration order is walk order.  The built-in NSS module is always
 * registered first at create time, so this only adds to it.
 */
void
chimera_vfs_identity_load_modules(
    struct chimera_vfs                  *vfs,
    const struct chimera_vfs_module_cfg *module_cfgs,
    int                                  num_modules);

/* Register one module directly (the loader and tests use this). */
void
chimera_vfs_identity_register_module(
    struct chimera_vfs                       *vfs,
    const struct chimera_vfs_identity_module *module,
    const char                               *cfgdata);

/* Non-zero if some registered module has every capability bit in `caps`. */
int
chimera_vfs_identity_has_capability(
    struct chimera_vfs *vfs,
    uint32_t            caps);

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

/*
 * The names the host is domain-joined with, from the first module that has
 * CAP_DOMAIN_INFO and answers.  Synchronous and potentially blocking (a
 * winbindd round trip): for startup, never the request path.  Returns OK with
 * *out filled, NOT_MINE when no module can say, UNAVAILABLE when the one that
 * could is down.
 */
enum chimera_vfs_identity_status
chimera_vfs_identity_domain_info(
    struct chimera_vfs                      *vfs,
    struct chimera_vfs_identity_domain_info *out);
