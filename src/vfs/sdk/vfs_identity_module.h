// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/*
 * Chimera identity module SDK: the contract between the identity engine and
 * an identity backend.
 *
 * An identity module is a struct chimera_vfs_identity_module of callbacks plus
 * a capability mask.  The engine (vfs_identity.c) keeps the user cache in
 * front of the modules and walks them, in configuration order, for whatever a
 * caller asks; a module answers only for itself and never sees the others.
 * In-tree modules are linked into chimera; out-of-tree modules are built as
 * shared objects exporting a `struct chimera_vfs_identity_module
 * identity_<name>` symbol and loaded via the identity configuration's
 * module_path.
 *
 * This version of the contract covers the resolve family -- name / uid / gid /
 * SID to identity record -- plus domain_info, which is not identity at all but
 * domain membership: the names the host is joined to a domain with.  Every op
 * is optional and gated by a capability bit; a module implements the subset it
 * can.
 *
 * All ops are blocking calls made on an engine worker thread, never on an
 * event loop.  A module must not call back into the engine.
 */

#include <stdint.h>
#include <stddef.h>

#include "vfs_cred.h"

struct prometheus_metrics;

/* Version of the identity module contract (this header).  Bumped on any
 * incompatible change; the engine refuses a module built against a different
 * version so a stale out-of-tree binary fails loudly at load time. */
#define CHIMERA_VFS_IDENTITY_SDK_VERSION     1

#define CHIMERA_VFS_IDENTITY_SID_MAX_LEN     80
#define CHIMERA_VFS_IDENTITY_NAME_MAX_LEN    256

/* name / uid / gid / SID  <->  identity record */
#define CHIMERA_VFS_IDENTITY_CAP_LOOKUP      (1U << 0)
/* the NetBIOS / DNS names the host is domain-joined with */
#define CHIMERA_VFS_IDENTITY_CAP_DOMAIN_INFO (1U << 1)

enum chimera_vfs_identity_key {
    CHIMERA_VFS_IDENTITY_BY_UID,
    CHIMERA_VFS_IDENTITY_BY_GID,
    CHIMERA_VFS_IDENTITY_BY_NAME,
    CHIMERA_VFS_IDENTITY_BY_SID,
};

/*
 * What a module says about a request.  NOT_MINE means the module does not
 * know the key and the engine moves on to the next module.  UNAVAILABLE
 * routes the same way but is reported differently, since "the backend is
 * down" and "no such account" call for different remedies.
 */
enum chimera_vfs_identity_status {
    CHIMERA_VFS_IDENTITY_OK          = 0,
    CHIMERA_VFS_IDENTITY_NOT_MINE    = 1,
    CHIMERA_VFS_IDENTITY_UNAVAILABLE = 2,
};

struct chimera_vfs_identity_user {
    uint32_t uid;
    uint32_t gid;
    uint32_t ngids;
    int      username_len;
    uint32_t gids[CHIMERA_VFS_CRED_MAX_GIDS];
    char     username[CHIMERA_VFS_IDENTITY_NAME_MAX_LEN];
    /* Empty when the backend has no notion of a SID (NSS); the engine then
     * falls back to the algorithmic form when a SID is needed. */
    char     sid[CHIMERA_VFS_IDENTITY_SID_MAX_LEN];
};

struct chimera_vfs_identity_group {
    uint32_t gid;
    int      groupname_len;
    char     groupname[CHIMERA_VFS_IDENTITY_NAME_MAX_LEN];
    char     sid[CHIMERA_VFS_IDENTITY_SID_MAX_LEN];
};

/*
 * A resolved identity.  An identity is either a user or a group, and the two
 * are not interchangeable -- a SID in particular is ambiguous until resolved,
 * so the record tags which kind came back rather than forcing a group through
 * a user-shaped record.
 */
struct chimera_vfs_identity_result {
    int                               is_group;
    struct chimera_vfs_identity_user  user;  /* valid when !is_group */
    struct chimera_vfs_identity_group group; /* valid when  is_group */
};

/* The names the host is joined to its domain with.  Any the backend does not
 * know are left empty. */
struct chimera_vfs_identity_domain_info {
    char netbios_name[16];
    char netbios_domain[CHIMERA_VFS_IDENTITY_NAME_MAX_LEN];
    char dns_domain[CHIMERA_VFS_IDENTITY_NAME_MAX_LEN];
};

struct chimera_vfs_identity_module {
    /* Required
     * Set to CHIMERA_VFS_IDENTITY_SDK_VERSION.  Checked at registration so a
     * module built against an incompatible SDK is rejected instead of loaded.
     */
    uint32_t    sdk_version;

    /* Required
     * Short name; the identity configuration keys modules by it and the
     * exported symbol is identity_<name>.
     */
    const char *name;

    /* Required
     * Bitwise OR of CHIMERA_VFS_IDENTITY_CAP_* above.  An op whose bit is
     * clear is never called and may be NULL.
     */
    uint32_t    capabilities;

    /* Optional
     * Called once at registration with the module's configuration JSON (an
     * empty string when none was given).  Returns the module's private state,
     * handed back to every op; NULL is a valid state.
     */
    void      * (*init)(
        const char                *cfgdata,
        struct prometheus_metrics *metrics);

    /* Optional
     * Called once at shutdown with the state init returned.
     */
    void        (*destroy)(
        void *private_data);

    /* CAP_LOOKUP
     * Resolve `key` -- `id` for BY_UID / BY_GID, `name` (a NUL-terminated
     * username or SID string) for BY_NAME / BY_SID -- into *out, which
     * arrives zeroed.  Fill out->user, or out->group with out->is_group set.
     * Return OK, or NOT_MINE when the key is unknown here.
     */
    enum chimera_vfs_identity_status (*lookup)(
        void *private_data,
        enum chimera_vfs_identity_key key,
        uint32_t id,
        const char *name,
        struct chimera_vfs_identity_result *out);

    /* CAP_DOMAIN_INFO
     * Fill *out (arrives zeroed) with the join names this backend knows.
     */
    enum chimera_vfs_identity_status (*domain_info)(
        void *private_data,
        struct chimera_vfs_identity_domain_info *out);
};
