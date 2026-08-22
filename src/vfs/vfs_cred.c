// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Compiled implementations of the credential helpers declared in
 * sdk/vfs_cred.h.  These live in the VFS core (rather than as header
 * inlines) so that backend modules incorporate no VFS code at build
 * time; see the SDK boundary rules in sdk/chimera_vfs_sdk.h.
 */

#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#ifdef __linux__
#include <sys/fsuid.h>
#include <sys/syscall.h>
#endif /* ifdef __linux__ */
#include <unistd.h>
#include <errno.h>

#include "sdk/vfs_cred.h"
#include "common/macros.h"

/*
 * Compact identity hash for a credential, used to key the open-handle cache so
 * each distinct caller gets its own handle (and its own authorization result).
 * Mixes flavor, uid, gid and the supplementary gids (FNV-1a).  Two requests
 * from the same caller hash equal; a hash collision between distinct callers
 * would at worst share a handle (no security boundary under AUTH_SYS, which the
 * client asserts anyway), so a 64-bit mix is ample.
 */
SYMBOL_EXPORT uint64_t
chimera_vfs_cred_hash(const struct chimera_vfs_cred *cred)
{
    uint64_t h = 1469598103934665603ULL; /* FNV-1a offset basis */
    uint32_t words[3];
    uint32_t i;

    /* Internal/server opens may carry no credential; they are exempt from
     * engine enforcement anyway, so share a single cache bucket. */
    if (!cred) {
        return h;
    }

    words[0] = (uint32_t) cred->flavor;
    words[1] = cred->uid;
    words[2] = cred->gid;

    for (i = 0; i < 3; i++) {
        h = (h ^ words[i]) * 1099511628211ULL;
    }

    for (i = 0; i < cred->ngids; i++) {
        h = (h ^ cred->gids[i]) * 1099511628211ULL;
    }

    return h;
} /* chimera_vfs_cred_hash */

/*
 * Return a pointer to the cached server process credentials.
 *
 * Captures the UID and GID of the running server process on first call so
 * they can be restored after impersonating a client credential.  Safe to call
 * from multiple threads: racing initializations write identical values.
 */
SYMBOL_EXPORT struct chimera_vfs_cred *
chimera_vfs_get_server_cred(void)
{
    static struct chimera_vfs_cred cred = { 0 };

    if (cred.flavor == CHIMERA_VFS_AUTH_NONE) {
        cred.flavor = CHIMERA_VFS_AUTH_UNIX;
        cred.uid    = getuid();
        cred.gid    = getgid();
        cred.ngids  = 0;
    }
    return &cred;
} /* chimera_vfs_get_server_cred */

SYMBOL_EXPORT void
chimera_vfs_cred_init_unix(
    struct chimera_vfs_cred *cred,
    uint32_t                 uid,
    uint32_t                 gid,
    uint32_t                 ngids,
    const uint32_t          *gids)
{
    cred->flavor = CHIMERA_VFS_AUTH_UNIX;
    cred->uid    = uid;
    cred->gid    = gid;
    cred->origin = NULL;

    if (ngids > CHIMERA_VFS_CRED_MAX_GIDS) {
        ngids = CHIMERA_VFS_CRED_MAX_GIDS;
    }

    cred->ngids = ngids;
    if (ngids > 0 && gids) {
        memcpy(cred->gids, gids, ngids * sizeof(uint32_t));
    }
} /* chimera_vfs_cred_init_unix */

SYMBOL_EXPORT void
chimera_vfs_cred_init_attr(
    struct chimera_vfs_cred *cred,
    uint32_t                 uid,
    uint32_t                 gid,
    uint32_t                 ngids,
    const uint32_t          *gids)
{
    (void) ngids;
    (void) gids;

    cred->flavor = CHIMERA_VFS_AUTH_ATTR;
    cred->uid    = uid;
    cred->gid    = gid;
    cred->ngids  = 0;
    cred->origin = NULL;
} /* chimera_vfs_cred_init_attr */

SYMBOL_EXPORT int
chimera_setup_credential(
    const struct chimera_vfs_cred *cred,
    struct chimera_vfs_attrs      *set_attrs)
{
    const struct chimera_vfs_cred *sc = chimera_vfs_get_server_cred();

    switch (cred->flavor) {
        case CHIMERA_VFS_AUTH_NONE:
            /* No credential provided, do nothing */
            break;
        case CHIMERA_VFS_AUTH_UNIX:
            if ((cred->uid == sc->uid) &&
                (cred->gid == sc->gid)) {
                return 0;
            }
#ifdef __linux__
            /* Per-thread supplementary groups: the raw syscall sets only this
             * thread (glibc setgroups() would broadcast to all threads). */
            if (syscall(SYS_setgroups, (size_t) cred->ngids,
                        cred->ngids ? cred->gids : NULL) < 0) {
                return errno;
            }
            /* fsgid before fsuid so we keep the privilege to set the gid. */
            setfsgid(cred->gid);
            setfsuid(cred->uid);
            /* setfsuid silently no-ops without privilege; it returns the
             * previous fsuid, so a second call returns the now-current one --
             * verify the impersonation actually took effect. */
            if ((uint32_t) setfsuid(cred->uid) != cred->uid) {
                setfsuid(sc->uid);
                setfsgid(sc->gid);
                syscall(SYS_setgroups, (size_t) 0, NULL);
                return EPERM;
            }
#else  /* ifdef __linux__ */
            /* No per-thread fsuid/fsgid outside Linux, so a passthrough backend
             * cannot impersonate the client here.  Fail closed rather than
             * silently performing the access as the server identity.  Only the
             * linux and io_uring backends call this, and neither is built off
             * Linux, so this is unreachable in practice. */
            return EPERM;
#endif /* ifdef __linux__ */
            break;
        case CHIMERA_VFS_AUTH_ATTR:
            if (set_attrs != NULL) {
                if (!(set_attrs->va_set_mask & CHIMERA_VFS_ATTR_UID)) {
                    set_attrs->va_uid       = cred->uid;
                    set_attrs->va_set_mask |= CHIMERA_VFS_ATTR_UID;
                }
                if (!(set_attrs->va_set_mask & CHIMERA_VFS_ATTR_GID)) {
                    set_attrs->va_gid       = cred->gid;
                    set_attrs->va_set_mask |= CHIMERA_VFS_ATTR_GID;
                }
            }
            break;
        default:
            /* Unknown credential flavor */
            return EINVAL;
    } // switch
    return 0;
} /* chimera_setup_credential */

SYMBOL_EXPORT int
chimera_restore_privilege(const struct chimera_vfs_cred *cred)
{
    const struct chimera_vfs_cred *sc = chimera_vfs_get_server_cred();

    if (cred->flavor == CHIMERA_VFS_AUTH_UNIX) {
        /* restore base privileges */
        if ((cred->uid == sc->uid) &&
            (cred->gid == sc->gid)) {
            return 0;
        }
#ifdef __linux__
        setfsuid(sc->uid);
        setfsgid(sc->gid);
        if (syscall(SYS_setgroups, (size_t) 0, NULL) < 0) {
            return errno;
        }
#else  /* ifdef __linux__ */
        /* chimera_setup_credential() never impersonated here (it returned
         * EPERM), so there is nothing to restore. */
        return 0;
#endif /* ifdef __linux__ */
    }
    return 0;
} /* chimera_restore_privilege */

SYMBOL_EXPORT uint32_t
chimera_vfs_killpriv_mode(
    const struct chimera_vfs_cred *cred,
    uint32_t                       mode)
{
    if (!cred || cred->flavor == CHIMERA_VFS_AUTH_NONE || cred->uid == 0) {
        return mode;
    }

    if (!S_ISREG(mode)) {
        return mode;
    }

    if (mode & S_ISUID) {
        mode &= ~(uint32_t) S_ISUID;
    }

    if ((mode & S_ISGID) && (mode & S_IXGRP)) {
        mode &= ~(uint32_t) S_ISGID;
    }

    return mode;
} /* chimera_vfs_killpriv_mode */
