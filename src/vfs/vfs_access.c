// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <sys/stat.h>

#include "vfs.h"
#include "vfs_access.h"
#include "vfs_attrs.h"
#include "vfs_acl.h"
#include "vfs_cred.h"
#include "common/macros.h"

SYMBOL_EXPORT uint32_t
chimera_vfs_access_check(
    const struct chimera_vfs_attrs *attr,
    const struct chimera_vfs_cred  *cred,
    uint32_t                        requested)
{
    const struct chimera_acl *acl = NULL;
    uint32_t                  mode;
    uint64_t                  owner_uid, owner_gid;
    int                       is_dir;

    /* AUTH_NONE means the caller did not establish an identity; treat as a
     * full grant (the protocol layer is responsible for export-level policy). */
    if (cred->flavor == CHIMERA_VFS_AUTH_NONE) {
        return requested;
    }

    if (attr->va_set_mask & CHIMERA_VFS_ATTR_ACL) {
        acl = attr->va_acl;
    }

    mode      = (attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) ? attr->va_mode : 0;
    owner_uid = (attr->va_set_mask & CHIMERA_VFS_ATTR_UID) ? attr->va_uid : 0;
    owner_gid = (attr->va_set_mask & CHIMERA_VFS_ATTR_GID) ? attr->va_gid : 0;
    is_dir    = S_ISDIR(mode);

    return chimera_acl_access_check(acl, mode, owner_uid, owner_gid,
                                    cred, requested, is_dir);
} /* chimera_vfs_access_check */

SYMBOL_EXPORT int
chimera_vfs_gate_needed(
    uint64_t                       module_capabilities,
    const struct chimera_vfs_cred *cred)
{
    /* Backend enforces DAC itself (kernel): the engine would double-evaluate. */
    if (module_capabilities & CHIMERA_VFS_CAP_DELEGATES_DAC) {
        return 0;
    }

    /* AUTH_NONE and root are granted unconditionally by the engine; skip the
     * pre-step attr/ACL fetch the gate would otherwise need. */
    if (cred->flavor == CHIMERA_VFS_AUTH_NONE) {
        return 0;
    }

    if (cred->uid == 0) {
        return 0;
    }

    return 1;
} /* chimera_vfs_gate_needed */

SYMBOL_EXPORT enum chimera_vfs_error
chimera_vfs_gate(
    const struct chimera_vfs_attrs *attr,
    const struct chimera_vfs_cred  *cred,
    uint32_t                        required)
{
    if (required == 0) {
        return CHIMERA_VFS_OK;
    }

    if (chimera_vfs_access_allowed(attr, cred, required)) {
        return CHIMERA_VFS_OK;
    }

    return CHIMERA_VFS_EACCES;
} /* chimera_vfs_gate */

SYMBOL_EXPORT int
chimera_vfs_delete_allowed(
    const struct chimera_vfs_attrs *parent_attr,
    const struct chimera_vfs_attrs *child_attr,
    const struct chimera_vfs_cred  *cred)
{
    int      dc, d, allow;
    uint32_t parent_mode;

    /* NFSv4/Windows: a name may be deleted if the parent grants DELETE_CHILD or
     * the child itself grants DELETE.  On mode-only objects DELETE_CHILD is
     * synthesised from the directory's write+execute bits (see
     * mode_access_check) and DELETE is held implicitly by the child's owner. */
    dc = chimera_vfs_access_allowed(parent_attr, cred, CHIMERA_ACE_DELETE_CHILD);
    d  = child_attr &&
        chimera_vfs_access_allowed(child_attr, cred, CHIMERA_ACE_DELETE);

    allow = dc || d;

    if (!allow) {
        return 0;
    }

    /* POSIX sticky directory (S_ISVTX): even with delete rights, only the
     * child's owner, the directory's owner, or root may remove the entry. */
    parent_mode = (parent_attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) ?
        parent_attr->va_mode : 0;

    if (parent_mode & S_ISVTX) {
        uint64_t child_uid = (child_attr &&
                              (child_attr->va_set_mask & CHIMERA_VFS_ATTR_UID)) ?
            child_attr->va_uid : (uint64_t) -1;
        uint64_t parent_uid = (parent_attr->va_set_mask & CHIMERA_VFS_ATTR_UID) ?
            parent_attr->va_uid : (uint64_t) -1;

        if (cred->uid != 0 &&
            (uint64_t) cred->uid != child_uid &&
            (uint64_t) cred->uid != parent_uid) {
            return 0;
        }
    }

    return 1;
} /* chimera_vfs_delete_allowed */
