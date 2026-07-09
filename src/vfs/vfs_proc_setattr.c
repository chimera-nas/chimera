// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include <stdlib.h>
#include <sys/stat.h>
#include "vfs_procs.h"
#include "vfs_state.h"
#include "vfs_internal.h"
#include "vfs_attr_cache.h"
#include "vfs_access.h"
#include "vfs_acl.h"
#include "common/misc.h"
#include "common/macros.h"

/*
 * Map the attributes a SETATTR is changing to the canonical ACE rights the
 * caller must hold.  A chmod (or an explicit ACL set) rewrites the special-who
 * ACEs, so it is treated as an ACL write; chown/chgrp require WRITE_OWNER; a
 * size change is a data write; a metadata-time set requires WRITE_ATTRIBUTES,
 * except utimes-to-now ("touch"), which a data writer is allowed to do.
 *
 * `cur` is the object's current attributes (NULL if not yet fetched).  When
 * present, an owner/group field set to the value it already holds is not a
 * change and does not require WRITE_OWNER -- which matters because a SET_INFO
 * security descriptor commonly restates the existing owner alongside a DACL,
 * and the owner holds WRITE_ACL but not WRITE_OWNER implicitly.
 */
static uint32_t
chimera_vfs_setattr_required(
    const struct chimera_vfs_attrs *set_attr,
    const struct chimera_vfs_attrs *cur)
{
    uint64_t m        = set_attr->va_set_mask;
    uint32_t required = 0;
    int      chowns;

    if (m & (CHIMERA_VFS_ATTR_ACL | CHIMERA_VFS_ATTR_MODE)) {
        required |= CHIMERA_ACE_WRITE_ACL;
    }

    chowns = ((m & CHIMERA_VFS_ATTR_UID) &&
              !(cur && (cur->va_set_mask & CHIMERA_VFS_ATTR_UID) &&
                cur->va_uid == set_attr->va_uid)) ||
        ((m & CHIMERA_VFS_ATTR_GID) &&
         !(cur && (cur->va_set_mask & CHIMERA_VFS_ATTR_GID) &&
           cur->va_gid == set_attr->va_gid));

    if (chowns) {
        required |= CHIMERA_ACE_WRITE_OWNER;
    }

    if (m & CHIMERA_VFS_ATTR_SIZE) {
        required |= CHIMERA_ACE_WRITE_DATA;
    }

    if (m & CHIMERA_VFS_ATTR_DOS_ATTRIBUTES) {
        required |= CHIMERA_ACE_WRITE_ATTRIBUTES;
    }

    if (m & CHIMERA_VFS_ATTR_ATIME) {
        required |= (set_attr->va_atime.tv_nsec == CHIMERA_VFS_TIME_NOW) ?
            CHIMERA_ACE_WRITE_DATA : CHIMERA_ACE_WRITE_ATTRIBUTES;
    }

    if (m & CHIMERA_VFS_ATTR_MTIME) {
        required |= (set_attr->va_mtime.tv_nsec == CHIMERA_VFS_TIME_NOW) ?
            CHIMERA_ACE_WRITE_DATA : CHIMERA_ACE_WRITE_ATTRIBUTES;
    }

    return required;
} /* chimera_vfs_setattr_required */

/*
 * Map a denied setattr to the POSIX errno.  Operations that require ownership
 * of the file -- chmod (mode/ACL), chown/chgrp (uid/gid), and setting a
 * timestamp to an explicit value -- fail with EPERM for a non-owner without
 * privilege.  Operations that merely require write permission -- truncate
 * (size) and setting a timestamp to "now" -- fail with EACCES.  When both
 * classes are present, the ownership requirement dominates (EPERM).
 */
/* Is `cred` a member of group `gid` (primary or supplementary)? */
static int
chimera_vfs_setattr_cred_in_group(
    const struct chimera_vfs_cred *cred,
    uint64_t                       gid)
{
    if ((uint64_t) cred->gid == gid) {
        return 1;
    }
    for (uint32_t i = 0; i < cred->ngids; i++) {
        if ((uint64_t) cred->gids[i] == gid) {
            return 1;
        }
    }
    return 0;
} /* chimera_vfs_setattr_cred_in_group */

/*
 * POSIX chown(2) eligibility for a non-privileged caller against the file's
 * current owner/group.  Only the super-user may change the owner (uid); the
 * group (gid) may be changed only by the file's owner and only to a group in
 * the caller's group set.  A field left unset, or set to its current value, is
 * not a change.  Returns CHIMERA_VFS_OK when permitted, else CHIMERA_VFS_EPERM.
 */
static enum chimera_vfs_error
chimera_vfs_setattr_chown_check(
    const struct chimera_vfs_cred  *cred,
    const struct chimera_vfs_attrs *set_attr,
    const struct chimera_vfs_attrs *cur)
{
    uint64_t m  = set_attr->va_set_mask;
    int has_uid = (m & CHIMERA_VFS_ATTR_UID) != 0;
    int has_gid = (m & CHIMERA_VFS_ATTR_GID) != 0;

    if (cred->uid == 0) {
        return CHIMERA_VFS_OK;
    }

    /* chown(path, -1, -1) names no owner or group and is always permitted. */
    if (!has_uid && !has_gid) {
        return CHIMERA_VFS_OK;
    }

    /* To perform any chown a non-privileged caller must own the file. */
    if (!(cur->va_set_mask & CHIMERA_VFS_ATTR_UID) ||
        (uint64_t) cred->uid != cur->va_uid) {
        return CHIMERA_VFS_EPERM;
    }

    /* The owner may not change the owner (uid) to a different value. */
    if (has_uid &&
        (cur->va_set_mask & CHIMERA_VFS_ATTR_UID) &&
        set_attr->va_uid != cur->va_uid) {
        return CHIMERA_VFS_EPERM;
    }

    /* The owner may change the group only to one it is a member of. */
    if (has_gid &&
        (cur->va_set_mask & CHIMERA_VFS_ATTR_GID) &&
        set_attr->va_gid != cur->va_gid &&
        !chimera_vfs_setattr_cred_in_group(cred, set_attr->va_gid)) {
        return CHIMERA_VFS_EPERM;
    }

    return CHIMERA_VFS_OK;
} /* chimera_vfs_setattr_chown_check */

/*
 * NFSv4 ACL_WRITE_OWNER eligibility for a non-owner who has been granted the
 * WRITE_OWNER right by an explicit ACE.  Unlike POSIX chown(2) (which lets only
 * the super-user change the uid), WRITE_OWNER lets the holder "take" the object:
 * but, following the BSD/Solaris NFSv4 implementations, only to its own identity
 * -- the new uid must equal the caller's uid, and the new gid must be a group
 * the caller is a member of.  A field left unset, or restated to its current
 * value, is not a change.  Returns CHIMERA_VFS_OK when permitted, else EPERM.
 */
static enum chimera_vfs_error
chimera_vfs_setattr_write_owner_check(
    const struct chimera_vfs_cred  *cred,
    const struct chimera_vfs_attrs *set_attr,
    const struct chimera_vfs_attrs *cur)
{
    uint64_t m = set_attr->va_set_mask;

    if ((m & CHIMERA_VFS_ATTR_UID) &&
        !(cur && (cur->va_set_mask & CHIMERA_VFS_ATTR_UID) &&
          cur->va_uid == set_attr->va_uid) &&
        set_attr->va_uid != (uint64_t) cred->uid) {
        return CHIMERA_VFS_EPERM;
    }

    if ((m & CHIMERA_VFS_ATTR_GID) &&
        !(cur && (cur->va_set_mask & CHIMERA_VFS_ATTR_GID) &&
          cur->va_gid == set_attr->va_gid) &&
        !chimera_vfs_setattr_cred_in_group(cred, set_attr->va_gid)) {
        return CHIMERA_VFS_EPERM;
    }

    return CHIMERA_VFS_OK;
} /* chimera_vfs_setattr_write_owner_check */

static enum chimera_vfs_error
chimera_vfs_setattr_denied_error(const struct chimera_vfs_attrs *set_attr)
{
    uint64_t m = set_attr->va_set_mask;

    if (m & (CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_ACL |
             CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_GID)) {
        return CHIMERA_VFS_EPERM;
    }

    if ((m & CHIMERA_VFS_ATTR_ATIME) &&
        set_attr->va_atime.tv_nsec != CHIMERA_VFS_TIME_NOW) {
        return CHIMERA_VFS_EPERM;
    }

    if ((m & CHIMERA_VFS_ATTR_MTIME) &&
        set_attr->va_mtime.tv_nsec != CHIMERA_VFS_TIME_NOW) {
        return CHIMERA_VFS_EPERM;
    }

    return CHIMERA_VFS_EACCES;
} /* chimera_vfs_setattr_denied_error */

static void
chimera_vfs_setattr_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread     *thread   = request->thread;
    chimera_vfs_setattr_callback_t callback = request->proto_callback;

    if (request->status == CHIMERA_VFS_OK) {
        chimera_vfs_attr_cache_insert(thread, thread->vfs->vfs_attr_cache,
                                      request->fh_hash,
                                      request->fh,
                                      request->fh_len,
                                      &request->setattr.r_post_attr);
    }

    chimera_vfs_complete(request);

    callback(request->status,
             &request->setattr.r_pre_attr,
             request->setattr.set_attr,
             &request->setattr.r_post_attr,
             request->proto_private_data);

    chimera_vfs_request_free(thread, request);
} /* chimera_vfs_setattr_complete */

static void
chimera_vfs_setattr_dispatch(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_transaction *txn,
    struct chimera_vfs_open_handle *handle,
    struct chimera_vfs_attrs       *set_attr,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    chimera_vfs_setattr_callback_t  callback,
    void                           *private_data)
{
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc_by_handle(thread, cred, handle);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), NULL, NULL, NULL, private_data);
        return;
    }

    request->transaction = txn;

    request->opcode         = CHIMERA_VFS_OP_SETATTR;
    request->complete       = chimera_vfs_setattr_complete;
    request->setattr.handle = handle;
    /* Identify the mutating handle so the caching-lease recall below skips a
     * lease anchored to this same handle (the holder is coherent with its own
     * setattr -- see chimera_vfs_break_caching_file). */
    request->io_handle                       = handle;
    request->setattr.set_attr                = set_attr;
    request->setattr.r_pre_attr.va_req_mask  = pre_attr_mask;
    request->setattr.r_pre_attr.va_set_mask  = 0;
    request->setattr.r_post_attr.va_req_mask = post_attr_mask | CHIMERA_VFS_ATTR_MASK_CACHEABLE;
    request->setattr.r_post_attr.va_set_mask = 0;
    request->proto_callback                  = callback;
    request->proto_private_data              = private_data;

    /* Recall flavor depends on whether this setattr changes file *data*:
     *
     *   - A SIZE change (truncate/extend) alters the bytes, so a holder's cached
     *     read data is now stale: recall fully (flush_only = 0 -> break to NONE),
     *     invalidating the read cache.  Retaining R here returns stale bytes
     *     (fsx MAPREAD corruption).
     *   - An attribute-only setattr (mtime/atime/DOS/ACL/owner -- no data change)
     *     must still flush a write-caching holder's dirty pages, because the
     *     resulting mtime bump makes other clients revalidate and re-read from
     *     the backend; but the holder's read cache stays valid (the data did not
     *     change), so flush_only = 1 keeps R|H.  This is what stops a single
     *     client's metadata storm (rewinddir/fsstress Create+SetInfo+Close churn)
     *     from recalling its own lease on every attribute set while preserving
     *     coherence. */
    int flush_only = (set_attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE) ? 0 : 1;

    chimera_vfs_io_recall(request, request->fh, request->fh_len,
                          request->fh_hash, flush_only,
                          chimera_vfs_dispatch);
} /* chimera_vfs_setattr_dispatch */

/*
 * Continuation context for the enforcement pre-step: a getattr+ACL is issued
 * before the mutation so the gate can authorize it, then the real SETATTR is
 * dispatched (or the caller's callback is completed with EACCES).
 */
struct chimera_vfs_setattr_gate {
    struct chimera_vfs_thread      *thread;
    const struct chimera_vfs_cred  *cred;
    struct chimera_vfs_transaction *txn;
    struct chimera_vfs_open_handle *handle;
    struct chimera_vfs_attrs       *set_attr;
    uint64_t                        pre_attr_mask;
    uint64_t                        post_attr_mask;
    chimera_vfs_setattr_callback_t  callback;
    void                           *private_data;
};

static void
chimera_vfs_setattr_gate_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_setattr_gate *gate = private_data;
    uint32_t                         required;

    if (error_code != CHIMERA_VFS_OK) {
        gate->callback(error_code, NULL, NULL, NULL, gate->private_data);
        free(gate);
        return;
    }

    /* Recompute against the now-known current attributes so a no-op
     * owner/group restate does not demand WRITE_OWNER. */
    required = chimera_vfs_setattr_required(gate->set_attr, attr);

    /* POSIX: the file's owner (and the super-user) may always update its
     * timestamps -- to an explicit value or to "now" -- regardless of write
     * permission.  For a setattr that changes only atime/mtime, the owner is
     * therefore unconditionally authorized. */
    {
        uint64_t m     = gate->set_attr->va_set_mask;
        int      owner = (gate->cred->uid == 0) ||
            ((attr->va_set_mask & CHIMERA_VFS_ATTR_UID) &&
             (uint64_t) gate->cred->uid == attr->va_uid);

        if (owner && (m & (CHIMERA_VFS_ATTR_ATIME | CHIMERA_VFS_ATTR_MTIME)) &&
            !(m & ~(CHIMERA_VFS_ATTR_ATIME | CHIMERA_VFS_ATTR_MTIME |
                    CHIMERA_VFS_ATTR_CTIME))) {
            required = 0;
        }
    }

    /* WRITE_OWNER (the chown/chgrp right) is authorized separately below; gate
     * the remaining rights (chmod/ACL/size/timestamp) here. */
    if (chimera_vfs_gate(attr, gate->cred, required & ~CHIMERA_ACE_WRITE_OWNER) != CHIMERA_VFS_OK) {
        gate->callback(chimera_vfs_setattr_denied_error(gate->set_attr),
                       NULL, NULL, NULL, gate->private_data);
        free(gate);
        return;
    }

    /* Authorize an owner/group change.  The POSIX chown(2) rules
     * (chimera_vfs_setattr_chown_check: only root changes uid; the owner may
     * chgrp to a group it belongs to) are always required.  On top of that the
     * caller must hold the chown right: either it is the file's owner (a POSIX
     * owner may chgrp its own object -- on a mode-only object this is the only
     * signal, since Windows withholds the owner's implicit WRITE_OWNER) or an
     * explicit WRITE_OWNER ACE grants it.  This keeps the synthesised-ACL and
     * mode-only (e.g. NFSv3) representations consistent: an owner's POSIX-legal
     * chgrp is permitted in both, while a uid change still demands privilege. */
    if (gate->set_attr->va_set_mask & (CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_GID)) {
        int is_owner = (attr->va_set_mask & CHIMERA_VFS_ATTR_UID) &&
            (uint64_t) gate->cred->uid == attr->va_uid;

        /* A non-owner who is not root, but who holds the NFSv4 WRITE_OWNER right
         * via an explicit ACE, is authorized under WRITE_OWNER semantics
         * (chimera_vfs_setattr_write_owner_check: take ownership to one's own
         * uid / a group one belongs to) rather than the POSIX chown(2) rules
         * (which would reject any non-owner change).  This is what lets a
         * WRITE_OWNER-granted user chgrp a file they do not own.  The owner and
         * root keep the POSIX rules (the owner may chgrp to a group it belongs
         * to; only root may change uid). */
        int has_write_owner = !is_owner && gate->cred->uid != 0 &&
            (required & CHIMERA_ACE_WRITE_OWNER) &&
            chimera_vfs_gate(attr, gate->cred, CHIMERA_ACE_WRITE_OWNER) == CHIMERA_VFS_OK;

        if (has_write_owner) {
            if (chimera_vfs_setattr_write_owner_check(gate->cred, gate->set_attr, attr) != CHIMERA_VFS_OK) {
                gate->callback(CHIMERA_VFS_EPERM, NULL, NULL, NULL, gate->private_data);
                free(gate);
                return;
            }
        } else {
            /* POSIX chown(2) rules are always required when a uid/gid is named
             * (even a no-op restate by a non-owner is EPERM).  An actual change
             * (required & WRITE_OWNER) additionally needs the chown right: the
             * caller is the owner, is root, or an explicit WRITE_OWNER ACE
             * grants it.  A no-op restate carries no WRITE_OWNER requirement. */
            if (chimera_vfs_setattr_chown_check(gate->cred, gate->set_attr, attr) != CHIMERA_VFS_OK ||
                ((required & CHIMERA_ACE_WRITE_OWNER) && !is_owner && gate->cred->uid != 0 &&
                 chimera_vfs_gate(attr, gate->cred, CHIMERA_ACE_WRITE_OWNER) != CHIMERA_VFS_OK)) {
                gate->callback(CHIMERA_VFS_EPERM, NULL, NULL, NULL, gate->private_data);
                free(gate);
                return;
            }
        }
    }

    /* A successful change of owner or group by a non-privileged caller clears
     * the set-user-ID and set-group-ID bits of a non-directory (POSIX/Linux
     * kill-priv: don't let a privileged binary survive an ownership change). */
    if (gate->cred->uid != 0 &&
        !(gate->set_attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) &&
        (attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) &&
        (attr->va_mode & S_IFMT) != S_IFDIR &&
        (attr->va_mode & (S_ISUID | S_ISGID))) {
        uint64_t m            = gate->set_attr->va_set_mask;
        int      owner_change =
            ((m & CHIMERA_VFS_ATTR_UID) && (attr->va_set_mask & CHIMERA_VFS_ATTR_UID) &&
             gate->set_attr->va_uid != attr->va_uid) ||
            ((m & CHIMERA_VFS_ATTR_GID) && (attr->va_set_mask & CHIMERA_VFS_ATTR_GID) &&
             gate->set_attr->va_gid != attr->va_gid);

        if (owner_change) {
            gate->set_attr->va_mode      = attr->va_mode & ~(uint64_t) (S_ISUID | S_ISGID);
            gate->set_attr->va_set_mask |= CHIMERA_VFS_ATTR_MODE;
        }
    }

    /* POSIX: when a non-privileged process whose group set does not include the
     * file's owning group chmods a non-directory, the set-group-ID bit is
     * cleared on success (it would otherwise let a user grant themselves the
     * file group's identity on execution). */
    if ((gate->set_attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) &&
        (gate->set_attr->va_mode & S_ISGID) &&
        gate->cred->uid != 0 &&
        (attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) &&
        (attr->va_mode & S_IFMT) != S_IFDIR &&
        (attr->va_set_mask & CHIMERA_VFS_ATTR_GID) &&
        !chimera_vfs_setattr_cred_in_group(gate->cred, attr->va_gid)) {
        gate->set_attr->va_mode &= ~(uint64_t) S_ISGID;
    }

    chimera_vfs_setattr_dispatch(gate->thread, gate->cred, gate->txn, gate->handle,
                                 gate->set_attr, gate->pre_attr_mask,
                                 gate->post_attr_mask, gate->callback,
                                 gate->private_data);
    free(gate);
} /* chimera_vfs_setattr_gate_complete */

SYMBOL_EXPORT void
chimera_vfs_setattr(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_transaction *txn,
    struct chimera_vfs_open_handle *handle,
    struct chimera_vfs_attrs       *set_attr,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    chimera_vfs_setattr_callback_t  callback,
    void                           *private_data)
{
    struct chimera_vfs_setattr_gate *gate;
    uint32_t                         required;

    /* Engine-authoritative backend and a non-exempt caller: authorize the
     * mutation against a fresh attr+ACL fetch before applying it.  (For
     * delegated/AUTH_NONE/root cases the kernel or engine grants anyway, so we
     * dispatch directly and avoid the extra round-trip.) */
    if (chimera_vfs_gate_needed(handle->vfs_module->capabilities, cred)) {
        /* Preliminary (current attrs unknown): worst-case required set, just to
         * decide whether the pre-step is needed.  The real check in the gate
         * callback recomputes once the current owner/group is known. */
        required = chimera_vfs_setattr_required(set_attr, NULL);

        if (required) {
            gate = malloc(sizeof(*gate));

            gate->thread         = thread;
            gate->cred           = cred;
            gate->txn            = txn;
            gate->handle         = handle;
            gate->set_attr       = set_attr;
            gate->pre_attr_mask  = pre_attr_mask;
            gate->post_attr_mask = post_attr_mask;
            gate->callback       = callback;
            gate->private_data   = private_data;

            chimera_vfs_getattr(thread, cred, txn, handle,
                                CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_ACL,
                                chimera_vfs_setattr_gate_complete, gate);
            return;
        }
    }

    chimera_vfs_setattr_dispatch(thread, cred, txn, handle, set_attr,
                                 pre_attr_mask, post_attr_mask,
                                 callback, private_data);
} /* chimera_vfs_setattr */