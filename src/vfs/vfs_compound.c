// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * The compound executor (contract in vfs_compound.h).
 *
 * A sequence is a small array of ops plus a cursor and a CURRENT object.  The
 * executor issues one op at a time through the ordinary per-op API -- so the
 * backend, the caches, the DAC gate and the claim layer all see exactly what
 * they see for an unsequenced op -- and each completion advances the cursor.
 *
 * The CURRENT object is kept as an open handle rather than a file handle,
 * which is the one real difference from how a protocol server chains by hand.
 * A server holding a raw fh has to re-open it for every op that needs a
 * handle; NFS4's LOOKUP, GETATTR and ACCESS each do their own open_fh of the
 * same current fh today.  Holding the handle across the sequence collapses
 * that to one open per object, and the handle is released as soon as the
 * current object changes.
 *
 * THE SAVED SLOT holds a file handle and nothing else.  It would be tempting to
 * park the open handle there too and hand it back on RESTOREFH, but an open
 * handle is a refcount in one of the VFS open caches with exactly one owner
 * here: putting the same pointer in two slots means every release has to know
 * whether the other slot still refers to it, and the escape hatch
 * (chimera_vfs_dup_handle, to take a second reference) aborts outright on a
 * synthetic handle -- which is what an inferred open of a native backend
 * produces, i.e. the common case.  So SAVEFH copies the handle *name* and
 * RESTOREFH re-opens: one extra open on a path a protocol takes rarely, in
 * exchange for a slot that cannot double-release or leak.
 */

#include <stdlib.h>
#include <string.h>

#include "vfs_compound.h"
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "vfs_release.h"
#include "sdk/vfs_access.h"
#include "common/macros.h"

struct chimera_vfs_compound {
    struct chimera_vfs_thread      *thread;
    const struct chimera_vfs_cred  *cred;

    struct chimera_vfs_compound_op  ops[CHIMERA_VFS_COMPOUND_MAX_OPS];
    uint32_t                        num_ops;
    uint32_t                        index;       /* op being executed        */
    uint32_t                        completed;   /* ops that ran             */
    enum chimera_vfs_error          status;

    /* The current object: its file handle, and an open handle for it once
     * something has needed one.  handle_flags records what that handle was
     * opened with, so an op needing more than it carries (a LOOKUP wanting a
     * directory open) can re-open rather than settle for less. */
    uint8_t                         fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        fh_len;
    struct chimera_vfs_open_handle *handle;
    unsigned int                    handle_flags;

    /* The saved slot (SAVEFH/RESTOREFH): a file handle, no open handle. */
    uint8_t                         saved_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        saved_fh_len;

    chimera_vfs_compound_callback_t callback;
    void                           *private_data;
};

static void chimera_vfs_compound_step(
    struct chimera_vfs_compound *compound);

SYMBOL_EXPORT struct chimera_vfs_compound *
chimera_vfs_compound_alloc(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred)
{
    struct chimera_vfs_compound *compound;

    compound         = calloc(1, sizeof(*compound));
    compound->thread = thread;
    compound->cred   = cred;
    compound->status = CHIMERA_VFS_OK;

    return compound;
} /* chimera_vfs_compound_alloc */

SYMBOL_EXPORT void
chimera_vfs_compound_free(struct chimera_vfs_compound *compound)
{
    uint32_t i;

    if (compound->handle) {
        chimera_vfs_release(compound->thread, compound->handle);
    }

    for (i = 0; i < compound->num_ops; i++) {
        free(compound->ops[i].target);
        free(compound->ops[i].entries);
        free(compound->ops[i].buffer);
    }

    free(compound);
} /* chimera_vfs_compound_free */

/* Claim the next op slot, or -1 when the sequence is full. */
static struct chimera_vfs_compound_op *
chimera_vfs_compound_next_op(
    struct chimera_vfs_compound      *compound,
    enum chimera_vfs_compound_op_type type,
    int                              *index)
{
    struct chimera_vfs_compound_op *op;

    if (compound->num_ops >= CHIMERA_VFS_COMPOUND_MAX_OPS) {
        *index = -1;
        return NULL;
    }

    *index      = (int) compound->num_ops++;
    op          = &compound->ops[*index];
    op->type    = (uint8_t) type;
    op->status  = CHIMERA_VFS_UNSET;

    return op;
} /* chimera_vfs_compound_next_op */

SYMBOL_EXPORT int
chimera_vfs_compound_add_putfh(
    struct chimera_vfs_compound *compound,
    const void                  *fh,
    int                          fhlen)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    if (fhlen <= 0 || fhlen > CHIMERA_VFS_FH_SIZE) {
        return -1;
    }

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_PUTFH, &index);

    if (!op) {
        return -1;
    }

    memcpy(op->arg_fh, fh, fhlen);
    op->arg_fh_len = (uint32_t) fhlen;

    return index;
} /* chimera_vfs_compound_add_putfh */

SYMBOL_EXPORT int
chimera_vfs_compound_add_lookup(
    struct chimera_vfs_compound *compound,
    const char                  *name,
    int                          namelen,
    uint64_t                     attr_mask)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    if (namelen <= 0 || namelen > CHIMERA_VFS_COMPOUND_NAME_MAX) {
        return -1;
    }

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_LOOKUP, &index);

    if (!op) {
        return -1;
    }

    memcpy(op->name, name, namelen);
    op->name[namelen] = '\0';
    op->name_len      = (uint32_t) namelen;
    op->attr_mask     = attr_mask;

    return index;
} /* chimera_vfs_compound_add_lookup */

SYMBOL_EXPORT int
chimera_vfs_compound_add_getattr(
    struct chimera_vfs_compound *compound,
    uint64_t                     attr_mask)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_GETATTR, &index);

    if (!op) {
        return -1;
    }

    op->attr_mask = attr_mask;

    return index;
} /* chimera_vfs_compound_add_getattr */

SYMBOL_EXPORT int
chimera_vfs_compound_add_access(
    struct chimera_vfs_compound *compound,
    uint32_t                     requested)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_ACCESS, &index);

    if (!op) {
        return -1;
    }

    op->requested = requested;
    /* The ACL as well as the mode: chimera_vfs_access_check evaluates a native
     * ACL when the object has one and only falls back to the mode bits when it
     * does not, so fetching stat alone would silently answer every ACL-bearing
     * object from its mode. */
    op->attr_mask = CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_ACL;

    return index;
} /* chimera_vfs_compound_add_access */

SYMBOL_EXPORT int
chimera_vfs_compound_add_getfh(struct chimera_vfs_compound *compound)
{
    int index;

    chimera_vfs_compound_next_op(compound,
                                 CHIMERA_VFS_COMPOUND_OP_GETFH, &index);

    return index;
} /* chimera_vfs_compound_add_getfh */

SYMBOL_EXPORT int
chimera_vfs_compound_add_readlink(struct chimera_vfs_compound *compound)
{
    int index;

    chimera_vfs_compound_next_op(compound,
                                 CHIMERA_VFS_COMPOUND_OP_READLINK, &index);

    return index;
} /* chimera_vfs_compound_add_readlink */

SYMBOL_EXPORT int
chimera_vfs_compound_add_savefh(struct chimera_vfs_compound *compound)
{
    int index;

    chimera_vfs_compound_next_op(compound,
                                 CHIMERA_VFS_COMPOUND_OP_SAVEFH, &index);

    return index;
} /* chimera_vfs_compound_add_savefh */

SYMBOL_EXPORT int
chimera_vfs_compound_add_restorefh(struct chimera_vfs_compound *compound)
{
    int index;

    chimera_vfs_compound_next_op(compound,
                                 CHIMERA_VFS_COMPOUND_OP_RESTOREFH, &index);

    return index;
} /* chimera_vfs_compound_add_restorefh */

SYMBOL_EXPORT int
chimera_vfs_compound_add_lookupp(
    struct chimera_vfs_compound *compound,
    uint64_t                     attr_mask)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_LOOKUPP, &index);

    if (!op) {
        return -1;
    }

    op->attr_mask = attr_mask;

    return index;
} /* chimera_vfs_compound_add_lookupp */

SYMBOL_EXPORT int
chimera_vfs_compound_add_commit(
    struct chimera_vfs_compound *compound,
    uint64_t                     offset,
    uint64_t                     count,
    uint64_t                     pre_attr_mask)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_COMMIT, &index);

    if (!op) {
        return -1;
    }

    op->offset    = offset;
    op->count     = count;
    op->attr_mask = pre_attr_mask;

    return index;
} /* chimera_vfs_compound_add_commit */

SYMBOL_EXPORT int
chimera_vfs_compound_add_readdir(
    struct chimera_vfs_compound *compound,
    uint64_t                     cookie,
    uint64_t                     verifier,
    uint32_t                     dircount,
    uint32_t                     maxcount,
    uint32_t                     max_entries,
    uint64_t                     attr_mask)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    if (max_entries > CHIMERA_VFS_COMPOUND_READDIR_MAX_ENTRIES) {
        return -1;
    }

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_READDIR, &index);

    if (!op) {
        return -1;
    }

    op->cookie      = cookie;
    op->verifier    = verifier;
    op->dircount    = dircount;
    op->maxcount    = maxcount;
    op->max_entries = max_entries;
    op->attr_mask   = attr_mask;

    return index;
} /* chimera_vfs_compound_add_readdir */

SYMBOL_EXPORT int
chimera_vfs_compound_add_getxattr(
    struct chimera_vfs_compound *compound,
    const char                  *name,
    int                          namelen,
    uint32_t                     value_max)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    if (namelen <= 0 || namelen > CHIMERA_VFS_COMPOUND_NAME_MAX) {
        return -1;
    }

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_GETXATTR, &index);

    if (!op) {
        return -1;
    }

    memcpy(op->name, name, namelen);
    op->name[namelen] = '\0';
    op->name_len      = (uint32_t) namelen;
    op->buffer_max    = value_max;

    return index;
} /* chimera_vfs_compound_add_getxattr */

SYMBOL_EXPORT int
chimera_vfs_compound_add_setxattr(
    struct chimera_vfs_compound *compound,
    uint32_t                     option,
    const char                  *name,
    int                          namelen,
    const void                  *value,
    uint32_t                     value_len)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    if (namelen <= 0 || namelen > CHIMERA_VFS_COMPOUND_NAME_MAX) {
        return -1;
    }

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_SETXATTR, &index);

    if (!op) {
        return -1;
    }

    memcpy(op->name, name, namelen);
    op->name[namelen]   = '\0';
    op->name_len        = (uint32_t) namelen;
    op->xattr_option    = option;
    op->xattr_value     = value;
    op->xattr_value_len = value_len;

    return index;
} /* chimera_vfs_compound_add_setxattr */

SYMBOL_EXPORT int
chimera_vfs_compound_add_listxattrs(
    struct chimera_vfs_compound *compound,
    uint64_t                     cookie,
    uint32_t                     max_bytes)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_LISTXATTRS,
                                      &index);

    if (!op) {
        return -1;
    }

    op->cookie     = cookie;
    op->buffer_max = max_bytes;

    return index;
} /* chimera_vfs_compound_add_listxattrs */

SYMBOL_EXPORT int
chimera_vfs_compound_add_removexattr(
    struct chimera_vfs_compound *compound,
    const char                  *name,
    int                          namelen)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    if (namelen <= 0 || namelen > CHIMERA_VFS_COMPOUND_NAME_MAX) {
        return -1;
    }

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_REMOVEXATTR,
                                      &index);

    if (!op) {
        return -1;
    }

    memcpy(op->name, name, namelen);
    op->name[namelen] = '\0';
    op->name_len      = (uint32_t) namelen;

    return index;
} /* chimera_vfs_compound_add_removexattr */

/* ---------------------------------------------------------------------- */
/* Execution                                                              */
/* ---------------------------------------------------------------------- */

static void
chimera_vfs_compound_finish(
    struct chimera_vfs_compound *compound,
    enum chimera_vfs_error       status)
{
    compound->status = status;
    compound->callback(compound, compound->private_data);
} /* chimera_vfs_compound_finish */

/* One op finished.  Record it and either advance or stop. */
static void
chimera_vfs_compound_op_done(
    struct chimera_vfs_compound *compound,
    enum chimera_vfs_error       status)
{
    struct chimera_vfs_compound_op *done = &compound->ops[compound->index];

    done->status        = status;
    compound->completed = compound->index + 1;

    /* Record what the op ended up addressing, so a caller describing the
     * object in its reply does not have to re-derive it. */
    if (compound->fh_len) {
        memcpy(done->fh, compound->fh, compound->fh_len);
        done->fh_len = compound->fh_len;
    }

    if (status != CHIMERA_VFS_OK) {
        /* Stop at the first failure: every later op addresses what an earlier
         * one resolved, so there is nothing coherent to run them against. */
        chimera_vfs_compound_finish(compound, status);
        return;
    }

    compound->index++;
    chimera_vfs_compound_step(compound);
} /* chimera_vfs_compound_op_done */

/* The current object changed: drop the handle we were holding for the old
 * one.  The next op that needs a handle opens the new fh. */
static void
chimera_vfs_compound_set_current(
    struct chimera_vfs_compound *compound,
    const void                  *fh,
    uint32_t                     fh_len)
{
    if (compound->handle) {
        chimera_vfs_release(compound->thread, compound->handle);
        compound->handle = NULL;
    }

    compound->handle_flags = 0;

    memcpy(compound->fh, fh, fh_len);
    compound->fh_len = fh_len;
} /* chimera_vfs_compound_set_current */

static void
chimera_vfs_compound_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct chimera_vfs_compound *compound = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_compound_op_done(compound, error_code);
        return;
    }

    compound->handle = handle;

    /* Re-enter the op that needed the handle. */
    chimera_vfs_compound_step(compound);
} /* chimera_vfs_compound_open_callback */

/* Copy a backend's attrs into an op result.
 *
 * va_acl is dropped rather than copied.  A backend reports the ACL by pointing
 * va_acl at its own live inode state (memfs: attr.va_acl = inode->acl), valid
 * only for the duration of its completion -- so a struct copy that survives
 * the callback carries a pointer to memory the caller must not read.  Storing
 * it would leave every consumer one dereference away from a use-after-free
 * that no test would reliably catch.
 *
 * So the sequence does not offer ACLs: the bit is cleared with the pointer, a
 * caller that asks for one sees it absent rather than dangling, and a caller
 * that needs one issues the getattr itself.  Anything computed FROM the ACL
 * while it was live -- ACCESS's granted mask -- is unaffected. */
static void
chimera_vfs_compound_store_attr(
    struct chimera_vfs_compound_op *op,
    const struct chimera_vfs_attrs *attr)
{
    op->attr = *attr;

    op->attr.va_acl       = NULL;
    op->attr.va_req_mask &= ~CHIMERA_VFS_ATTR_ACL;
    op->attr.va_set_mask &= ~CHIMERA_VFS_ATTR_ACL;
} /* chimera_vfs_compound_store_attr */

static void
chimera_vfs_compound_lookup_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = &compound->ops[compound->index];

    (void) dir_attr;

    if (error_code == CHIMERA_VFS_OK) {
        if (attr) {
            chimera_vfs_compound_store_attr(op, attr);
        }

        if (attr && (attr->va_set_mask & CHIMERA_VFS_ATTR_FH)) {
            chimera_vfs_compound_set_current(compound, attr->va_fh,
                                             attr->va_fh_len);
        } else {
            /* A backend that resolves names without producing a handle
             * cannot be chained: the sequence has no way to address what it
             * just found. */
            error_code = CHIMERA_VFS_ENOTSUP;
        }
    }

    chimera_vfs_compound_op_done(compound, error_code);
} /* chimera_vfs_compound_lookup_callback */

static void
chimera_vfs_compound_getattr_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = &compound->ops[compound->index];

    if (error_code == CHIMERA_VFS_OK && attr) {
        chimera_vfs_compound_store_attr(op, attr);

        if (op->type == CHIMERA_VFS_COMPOUND_OP_ACCESS) {
            op->granted = chimera_vfs_access_check(attr, compound->cred,
                                                   op->requested);
        }
    }

    chimera_vfs_compound_op_done(compound, error_code);
} /* chimera_vfs_compound_getattr_callback */

/* readlink writes into a caller-supplied buffer and reports only the length,
 * so the target buffer is allocated before the call and sized here. */
#define CHIMERA_VFS_COMPOUND_TARGET_MAX 4096

static void
chimera_vfs_compound_readlink_callback(
    enum chimera_vfs_error    error_code,
    int                       targetlen,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = &compound->ops[compound->index];

    (void) attr;

    if (error_code == CHIMERA_VFS_OK && targetlen > 0) {
        op->target[targetlen] = '\0';
        op->target_len        = (uint32_t) targetlen;
    }

    chimera_vfs_compound_op_done(compound, error_code);
} /* chimera_vfs_compound_readlink_callback */

static void
chimera_vfs_compound_commit_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = &compound->ops[compound->index];

    (void) post_attr;

    if (error_code == CHIMERA_VFS_OK && pre_attr) {
        chimera_vfs_compound_store_attr(op, pre_attr);
    }

    chimera_vfs_compound_op_done(compound, error_code);
} /* chimera_vfs_compound_commit_callback */

/* One directory entry.  Returns non-zero to stop the enumeration, which is how
 * every readdir caller applies a budget; the backend then reports eof=0 and the
 * cookie of the entry it was refused. */
static int
chimera_vfs_compound_readdir_entry(
    uint64_t                        inum,
    uint64_t                        cookie,
    const char                     *name,
    int                             namelen,
    const struct chimera_vfs_attrs *attrs,
    void                           *arg)
{
    struct chimera_vfs_compound        *compound = arg;
    struct chimera_vfs_compound_op     *op       = &compound->ops[compound->index];
    struct chimera_vfs_compound_dirent *dirent;

    if (op->num_entries >= op->max_entries) {
        return -1;
    }

    /* A name the entry struct cannot hold would be silently truncated into a
     * different name, so stop rather than report one. */
    if (namelen <= 0 || namelen > CHIMERA_VFS_COMPOUND_NAME_MAX) {
        return -1;
    }

    dirent = &op->entries[op->num_entries];

    dirent->inum     = inum;
    dirent->cookie   = cookie;
    dirent->name_len = (uint32_t) namelen;
    memcpy(dirent->name, name, namelen);
    dirent->name[namelen] = '\0';

    dirent->attr = *attrs;
    /* Same rule as every other attribute result here: the backend owns the ACL
     * only while this callback runs. */
    dirent->attr.va_acl       = NULL;
    dirent->attr.va_req_mask &= ~CHIMERA_VFS_ATTR_ACL;
    dirent->attr.va_set_mask &= ~CHIMERA_VFS_ATTR_ACL;

    op->num_entries++;

    return 0;
} /* chimera_vfs_compound_readdir_entry */

static void
chimera_vfs_compound_readdir_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        cookie,
    uint64_t                        verifier,
    uint32_t                        eof,
    struct chimera_vfs_attrs       *dir_attr,
    void                           *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = &compound->ops[compound->index];

    (void) handle;
    (void) dir_attr;

    if (error_code == CHIMERA_VFS_OK) {
        op->r_cookie   = cookie;
        op->r_verifier = verifier;
        op->eof        = eof;
    }

    chimera_vfs_compound_op_done(compound, error_code);
} /* chimera_vfs_compound_readdir_callback */

static void
chimera_vfs_compound_get_xattr_callback(
    enum chimera_vfs_error error_code,
    uint32_t               value_len,
    void                  *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = &compound->ops[compound->index];

    if (error_code == CHIMERA_VFS_OK) {
        op->buffer_len = value_len;
    }

    chimera_vfs_compound_op_done(compound, error_code);
} /* chimera_vfs_compound_get_xattr_callback */

static void
chimera_vfs_compound_list_xattrs_callback(
    enum chimera_vfs_error error_code,
    const char            *names,
    uint32_t               names_len,
    uint32_t               count,
    uint32_t               eof,
    uint64_t               cookie,
    void                  *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = &compound->ops[compound->index];

    /* The backend wrote the names into the buffer we gave it, which is
     * op->buffer; `names` is that same pointer handed back. */
    (void) names;

    if (error_code == CHIMERA_VFS_OK) {
        op->buffer_len   = names_len;
        op->buffer_count = count;
        op->eof          = eof;
        op->r_cookie     = cookie;
    }

    chimera_vfs_compound_op_done(compound, error_code);
} /* chimera_vfs_compound_list_xattrs_callback */

/* SETXATTR and REMOVEXATTR report the object's ctime either side of the
 * change; both callbacks land here. */
static void
chimera_vfs_compound_xattr_change_callback(
    enum chimera_vfs_error          error_code,
    const struct chimera_vfs_attrs *pre_attr,
    const struct chimera_vfs_attrs *post_attr,
    void                           *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = &compound->ops[compound->index];

    if (error_code == CHIMERA_VFS_OK) {
        if (pre_attr) {
            op->pre_ctime = pre_attr->va_ctime;
        }
        if (post_attr) {
            op->post_ctime = post_attr->va_ctime;
        }
    }

    chimera_vfs_compound_op_done(compound, error_code);
} /* chimera_vfs_compound_xattr_change_callback */

/*
 * The flags this op needs the current object opened with, or 0 if it addresses
 * the current object without a handle at all.
 *
 * LOOKUP, LOOKUPP and READDIR ask for a directory open, which is what every
 * protocol's own versions of them do today: reaching through a non-directory
 * then fails on the open, uniformly, instead of on however a particular
 * backend's lookup_at chooses to report a non-directory parent.
 *
 * COMMIT and the xattr ops ask for a *data* open (no CHIMERA_VFS_OPEN_PATH),
 * again matching what those operations do outside a sequence.
 */
static unsigned int
chimera_vfs_compound_op_open_flags(uint8_t type)
{
    switch (type) {
        case CHIMERA_VFS_COMPOUND_OP_LOOKUP:
        case CHIMERA_VFS_COMPOUND_OP_LOOKUPP:
        case CHIMERA_VFS_COMPOUND_OP_READDIR:
            return CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH |
                   CHIMERA_VFS_OPEN_DIRECTORY;
        case CHIMERA_VFS_COMPOUND_OP_GETATTR:
        case CHIMERA_VFS_COMPOUND_OP_ACCESS:
        case CHIMERA_VFS_COMPOUND_OP_READLINK:
            return CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH;
        case CHIMERA_VFS_COMPOUND_OP_COMMIT:
        case CHIMERA_VFS_COMPOUND_OP_GETXATTR:
        case CHIMERA_VFS_COMPOUND_OP_SETXATTR:
        case CHIMERA_VFS_COMPOUND_OP_LISTXATTRS:
        case CHIMERA_VFS_COMPOUND_OP_REMOVEXATTR:
            return CHIMERA_VFS_OPEN_INFERRED;
        default:
            return 0;
    } /* switch */
} /* chimera_vfs_compound_op_open_flags */

/*
 * Can the handle we are holding serve an op that wants `want`?
 *
 * Only if it carries every flag `want` asks for AND sits on the same side of
 * CHIMERA_VFS_OPEN_PATH.  The second half is not pedantry: a path open and a
 * data open come out of two different open caches and are two different things
 * to a backend (O_PATH versus a real descriptor), so a data op offered a path
 * handle is not being given "more than it asked for", it is being given the
 * wrong handle -- and by the subset rule alone it would take it, because a data
 * open's flags are a subset of a path open's.
 */
static int
chimera_vfs_compound_handle_serves(
    unsigned int have,
    unsigned int want)
{
    if (want & ~have) {
        return 0;
    }

    return ((have ^ want) & CHIMERA_VFS_OPEN_PATH) == 0;
} /* chimera_vfs_compound_handle_serves */

static void
chimera_vfs_compound_step(struct chimera_vfs_compound *compound)
{
    struct chimera_vfs_compound_op *op;
    unsigned int                    open_flags;

    if (compound->index >= compound->num_ops) {
        chimera_vfs_compound_finish(compound, CHIMERA_VFS_OK);
        return;
    }

    op         = &compound->ops[compound->index];
    open_flags = chimera_vfs_compound_op_open_flags(op->type);

    if (open_flags) {
        if (compound->fh_len == 0) {
            /* No current object: the sequence addressed one before naming
             * one.  A caller builds these itself, so this is its bug. */
            chimera_vfs_compound_op_done(compound, CHIMERA_VFS_EINVAL);
            return;
        }

        if (!compound->handle ||
            !chimera_vfs_compound_handle_serves(compound->handle_flags,
                                                open_flags)) {
            /* Open the current object once; every op that follows on the same
             * object reuses this handle -- unless it needs flags the handle
             * was not opened with, in which case it is re-opened. */
            if (compound->handle) {
                chimera_vfs_release(compound->thread, compound->handle);
                compound->handle = NULL;
            }

            compound->handle_flags = open_flags;

            chimera_vfs_open_fh(compound->thread, compound->cred,
                                compound->fh, (int) compound->fh_len,
                                open_flags,
                                chimera_vfs_compound_open_callback, compound);
            return;
        }
    }

    switch (op->type) {
        case CHIMERA_VFS_COMPOUND_OP_PUTFH:
            chimera_vfs_compound_set_current(compound, op->arg_fh,
                                             op->arg_fh_len);
            chimera_vfs_compound_op_done(compound, CHIMERA_VFS_OK);
            break;

        case CHIMERA_VFS_COMPOUND_OP_GETFH:
            if (compound->fh_len == 0) {
                chimera_vfs_compound_op_done(compound, CHIMERA_VFS_EINVAL);
                break;
            }
            /* The fh is recorded for every op below; GETFH exists so the
             * caller has an index to read it from. */
            chimera_vfs_compound_op_done(compound, CHIMERA_VFS_OK);
            break;

        case CHIMERA_VFS_COMPOUND_OP_SAVEFH:
            if (compound->fh_len == 0) {
                chimera_vfs_compound_op_done(compound, CHIMERA_VFS_EINVAL);
                break;
            }
            /* A handle copy, not a handle: see the SAVED SLOT note above. */
            memcpy(compound->saved_fh, compound->fh, compound->fh_len);
            compound->saved_fh_len = compound->fh_len;
            chimera_vfs_compound_op_done(compound, CHIMERA_VFS_OK);
            break;

        case CHIMERA_VFS_COMPOUND_OP_RESTOREFH:
            if (compound->saved_fh_len == 0) {
                chimera_vfs_compound_op_done(compound, CHIMERA_VFS_EINVAL);
                break;
            }
            /* set_current releases whatever handle the old current object had;
             * the saved slot never held one, so nothing is released twice and
             * nothing is left behind. */
            chimera_vfs_compound_set_current(compound, compound->saved_fh,
                                             compound->saved_fh_len);
            chimera_vfs_compound_op_done(compound, CHIMERA_VFS_OK);
            break;

        case CHIMERA_VFS_COMPOUND_OP_LOOKUP:
            chimera_vfs_lookup_at(compound->thread, compound->cred,
                                  compound->handle,
                                  op->name, op->name_len,
                                  op->attr_mask | CHIMERA_VFS_ATTR_FH,
                                  0,
                                  chimera_vfs_compound_lookup_callback,
                                  compound);
            break;

        case CHIMERA_VFS_COMPOUND_OP_LOOKUPP:
            /* ".." through the open directory: the parent, by the one route
             * every backend supports.  The result chains exactly as a LOOKUP's
             * does, so they share a completion. */
            chimera_vfs_lookup_at(compound->thread, compound->cred,
                                  compound->handle,
                                  "..", 2,
                                  op->attr_mask | CHIMERA_VFS_ATTR_FH,
                                  0,
                                  chimera_vfs_compound_lookup_callback,
                                  compound);
            break;

        case CHIMERA_VFS_COMPOUND_OP_COMMIT:
            chimera_vfs_commit(compound->thread, compound->cred,
                               compound->handle,
                               op->offset, op->count,
                               op->attr_mask, 0,
                               chimera_vfs_compound_commit_callback,
                               compound);
            break;

        case CHIMERA_VFS_COMPOUND_OP_READDIR:
            if (!op->entries && op->max_entries) {
                op->entries = calloc(op->max_entries, sizeof(*op->entries));
            }
            op->num_entries = 0;
            chimera_vfs_readdir(compound->thread, compound->cred,
                                compound->handle,
                                op->attr_mask,
                                0,
                                op->cookie,
                                op->verifier,
                                0,
                                NULL, 0,
                                chimera_vfs_compound_readdir_entry,
                                chimera_vfs_compound_readdir_callback,
                                compound);
            break;

        case CHIMERA_VFS_COMPOUND_OP_GETXATTR:
            if (!op->buffer && op->buffer_max) {
                op->buffer = calloc(1, op->buffer_max);
            }
            chimera_vfs_get_xattr(compound->thread, compound->cred,
                                  compound->handle,
                                  op->name, op->name_len,
                                  op->buffer, op->buffer_max,
                                  chimera_vfs_compound_get_xattr_callback,
                                  compound);
            break;

        case CHIMERA_VFS_COMPOUND_OP_SETXATTR:
            chimera_vfs_set_xattr(compound->thread, compound->cred,
                                  compound->handle,
                                  op->xattr_option,
                                  op->name, op->name_len,
                                  op->xattr_value, op->xattr_value_len,
                                  chimera_vfs_compound_xattr_change_callback,
                                  compound);
            break;

        case CHIMERA_VFS_COMPOUND_OP_LISTXATTRS:
            if (!op->buffer && op->buffer_max) {
                op->buffer = calloc(1, op->buffer_max);
            }
            chimera_vfs_list_xattrs(compound->thread, compound->cred,
                                    compound->handle,
                                    op->cookie,
                                    op->buffer, op->buffer_max,
                                    chimera_vfs_compound_list_xattrs_callback,
                                    compound);
            break;

        case CHIMERA_VFS_COMPOUND_OP_REMOVEXATTR:
            chimera_vfs_remove_xattr(compound->thread, compound->cred,
                                     compound->handle,
                                     op->name, op->name_len,
                                     chimera_vfs_compound_xattr_change_callback,
                                     compound);
            break;

        case CHIMERA_VFS_COMPOUND_OP_GETATTR:
        case CHIMERA_VFS_COMPOUND_OP_ACCESS:
            chimera_vfs_getattr(compound->thread, compound->cred,
                                compound->handle, op->attr_mask,
                                chimera_vfs_compound_getattr_callback,
                                compound);
            break;

        case CHIMERA_VFS_COMPOUND_OP_READLINK:
            if (!op->target) {
                op->target = calloc(1, CHIMERA_VFS_COMPOUND_TARGET_MAX + 1);
            }
            chimera_vfs_readlink(compound->thread, compound->cred,
                                 compound->handle,
                                 op->target, CHIMERA_VFS_COMPOUND_TARGET_MAX,
                                 0,
                                 chimera_vfs_compound_readlink_callback,
                                 compound);
            break;

        default:
            chimera_vfs_compound_op_done(compound, CHIMERA_VFS_ENOTSUP);
            break;
    } /* switch */
} /* chimera_vfs_compound_step */

SYMBOL_EXPORT void
chimera_vfs_compound_submit(
    struct chimera_vfs_compound    *compound,
    chimera_vfs_compound_callback_t callback,
    void                           *private_data)
{
    compound->callback     = callback;
    compound->private_data = private_data;
    compound->index        = 0;
    compound->completed    = 0;

    chimera_vfs_compound_step(compound);
} /* chimera_vfs_compound_submit */

/* ---------------------------------------------------------------------- */
/* Results                                                                */
/* ---------------------------------------------------------------------- */

SYMBOL_EXPORT uint32_t
chimera_vfs_compound_num_ops(const struct chimera_vfs_compound *compound)
{
    return compound->num_ops;
} /* chimera_vfs_compound_num_ops */

SYMBOL_EXPORT uint32_t
chimera_vfs_compound_num_completed(const struct chimera_vfs_compound *compound)
{
    return compound->completed;
} /* chimera_vfs_compound_num_completed */

SYMBOL_EXPORT enum chimera_vfs_error
chimera_vfs_compound_status(const struct chimera_vfs_compound *compound)
{
    return compound->status;
} /* chimera_vfs_compound_status */

SYMBOL_EXPORT const struct chimera_vfs_compound_op *
chimera_vfs_compound_op(
    const struct chimera_vfs_compound *compound,
    uint32_t                           index)
{
    if (index >= compound->num_ops) {
        return NULL;
    }

    return &compound->ops[index];
} /* chimera_vfs_compound_op */
