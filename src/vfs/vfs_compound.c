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
     * something has needed one. */
    uint8_t                         fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        fh_len;
    struct chimera_vfs_open_handle *handle;

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
    op->attr_mask = CHIMERA_VFS_ATTR_MASK_STAT;

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
    compound->ops[compound->index].status = status;
    compound->completed                   = compound->index + 1;

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
            op->attr = *attr;
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
        op->attr = *attr;

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

/* Does this op address the current object through a handle? */
static int
chimera_vfs_compound_op_needs_handle(uint8_t type)
{
    switch (type) {
        case CHIMERA_VFS_COMPOUND_OP_LOOKUP:
        case CHIMERA_VFS_COMPOUND_OP_GETATTR:
        case CHIMERA_VFS_COMPOUND_OP_ACCESS:
        case CHIMERA_VFS_COMPOUND_OP_READLINK:
            return 1;
        default:
            return 0;
    } /* switch */
} /* chimera_vfs_compound_op_needs_handle */

static void
chimera_vfs_compound_step(struct chimera_vfs_compound *compound)
{
    struct chimera_vfs_compound_op *op;

    if (compound->index >= compound->num_ops) {
        chimera_vfs_compound_finish(compound, CHIMERA_VFS_OK);
        return;
    }

    op = &compound->ops[compound->index];

    if (chimera_vfs_compound_op_needs_handle(op->type)) {
        if (compound->fh_len == 0) {
            /* No current object: the sequence addressed one before naming
             * one.  A caller builds these itself, so this is its bug. */
            chimera_vfs_compound_op_done(compound, CHIMERA_VFS_EINVAL);
            return;
        }

        if (!compound->handle) {
            /* Open the current object once; every op that follows on the same
             * object reuses this handle. */
            chimera_vfs_open_fh(compound->thread, compound->cred,
                                compound->fh, (int) compound->fh_len,
                                CHIMERA_VFS_OPEN_INFERRED |
                                CHIMERA_VFS_OPEN_PATH,
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
            memcpy(op->fh, compound->fh, compound->fh_len);
            op->fh_len = compound->fh_len;
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
