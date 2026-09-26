// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include <sys/xattr.h>

#include "fuse_internal.h"
#include "vfs/vfs_procs.h"

/* Reply payloads are staged in the request buffer's reply area. */
#define CHIMERA_FUSE_XATTR_MAX (CHIMERA_FUSE_BUFSZ - CHIMERA_FUSE_REPLY_OFF)

/* --- GETXATTR --- */

static void
chimera_fuse_getxattr_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_fuse_request          *req = private_data;
    const struct chimera_vfs_compound_op *op;
    enum chimera_vfs_error                status;
    struct fuse_getxattr_out              out;

    status = chimera_vfs_compound_status(compound);

    if (status != CHIMERA_VFS_OK) {
        chimera_fuse_reply(req, chimera_fuse_errno(status), NULL, 0);
        return;
    }

    op = chimera_vfs_compound_op(compound,
                                 chimera_vfs_compound_num_ops(compound) - 1);

    /* The sequence staged the value in its own buffer; the size probe never
     * reads it, and a real read copies only what the kernel asked for, bounded
     * by the kernel's requested size. */
    if (req->u.xattr.size && op->buffer_len &&
        op->buffer_len <= req->u.xattr.size) {
        memcpy(chimera_fuse_reply_space(req), op->buffer, op->buffer_len);
    }

    if (req->u.xattr.size == 0) {
        /* Size probe. */
        memset(&out, 0, sizeof(out));
        out.size = op->buffer_len;
        chimera_fuse_reply(req, 0, &out, sizeof(out));
        return;
    }

    if (op->buffer_len > req->u.xattr.size) {
        chimera_fuse_reply(req, ERANGE, NULL, 0);
        return;
    }

    chimera_fuse_reply(req, 0, chimera_fuse_reply_space(req), op->buffer_len);
} /* chimera_fuse_getxattr_sequence_complete */

void
chimera_fuse_op_getxattr(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    const struct fuse_getxattr_in *in   = arg;
    const char                    *name = (const char *) (in + 1);

    if (arglen < sizeof(*in)) {
        chimera_fuse_reply(req, EINVAL, NULL, 0);
        return;
    }

    req->u.xattr.size = in->size;

    if (chimera_fuse_resolve_nodeid(req) != 0) {
        chimera_fuse_reply(req, ESTALE, NULL, 0);
        return;
    }

    req->compound = chimera_vfs_compound_alloc(req->thread->vfs_thread,
                                               &req->cred);

    chimera_vfs_compound_add_putfh(req->compound, req->fh, (int) req->fh_len);
    /* The open the sequence used to do for this op, said out loud. */
    chimera_vfs_compound_add_open_current(req->compound,
                                          CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
                                          0);

    chimera_vfs_compound_add_getxattr(req->compound, name, (int) strlen(name),
                                      CHIMERA_FUSE_XATTR_MAX);

    chimera_vfs_compound_submit(req->compound,
                                chimera_fuse_getxattr_sequence_complete, req);
} /* chimera_fuse_op_getxattr */

/* --- SETXATTR --- */

static void
chimera_fuse_xattr_status_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_fuse_request *req = private_data;

    chimera_fuse_reply(req,
                       chimera_fuse_errno(chimera_vfs_compound_status(compound)),
                       NULL, 0);
} /* chimera_fuse_xattr_status_complete */

void
chimera_fuse_op_setxattr(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    /* Without FUSE_SETXATTR_EXT negotiated the kernel sends the legacy
     * 8-byte struct: u32 size, u32 flags. */
    const uint32_t *in    = arg;
    uint32_t        size  = in[0];
    uint32_t        flags = in[1];
    const char     *name  = (const char *) arg + FUSE_COMPAT_SETXATTR_IN_SIZE;
    const void     *value = name + strlen(name) + 1;
    uint32_t        option;

    if (arglen < FUSE_COMPAT_SETXATTR_IN_SIZE) {
        chimera_fuse_reply(req, EINVAL, NULL, 0);
        return;
    }

    switch (flags) {
        case 0:
            option = CHIMERA_VFS_XATTR_EITHER;
            break;
        case XATTR_CREATE:
            option = CHIMERA_VFS_XATTR_CREATE;
            break;
        case XATTR_REPLACE:
            option = CHIMERA_VFS_XATTR_REPLACE;
            break;
        default:
            chimera_fuse_reply(req, EINVAL, NULL, 0);
            return;
    } /* switch */

    if (chimera_fuse_resolve_nodeid(req) != 0) {
        chimera_fuse_reply(req, ESTALE, NULL, 0);
        return;
    }

    req->compound = chimera_vfs_compound_alloc(req->thread->vfs_thread,
                                               &req->cred);

    chimera_vfs_compound_add_putfh(req->compound, req->fh, (int) req->fh_len);
    /* The value is borrowed: it points into the request buffer, which outlives
     * the sequence. */
    /* The open the sequence used to do for this op, said out loud. */
    chimera_vfs_compound_add_open_current(req->compound,
                                          CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
                                          0);

    chimera_vfs_compound_add_setxattr(req->compound, option,
                                      name, (int) strlen(name),
                                      value, size);

    chimera_vfs_compound_submit(req->compound,
                                chimera_fuse_xattr_status_complete, req);
} /* chimera_fuse_op_setxattr */

/* --- LISTXATTR --- */

static void
chimera_fuse_listxattr_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_fuse_request          *req = private_data;
    const struct chimera_vfs_compound_op *op;
    enum chimera_vfs_error                status;
    struct fuse_getxattr_out              out;

    status = chimera_vfs_compound_status(compound);

    if (status != CHIMERA_VFS_OK) {
        chimera_fuse_reply(req, chimera_fuse_errno(status), NULL, 0);
        return;
    }

    op = chimera_vfs_compound_op(compound,
                                 chimera_vfs_compound_num_ops(compound) - 1);

    if (!op->eof) {
        /* The full list exceeds our reply staging area. */
        chimera_fuse_reply(req, ERANGE, NULL, 0);
        return;
    }

    if (req->u.xattr.size == 0) {
        memset(&out, 0, sizeof(out));
        out.size = op->buffer_len;
        chimera_fuse_reply(req, 0, &out, sizeof(out));
        return;
    }

    if (op->buffer_len > req->u.xattr.size) {
        chimera_fuse_reply(req, ERANGE, NULL, 0);
        return;
    }

    chimera_fuse_reply(req, 0, op->buffer, op->buffer_len);
} /* chimera_fuse_listxattr_sequence_complete */

void
chimera_fuse_op_listxattr(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    const struct fuse_getxattr_in *in = arg;

    if (arglen < sizeof(*in)) {
        chimera_fuse_reply(req, EINVAL, NULL, 0);
        return;
    }

    req->u.xattr.size = in->size;

    if (chimera_fuse_resolve_nodeid(req) != 0) {
        chimera_fuse_reply(req, ESTALE, NULL, 0);
        return;
    }

    req->compound = chimera_vfs_compound_alloc(req->thread->vfs_thread,
                                               &req->cred);

    chimera_vfs_compound_add_putfh(req->compound, req->fh, (int) req->fh_len);
    /* The open the sequence used to do for this op, said out loud. */
    chimera_vfs_compound_add_open_current(req->compound,
                                          CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
                                          0);

    chimera_vfs_compound_add_listxattrs(req->compound, 0,
                                        CHIMERA_FUSE_XATTR_MAX);

    chimera_vfs_compound_submit(req->compound,
                                chimera_fuse_listxattr_sequence_complete, req);
} /* chimera_fuse_op_listxattr */

/* --- REMOVEXATTR --- */

void
chimera_fuse_op_removexattr(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    const char *name = arg;

    if (chimera_fuse_resolve_nodeid(req) != 0) {
        chimera_fuse_reply(req, ESTALE, NULL, 0);
        return;
    }

    req->compound = chimera_vfs_compound_alloc(req->thread->vfs_thread,
                                               &req->cred);

    chimera_vfs_compound_add_putfh(req->compound, req->fh, (int) req->fh_len);
    /* The open the sequence used to do for this op, said out loud. */
    chimera_vfs_compound_add_open_current(req->compound,
                                          CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
                                          0);

    chimera_vfs_compound_add_removexattr(req->compound, name,
                                         (int) strlen(name));

    chimera_vfs_compound_submit(req->compound,
                                chimera_fuse_xattr_status_complete, req);
} /* chimera_fuse_op_removexattr */
