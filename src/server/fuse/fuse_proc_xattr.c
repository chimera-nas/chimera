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
chimera_fuse_getxattr_complete(
    enum chimera_vfs_error error_code,
    uint32_t               value_len,
    void                  *private_data)
{
    struct chimera_fuse_request *req = private_data;
    struct fuse_getxattr_out     out;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_fuse_reply(req, chimera_fuse_errno(error_code), NULL, 0);
        return;
    }

    if (req->u.xattr.size == 0) {
        /* Size probe. */
        memset(&out, 0, sizeof(out));
        out.size = value_len;
        chimera_fuse_reply(req, 0, &out, sizeof(out));
        return;
    }

    if (value_len > req->u.xattr.size) {
        chimera_fuse_reply(req, ERANGE, NULL, 0);
        return;
    }

    chimera_fuse_reply(req, 0, chimera_fuse_reply_space(req), value_len);
} /* chimera_fuse_getxattr_complete */

static void
chimera_fuse_getxattr_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_fuse_request   *req  = private_data;
    const struct fuse_in_header   *hdr  = evpl_iovec_data(&req->buf);
    const struct fuse_getxattr_in *in   = (const struct fuse_getxattr_in *) (hdr + 1);
    const char                    *name = (const char *) (in + 1);

    if (error_code != CHIMERA_VFS_OK) {
        chimera_fuse_reply(req, chimera_fuse_errno(error_code), NULL, 0);
        return;
    }

    req->handle = oh;

    chimera_vfs_get_xattr(req->thread->vfs_thread, &req->cred, oh,
                          name, strlen(name),
                          chimera_fuse_reply_space(req),
                          CHIMERA_FUSE_XATTR_MAX,
                          chimera_fuse_getxattr_complete, req);
} /* chimera_fuse_getxattr_open_callback */

void
chimera_fuse_op_getxattr(
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

    chimera_vfs_open_fh(req->thread->vfs_thread, &req->cred,
                        req->fh, req->fh_len,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
                        chimera_fuse_getxattr_open_callback, req);
} /* chimera_fuse_op_getxattr */

/* --- SETXATTR --- */

static void
chimera_fuse_setxattr_complete(
    enum chimera_vfs_error          error_code,
    const struct chimera_vfs_attrs *pre_attr,
    const struct chimera_vfs_attrs *post_attr,
    void                           *private_data)
{
    struct chimera_fuse_request *req = private_data;

    chimera_fuse_reply(req, chimera_fuse_errno(error_code), NULL, 0);
} /* chimera_fuse_setxattr_complete */

static void
chimera_fuse_setxattr_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_fuse_request *req = private_data;
    const struct fuse_in_header *hdr = evpl_iovec_data(&req->buf);
    /* Without FUSE_SETXATTR_EXT negotiated the kernel sends the legacy
     * 8-byte struct: u32 size, u32 flags. */
    const uint32_t              *in    = (const uint32_t *) (hdr + 1);
    uint32_t                     size  = in[0];
    uint32_t                     flags = in[1];
    const char                  *name  = (const char *) (hdr + 1) +
        FUSE_COMPAT_SETXATTR_IN_SIZE;
    const void                  *value = name + strlen(name) + 1;
    uint32_t                     option;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_fuse_reply(req, chimera_fuse_errno(error_code), NULL, 0);
        return;
    }

    req->handle = oh;

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

    chimera_vfs_set_xattr(req->thread->vfs_thread, &req->cred, oh,
                          option,
                          name, strlen(name),
                          value, size,
                          chimera_fuse_setxattr_complete, req);
} /* chimera_fuse_setxattr_open_callback */

void
chimera_fuse_op_setxattr(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    if (arglen < FUSE_COMPAT_SETXATTR_IN_SIZE) {
        chimera_fuse_reply(req, EINVAL, NULL, 0);
        return;
    }

    if (chimera_fuse_resolve_nodeid(req) != 0) {
        chimera_fuse_reply(req, ESTALE, NULL, 0);
        return;
    }

    chimera_vfs_open_fh(req->thread->vfs_thread, &req->cred,
                        req->fh, req->fh_len,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
                        chimera_fuse_setxattr_open_callback, req);
} /* chimera_fuse_op_setxattr */

/* --- LISTXATTR --- */

static void
chimera_fuse_listxattr_complete(
    enum chimera_vfs_error error_code,
    const char            *names,
    uint32_t               names_len,
    uint32_t               count,
    uint32_t               eof,
    uint64_t               cookie,
    void                  *private_data)
{
    struct chimera_fuse_request *req = private_data;
    struct fuse_getxattr_out     out;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_fuse_reply(req, chimera_fuse_errno(error_code), NULL, 0);
        return;
    }

    if (!eof) {
        /* The full list exceeds our reply staging area. */
        chimera_fuse_reply(req, ERANGE, NULL, 0);
        return;
    }

    if (req->u.xattr.size == 0) {
        memset(&out, 0, sizeof(out));
        out.size = names_len;
        chimera_fuse_reply(req, 0, &out, sizeof(out));
        return;
    }

    if (names_len > req->u.xattr.size) {
        chimera_fuse_reply(req, ERANGE, NULL, 0);
        return;
    }

    chimera_fuse_reply(req, 0, names, names_len);
} /* chimera_fuse_listxattr_complete */

static void
chimera_fuse_listxattr_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_fuse_request *req = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_fuse_reply(req, chimera_fuse_errno(error_code), NULL, 0);
        return;
    }

    req->handle = oh;

    chimera_vfs_list_xattrs(req->thread->vfs_thread, &req->cred, oh,
                            0,
                            chimera_fuse_reply_space(req),
                            CHIMERA_FUSE_XATTR_MAX,
                            chimera_fuse_listxattr_complete, req);
} /* chimera_fuse_listxattr_open_callback */

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

    chimera_vfs_open_fh(req->thread->vfs_thread, &req->cred,
                        req->fh, req->fh_len,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
                        chimera_fuse_listxattr_open_callback, req);
} /* chimera_fuse_op_listxattr */

/* --- REMOVEXATTR --- */

static void
chimera_fuse_removexattr_complete(
    enum chimera_vfs_error          error_code,
    const struct chimera_vfs_attrs *pre_attr,
    const struct chimera_vfs_attrs *post_attr,
    void                           *private_data)
{
    struct chimera_fuse_request *req = private_data;

    chimera_fuse_reply(req, chimera_fuse_errno(error_code), NULL, 0);
} /* chimera_fuse_removexattr_complete */

static void
chimera_fuse_removexattr_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_fuse_request *req  = private_data;
    const struct fuse_in_header *hdr  = evpl_iovec_data(&req->buf);
    const char                  *name = (const char *) (hdr + 1);

    if (error_code != CHIMERA_VFS_OK) {
        chimera_fuse_reply(req, chimera_fuse_errno(error_code), NULL, 0);
        return;
    }

    req->handle = oh;

    chimera_vfs_remove_xattr(req->thread->vfs_thread, &req->cred, oh,
                             name, strlen(name),
                             chimera_fuse_removexattr_complete, req);
} /* chimera_fuse_removexattr_open_callback */

void
chimera_fuse_op_removexattr(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    if (chimera_fuse_resolve_nodeid(req) != 0) {
        chimera_fuse_reply(req, ESTALE, NULL, 0);
        return;
    }

    chimera_vfs_open_fh(req->thread->vfs_thread, &req->cred,
                        req->fh, req->fh_len,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
                        chimera_fuse_removexattr_open_callback, req);
} /* chimera_fuse_op_removexattr */
