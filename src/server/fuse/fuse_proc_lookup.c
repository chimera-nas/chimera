// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>

#include "fuse_internal.h"
#include "fuse_attr.h"
#include "vfs/vfs_procs.h"

static void
chimera_fuse_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data)
{
    struct chimera_fuse_request *req = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_fuse_reply(req, chimera_fuse_errno(error_code), NULL, 0);
        return;
    }

    chimera_fuse_reply_entry(req, attr, NULL, 0);
} /* chimera_fuse_lookup_complete */

static void
chimera_fuse_lookup_open_callback(
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

    chimera_vfs_lookup_at(req->thread->vfs_thread, &req->cred, oh,
                          name, strlen(name),
                          CHIMERA_FUSE_ATTR_MASK, 0,
                          chimera_fuse_lookup_complete, req);
} /* chimera_fuse_lookup_open_callback */

void
chimera_fuse_op_lookup(
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
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH |
                        CHIMERA_VFS_OPEN_DIRECTORY,
                        chimera_fuse_lookup_open_callback, req);
} /* chimera_fuse_op_lookup */

void
chimera_fuse_op_forget(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    const struct fuse_forget_in *in = arg;

    if (arglen >= sizeof(*in) && req->nodeid != FUSE_ROOT_ID) {
        if (chimera_fuse_node_forget(req->channel->mount->node_table,
                                     req->nodeid, in->nlookup)) {
            chimera_fuse_watch_forget(req->channel->mount,
                                      req->thread->vfs_thread->vfs,
                                      req->nodeid);
            chimera_fuse_grant_forget(req->channel->mount,
                                      req->thread->vfs_thread->vfs->vfs_state,
                                      req->nodeid);
        }
    }

    /* FORGET has no reply. */
    chimera_fuse_request_free(req->thread, req);
} /* chimera_fuse_op_forget */

void
chimera_fuse_op_batch_forget(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    const struct fuse_batch_forget_in *in  = arg;
    const struct fuse_forget_one      *one = (const struct fuse_forget_one *) (in + 1);
    uint32_t                           i, count;

    if (arglen >= sizeof(*in)) {
        count = (arglen - sizeof(*in)) / sizeof(*one);

        if (count > in->count) {
            count = in->count;
        }

        for (i = 0; i < count; i++) {
            if (one[i].nodeid == FUSE_ROOT_ID) {
                continue;
            }
            if (chimera_fuse_node_forget(req->channel->mount->node_table,
                                         one[i].nodeid, one[i].nlookup)) {
                chimera_fuse_watch_forget(req->channel->mount,
                                          req->thread->vfs_thread->vfs,
                                          one[i].nodeid);
                chimera_fuse_grant_forget(req->channel->mount,
                                          req->thread->vfs_thread->vfs->vfs_state,
                                          one[i].nodeid);
            }
        }
    }

    /* BATCH_FORGET has no reply. */
    chimera_fuse_request_free(req->thread, req);
} /* chimera_fuse_op_batch_forget */
