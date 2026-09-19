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
    void                     *private_data)
{
    struct chimera_fuse_request *req   = private_data;
    struct chimera_fuse_mount   *mount = req->channel->mount;

    if (error_code == CHIMERA_VFS_ENOENT &&
        mount->negative_timeout_ms > 0 &&
        (!mount->coherence_sync ||
         req->entry_cover == CHIMERA_FUSE_COVER_HELD)) {
        /* Cache the miss: nodeid 0 with a validity makes the kernel hold a
         * NEGATIVE dentry, saving a round trip per repeated miss.  Safe in
         * coherence=sync because a foreign create of this name gates on the
         * parent watch (held since before this request), and its
         * INVAL_ENTRY serializes behind us on the parent's kernel lock.
         * In ttl mode this is the opt-in negative_timeout_ms bound. */
        struct fuse_entry_out entry;

        memset(&entry, 0, sizeof(entry));
        entry.entry_valid      = mount->negative_timeout_ms / 1000;
        entry.entry_valid_nsec = (mount->negative_timeout_ms % 1000) * 1000000;

        chimera_fuse_reply(req, 0, &entry, sizeof(entry));
        return;
    }

    if (error_code != CHIMERA_VFS_OK) {
        chimera_fuse_reply(req, chimera_fuse_errno(error_code), NULL, 0);
        return;
    }

    chimera_fuse_reply_entry(req, attr, NULL, 0);
} /* chimera_fuse_lookup_complete */

static void
chimera_fuse_lookup_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_fuse_request          *req = private_data;
    const struct chimera_vfs_compound_op *op;

    op = chimera_vfs_compound_op(compound,
                                 chimera_vfs_compound_num_ops(compound) - 1);

    chimera_fuse_lookup_complete(chimera_vfs_compound_status(compound),
                                 (struct chimera_vfs_attrs *) &op->attr, req);
} /* chimera_fuse_lookup_sequence_complete */

void
chimera_fuse_op_lookup(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    const char *name = (const char *) (hdr + 1);

    if (chimera_fuse_resolve_nodeid(req) != 0) {
        chimera_fuse_reply(req, ESTALE, NULL, 0);
        return;
    }

    /* Watch the parent BEFORE the backend resolve: a dentry (positive or
     * negative) may only carry a TTL when the watch predates the request,
     * so a racing foreign mutation is guaranteed to gate on it. */
    req->entry_cover = chimera_fuse_watch_dir(req->thread, req->channel->mount,
                                              req->nodeid,
                                              req->fh, req->fh_len);

    req->compound = chimera_vfs_compound_alloc(req->thread->vfs_thread,
                                               &req->cred);

    chimera_vfs_compound_add_putfh(req->compound, req->fh, (int) req->fh_len);
    /* The open the sequence used to do for this op, said out loud. */
    chimera_vfs_compound_add_open_current(req->compound,
                                          CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH |
                                          CHIMERA_VFS_OPEN_DIRECTORY,
                                          0);

    /* The entry reply describes the object alone; the directory's attributes
     * have no reader here. */
    chimera_vfs_compound_add_lookup(req->compound, name, (int) strlen(name),
                                    CHIMERA_FUSE_ATTR_MASK, 0);

    chimera_vfs_compound_submit(req->compound,
                                chimera_fuse_lookup_sequence_complete, req);
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
