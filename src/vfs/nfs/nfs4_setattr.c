// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"
#include "nfs4_open_state.h"

struct chimera_nfs4_setattr_ctx {
    struct chimera_nfs_thread        *thread;
    struct chimera_nfs_client_server *server;
    uint32_t                          attr_mask[2];
    uint8_t                           attr_vals[128];
    uint32_t                          pre_index, set_index, post_index;
};

static void
chimera_nfs4_setattr_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct COMPOUND4res         *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request      *request = private_data;
    struct chimera_nfs4_setattr_ctx *ctx     = request->plugin_data;
    struct nfs_resop4               *setattr_res;

    if (unlikely(status)) {
        request->status = CHIMERA_VFS_EFAULT;
        request->complete(request);
        return;
    }

    if (ctx->pre_index && res->num_resarray > ctx->pre_index &&
        res->resarray[ctx->pre_index].resop == OP_GETATTR &&
        res->resarray[ctx->pre_index].opgetattr.status == NFS4_OK) {
        chimera_nfs4_unmarshall_fattr(
            &res->resarray[ctx->pre_index].opgetattr.resok4.obj_attributes,
            &request->setattr.r_pre_attr);
    }
    if (res->status != NFS4_OK) {
        request->status = chimera_nfs4_status_to_errno(res->status);
        request->complete(request);
        return;
    }

    /* Check SEQUENCE result */
    if (res->num_resarray < 1 || res->resarray[0].opsequence.sr_status != NFS4_OK) {
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }

    /* Check PUTFH result */
    if (res->num_resarray < 2 || res->resarray[1].opputfh.status != NFS4_OK) {
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }

    /* Check SETATTR result */
    if (ctx->set_index && res->num_resarray <= ctx->set_index) {
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }
    setattr_res = &res->resarray[ctx->set_index];
    if (ctx->set_index && setattr_res->opsetattr.status != NFS4_OK) {
        request->status = chimera_nfs4_status_to_errno(setattr_res->opsetattr.status);
        request->complete(request);
        return;
    }

    if (ctx->post_index) {
        if (res->num_resarray <= ctx->post_index ||
            res->resarray[ctx->post_index].resop != OP_GETATTR ||
            res->resarray[ctx->post_index].opgetattr.status != NFS4_OK) {
            request->status = CHIMERA_VFS_EIO;
            request->complete(request);
            return;
        }
        chimera_nfs4_unmarshall_fattr(
            &res->resarray[ctx->post_index].opgetattr.resok4.obj_attributes,
            &request->setattr.r_post_attr);
    }
    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs4_setattr_callback */

void
chimera_nfs4_setattr(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_nfs_client_server_thread *server_thread = chimera_nfs_thread_get_server_thread(thread, request->fh,
                                                                                                  request->fh_len);
    struct chimera_nfs_client_server        *server;
    struct chimera_nfs4_client_session      *session;
    struct chimera_nfs4_setattr_ctx         *ctx;
    struct chimera_vfs_attrs                *set_attr;
    struct COMPOUND4args                     args;
    struct nfs_argop4                        argarray[5];
    uint32_t                                 stat_mask[2], set_index;
    struct evpl_rpc2_cred                    rpc2_cred;
    uint8_t                                 *fh;
    int                                      fhlen;
    uint8_t                                 *attr_ptr;
    int                                      attr_len;
    char                                     owner_str[32];
    char                                     group_str[32];
    int                                      owner_len;
    int                                      group_len;

    ctx = request->plugin_data;

    if (!server_thread) {
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    server  = server_thread->server;
    session = server->nfs4_session;

    if (!session) {
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    ctx->thread    = thread;
    ctx->server    = server;
    ctx->pre_index = ctx->set_index = ctx->post_index = 0;

    chimera_nfs4_map_fh(request->fh, request->fh_len, &fh, &fhlen);

    /* Build fattr4 from set_attr */
    set_attr = request->setattr.set_attr;

    memset(ctx->attr_mask, 0, sizeof(ctx->attr_mask));
    attr_ptr = ctx->attr_vals;
    attr_len = 0;

    /* Attributes must be encoded in ascending order by attribute number:
     * SIZE (4), MODE (33), OWNER (36), OWNER_GROUP (37)
     */

    /* Encode SIZE if requested - FATTR4_SIZE = 4 */
    if (set_attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE) {
        ctx->attr_mask[0]     |= (1 << FATTR4_SIZE);
        *(uint64_t *) attr_ptr = chimera_nfs_hton64(set_attr->va_size);
        attr_ptr              += sizeof(uint64_t);
        attr_len              += sizeof(uint64_t);
    }

    /* Encode MODE if requested - FATTR4_MODE = 33 */
    if (set_attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) {
        ctx->attr_mask[1]     |= (1 << (FATTR4_MODE - 32));
        *(uint32_t *) attr_ptr = chimera_nfs_hton32(set_attr->va_mode & 07777);
        attr_ptr              += sizeof(uint32_t);
        attr_len              += sizeof(uint32_t);
    }

    /* Encode OWNER (uid) if requested - FATTR4_OWNER = 36 */
    if (set_attr->va_set_mask & CHIMERA_VFS_ATTR_UID) {
        ctx->attr_mask[1] |= (1 << (FATTR4_OWNER - 32));
        /* Convert numeric uid to string */
        owner_len              = snprintf(owner_str, sizeof(owner_str), "%lu", (unsigned long) set_attr->va_uid);
        *(uint32_t *) attr_ptr = chimera_nfs_hton32(owner_len);
        attr_ptr              += sizeof(uint32_t);
        attr_len              += sizeof(uint32_t);
        memcpy(attr_ptr, owner_str, owner_len);
        attr_ptr += owner_len;
        attr_len += owner_len;
        /* Pad to 4-byte boundary */
        while (attr_len % 4) {
            *attr_ptr++ = 0;
            attr_len++;
        }
    }

    /* Encode OWNER_GROUP (gid) if requested - FATTR4_OWNER_GROUP = 37 */
    if (set_attr->va_set_mask & CHIMERA_VFS_ATTR_GID) {
        ctx->attr_mask[1] |= (1 << (FATTR4_OWNER_GROUP - 32));
        /* Convert numeric gid to string */
        group_len              = snprintf(group_str, sizeof(group_str), "%lu", (unsigned long) set_attr->va_gid);
        *(uint32_t *) attr_ptr = chimera_nfs_hton32(group_len);
        attr_ptr              += sizeof(uint32_t);
        attr_len              += sizeof(uint32_t);
        memcpy(attr_ptr, group_str, group_len);
        attr_ptr += group_len;
        attr_len += group_len;
        /* Pad to 4-byte boundary */
        while (attr_len % 4) {
            *attr_ptr++ = 0;
            attr_len++;
        }
    }

    /* Encode TIME_ACCESS_SET if requested - FATTR4_TIME_ACCESS_SET = 47.
     * The settime4 union is a time_how4 discriminator followed (only for
     * SET_TO_CLIENT_TIME4) by an nfstime4 (int64 seconds, uint32 nseconds).
     * The TIME_OMIT sentinel means "leave this timestamp alone" so we drop
     * the attribute entirely; TIME_NOW maps to SET_TO_SERVER_TIME4. */
    if ((set_attr->va_set_mask & CHIMERA_VFS_ATTR_ATIME) &&
        set_attr->va_atime.tv_nsec != CHIMERA_VFS_TIME_OMIT) {
        ctx->attr_mask[1] |= (1 << (FATTR4_TIME_ACCESS_SET - 32));

        if (set_attr->va_atime.tv_nsec == CHIMERA_VFS_TIME_NOW) {
            *(uint32_t *) attr_ptr = chimera_nfs_hton32(SET_TO_SERVER_TIME4);
            attr_ptr              += sizeof(uint32_t);
            attr_len              += sizeof(uint32_t);
        } else {
            *(uint32_t *) attr_ptr = chimera_nfs_hton32(SET_TO_CLIENT_TIME4);
            attr_ptr              += sizeof(uint32_t);
            attr_len              += sizeof(uint32_t);
            *(uint64_t *) attr_ptr = chimera_nfs_hton64(set_attr->va_atime.tv_sec);
            attr_ptr              += sizeof(uint64_t);
            attr_len              += sizeof(uint64_t);
            *(uint32_t *) attr_ptr = chimera_nfs_hton32(set_attr->va_atime.tv_nsec);
            attr_ptr              += sizeof(uint32_t);
            attr_len              += sizeof(uint32_t);
        }
    }

    /* Encode TIME_MODIFY_SET if requested - FATTR4_TIME_MODIFY_SET = 53. */
    if ((set_attr->va_set_mask & CHIMERA_VFS_ATTR_MTIME) &&
        set_attr->va_mtime.tv_nsec != CHIMERA_VFS_TIME_OMIT) {
        ctx->attr_mask[1] |= (1 << (FATTR4_TIME_MODIFY_SET - 32));

        if (set_attr->va_mtime.tv_nsec == CHIMERA_VFS_TIME_NOW) {
            *(uint32_t *) attr_ptr = chimera_nfs_hton32(SET_TO_SERVER_TIME4);
            attr_ptr              += sizeof(uint32_t);
            attr_len              += sizeof(uint32_t);
        } else {
            *(uint32_t *) attr_ptr = chimera_nfs_hton32(SET_TO_CLIENT_TIME4);
            attr_ptr              += sizeof(uint32_t);
            attr_len              += sizeof(uint32_t);
            *(uint64_t *) attr_ptr = chimera_nfs_hton64(set_attr->va_mtime.tv_sec);
            attr_ptr              += sizeof(uint64_t);
            attr_len              += sizeof(uint64_t);
            *(uint32_t *) attr_ptr = chimera_nfs_hton32(set_attr->va_mtime.tv_nsec);
            attr_ptr              += sizeof(uint32_t);
            attr_len              += sizeof(uint32_t);
        }
    }

    /* attr_ptr is advanced past the final attribute for symmetry with the
     * blocks above (so another attribute can be appended safely); only attr_len
     * is consulted below.  Reference it so the trailing advance isn't flagged as
     * a dead store. */
    (void) attr_ptr;

    /* Keep requested pre/post attributes in the same remote compound. */
    memset(&args, 0, sizeof(args));
    args.tag.len      = 0;
    args.minorversion = 1;
    args.argarray     = argarray;
    args.num_argarray = 2;
    chimera_nfs4_attr_request_stat(stat_mask);

    /* Op 0: SEQUENCE */
    argarray[0].argop = OP_SEQUENCE;

    /* Op 1: PUTFH - set current file handle */
    argarray[1].argop               = OP_PUTFH;
    argarray[1].opputfh.object.data = fh;
    argarray[1].opputfh.object.len  = fhlen;

    if (request->setattr.r_pre_attr.va_req_mask) {
        ctx->pre_index                                      = args.num_argarray++;
        argarray[ctx->pre_index].argop                      = OP_GETATTR;
        argarray[ctx->pre_index].opgetattr.attr_request     = stat_mask;
        argarray[ctx->pre_index].opgetattr.num_attr_request = 2;
    }
    set_index                 = args.num_argarray;
    argarray[set_index].argop = OP_SETATTR;

    /* The open's stateid when this handle carries one, else the anonymous
     * stateid.  A size-changing SETATTR through an open descriptor is
     * authorized by the OPEN (RFC 8881 §18.30: the stateid is consulted
     * exactly when size is set), the way ftruncate(2) is authorized by its
     * descriptor -- with the anonymous stateid the server instead re-checks
     * the file's current permissions against the caller. */
    {
        struct chimera_nfs4_open_state *open_state =
            (struct chimera_nfs4_open_state *)
            request->setattr.handle->vfs_private;

        if (open_state && (!(set_attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE) ||
                           (open_state->access & OPEN4_SHARE_ACCESS_WRITE))) {
            argarray[set_index].opsetattr.stateid = open_state->stateid;
            /* Another handle can coalesce this session's OPEN identity and
            * advance its version. Session I/O uses its current version. */
            argarray[set_index].opsetattr.stateid.seqid = 0;
        } else {
            /* OPEN UNCHECKED may truncate while requesting only read access.
             * VFS already checked write permission; authorize that mutation
             * by the credentials, not the read-only upstream OPEN. */
            memset(&argarray[set_index].opsetattr.stateid, 0,
                   sizeof(argarray[set_index].opsetattr.stateid));
        }
    }

    /* Set attribute mask - use 2 words if we have any bit in word 1, else 1 word */
    if (ctx->attr_mask[1]) {
        argarray[set_index].opsetattr.obj_attributes.num_attrmask = 2;
    } else if (ctx->attr_mask[0]) {
        argarray[set_index].opsetattr.obj_attributes.num_attrmask = 1;
    }

    argarray[set_index].opsetattr.obj_attributes.attrmask       = ctx->attr_mask;
    argarray[set_index].opsetattr.obj_attributes.attr_vals.data = ctx->attr_vals;
    argarray[set_index].opsetattr.obj_attributes.attr_vals.len  = attr_len;
    if (ctx->attr_mask[0] || ctx->attr_mask[1]) {
        ctx->set_index = args.num_argarray++;
    }
    if (request->setattr.r_post_attr.va_req_mask) {
        ctx->post_index                                      = args.num_argarray++;
        argarray[ctx->post_index].argop                      = OP_GETATTR;
        argarray[ctx->post_index].opgetattr.attr_request     = stat_mask;
        argarray[ctx->post_index].opgetattr.num_attr_request = 2;
    }

    chimera_nfs_init_rpc2_cred(&rpc2_cred, request->cred,
                               request->thread->vfs->machine_name,
                               request->thread->vfs->machine_name_len);

    chimera_nfs4_compound_call(
        thread,
        shared,
        server_thread,
        request,
        &args,
        &rpc2_cred,
        0, 0, NULL, 0, 0,
        chimera_nfs4_setattr_callback,
        request,
        chimera_nfs4_dispatch, private_data);
} /* chimera_nfs4_setattr */ /* chimera_nfs4_setattr */
