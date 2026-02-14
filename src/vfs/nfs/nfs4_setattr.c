// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"

struct chimera_nfs4_setattr_ctx {
    struct chimera_nfs_thread        *thread;
    struct chimera_nfs_client_server *server;
    uint32_t                          attr_mask[2];
    uint8_t                           attr_vals[128];
};

static void
chimera_nfs4_setattr_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct COMPOUND4res         *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request *request = private_data;
    struct nfs_resop4          *setattr_res;

    if (unlikely(status)) {
        request->status = CHIMERA_VFS_EFAULT;
        request->complete(request);
        return;
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
    if (res->num_resarray < 3) {
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }
    setattr_res = &res->resarray[2];
    if (setattr_res->opsetattr.status != NFS4_OK) {
        request->status = chimera_nfs4_status_to_errno(setattr_res->opsetattr.status);
        request->complete(request);
        return;
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
    struct nfs_argop4                        argarray[3];
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

    ctx->thread = thread;
    ctx->server = server;

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

    /* Build compound: SEQUENCE + PUTFH + SETATTR */
    memset(&args, 0, sizeof(args));
    args.tag.len      = 0;
    args.minorversion = 1;
    args.argarray     = argarray;
    args.num_argarray = 3;

    /* Op 0: SEQUENCE */
    argarray[0].argop = OP_SEQUENCE;
    memcpy(argarray[0].opsequence.sa_sessionid, session->sessionid, NFS4_SESSIONID_SIZE);
    argarray[0].opsequence.sa_sequenceid     = chimera_nfs4_get_sequenceid(session, server_thread->slot_id);
    argarray[0].opsequence.sa_slotid         = server_thread->slot_id;
    argarray[0].opsequence.sa_highest_slotid = session->max_slots - 1;
    argarray[0].opsequence.sa_cachethis      = 0;

    /* Op 1: PUTFH - set current file handle */
    argarray[1].argop               = OP_PUTFH;
    argarray[1].opputfh.object.data = fh;
    argarray[1].opputfh.object.len  = fhlen;

    /* Op 2: SETATTR */
    argarray[2].argop = OP_SETATTR;

    /* Anonymous stateid (all zeros) for setattr on unopened files */
    memset(&argarray[2].opsetattr.stateid, 0, sizeof(argarray[2].opsetattr.stateid));

    /* Set attribute mask - use 2 words if we have any bit in word 1, else 1 word */
    if (ctx->attr_mask[1]) {
        argarray[2].opsetattr.obj_attributes.num_attrmask = 2;
    } else if (ctx->attr_mask[0]) {
        argarray[2].opsetattr.obj_attributes.num_attrmask = 1;
    } else {
        /* No attributes to set - just return OK */
        request->status = CHIMERA_VFS_OK;
        request->complete(request);
        return;
    }

    argarray[2].opsetattr.obj_attributes.attrmask       = ctx->attr_mask;
    argarray[2].opsetattr.obj_attributes.attr_vals.data = ctx->attr_vals;
    argarray[2].opsetattr.obj_attributes.attr_vals.len  = attr_len;

    chimera_nfs_init_rpc2_cred(&rpc2_cred, request->cred,
                               request->thread->vfs->machine_name,
                               request->thread->vfs->machine_name_len);

    shared->nfs_v4.send_call_NFSPROC4_COMPOUND(
        &shared->nfs_v4.rpc2,
        thread->evpl,
        server_thread->nfs_conn,
        &rpc2_cred,
        &args,
        0, 0, 0,
        chimera_nfs4_setattr_callback,
        request);
} /* chimera_nfs4_setattr */ /* chimera_nfs4_setattr */
