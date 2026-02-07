// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"

struct chimera_nfs4_readdir_ctx {
    struct chimera_nfs_thread        *thread;
    struct chimera_nfs_client_server *server;
    uint32_t                          attr_request[2];
};

/*
 * Parse readdir entry attributes from fattr4
 * This is a specialized version for readdir that handles FATTR4_FILEHANDLE
 */
static void
chimera_nfs4_readdir_parse_attrs(
    const struct fattr4      *fattr,
    struct chimera_vfs_attrs *attr,
    uint64_t                 *fileid,
    uint8_t                  *fh_data,
    int                      *fh_len)
{
    void    *data    = fattr->attr_vals.data;
    void    *dataend = data + fattr->attr_vals.len;
    uint32_t type;
    uint32_t len;

    *fileid           = 0;
    *fh_len           = 0;
    attr->va_set_mask = 0;

    if (fattr->num_attrmask < 1) {
        return;
    }

    /* Parse attributes in bitmap order */

    /* FATTR4_TYPE = 1 */
    if (fattr->attrmask[0] & (1 << FATTR4_TYPE)) {
        if (data + sizeof(uint32_t) > dataend) {
            return;
        }
        type               = chimera_nfs_ntoh32(*(uint32_t *) data);
        data              += sizeof(uint32_t);
        attr->va_set_mask |= CHIMERA_VFS_ATTR_MODE;
        switch (type) {
            case NF4REG:
                attr->va_mode = S_IFREG;
                break;
            case NF4DIR:
                attr->va_mode = S_IFDIR;
                break;
            case NF4BLK:
                attr->va_mode = S_IFBLK;
                break;
            case NF4CHR:
                attr->va_mode = S_IFCHR;
                break;
            case NF4LNK:
                attr->va_mode = S_IFLNK;
                break;
            case NF4SOCK:
                attr->va_mode = S_IFSOCK;
                break;
            case NF4FIFO:
                attr->va_mode = S_IFIFO;
                break;
            default:
                attr->va_mode = S_IFREG;
                break;
        } /* switch */
    }

    /* FATTR4_SIZE = 4 */
    if (fattr->attrmask[0] & (1 << FATTR4_SIZE)) {
        if (data + sizeof(uint64_t) > dataend) {
            return;
        }
        attr->va_size      = chimera_nfs_ntoh64(*(uint64_t *) data);
        data              += sizeof(uint64_t);
        attr->va_set_mask |= CHIMERA_VFS_ATTR_SIZE;
    }

    /* FATTR4_FILEHANDLE = 19 - opaque<NFS4_FHSIZE> */
    if (fattr->attrmask[0] & (1 << FATTR4_FILEHANDLE)) {
        if (data + sizeof(uint32_t) > dataend) {
            return;
        }
        len   = chimera_nfs_ntoh32(*(uint32_t *) data);
        data += sizeof(uint32_t);

        if (data + len > dataend || len > NFS4_FHSIZE) {
            return;
        }

        memcpy(fh_data, data, len);
        *fh_len = len;
        data   += len;

        /* XDR padding to 4-byte boundary */
        if (len % 4) {
            data += 4 - (len % 4);
        }
    }

    /* FATTR4_FILEID = 20 */
    if (fattr->attrmask[0] & (1 << FATTR4_FILEID)) {
        if (data + sizeof(uint64_t) > dataend) {
            return;
        }
        *fileid            = chimera_nfs_ntoh64(*(uint64_t *) data);
        attr->va_ino       = *fileid;
        data              += sizeof(uint64_t);
        attr->va_set_mask |= CHIMERA_VFS_ATTR_INUM;
    }

    if (fattr->num_attrmask < 2) {
        return;
    }

    /* FATTR4_MODE = 33 */
    if (fattr->attrmask[1] & (1 << (FATTR4_MODE - 32))) {
        if (data + sizeof(uint32_t) > dataend) {
            return;
        }
        attr->va_mode     |= chimera_nfs_ntoh32(*(uint32_t *) data) & ~S_IFMT;
        data              += sizeof(uint32_t);
        attr->va_set_mask |= CHIMERA_VFS_ATTR_MODE;
    }

    /* FATTR4_NUMLINKS = 35 */
    if (fattr->attrmask[1] & (1 << (FATTR4_NUMLINKS - 32))) {
        if (data + sizeof(uint32_t) > dataend) {
            return;
        }
        attr->va_nlink     = chimera_nfs_ntoh32(*(uint32_t *) data);
        data              += sizeof(uint32_t);
        attr->va_set_mask |= CHIMERA_VFS_ATTR_NLINK;
    }
} /* chimera_nfs4_readdir_parse_attrs */

static void
chimera_nfs4_readdir_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct COMPOUND4res         *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request      *request = private_data;
    struct chimera_nfs4_readdir_ctx *ctx;
    struct nfs_resop4               *readdir_res;
    struct entry4                   *entry;
    struct chimera_vfs_attrs         attrs;
    uint64_t                         fileid;
    uint8_t                          remote_fh[NFS4_FHSIZE];
    int                              remote_fh_len;
    uint8_t                          fh_fragment[NFS4_FHSIZE + 1];
    int                              fh_fragment_len;
    int                              rc, eof = 0;

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

    /* Check SEQUENCE result (index 0) */
    if (res->num_resarray < 1 || res->resarray[0].opsequence.sr_status != NFS4_OK) {
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }

    /* Check PUTFH result (index 1) */
    if (res->num_resarray < 2 || res->resarray[1].opputfh.status != NFS4_OK) {
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }

    /* Check READDIR result (index 2) */
    if (res->num_resarray < 3) {
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }
    readdir_res = &res->resarray[2];
    if (readdir_res->opreaddir.status != NFS4_OK) {
        request->status = chimera_nfs4_status_to_errno(readdir_res->opreaddir.status);
        request->complete(request);
        return;
    }

    ctx = request->plugin_data;

    /* Copy cookie verifier */
    memcpy(&request->readdir.r_verifier, readdir_res->opreaddir.resok4.cookieverf,
           sizeof(request->readdir.r_verifier));

    entry = readdir_res->opreaddir.resok4.reply.entries;
    eof   = readdir_res->opreaddir.resok4.reply.eof;

    while (entry) {
        attrs.va_set_mask = 0;

        /* Parse attributes including file handle */
        chimera_nfs4_readdir_parse_attrs(&entry->attrs, &attrs, &fileid, remote_fh, &remote_fh_len);

        /* Build full file handle: [server_index][remote_fh] */
        if (remote_fh_len > 0) {
            fh_fragment[0] = ctx->server->index;
            memcpy(fh_fragment + 1, remote_fh, remote_fh_len);
            fh_fragment_len = 1 + remote_fh_len;

            attrs.va_set_mask |= CHIMERA_VFS_ATTR_FH;
            attrs.va_fh_len    = chimera_vfs_encode_fh_parent(request->fh, fh_fragment, fh_fragment_len, attrs.va_fh);
        }

        rc = request->readdir.callback(fileid,
                                       entry->cookie,
                                       (const char *) entry->name.data,
                                       entry->name.len,
                                       &attrs,
                                       request->proto_private_data);

        request->readdir.r_cookie = entry->cookie;

        if (rc) {
            eof = 0;
            break;
        }

        entry = entry->nextentry;
    }

    request->readdir.r_eof = eof;

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs4_readdir_callback */

void
chimera_nfs4_readdir(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_nfs_client_server_thread *server_thread = chimera_nfs_thread_get_server_thread(thread, request->fh,
                                                                                                  request->fh_len);
    struct chimera_nfs_client_server        *server;
    struct chimera_nfs4_client_session      *session;
    struct chimera_nfs4_readdir_ctx         *ctx;
    struct COMPOUND4args                     args;
    struct nfs_argop4                        argarray[3];
    struct evpl_rpc2_cred                    rpc2_cred;
    uint8_t                                 *fh;
    int                                      fhlen;

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

    ctx         = request->plugin_data;
    ctx->thread = thread;
    ctx->server = server;

    chimera_nfs4_map_fh(request->fh, request->fh_len, &fh, &fhlen);

    /* Build compound: SEQUENCE + PUTFH + READDIR */
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

    /* Op 1: PUTFH - set current file handle to directory */
    argarray[1].argop               = OP_PUTFH;
    argarray[1].opputfh.object.data = fh;
    argarray[1].opputfh.object.len  = fhlen;

    /* Op 2: READDIR */
    argarray[2].argop              = OP_READDIR;
    argarray[2].opreaddir.cookie   = request->readdir.cookie;
    argarray[2].opreaddir.dircount = 8192;
    argarray[2].opreaddir.maxcount = 8192;

    /* Copy cookie verifier */
    memcpy(argarray[2].opreaddir.cookieverf, &request->readdir.verifier, sizeof(argarray[2].opreaddir.cookieverf));

    /* Request attributes: TYPE, SIZE, FILEHANDLE, FILEID, MODE, NUMLINKS */
    ctx->attr_request[0] = (1 << FATTR4_TYPE) | (1 << FATTR4_SIZE) |
        (1 << FATTR4_FILEHANDLE) | (1 << FATTR4_FILEID);
    ctx->attr_request[1] = (1 << (FATTR4_MODE - 32)) | (1 << (FATTR4_NUMLINKS - 32));

    argarray[2].opreaddir.attr_request     = ctx->attr_request;
    argarray[2].opreaddir.num_attr_request = 2;

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
        chimera_nfs4_readdir_callback,
        request);
} /* chimera_nfs4_readdir */
