// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"
#include "nfs_common/nfs3_attr.h"
#include "nfs_common/nfs3_status.h"

struct chimera_nfs3_readdir_ctx {
    struct chimera_nfs_client_server *server;
};

static void
chimera_nfs3_readdir_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct READDIRPLUS3res      *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request      *request = private_data;
    struct entryplus3               *entry;
    struct chimera_nfs3_readdir_ctx *ctx;
    struct chimera_vfs_attrs         attrs;
    int                              rc, eof = 0;

    if (unlikely(status)) {
        request->status = CHIMERA_VFS_EFAULT;
        request->complete(request);
        return;
    }

    if (res->status != NFS3_OK) {

        if (res->resfail.dir_attributes.attributes_follow) {
            chimera_nfs3_unmarshall_attrs(&res->resfail.dir_attributes.attributes, &request->readdir.r_dir_attr);
        }

        request->status = nfs3_client_status_to_chimera_vfs_error(res->status);
        request->complete(request);
        return;
    }

    if (res->resok.dir_attributes.attributes_follow) {
        chimera_nfs3_unmarshall_attrs(&res->resok.dir_attributes.attributes, &request->readdir.r_dir_attr);
    }

    memcpy(&request->readdir.r_verifier, res->resok.cookieverf, sizeof(request->readdir.r_verifier));

    ctx = request->plugin_data;

    entry = res->resok.reply.entries;

    eof = res->resok.reply.eof;

    while (entry) {

        attrs.va_set_mask = 0;

        if (entry->name_handle.handle_follows) {
            chimera_nfs3_unmarshall_fh(&entry->name_handle.handle, ctx->server->index, request->fh, &attrs);
        }

        if (entry->name_attributes.attributes_follow) {
            chimera_nfs3_unmarshall_attrs(&entry->name_attributes.attributes, &attrs);
        }

        rc = request->readdir.callback(entry->fileid,
                                       entry->cookie,
                                       entry->name.str,
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
} /* chimera_nfs3_readdir_callback */

void
chimera_nfs3_readdir(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_nfs_client_server_thread *server_thread = chimera_nfs_thread_get_server_thread(thread, request->fh,
                                                                                                  request->fh_len);
    struct chimera_nfs3_readdir_ctx         *ctx;
    struct READDIRPLUS3args                  args;
    struct evpl_rpc2_cred                    rpc2_cred;
    uint8_t                                 *fh;
    int                                      fhlen;

    if (!server_thread) {
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    chimera_nfs3_map_fh(request->fh, request->fh_len, &fh, &fhlen);

    args.dir.data.data = fh;
    args.dir.data.len  = fhlen;
    args.cookie        = request->readdir.cookie;
    args.dircount      = 1024;
    args.maxcount      = 1024;

    memcpy(args.cookieverf, &request->readdir.verifier, sizeof(args.cookieverf));

    ctx         = request->plugin_data;
    ctx->server = server_thread->server;

    chimera_nfs_init_rpc2_cred(&rpc2_cred, request->cred,
                               request->thread->vfs->machine_name,
                               request->thread->vfs->machine_name_len);

    shared->nfs_v3.send_call_NFSPROC3_READDIRPLUS(&shared->nfs_v3.rpc2, thread->evpl, server_thread->nfs_conn, &
                                                  rpc2_cred,
                                                  &args, 0, 0, 0, chimera_nfs3_readdir_callback, request);
} /* chimera_nfs3_readdir */

