// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"

static void
nfs3_lookup_callback(
    struct evpl       *evpl,
    struct LOOKUP3res *res,
    int                status,
    void              *private_data)
{
    struct chimera_vfs_request *request = private_data;

    if (status != NFS3_OK) {
        request->status = nfs3_client_status_to_chimera_vfs_error(status);
        request->complete(request);
        return;
    }

    if (res->resok.obj_attributes.attributes_follow) {
        chimera_nfs3_unmarshall_attrs(&res->resok.obj_attributes.attributes, &request->lookup.r_attr);
    }

    if (res->resok.dir_attributes.attributes_follow) {
        chimera_nfs3_unmarshall_attrs(&res->resok.dir_attributes.attributes, &request->lookup.r_dir_attr);
    }

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* nfs3_lookup_callback */

void
nfs3_lookup(
    struct nfs_thread          *thread,
    struct nfs_shared          *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct nfs_client_server_thread *server_thread = nfs_thread_get_server_thread(thread, request->fh, request->fh_len);
    struct LOOKUP3args               args;
    uint8_t                         *fh;
    int                              fhlen;

    if (!server_thread) {
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    nfs3_map_fh(request->fh, request->fh_len, &fh, &fhlen);

    args.what.dir.data.data = fh;
    args.what.dir.data.len  = fhlen;
    args.what.name.str      = (char *) request->lookup.component;
    args.what.name.len      = request->lookup.component_len;

    shared->nfs_v3.send_call_NFSPROC3_LOOKUP(&shared->nfs_v3.rpc2, thread->evpl, server_thread->nfs_conn, &args,
                                             nfs3_lookup_callback, request);
} /* nfs3_lookup */

