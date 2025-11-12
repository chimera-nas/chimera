// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"

static void
chimera_nfs3_read_callback(
    struct evpl     *evpl,
    struct READ3res *res,
    int              status,
    void            *private_data)
{
    struct chimera_vfs_request *request = private_data;

    if (unlikely(status)) {
        request->status = CHIMERA_VFS_EFAULT;
        request->complete(request);
        return;
    }

    if (res->status != NFS3_OK) {

        if (res->resfail.file_attributes.attributes_follow) {
            chimera_nfs3_unmarshall_attrs(&res->resfail.file_attributes.attributes, &request->read.r_attr);
        }

        request->status = nfs3_client_status_to_chimera_vfs_error(res->status);
        request->complete(request);
        return;
    }

    if (res->resok.file_attributes.attributes_follow) {
        chimera_nfs3_unmarshall_attrs(&res->resok.file_attributes.attributes, &request->read.r_attr);
    }

    request->read.r_length = res->resok.count;
    request->read.r_eof    = res->resok.eof;
    request->status        = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs3_read_callback */

void
chimera_nfs3_read(
    struct chimera_nfs_thread          *thread,
    struct chimera_nfs_shared          *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_nfs_client_server_thread *server_thread = chimera_nfs_thread_get_server_thread(thread, request->fh, request->fh_len);
    struct READ3args                 args;
    uint8_t                         *fh;
    int                              fhlen;

    if (!server_thread) {
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    chimera_nfs3_map_fh(request->fh, request->fh_len, &fh, &fhlen);

    args.file.data.data = fh;
    args.file.data.len  = fhlen;
    args.offset         = request->read.offset;
    args.count          = request->read.length;

    shared->nfs_v3.send_call_NFSPROC3_READ(&shared->nfs_v3.rpc2, thread->evpl, server_thread->nfs_conn, &args,
                                           chimera_nfs3_read_callback, request);
} /* chimera_nfs3_read */

