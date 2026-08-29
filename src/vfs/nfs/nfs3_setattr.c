// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"
#include "nfs3_open_state.h"
#include "nfs_common/nfs3_attr.h"
#include "nfs_common/nfs3_status.h"

static void
chimera_nfs3_setattr_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct SETATTR3res          *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request *request = private_data;

    if (unlikely(status)) {
        request->status = CHIMERA_VFS_EFAULT;
        request->complete(request);
        return;
    }

    if (res->status != NFS3_OK) {

        chimera_nfs3_get_wcc_data(&request->setattr.r_pre_attr, &request->setattr.r_post_attr, &res->resfail.obj_wcc);

        request->status = nfs3_client_status_to_chimera_vfs_error(res->status);
        request->complete(request);
        return;
    }

    chimera_nfs3_get_wcc_data(&request->setattr.r_pre_attr, &request->setattr.r_post_attr, &res->resok.obj_wcc);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs3_setattr_callback */

void
chimera_nfs3_setattr(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_nfs_client_server_thread *server_thread = chimera_nfs_thread_get_server_thread(thread, request->fh,
                                                                                                  request->fh_len);
    struct SETATTR3args                      args;
    struct evpl_rpc2_cred                    rpc2_cred;
    uint8_t                                 *fh;
    int                                      fhlen;

    if (!server_thread) {
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    chimera_nfs3_map_fh(request->fh, request->fh_len, &fh, &fhlen);

    args.object.data.data = fh;
    args.object.data.len  = fhlen;

    chimera_nfs_va_to_sattr3(&args.new_attributes, request->setattr.set_attr);

    args.guard.check = 0;

    {
        /* A size-only setattr through an open handle is ftruncate(2): its
         * rights were bound at open, so it goes out with the opening
         * credential like the rest of the I/O family (see nfs3_open_state.h).
         * Ownership-sensitive attributes (mode/uid/gid/times) keep the
         * caller's credential -- chmod/chown authorize against the CALLER,
         * open rights are irrelevant to them. */
        struct chimera_nfs3_open_state *dac_state = request->setattr.handle ?
            (struct chimera_nfs3_open_state *)
            request->setattr.handle->vfs_private : NULL;
        const struct chimera_vfs_cred  *io_cred = request->cred;

        if (dac_state && dac_state->open_cred_valid &&
            (request->setattr.set_attr->va_set_mask &
             CHIMERA_VFS_ATTR_SIZE) &&
            !(request->setattr.set_attr->va_set_mask &
              (CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_UID |
               CHIMERA_VFS_ATTR_GID))) {
            io_cred = &dac_state->open_cred;
        }

        chimera_nfs_init_rpc2_cred(&rpc2_cred, io_cred,
                                   request->thread->vfs->machine_name,
                                   request->thread->vfs->machine_name_len);
    }

    shared->nfs_v3.send_call_NFSPROC3_SETATTR(&shared->nfs_v3.rpc2, thread->evpl, server_thread->nfs_conn, &rpc2_cred,
                                              &args, 0, 0, NULL, 0, 0, chimera_nfs3_setattr_callback, request);
} /* chimera_nfs3_setattr */

