// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_attr.h"
#include "server/server.h"
#include "vfs/vfs_compound.h"
#include "nfs4_status.h"

/*
 * PUTFH(source) SAVEFH PUTFH(target dir) OPEN_CURRENT(dir) LINK.
 *
 * link_at takes both objects as FILE HANDLES -- the saved slot for the object
 * being linked, the current one for the directory the new name goes in -- so
 * the two PUTFHs and the SAVEFH only put the cursors where the op reads them.
 * The open is the target directory's openability check the per-op path made
 * before the link.
 */
#define NFS4_LINK_OP_LINK 4

static void
chimera_nfs4_link_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs_request                   *req = private_data;
    struct LINK4res                      *res = &req->res_compound.resarray[req->index].oplink;
    const struct chimera_vfs_compound_op *op;
    enum chimera_vfs_error                error_code;
    nfsstat4                              status;

    error_code = chimera_vfs_compound_status(compound);

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_compound_free(compound);
        status      = chimera_nfs4_errno_to_nfsstat4(error_code);
        res->status = status;
        chimera_nfs4_compound_complete(req, status);
        return;
    }

    op = chimera_vfs_compound_op(compound, NFS4_LINK_OP_LINK);

    struct chimera_vfs_attrs pre  = op->dir_pre_attr;
    struct chimera_vfs_attrs post = op->dir_post_attr;

    chimera_vfs_compound_free(compound);

    res->status = NFS4_OK;

    chimera_nfs4_set_changeinfo(&res->resok4.cinfo, &pre, &post);

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_link_complete */

void
chimera_nfs4_link(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct LINK4args            *args = &argop->oplink;
    struct LINK4res             *res  = &resop->oplink;
    struct chimera_vfs_compound *compound;

    if (req->fhlen == 0 || req->saved_fhlen == 0) {
        res->status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    res->status = chimera_nfs4_validate_name(&args->newname);

    if (res->status != NFS4_OK) {
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    req->handle = NULL;

    /* RFC 8881 §18.9.4 (hard link to a delegated file must recall the
     * delegation) is enforced centrally by the VFS link, which recalls any
     * caching lease on the SAVEFH source before linking. */
    compound = chimera_vfs_compound_alloc(thread->vfs_thread, &req->cred);

    chimera_vfs_compound_add_putfh(compound, req->saved_fh, req->saved_fhlen);
    chimera_vfs_compound_add_savefh(compound);
    chimera_vfs_compound_add_putfh(compound, req->fh, req->fhlen);
    chimera_vfs_compound_add_open_current(compound,
                                          CHIMERA_VFS_OPEN_INFERRED |
                                          CHIMERA_VFS_OPEN_PATH |
                                          CHIMERA_VFS_OPEN_DIRECTORY, 0);
    chimera_vfs_compound_add_link(compound,
                                  (const char *) args->newname.data,
                                  (int) args->newname.len,
                                  0,
                                  CHIMERA_VFS_ATTR_CHANGE |
                                  CHIMERA_VFS_ATTR_CTIME,
                                  CHIMERA_VFS_ATTR_CHANGE |
                                  CHIMERA_VFS_ATTR_CTIME);

    chimera_vfs_compound_submit(compound, chimera_nfs4_link_complete, req);
} /* chimera_nfs4_link */
