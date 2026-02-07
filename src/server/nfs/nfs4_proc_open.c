// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_attr.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"

static void
chimera_nfs4_open_at_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    struct chimera_vfs_attrs       *dir_pre_attr,
    struct chimera_vfs_attrs       *dir_post_attr,
    void                           *private_data)
{
    struct nfs_request             *req           = private_data;
    struct nfs4_session            *session       = req->session;
    struct OPEN4args               *args          = &req->args_compound->argarray[req->index].opopen;
    struct OPEN4res                *res           = &req->res_compound.resarray[req->index].opopen;
    struct chimera_vfs_open_handle *parent_handle = req->handle;
    struct nfs4_state              *state;
    int                             rc;

    if (error_code != CHIMERA_VFS_OK) {
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_vfs_release(req->thread->vfs_thread, parent_handle);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    state                    = nfs4_session_alloc_slot(session);
    state->nfs4_state_handle = handle;

    res->status                            = NFS4_OK;
    res->resok4.stateid                    = state->nfs4_state_id;
    res->resok4.cinfo.atomic               = 0;
    res->resok4.cinfo.before               = 0;
    res->resok4.cinfo.after                = 0;
    res->resok4.rflags                     = 0;
    res->resok4.num_attrset                = 0;
    res->resok4.delegation.delegation_type = OPEN_DELEGATE_NONE;

    if (args->openhow.opentype == OPEN4_CREATE &&
        (args->openhow.how.mode == UNCHECKED4 || args->openhow.how.mode == GUARDED4)) {
        rc = xdr_dbuf_alloc_array(&res->resok4, attrset, 4, req->encoding->dbuf);
        chimera_nfs_abort_if(rc, "Failed to allocate array");
        res->resok4.num_attrset = chimera_nfs4_mask2attr(set_attr,
                                                         args->openhow.how.createattrs.num_attrmask,
                                                         args->openhow.how.createattrs.attrmask,
                                                         res->resok4.attrset);
    } else {
        res->resok4.num_attrset = 0;
    }

    memcpy(req->fh, handle->fh, handle->fh_len);
    req->fhlen = handle->fh_len;

    chimera_nfs4_set_changeinfo(&res->resok4.cinfo, dir_pre_attr, dir_post_attr);

    chimera_vfs_release(req->thread->vfs_thread, parent_handle);

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_open_at_complete */

static void
chimera_nfs4_open_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request             *req           = private_data;
    struct nfs4_session            *session       = req->session;
    struct OPEN4res                *res           = &req->res_compound.resarray[req->index].opopen;
    struct chimera_vfs_open_handle *parent_handle = req->handle;
    struct nfs4_state              *state;

    if (error_code != CHIMERA_VFS_OK) {
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_vfs_release(req->thread->vfs_thread, parent_handle);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    state                    = nfs4_session_alloc_slot(session);
    state->nfs4_state_handle = handle;

    res->status                            = NFS4_OK;
    res->resok4.stateid                    = state->nfs4_state_id;
    res->resok4.cinfo.atomic               = 0;
    res->resok4.cinfo.before               = 0;
    res->resok4.cinfo.after                = 0;
    res->resok4.rflags                     = 0;
    res->resok4.num_attrset                = 0;
    res->resok4.delegation.delegation_type = OPEN_DELEGATE_NONE;

    /* XXX Not sure what to do here since there is no directory */
    res->resok4.cinfo.atomic = 0;
    res->resok4.cinfo.before = 0;
    res->resok4.cinfo.after  = 0;

    chimera_vfs_release(req->thread->vfs_thread, parent_handle);

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_open_complete */

static void
chimera_nfs4_open_parent_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *parent_handle,
    void                           *private_data)
{
    struct nfs_request       *req   = private_data;
    struct OPEN4args         *args  = &req->args_compound->argarray[req->index].opopen;
    unsigned int              flags = 0;
    struct chimera_vfs_attrs *attr;

    req->handle = parent_handle;

    if (error_code != CHIMERA_VFS_OK) {
        struct OPEN4res *res = &req->res_compound.resarray[req->index].opopen;
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    attr = xdr_dbuf_alloc_space(sizeof(*attr), req->encoding->dbuf);
    chimera_nfs_abort_if(attr == NULL, "Failed to allocate space");

    attr->va_req_mask = 0;
    attr->va_set_mask = 0;

    if (args->openhow.opentype == OPEN4_CREATE) {
        flags |= CHIMERA_VFS_OPEN_CREATE;

        switch (args->openhow.how.mode) {
            case GUARDED4:
                /* GUARDED4 = create only if file doesn't exist (like O_EXCL) */
                flags |= CHIMERA_VFS_OPEN_EXCLUSIVE;
            /* fallthrough */
            case UNCHECKED4:
                chimera_nfs4_unmarshall_attrs(attr,
                                              args->openhow.how.createattrs.num_attrmask,
                                              args->openhow.how.createattrs.attrmask,
                                              args->openhow.how.createattrs.attr_vals.data,
                                              args->openhow.how.createattrs.attr_vals.len);
                break;
            case EXCLUSIVE4:
            case EXCLUSIVE4_1:
                break;
        } /* switch */
    }

    if (args->share_access == OPEN4_SHARE_ACCESS_READ) {
        flags |= CHIMERA_VFS_OPEN_READ_ONLY;
    }

    switch (args->claim.claim) {
        case CLAIM_NULL:
            chimera_vfs_open_at(req->thread->vfs_thread, &req->cred,
                                parent_handle,
                                args->claim.file.data,
                                args->claim.file.len,
                                flags,
                                attr,
                                CHIMERA_VFS_ATTR_FH,
                                CHIMERA_VFS_ATTR_MTIME,
                                CHIMERA_VFS_ATTR_MTIME,
                                chimera_nfs4_open_at_complete,
                                req);
            break;
        case CLAIM_PREVIOUS:
        case CLAIM_FH:
            chimera_vfs_open_fh(req->thread->vfs_thread, &req->cred,
                                req->fh,
                                req->fhlen,
                                flags,
                                chimera_nfs4_open_complete,
                                req);
            break;
        default:
            abort();
    } /* switch */

} /* chimera_nfs4_open_complete */

void
chimera_nfs4_open(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct OPEN4args *args = &argop->opopen;

    if (!req->session) {
        req->session = nfs4_session_find_by_clientid(
            &thread->shared->nfs4_shared_clients,
            args->owner.clientid);

        if (req->session) {
            evpl_rpc2_conn_set_private_data(req->conn, req->session);
        }
    }

    chimera_vfs_open_fh(thread->vfs_thread, &req->cred,
                        req->fh,
                        req->fhlen,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY,
                        chimera_nfs4_open_parent_complete,
                        req);
} /* chimera_nfs4_open */
