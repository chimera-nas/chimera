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
    struct evpl_rpc2_msg           *msg           = req->msg;
    struct nfs4_session            *session       = req->session;
    struct OPEN4res                *res           = &req->res_compound.resarray[req->index].opopen;
    struct chimera_vfs_open_handle *parent_handle = req->handle;
    struct nfs4_state              *state;

    if (error_code != NFS4_OK) {
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->status);
    } else {

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

        xdr_dbuf_alloc_space(res->resok4.attrset, sizeof(uint32_t) * 4, msg->dbuf);
        res->resok4.num_attrset = chimera_nfs4_mask2attr(set_attr, res->resok4.attrset, 4);

        memcpy(req->fh, handle->fh, handle->fh_len);
        req->fhlen = handle->fh_len;

        chimera_nfs4_set_changeinfo(&res->resok4.cinfo, dir_pre_attr, dir_post_attr);
    }

    chimera_vfs_release(req->thread->vfs_thread, parent_handle);

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_open_at_complete */

static void
chimera_nfs4_open_parent_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *parent_handle,
    void                           *private_data)
{
    struct nfs_request       *req   = private_data;
    struct evpl_rpc2_msg     *msg   = req->msg;
    struct OPEN4args         *args  = &req->args_compound->argarray[req->index].opopen;
    unsigned int              flags = 0;
    struct chimera_vfs_attrs *attr;

    req->handle = parent_handle;

    if (error_code != NFS4_OK) {
        struct OPEN4res *res = &req->res_compound.resarray[req->index].opopen;
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    xdr_dbuf_alloc_space(attr, sizeof(*attr), msg->dbuf);

    attr->va_req_mask = 0;

    if (args->openhow.opentype == OPEN4_CREATE) {
        flags |= CHIMERA_VFS_OPEN_CREATE;

        switch (args->openhow.how.mode) {
            case UNCHECKED4:
            case GUARDED4:
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

    switch (args->claim.claim) {
        case CLAIM_NULL:
            chimera_vfs_open_at(req->thread->vfs_thread,
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
    chimera_vfs_open(thread->vfs_thread,
                     req->fh,
                     req->fhlen,
                     CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY,
                     chimera_nfs4_open_parent_complete,
                     req);
} /* chimera_nfs4_open */
