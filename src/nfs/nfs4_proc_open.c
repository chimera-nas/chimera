#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "vfs/vfs_procs.h"
static void
chimera_nfs4_open_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request  *req     = private_data;
    struct nfs4_session *session = req->session;
    struct OPEN4res     *res     = &req->res_compound.resarray[req->index].
        opopen;
    struct nfs4_state   *state;

    state = nfs4_session_alloc_slot(session);

    state->nfs4_state_handle = *handle;

    chimera_nfs_debug("open complete: seqid %u private %lu handle %p",
                      state->nfs4_state_id.seqid,
                      handle->vfs_private);

    res->status                            = NFS4_OK;
    res->resok4.stateid                    = state->nfs4_state_id;
    res->resok4.cinfo.atomic               = 0;
    res->resok4.cinfo.before               = 0;
    res->resok4.cinfo.after                = 0;
    res->resok4.rflags                     = 0;
    res->resok4.num_attrset                = 0;
    res->resok4.delegation.delegation_type = OPEN_DELEGATE_NONE;

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_open_complete */

void
chimera_nfs4_open(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    unsigned int      flags = 0;

    struct OPEN4args *args = &argop->opopen;

    if (args->openhow.opentype == OPEN4_CREATE) {
        flags |= CHIMERA_VFS_OPEN_CREATE;
    }

    switch (args->share_access) {
        case OPEN4_SHARE_ACCESS_READ:
            flags |= CHIMERA_VFS_OPEN_RDONLY;
            break;
        case OPEN4_SHARE_ACCESS_WRITE:
            flags |= CHIMERA_VFS_OPEN_WRONLY;
            break;
        case OPEN4_SHARE_ACCESS_BOTH:
            flags |= CHIMERA_VFS_OPEN_RDWR;
            break;
    } /* switch */

    switch (args->claim.claim) {
        case CLAIM_NULL:
            chimera_vfs_open_at(thread->vfs,
                                req->fh,
                                req->fhlen,
                                args->claim.file.data,
                                args->claim.file.len,
                                flags,
                                0,
                                chimera_nfs4_open_complete,
                                req);
            break;
        default:
            abort();
    } /* switch */
} /* chimera_nfs4_open */