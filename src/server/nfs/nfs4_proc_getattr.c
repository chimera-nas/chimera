// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_attr.h"
#include "nfs4_session.h"
#include "nfs4_state.h"
#include "nfs4_callback.h"
#include "server/server.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"

/* Parked-GETATTR context while a CB_GETATTR to a write-delegation holder is
 * outstanding.  Holds a copy of the server's attrs (the VFS completion's are
 * transient); the holder's reply overrides change/size. */
struct nfs4_getattr_park {
    struct nfs_request      *req;
    struct chimera_vfs_attrs attr;
};

static void
chimera_nfs4_getattr_finish(
    struct nfs_request       *req,
    struct chimera_vfs_attrs *attr)
{
    struct GETATTR4args     *args = &req->args_compound->argarray[req->index].opgetattr;
    struct GETATTR4res      *res  = &req->res_compound.resarray[req->index].opgetattr;
    struct chimera_vfs_attrs marshall_attr;
    int                      rc;

    res->status = NFS4_OK;

    rc = xdr_dbuf_alloc_array(&res->resok4.obj_attributes, attrmask, 3, req->encoding->dbuf);

    if (rc) {
        res->status = NFS4ERR_RESOURCE;
        if (req->handle) {
            chimera_vfs_release(req->thread->vfs_thread, req->handle);
        }
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* Size the attribute buffer to hold a variable-length ACL, but only when
     * the client actually requested FATTR4_ACL -- otherwise every getattr would
     * over-reserve (e.g. a plain SIZE query that the backend answers with the
     * full stat set), bloating large COMPOUNDs.  When the ACL is requested we
     * size it from the object's ACL if present, or from a mode-synthesised ACL
     * (mode-only backends) otherwise. */
    uint32_t attrvals_cap  = 4096;
    int      acl_requested = args->num_attr_request >= 1 &&
        (args->attr_request[0] & (1 << FATTR4_ACL));
    if (acl_requested) {
        if (attr->va_set_mask & CHIMERA_VFS_ATTR_ACL) {
            attrvals_cap += chimera_nfs4_acl_wire_size(attr->va_acl);
        } else {
            attrvals_cap += chimera_nfs4_acl_wire_size(NULL) +
                8 * (4 * sizeof(uint32_t) + ((CHIMERA_IDMAP_WHO_MAX + 3) & ~3u));
        }
    }

    rc = xdr_dbuf_alloc_opaque(&res->resok4.obj_attributes.attr_vals,
                               attrvals_cap,
                               req->encoding->dbuf);

    if (rc) {
        res->status = NFS4ERR_RESOURCE;
        if (req->handle) {
            chimera_vfs_release(req->thread->vfs_thread, req->handle);
        }
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    marshall_attr = *attr;
    chimera_nfs4_attrs_fill_filehandle(&marshall_attr,
                                       args->num_attr_request,
                                       args->attr_request,
                                       req->fh,
                                       req->fhlen);

    chimera_nfs4_marshall_attrs(&marshall_attr,
                                args->num_attr_request,
                                args->attr_request,
                                &res->resok4.obj_attributes.num_attrmask,
                                res->resok4.obj_attributes.attrmask,
                                3,
                                res->resok4.obj_attributes.attr_vals.data,
                                &res->resok4.obj_attributes.attr_vals.len,
                                attrvals_cap,
                                req->minorversion,
                                chimera_nfs4_pnfs_layout_type(req->thread->vfs_thread,
                                                              req->thread->shared->vfs,
                                                              req->fh, req->fhlen),
                                chimera_nfs4_xattr_supported(req->thread->vfs_thread,
                                                             req->fh, req->fhlen),
                                chimera_server_config_get_nfs4_delegations(
                                    req->thread->shared->config),
                                req->thread->shared->nfs_lease_time_s);

    if (req->handle) {
        chimera_vfs_release(req->thread->vfs_thread, req->handle);
    }

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_getattr_finish */

/* Resume (on the requester's thread) after the write-delegation holder has
 * answered CB_GETATTR: override change/size with the holder's view, then
 * finish the GETATTR.  On failure, fall back to the server's own attrs. */
static void
chimera_nfs4_getattr_cb_resume(
    void    *priv,
    int      status,
    bool     got_change,
    uint64_t change,
    bool     got_size,
    uint64_t size)
{
    struct nfs4_getattr_park *park = priv;

    if (status == 0) {
        if (got_change) {
            /* The CHANGE attribute is encoded from va_ctime as
             * sec*1e9 + nsec; reconstruct a ctime that re-encodes to the
             * holder's exact change value. */
            park->attr.va_ctime.tv_sec  = change / 1000000000ULL;
            park->attr.va_ctime.tv_nsec = change % 1000000000ULL;
            park->attr.va_set_mask     |= CHIMERA_VFS_ATTR_CTIME;
        }
        if (got_size) {
            park->attr.va_size      = size;
            park->attr.va_set_mask |= CHIMERA_VFS_ATTR_SIZE;
        }
    }

    chimera_nfs4_getattr_finish(park->req, &park->attr);
    free(park);
} /* chimera_nfs4_getattr_cb_resume */

static void
chimera_nfs4_getattr_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request    *req = private_data;
    struct GETATTR4res    *res = &req->res_compound.resarray[req->index].opgetattr;
    struct nfs_client     *client;
    struct nfs_delegation *wdeleg;

    if (error_code != CHIMERA_VFS_OK) {
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* RFC 8881 §10.4.3 / §20.1: if another client holds a write delegation on
     * this file it may have uncommitted size/change locally, so query it via
     * CB_GETATTR and merge the result.  Only meaningful on 4.1+ with a known
     * requesting client and delegations enabled. */
    client = req->session ? req->session->client_unified : NULL;

    if (req->minorversion >= 1 && client &&
        chimera_server_config_get_nfs4_delegations(req->thread->shared->config) &&
        (wdeleg = nfs4_find_conflicting_write_deleg(req->thread, req->fh,
                                                    req->fhlen,
                                                    client->client_id)) != NULL) {
        struct nfs4_getattr_park *park = calloc(1, sizeof(*park));

        park->req  = req;
        park->attr = *attr;
        nfs4_cb_getattr(req->thread, wdeleg, park,
                        chimera_nfs4_getattr_cb_resume);
        return; /* parked; resume finishes the GETATTR */
    }

    chimera_nfs4_getattr_finish(req, attr);
} /* chimera_nfs4_getattr_complete */

static void
chimera_nfs4_getattr_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request  *req  = private_data;
    struct GETATTR4args *args = &req->args_compound->argarray[req->index].opgetattr;

    if (error_code == CHIMERA_VFS_OK) {
        req->handle = handle;

        uint64_t attr_mask = chimera_nfs4_attr2mask(args->attr_request,
                                                    args->num_attr_request);

        chimera_vfs_getattr(req->thread->vfs_thread, &req->cred,
                            handle,
                            attr_mask,
                            chimera_nfs4_getattr_complete,
                            req);
    } else {
        chimera_nfs4_compound_complete(req, chimera_nfs4_errno_to_nfsstat4(error_code));
    }
} /* chimera_nfs4_getattr_open_callback */

void
chimera_nfs4_getattr(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct GETATTR4args *args = &req->args_compound->argarray[req->index].opgetattr;
    struct GETATTR4res  *res  = &resop->opgetattr;

    if (req->fhlen == 0) {
        res->status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    res->status = chimera_nfs4_validate_getattr_request(args->num_attr_request,
                                                        args->attr_request);
    if (res->status != NFS4_OK) {
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    if (fh_is_nfs4_root(req->fh, req->fhlen)) {
        struct chimera_vfs_attrs attr;
        uint64_t                 attr_mask;
        attr_mask = chimera_nfs4_attr2mask(args->attr_request,
                                           args->num_attr_request);
        nfs4_root_getattr(thread, &attr, attr_mask);
        req->handle = NULL; /* No handle since root attributes are synthetic */
        chimera_nfs4_getattr_complete(CHIMERA_VFS_OK, &attr, req);
        return;
    }

    chimera_vfs_open_fh(thread->vfs_thread, &req->cred, NULL,
                        req->fh,
                        req->fhlen,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
                        chimera_nfs4_getattr_open_callback,
                        req);
} /* chimera_nfs4_getattr */
