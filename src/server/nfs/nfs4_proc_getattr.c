// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdlib.h>

#include "common/thread.h"
#include "nfs4_procs.h"
#include "nfs4_reply.h"
#include "nfs4_status.h"
#include "nfs4_attr.h"
#include "nfs4_named_attr.h"
#include "server/server.h"

/*
 * Marshal a GETATTR4 result from attributes already fetched for `fh`.
 *
 * Shared by synthetic namespace objects and the compound builder. Each result
 * can name a different object, so its filehandle is explicit rather than read
 * from the request's current cursor.
 *
 * Returns NFS4_OK, or NFS4ERR_RESOURCE when the reply buffer cannot hold the
 * attributes.  Touches neither the request's open handle nor the compound.
 */
nfsstat4
chimera_nfs4_getattr_fill(
    struct nfs_request             *req,
    uint16_t                        export_id,
    struct GETATTR4args            *args,
    struct GETATTR4res             *res,
    const struct chimera_vfs_attrs *attr,
    const uint8_t                  *fh,
    int                             fhlen,
    bool                            change_projected)
{
    struct chimera_vfs_attrs marshall_attr;
    int                      rc;

    rc = xdr_dbuf_alloc_array(&res->resok4.obj_attributes, attrmask, 3, req->encoding->dbuf);

    if (rc) {
        return NFS4ERR_RESOURCE;
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

    /* The encoder needs a worst-case workspace, but only its actual output
     * belongs in the retained response arena. A large ACL must not require
     * 272 bytes of permanent response storage for every short numeric ACE. */
    uint8_t *attrvals = malloc(attrvals_cap);
    if (!attrvals) {
        return NFS4ERR_RESOURCE;
    }

    marshall_attr = *attr;
    if (!change_projected) {
        struct nfs4_change_observation *observation;
        nfsstat4                        status = nfs4_change_project(req->thread->shared->nfs4_state_table.change_table,
                                                                     fh, fhlen, &marshall_attr, &req->
                                                                     change_observations, &observation);
        if (status != NFS4_OK) {
            free(attrvals);
            return status;
        }
    }
    chimera_nfs4_attrs_fill_filehandle(&marshall_attr,
                                       args->num_attr_request,
                                       args->attr_request,
                                       fh,
                                       fhlen);

    /* The synthetic named-attribute directory reports NF4ATTRDIR (invisible in
     * the underlying mode bits).  The named-attribute *files* themselves are
     * plain data forks and report NF4REG, matching how Solaris/Linux clients
     * (and SMB ADS) treat them -- NF4NAMEDATTR is avoided as some clients
     * mishandle it. */
    uint32_t type_override = 0;
    if (chimera_nfs4_fh_is_attrdir(fh, fhlen)) {
        type_override = NF4ATTRDIR;
    }

    chimera_nfs4_marshall_attrs(&marshall_attr,
                                args->num_attr_request,
                                args->attr_request,
                                &res->resok4.obj_attributes.num_attrmask,
                                res->resok4.obj_attributes.attrmask,
                                3,
                                attrvals,
                                &res->resok4.obj_attributes.attr_vals.len,
                                attrvals_cap,
                                req->minorversion,
                                chimera_nfs4_pnfs_layout_type(req->thread->vfs_thread,
                                                              req->thread->shared->vfs,
                                                              fh, fhlen),
                                chimera_nfs4_xattr_supported(req->thread->vfs_thread,
                                                             fh, fhlen),
                                chimera_server_config_get_nfs4_delegations(
                                    req->thread->shared->config),
                                req->thread->shared->nfs_lease_time_s,
                                export_id,
                                req->thread->shared->fh_key,
                                req->thread->shared->fh_sign,
                                type_override);

    rc = xdr_dbuf_opaque_copy(&res->resok4.obj_attributes.attr_vals, attrvals,
                              res->resok4.obj_attributes.attr_vals.len, req->encoding->dbuf);
    free(attrvals);
    if (rc) {
        return NFS4ERR_RESOURCE;
    }

    return NFS4_OK;
} /* chimera_nfs4_getattr_fill */


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
        res->status = chimera_nfs4_getattr_fill(req, req->export_id, args, res, &attr, req->fh, req->fhlen, false);
        if (res->status == NFS4_OK) {
            res->status = chimera_nfs4_reply_check(req, req->index,
                                                   req->reply_bytes + marshall_length_nfs_resop4(resop), req->
                                                   reply_chunk_bytes, true);
        }
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    chimera_nfs4_compound_single(thread, req);

} /* chimera_nfs4_getattr */
