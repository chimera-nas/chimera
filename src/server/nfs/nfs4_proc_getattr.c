// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_attr.h"
#include "nfs4_named_attr.h"
#include "nfs4_session.h"
#include "nfs4_state.h"
#include "nfs4_callback.h"
#include "server/server.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"

/* Parked-GETATTR context while a CB_GETATTR to a write-delegation holder is
 * outstanding.  Holds a copy of the server's LOCAL attrs (the VFS completion's
 * are transient).  `deleg` is the conflicting write delegation (held with a +1
 * ref by the CB_GETATTR machinery for the duration of the query); the §10.4.3
 * combine in the resume reads/updates its cached sc through it. */
struct nfs4_getattr_park {
    struct nfs_request      *req;
    struct nfs_delegation   *deleg;
    struct nfs_client       *holder_client;
    uint64_t                 querying_client_id;
    struct chimera_vfs_attrs attr;
};

/*
 * Marshal a GETATTR4 result from attributes already fetched for `fh`.
 *
 * Shared by the per-op path below and by the VFS-compound path, which runs the
 * tail of a COMPOUND as one VFS sequence and comes back holding one attribute
 * set per op -- each possibly for a different object, which is why the object's
 * file handle is passed explicitly instead of read from req->fh.
 *
 * Returns NFS4_OK, or NFS4ERR_RESOURCE when the reply buffer cannot hold the
 * attributes.  Touches neither the request's open handle nor the compound.
 */
nfsstat4
chimera_nfs4_getattr_fill(
    struct nfs_request             *req,
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

    rc = xdr_dbuf_alloc_opaque(&res->resok4.obj_attributes.attr_vals,
                               attrvals_cap,
                               req->encoding->dbuf);

    if (rc) {
        return NFS4ERR_RESOURCE;
    }

    marshall_attr = *attr;
    if (!change_projected) {
        struct nfs4_change_observation *observation;
        nfsstat4                        status = nfs4_change_project(req->thread->shared->nfs4_state_table.change_table,
                                                                     fh, fhlen, &marshall_attr, &req->
                                                                     change_observations, &observation);
        if (status != NFS4_OK) {
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
                                res->resok4.obj_attributes.attr_vals.data,
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
                                req->export_id,
                                req->thread->shared->fh_key,
                                req->thread->shared->fh_sign,
                                type_override);

    return NFS4_OK;
} /* chimera_nfs4_getattr_fill */

static void
chimera_nfs4_getattr_finish(
    struct nfs_request       *req,
    struct chimera_vfs_attrs *attr)
{
    struct GETATTR4args *args = &req->args_compound->argarray[req->index].opgetattr;
    struct GETATTR4res  *res  = &req->res_compound.resarray[req->index].opgetattr;

    res->status = chimera_nfs4_getattr_fill(req, args, res, attr,
                                            req->fh, req->fhlen, false);

    if (req->handle) {
        chimera_vfs_release(req->thread->vfs_thread, req->handle);
    }

    chimera_nfs4_compound_complete(req, res->status);
} /* chimera_nfs4_getattr_finish */

/* Legacy execution uses the same private combine journal as compounds. Stage
 * the response before publishing the sticky dirty flag and synthesized change,
 * so a marshalling failure does not consume an attribute version. */
static void
chimera_nfs4_getattr_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data);

static void
chimera_nfs4_getattr_cb_resume(
    void    *priv,
    int      status,
    bool     got_change,
    uint64_t change,
    bool     got_size,
    uint64_t size)
{
    struct nfs4_getattr_park             *park        = priv;
    struct nfs_request                   *req         = park->req;
    struct chimera_server_nfs_thread     *thread      = req->thread;
    struct nfs_delegation_combine_journal journal     = { 0 };
    struct GETATTR4args                  *args        = &req->args_compound->argarray[req->index].opgetattr;
    struct GETATTR4res                   *res         = &req->res_compound.resarray[req->index].opgetattr;
    nfsstat4                              result      = NFS4_OK;
    struct nfs4_change_observation       *observation = NULL;

    int                                   match = nfs4_write_delegation_matches(thread, req->fh, req->fhlen,
                                                                                park->querying_client_id, park->deleg);

    if (!match) {
        /* This legacy path fetched attrs before the callback. A returning
         * holder may have flushed newer data while we waited, so refetch. */
        nfs_client_finish_compound(park->holder_client, &thread->shared->nfs4_state_table,
                                   thread->vfs_thread);
        free(park);
        chimera_vfs_getattr(thread->vfs_thread, &req->cred, req->handle,
                            chimera_nfs4_attr2mask(args->attr_request, args->num_attr_request) |
                            CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME | CHIMERA_VFS_ATTR_SIZE,
                            chimera_nfs4_getattr_complete, req);
        return;
    }
    if (match < 0 || (match > 0 && (status || !got_change || !got_size))) {
        result = NFS4ERR_DELAY;
    } else if (match > 0) {
        result = nfs4_change_project(thread->shared->nfs4_state_table.change_table,
                                     req->fh, req->fhlen, &park->attr, &req->change_observations, &observation);
        if (result == NFS4_OK) {
            result = nfs_delegation_combine_reserve(park->deleg, park, &journal);
        }
        if (result == NFS4_OK) {
            struct timespec now;
            clock_gettime(CLOCK_REALTIME, &now);
            result = nfs_delegation_combine_attrs(&journal, change, got_size,
                                                  size, &now, &park->attr);
        }
    }
    if (result == NFS4_OK) {
        result = chimera_nfs4_getattr_fill(req, args, res, &park->attr, req->fh, req->fhlen, true);
        if (result == NFS4_OK) {
            nfs4_change_observe(observation, &park->attr);
        }
    }
    nfs_delegation_combine_finish(&journal, result == NFS4_OK,
                                  &thread->shared->nfs4_state_table, thread->vfs_thread);
    nfs_client_finish_compound(park->holder_client, &thread->shared->nfs4_state_table,
                               thread->vfs_thread);
    if (req->handle) {
        chimera_vfs_release(thread->vfs_thread, req->handle);
        req->handle = NULL;
    }
    free(park);
    res->status = result;
    chimera_nfs4_compound_complete(req, result);
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
    struct nfs_client     *holder_client;

    if (error_code != CHIMERA_VFS_OK) {
        if (req->handle) {
            chimera_vfs_release(req->thread->vfs_thread, req->handle);
            req->handle = NULL;
        }
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* RFC 7530 §10.4.3 / RFC 8881 §10.4.3: if another client holds a write
     * delegation on this file it may have uncommitted size/change locally, so
     * query it via CB_GETATTR and merge the result.  CB_GETATTR is an NFSv4.0
     * mechanism (RFC 7530 §18.1) that 4.1 inherited, and the send path serves
     * both (nfs4_callback.c prepends CB_SEQUENCE only for 4.1+), so the query
     * runs for any minor version -- gating it on 4.1+ left 4.0 peers reading
     * pre-modification size/change. */
    client = req->session ? req->session->client_unified : NULL;

    if (client &&
        chimera_server_config_get_nfs4_delegations(req->thread->shared->config) &&
        (wdeleg = nfs4_find_conflicting_write_deleg_pinned(req->thread, req->fh,
                                                           req->fhlen,
                                                           client->client_id, &holder_client)) != NULL) {
        struct nfs4_getattr_park *park = calloc(1, sizeof(*park));

        park->req                = req;
        park->deleg              = wdeleg;
        park->holder_client      = holder_client;
        park->querying_client_id = client->client_id;
        park->attr               = *attr;
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
                                                    args->num_attr_request) |
            CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME | CHIMERA_VFS_ATTR_SIZE;

        chimera_vfs_getattr(req->thread->vfs_thread, &req->cred,
                            handle,
                            attr_mask,
                            chimera_nfs4_getattr_complete,
                            req);
    } else {
        chimera_nfs4_compound_complete(req, chimera_nfs4_errno_to_nfsstat4(error_code));
    }
} /* chimera_nfs4_getattr_open_callback */

/* A named-attribute directory is synthetic: its attributes are the base file's
 * owner/timestamps presented as a directory.  Override mode/type/nlink so the
 * object reads back as NF4ATTRDIR, then run the normal marshalling path (which
 * derives the NF4ATTRDIR type override from req->fh). */
static void
chimera_nfs4_getattr_attrdir_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request *req = private_data;
    struct GETATTR4res *res = &req->res_compound.resarray[req->index].opgetattr;

    if (error_code != CHIMERA_VFS_OK) {
        if (req->handle) {
            chimera_vfs_release(req->thread->vfs_thread, req->handle);
            req->handle = NULL;
        }
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    attr->va_mode      = S_IFDIR | 0755;
    attr->va_nlink     = 2;
    attr->va_size      = 0;
    attr->va_set_mask |= CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_NLINK;

    chimera_nfs4_getattr_finish(req, attr);
} /* chimera_nfs4_getattr_attrdir_complete */

static void
chimera_nfs4_getattr_attrdir_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request  *req  = private_data;
    struct GETATTR4args *args = &req->args_compound->argarray[req->index].opgetattr;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_nfs4_compound_complete(req, chimera_nfs4_errno_to_nfsstat4(error_code));
        return;
    }

    req->handle = handle;

    chimera_vfs_getattr(req->thread->vfs_thread, &req->cred,
                        handle,
                        chimera_nfs4_attr2mask(args->attr_request,
                                               args->num_attr_request),
                        chimera_nfs4_getattr_attrdir_complete,
                        req);
} /* chimera_nfs4_getattr_attrdir_open_callback */

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

    /* GETATTR on a synthetic named-attribute directory: stat the base file it
     * wraps, then present it as a directory. */
    if (chimera_nfs4_fh_is_attrdir(req->fh, req->fhlen)) {
        const uint8_t *base;
        int            base_len;

        chimera_nfs4_attrdir_base(req->fh, req->fhlen, &base, &base_len);

        chimera_vfs_open_fh(thread->vfs_thread, &req->cred,
                            base, base_len,
                            CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
                            chimera_nfs4_getattr_attrdir_open_callback,
                            req);
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

    chimera_vfs_open_fh(thread->vfs_thread, &req->cred,
                        req->fh,
                        req->fhlen,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
                        chimera_nfs4_getattr_open_callback,
                        req);
} /* chimera_nfs4_getattr */
