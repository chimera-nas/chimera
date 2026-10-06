// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs.h"
#include "nfs4_procs.h"
#include "nfs4_named_attr.h"




static void
chimera_nfs4_lookup_resume(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    int                               at_root_export)
{
    struct LOOKUP4args       *args =
        &req->args_compound->argarray[req->index].oplookup;
    struct chimera_nfs_export sibling;

    /* At the "/" export's root, sibling exports are grafted over the real
     * directory as junctions: a name matching a sibling export enters that
     * export (shadowing any real entry of the same name), exactly as the
     * synthetic pseudo-root routes into exports. */
    if (at_root_export &&
        chimera_nfs_get_export_by_component(thread->shared,
                                            args->objname.data,
                                            args->objname.len,
                                            &sibling) == 0) {
        nfs4_root_lookup_export(thread, req, &sibling, sibling.path);
        return;
    }

    chimera_nfs4_compound_single(thread, req);
} /* chimera_nfs4_lookup_resume */

void
chimera_nfs4_lookup(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct LOOKUP4args *args = &argop->oplookup;
    struct LOOKUP4res  *res  = &resop->oplookup;

    if (req->fhlen == 0) {
        res->status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    res->status = chimera_nfs4_validate_name(&args->objname);

    if (res->status != NFS4_OK) {
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    if (fh_is_nfs4_root(req->fh, req->fhlen)) {
        nfs4_root_lookup(thread, req);
        return;
    }

    /* LOOKUP inside a named-attribute directory: open the base file and resolve
     * the named stream of this name. */
    if (chimera_nfs4_fh_is_attrdir(req->fh, req->fhlen)) {
        chimera_nfs4_compound_single(thread, req);
        return;
    }

    nfs4_root_junction_check(thread, req, chimera_nfs4_lookup_resume);
} /* chimera_nfs4_lookup */
