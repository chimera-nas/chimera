// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <sys/stat.h>

#include "nfs_internal.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"
#include "vfs/vfs_acl.h"
#include "vfs/vfs_acl_posix.h"

/*
 * Populate request->getattr.r_attr.va_acl by deriving it from the just-fetched
 * mode bits (the fallback when the upstream does not speak NFSACL or returns an
 * error for GETACL).  Always sets ATTR_ACL and completes OK.
 */
static void
chimera_nfs3_acl_fallback_from_mode(struct chimera_vfs_request *request)
{
    struct chimera_nfs3_acl_ctx *ctx = request->plugin_data;
    struct chimera_acl          *acl = (struct chimera_acl *) ctx->acl_buf;
    int                          n;

    n = chimera_acl_from_mode(request->getattr.r_attr.va_mode, acl,
                              CHIMERA_NFS3_CLIENT_ACL_MAX);
    if (n >= 0) {
        request->getattr.r_attr.va_acl       = acl;
        request->getattr.r_attr.va_set_mask |= CHIMERA_VFS_ATTR_ACL;
    }

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs3_acl_fallback_from_mode */

static void
chimera_nfs3_getacl_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct GETACL3res           *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct chimera_nfs3_acl_ctx   *ctx     = request->plugin_data;
    struct chimera_acl            *acl     = (struct chimera_acl *) ctx->acl_buf;
    struct chimera_posix_acl_entry pa[CHIMERA_NFS3_CLIENT_ACL_MAX];
    struct chimera_posix_acl_entry pd[CHIMERA_NFS3_CLIENT_ACL_MAX];
    uint32_t                       na, nd;
    int                            n;

    /* An RPC-level failure means the upstream does not export NFSACL on this
     * connection (PROG_UNAVAIL/PROG_MISMATCH) or the call was dropped.  Mark the
     * server so we stop probing and fall back to mode-derived ACLs. */
    if (unlikely(status)) {
        ctx->server_thread->server->nfsacl_unsupported = 1;
        chimera_nfs3_acl_fallback_from_mode(request);
        return;
    }

    /* A protocol error for GETACL (e.g. the export has ACLs disabled) is not
     * fatal to the getattr -- fall back to deriving the ACL from mode. */
    if (res->status != NACL_NFS3_OK) {
        chimera_nfs3_acl_fallback_from_mode(request);
        return;
    }

    na = res->resok.num_acl_access;
    nd = res->resok.num_acl_default;

    if (na > CHIMERA_NFS3_CLIENT_ACL_MAX || nd > CHIMERA_NFS3_CLIENT_ACL_MAX) {
        /* Larger than we translate for a proxied ACL; degrade to mode. */
        chimera_nfs3_acl_fallback_from_mode(request);
        return;
    }

    for (uint32_t i = 0; i < na; i++) {
        pa[i].e_tag  = res->resok.acl_access[i].tag & ~CHIMERA_NFS_ACL_DEFAULT;
        pa[i].e_id   = res->resok.acl_access[i].id;
        pa[i].e_perm = res->resok.acl_access[i].perm;
    }
    for (uint32_t i = 0; i < nd; i++) {
        pd[i].e_tag  = res->resok.acl_default[i].tag & ~CHIMERA_NFS_ACL_DEFAULT;
        pd[i].e_id   = res->resok.acl_default[i].id;
        pd[i].e_perm = res->resok.acl_default[i].perm;
    }

    n = chimera_acl_from_posix(pa, na, pd, nd, acl,
                               CHIMERA_NFS3_CLIENT_ACL_MAX);
    if (n < 0) {
        chimera_nfs3_acl_fallback_from_mode(request);
        return;
    }

    request->getattr.r_attr.va_acl       = acl;
    request->getattr.r_attr.va_set_mask |= CHIMERA_VFS_ATTR_ACL;

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs3_getacl_callback */

void
chimera_nfs3_acl_fetch(struct chimera_vfs_request *request)
{
    struct chimera_nfs3_acl_ctx             *ctx           = request->plugin_data;
    struct chimera_nfs_thread               *thread        = ctx->thread;
    struct chimera_nfs_shared               *shared        = ctx->shared;
    struct chimera_nfs_client_server_thread *server_thread = ctx->server_thread;
    struct GETACL3args                       args;
    struct evpl_rpc2_cred                    rpc2_cred;
    uint8_t                                 *fh;
    int                                      fhlen;

    /* Upstream already known not to support NFSACL: derive from mode. */
    if (server_thread->server->nfsacl_unsupported) {
        chimera_nfs3_acl_fallback_from_mode(request);
        return;
    }

    chimera_nfs3_map_fh(request->fh, request->fh_len, &fh, &fhlen);

    args.fh.data.data = fh;
    args.fh.data.len  = fhlen;
    args.mask         = CHIMERA_NFS_ACL | CHIMERA_NFS_ACLCNT;
    if (S_ISDIR(request->getattr.r_attr.va_mode)) {
        args.mask |= CHIMERA_NFS_DFACL | CHIMERA_NFS_DFACLCNT;
    }

    chimera_nfs_init_rpc2_cred(&rpc2_cred, request->cred,
                               request->thread->vfs->machine_name,
                               request->thread->vfs->machine_name_len);

    shared->nfsacl_v3.send_call_NFSACLPROC3_GETACL(
        &shared->nfsacl_v3.rpc2, thread->evpl, server_thread->nfs_conn,
        &rpc2_cred, &args, 0, 0, NULL, 0, 0,
        chimera_nfs3_getacl_callback, request);
} /* chimera_nfs3_acl_fetch */
