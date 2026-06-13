// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <sys/stat.h>

#include "nfs_internal.h"
#include "nfs_common/nfs3_status.h"
#include "vfs/vfs_acl.h"
#include "vfs/vfs_acl_posix.h"

static void
chimera_nfs3_setacl_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct SETACL3res           *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request  *request = private_data;
    struct chimera_nfs3_acl_ctx *ctx     = request->plugin_data;

    /* RPC-level failure: upstream does not speak NFSACL.  The non-ACL attrs were
     * already applied by the preceding SETATTR, so report success (the ACL store
     * is best-effort over a POSIX-ACL proxy -- documented lossy). */
    if (unlikely(status)) {
        ctx->server_thread->server->nfsacl_unsupported = 1;
        request->status                                = CHIMERA_VFS_OK;
        request->complete(request);
        return;
    }

    /* A protocol error (e.g. EACCES) is meaningful for an explicit ACL set --
     * surface it. */
    if (res->status != NACL_NFS3_OK) {
        request->status = nfs3_client_status_to_chimera_vfs_error(res->status);
        request->complete(request);
        return;
    }

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs3_setacl_callback */

void
chimera_nfs3_acl_store(struct chimera_vfs_request *request)
{
    struct chimera_nfs3_acl_ctx             *ctx           = request->plugin_data;
    struct chimera_nfs_thread               *thread        = ctx->thread;
    struct chimera_nfs_shared               *shared        = ctx->shared;
    struct chimera_nfs_client_server_thread *server_thread = ctx->server_thread;
    const struct chimera_vfs_attrs          *set_attr      = request->setattr.set_attr;
    struct SETACL3args                       args;
    struct evpl_rpc2_cred                    rpc2_cred;
    struct chimera_posix_acl_entry           pa[CHIMERA_NFS3_CLIENT_ACL_MAX];
    struct chimera_posix_acl_entry           pd[CHIMERA_NFS3_CLIENT_ACL_MAX];
    struct nfsacl_entry                      wa[CHIMERA_NFS3_CLIENT_ACL_MAX];
    struct nfsacl_entry                      wd[CHIMERA_NFS3_CLIENT_ACL_MAX];
    uint32_t                                 aclcnt = 0, dfaclcnt = 0;
    uint32_t                                 owner_uid, owner_gid;
    uint8_t                                 *fh;
    int                                      fhlen;
    int                                      rc;

    /* Upstream known not to support NFSACL: the ACL cannot be carried.  The
     * preceding SETATTR already applied any mode/owner; report success. */
    if (server_thread->server->nfsacl_unsupported || !set_attr->va_acl) {
        request->status = CHIMERA_VFS_OK;
        request->complete(request);
        return;
    }

    owner_uid = (set_attr->va_set_mask & CHIMERA_VFS_ATTR_UID) ? set_attr->va_uid : 0;
    owner_gid = (set_attr->va_set_mask & CHIMERA_VFS_ATTR_GID) ? set_attr->va_gid : 0;

    /* A directory ACL may carry inheritable (default) entries; permit their
     * emission (a non-directory simply has none). */
    rc = chimera_acl_to_posix(set_attr->va_acl, owner_uid, owner_gid, 1,
                              pa, CHIMERA_NFS3_CLIENT_ACL_MAX, &aclcnt,
                              pd, CHIMERA_NFS3_CLIENT_ACL_MAX, &dfaclcnt);
    if (rc < 0) {
        /* Too large to translate; leave the upstream ACL as the SETATTR-derived
         * mode set it (best-effort). */
        request->status = CHIMERA_VFS_OK;
        request->complete(request);
        return;
    }

    for (uint32_t i = 0; i < aclcnt; i++) {
        wa[i].tag  = pa[i].e_tag;
        wa[i].id   = pa[i].e_id;
        wa[i].perm = pa[i].e_perm;
    }
    for (uint32_t i = 0; i < dfaclcnt; i++) {
        wd[i].tag  = pd[i].e_tag | CHIMERA_NFS_ACL_DEFAULT;
        wd[i].id   = pd[i].e_id;
        wd[i].perm = pd[i].e_perm;
    }

    chimera_nfs3_map_fh(request->fh, request->fh_len, &fh, &fhlen);

    args.fh.data.data      = fh;
    args.fh.data.len       = fhlen;
    args.mask              = CHIMERA_NFS_ACL | CHIMERA_NFS_ACLCNT;
    args.acl_access_count  = aclcnt;
    args.num_acl_access    = aclcnt;
    args.acl_access        = wa;
    args.acl_default_count = dfaclcnt;
    args.num_acl_default   = dfaclcnt;
    args.acl_default       = wd;
    if (dfaclcnt) {
        args.mask |= CHIMERA_NFS_DFACL | CHIMERA_NFS_DFACLCNT;
    }

    chimera_nfs_init_rpc2_cred(&rpc2_cred, request->cred,
                               request->thread->vfs->machine_name,
                               request->thread->vfs->machine_name_len);

    shared->nfsacl_v3.send_call_NFSACLPROC3_SETACL(
        &shared->nfsacl_v3.rpc2, thread->evpl, server_thread->nfs_conn,
        &rpc2_cred, &args, 0, 0, NULL, 0, 0,
        chimera_nfs3_setacl_callback, request);
} /* chimera_nfs3_acl_store */
