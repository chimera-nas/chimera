// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_attr.h"
#include "nfs4_access.h"
#include "vfs/sdk/vfs_acl.h"
#include "vfs/sdk/vfs_access.h"

/*
 * The ACCESS4_* bits this server will actually evaluate for `attr`/`fh`.
 *
 * The server reports in `supported` exactly the requested bits it evaluated,
 * never undefined bits or bits not meaningful for the object type (RFC 7530
 * sec 16.1.4 / RFC 8276 sec 8.4).  The xattr access bits exist only in
 * NFSv4.2, so they are meaningful only when the client negotiated
 * minorversion >= 2 AND the backend implements xattrs -- on 4.0/4.1 bit 0x40
 * is undefined and must be ignored.
 *
 * Split out from the fill because the caller needs it to build the ACE mask it
 * evaluates; the VFS-compound path passes `fh` for the object that op ran
 * against rather than req->fh.
 */
uint32_t
chimera_nfs4_access_requested(
    struct nfs_request             *req,
    const struct ACCESS4args       *args,
    const struct chimera_vfs_attrs *attr,
    const uint8_t                  *fh,
    int                             fhlen)
{
    uint32_t meaningful;

    meaningful = chimera_nfs4_access_meaningful(
        S_ISDIR(attr->va_mode),
        req->minorversion >= 2 &&
        chimera_nfs4_xattr_supported(req->thread->vfs_thread, fh, fhlen));

    return args->access & meaningful;
} /* chimera_nfs4_access_requested */

/*
 * Fill an ACCESS4 result from the evaluated request bits and the ACE bits the
 * central gate granted.  `granted` may cover more than `requested` asked for
 * (the VFS compound evaluates the client's whole request); the mapping back is
 * limited to `requested` either way.
 *
 * `attr` is the object's and supplies the execute rule below.
 */
void
chimera_nfs4_access_fill(
    struct nfs_request             *req,
    struct ACCESS4res              *res,
    uint32_t                        requested,
    uint32_t                        granted,
    const struct chimera_vfs_attrs *attr)
{
    res->status           = NFS4_OK;
    res->resok4.supported = requested;
    res->resok4.access    = chimera_nfs4_access_from_granted(requested, granted);

    /* RFC 8881 18.1.4: even privileged callers may receive EXECUTE only
     * when a mode execute bit or an ALLOW ACE marks the object executable.
     * This ACCESS reporting rule does not change the shared DAC engine. */
    if (res->resok4.access & ACCESS4_EXECUTE) {
        bool                      executable = (attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) && (attr->va_mode & 0111);
        const struct chimera_acl *acl        = (attr->va_set_mask & CHIMERA_VFS_ATTR_ACL) ? attr->va_acl : NULL;
        if (!executable && acl) {
            for (uint32_t i = 0; i < acl->num_aces; i++) {
                if (acl->aces[i].type == CHIMERA_ACE_ALLOWED &&
                    (acl->aces[i].access_mask & CHIMERA_ACE_EXECUTE)) {
                    executable = true;
                    break;
                }
            }
        }
        if (!executable) {
            res->resok4.access &= ~ACCESS4_EXECUTE;
        }
    }

    /* A read-only export never grants write-class access, regardless of what
     * the ACL/mode would allow.  `supported` stays unmasked: the bits were
     * evaluated, just not granted (RFC 7530 sec 16.1). */
    if (chimera_nfs_export_id_is_ro(req->thread->shared, req->export_id)) {
        res->resok4.access &= ~(ACCESS4_MODIFY | ACCESS4_EXTEND |
                                ACCESS4_DELETE | ACCESS4_XAWRITE);
    }
} /* chimera_nfs4_access_fill */

void
chimera_nfs4_access(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct ACCESS4args *args = &argop->opaccess;
    struct ACCESS4res  *res  = &resop->opaccess;

    if (req->fhlen == 0) {
        res->status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, NFS4ERR_NOFILEHANDLE);
        return;
    }

    if (fh_is_nfs4_root(req->fh, req->fhlen)) {
        /* The pseudo-root is a directory with no backing store (no xattrs):
         * report only the directory-meaningful bits the client asked for.
         * It is immutable, so the write-class bits are never granted. */
        uint32_t meaningful = chimera_nfs4_access_meaningful(1, 0);

        res->status           = NFS4_OK;
        res->resok4.supported = args->access & meaningful;
        res->resok4.access    = args->access & meaningful &
            ~(ACCESS4_MODIFY | ACCESS4_EXTEND | ACCESS4_DELETE);
        chimera_nfs4_compound_complete(req, NFS4_OK);
        return;
    }

    chimera_nfs4_compound_single(thread, req);

} /* chimera_nfs4_access */
