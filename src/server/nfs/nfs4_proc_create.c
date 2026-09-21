// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <fcntl.h>

#include "nfs4_procs.h"
#include "nfs4_attr.h"
#include "vfs/vfs_compound.h"
#include "nfs4_status.h"

/* PUTFH, OPEN_CURRENT(dir), CREATE: the create is op 2 of the run. */
#define NFS4_CREATE_OP_CREATE 2

static void
chimera_nfs4_create_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs_request                   *req  = private_data;
    struct CREATE4args                   *args = &req->args_compound->argarray[req->index].opcreate;
    struct CREATE4res                    *res  = &req->res_compound.resarray[req->index].opcreate;
    const struct chimera_vfs_compound_op *op;
    enum chimera_vfs_error                error_code;

    error_code = chimera_vfs_compound_status(compound);

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_compound_free(compound);
        chimera_nfs4_compound_complete(req,
                                       chimera_nfs4_errno_to_nfsstat4(error_code));
        return;
    }

    op = chimera_vfs_compound_op(compound, NFS4_CREATE_OP_CREATE);

    /* The op's set_attr is what was actually applied; its attr describes the
     * object, fh included.  Both are read before the free. */
    struct chimera_vfs_attrs applied = op->set_attr;
    struct chimera_vfs_attrs pre     = op->dir_pre_attr;
    struct chimera_vfs_attrs post    = op->dir_post_attr;

    res->status = NFS4_OK;

    res->resok4.attrset = xdr_dbuf_alloc_space(4 * sizeof(uint32_t), req->encoding->dbuf);
    chimera_nfs_abort_if(res->resok4.attrset == NULL, "Failed to allocate space");
    res->resok4.num_attrset = chimera_nfs4_mask2attr(&applied,
                                                     args->createattrs.num_attrmask,
                                                     args->createattrs.attrmask,
                                                     res->resok4.attrset);

    if (!(op->attr.va_set_mask & CHIMERA_VFS_ATTR_FH)) {
        chimera_vfs_compound_free(compound);
        res->status = NFS4ERR_SERVERFAULT;
        chimera_nfs4_compound_complete(req, NFS4ERR_SERVERFAULT);
        return;
    }

    memcpy(req->fh, op->attr.va_fh, op->attr.va_fh_len);
    req->fhlen = op->attr.va_fh_len;

    chimera_vfs_compound_free(compound);

    chimera_nfs4_set_changeinfo(&res->resok4.cinfo, &pre, &post);

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_create_complete */

void
chimera_nfs4_create(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct CREATE4args          *args = &argop->opcreate;
    struct CREATE4res           *res  = &resop->opcreate;
    struct chimera_vfs_compound *compound;
    struct chimera_vfs_attrs    *attr;
    struct chimera_acl          *acl_buf      = NULL;
    unsigned                     acl_buf_aces = 0;
    const char                  *target       = NULL;
    int                          targetlen    = 0;
    uint8_t                      create_type;

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

    res->status = chimera_nfs4_validate_createattrs(args->createattrs.num_attrmask,
                                                    args->createattrs.attrmask);

    if (res->status != NFS4_OK) {
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    attr = xdr_dbuf_alloc_space(sizeof(*attr), req->encoding->dbuf);
    chimera_nfs_abort_if(attr == NULL, "Failed to allocate space");

    if (args->createattrs.num_attrmask >= 1 &&
        (args->createattrs.attrmask[0] & (1 << FATTR4_ACL))) {
        acl_buf = xdr_dbuf_alloc_space(chimera_acl_size(CHIMERA_ACL_MAX_ACES),
                                       req->encoding->dbuf);
        acl_buf_aces = acl_buf ? CHIMERA_ACL_MAX_ACES : 0;
    }

    chimera_nfs4_unmarshall_attrs(attr,
                                  args->createattrs.num_attrmask,
                                  args->createattrs.attrmask,
                                  args->createattrs.attr_vals.data,
                                  args->createattrs.attr_vals.len,
                                  acl_buf,
                                  acl_buf_aces);

    /* Which of the three shapes the CREATE takes has to be known before the
     * run is built, so the types this op cannot make are decided here rather
     * than after the directory has been opened -- an allow-list, exactly as
     * the VFS-compound path's scan writes it.  NFS4ERR_BADTYPE for a regular
     * file (that is what OPEN is for), NOTSUPP for the synthetic
     * named-attribute objects, and INVAL for a symlink with no target, are all
     * answers about the REQUEST and owe the current object nothing. */
    switch (args->objtype.type) {
        case NF4DIR:
            create_type = CHIMERA_VFS_COMPOUND_CREATE_DIR;
            break;
        case NF4LNK:
            if (args->objtype.linkdata.len == 0) {
                res->status = NFS4ERR_INVAL;
                chimera_nfs4_compound_complete(req, NFS4ERR_INVAL);
                return;
            }
            create_type = CHIMERA_VFS_COMPOUND_CREATE_SYMLINK;
            target      = (const char *) args->objtype.linkdata.data;
            targetlen   = (int) args->objtype.linkdata.len;
            break;
        case NF4BLK:
        case NF4CHR:
        case NF4SOCK:
        case NF4FIFO:
            create_type        = CHIMERA_VFS_COMPOUND_CREATE_NODE;
            attr->va_set_mask |= CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_RDEV;
            attr->va_rdev      = 0;

            switch (args->objtype.type) {
                case NF4BLK:
                    attr->va_mode = (attr->va_mode & ~S_IFMT) | S_IFBLK;
                    attr->va_rdev =
                        ((uint64_t) args->objtype.devdata.specdata1 << 32) |
                        (uint64_t) args->objtype.devdata.specdata2;
                    break;
                case NF4CHR:
                    attr->va_mode = (attr->va_mode & ~S_IFMT) | S_IFCHR;
                    attr->va_rdev =
                        ((uint64_t) args->objtype.devdata.specdata1 << 32) |
                        (uint64_t) args->objtype.devdata.specdata2;
                    break;
                case NF4SOCK:
                    attr->va_mode = (attr->va_mode & ~S_IFMT) | S_IFSOCK;
                    break;
                default: /* NF4FIFO */
                    attr->va_mode = (attr->va_mode & ~S_IFMT) | S_IFIFO;
                    break;
            } /* switch */
            break;
        case NF4ATTRDIR:
        case NF4NAMEDATTR:
            chimera_nfs4_compound_complete(req, NFS4ERR_NOTSUPP);
            return;
        default:
            chimera_nfs4_compound_complete(req, NFS4ERR_BADTYPE);
            return;
    } /* switch */

    req->handle = NULL;

    compound = chimera_vfs_compound_alloc(thread->vfs_thread, &req->cred);

    chimera_vfs_compound_add_putfh(compound, req->fh, req->fhlen);
    chimera_vfs_compound_add_open_current(compound,
                                          CHIMERA_VFS_OPEN_INFERRED |
                                          CHIMERA_VFS_OPEN_PATH |
                                          CHIMERA_VFS_OPEN_DIRECTORY, 0);
    chimera_vfs_compound_add_create(compound, create_type,
                                    (const char *) args->objname.data,
                                    (int) args->objname.len,
                                    target, targetlen,
                                    attr,
                                    CHIMERA_VFS_ATTR_FH,
                                    CHIMERA_VFS_ATTR_CHANGE |
                                    CHIMERA_VFS_ATTR_CTIME,
                                    CHIMERA_VFS_ATTR_CHANGE |
                                    CHIMERA_VFS_ATTR_CTIME);

    chimera_vfs_compound_submit(compound, chimera_nfs4_create_complete, req);
} /* chimera_nfs4_create */
