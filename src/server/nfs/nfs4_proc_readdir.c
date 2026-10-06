// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_attr.h"
#include "nfs4_status.h"
#include "server/server.h"

/*
 * Marshal one directory entry into the reply and append it to `cursor`,
 * enforcing the READDIR's maxcount and its compound's remaining reservations.  Returns 0 if
 * the entry was taken and -1 if it did not fit -- the signal that stops the
 * enumeration (and, for the VFS-compound path, the point at which the page
 * truncates).
 *
 * `dir_fh` is the directory being read: its backend decides the layout type and
 * xattr support reported for every entry. Synthetic directory enumeration and
 * ordinary compound READDIR share this marshaller. NULL dir_fh selects the
 * synthetic-root attribute advertisement, which has no backing capabilities.
 */
int
chimera_nfs4_readdir_entry_fill(
    struct nfs_request             *req,
    uint16_t                        export_id,
    struct READDIR4args            *args,
    struct nfs_nfs4_readdir_cursor *cursor,
    const uint8_t                  *dir_fh,
    int                             dir_fhlen,
    uint64_t                        cookie,
    const char                     *name,
    int                             namelen,
    const struct chimera_vfs_attrs *attrs)
{
    uint32_t                 wire_size;
    uint32_t                 dbuf_before = req->encoding->dbuf->used;
    struct entry4           *entry;
    int                      rc;
    struct chimera_vfs_attrs projected = *attrs;

    if (dir_fh && (attrs->va_set_mask & CHIMERA_VFS_ATTR_FH) && args->num_attr_request &&
        (args->attr_request[0] & (1U << FATTR4_CHANGE))) {
        struct nfs4_change_observation *observation;
        cursor->change_status = nfs4_change_project(req->thread->shared->nfs4_state_table.change_table,
                                                    attrs->va_fh, attrs->va_fh_len, &projected, &req->
                                                    change_observations, &observation);
        if (cursor->change_status != NFS4_OK) {
            return -1;
        }
        attrs = &projected;
    }

    entry = xdr_dbuf_alloc_space(sizeof(*entry), req->encoding->dbuf);
    if (!entry) {
        req->encoding->dbuf->used = dbuf_before;
        return -1;
    }

    rc = xdr_dbuf_opaque_copy(&entry->name, name, namelen, req->encoding->dbuf);
    if (rc) {
        req->encoding->dbuf->used = dbuf_before;
        return -1;
    }

    entry->cookie    = cookie;
    entry->nextentry = NULL;

    rc = xdr_dbuf_alloc_array(&entry->attrs, attrmask, 3, req->encoding->dbuf);
    if (rc) {
        req->encoding->dbuf->used = dbuf_before;
        return -1;
    }

    uint32_t acl_capacity = 0;
    if (args->num_attr_request && (args->attr_request[0] & (1U << FATTR4_ACL))) {
        uint8_t                   synthbuf[sizeof(struct chimera_acl) + 8 * sizeof(struct chimera_ace)];
        struct chimera_acl       *synth = (struct chimera_acl *) synthbuf;
        const struct chimera_acl *acl   = NULL;
        if (attrs->va_set_mask & CHIMERA_VFS_ATTR_ACL) {
            acl = attrs->va_acl;
        } else if (attrs->va_set_mask & CHIMERA_VFS_ATTR_MODE) {
            chimera_acl_from_mode(attrs->va_mode, synth, 8);
            acl = synth;
        }
        acl_capacity = chimera_nfs4_acl_wire_size(acl);
    }
    uint32_t attrvals_cap = chimera_nfs4_attr_capacity(args->num_attr_request,
                                                       args->attr_request, acl_capacity);
    uint32_t attrvals_mark = req->encoding->dbuf->used;

    rc = xdr_dbuf_alloc_opaque(&entry->attrs.attr_vals,
                               attrvals_cap,
                               req->encoding->dbuf);
    if (rc) {
        req->encoding->dbuf->used = dbuf_before;
        return -1;
    }

    chimera_nfs4_marshall_attrs(attrs,
                                args->num_attr_request,
                                args->attr_request,
                                &entry->attrs.num_attrmask,
                                entry->attrs.attrmask,
                                3,
                                entry->attrs.attr_vals.data,
                                &entry->attrs.attr_vals.len,
                                attrvals_cap,
                                dir_fh ? req->minorversion : 0,
                                /* entries share the directory's backend/fs */
                                dir_fh ? chimera_nfs4_pnfs_layout_type(req->thread->vfs_thread,
                                                                       req->thread->shared->vfs,
                                                                       dir_fh, dir_fhlen) : 0,
                                dir_fh ? chimera_nfs4_xattr_supported(req->thread->vfs_thread,
                                                                      dir_fh, dir_fhlen) : 0,
                                dir_fh && chimera_server_config_get_nfs4_delegations(
                                    req->thread->shared->config),
                                req->thread->shared->nfs_lease_time_s,
                                export_id,
                                req->thread->shared->fh_key,
                                req->thread->shared->fh_sign,
                                /* Named-attribute files report NF4REG (their
                                 * mode-derived type), like ordinary directory
                                 * entries -- see the type note in nfs4_proc_getattr. */
                                0);

    /* This is the last allocation for the entry. Reclaim the conservative
     * capacity, keeping the arena's alignment, before staging any successor.
     * Page limits count XDR bytes, not C structs or scratch reservations. */
    req->encoding->dbuf->used = attrvals_mark + ((entry->attrs.attr_vals.len + 7) & ~7U);
    wire_size                 = 24 + ((entry->name.len + 3) & ~3U) +
        4 * entry->attrs.num_attrmask + ((entry->attrs.attr_vals.len + 3) & ~3U);

    if ((uint64_t) cursor->count + wire_size > args->maxcount ||
        (uint64_t) req->encoding->dbuf->used + cursor->reply_reserve > (uint32_t) req->encoding->dbuf->size) {
        req->encoding->dbuf->used = dbuf_before;
        return -1;
    }

    cursor->count += wire_size;

    if (cursor->entries) {
        cursor->last->nextentry = entry;
        cursor->last            = entry;
    } else {
        cursor->entries = entry;
        cursor->last    = entry;
    }

    return 0;
} /* chimera_nfs4_readdir_entry_fill */

void
chimera_nfs4_readdir(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct READDIR4args *args = &argop->opreaddir;
    struct READDIR4res  *res  = &req->res_compound.resarray[req->index].opreaddir;

    if (req->fhlen == 0) {
        res->status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* maxcount bounds the entire READDIR4resok. If it cannot even hold the
     * cookie verifier and the empty-directory reply (8 + 4 + 4 bytes), the
     * server returns NFS4ERR_TOOSMALL. */
    if (args->maxcount < 16) {
        res->status = NFS4ERR_TOOSMALL;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* Write-only attributes (time_*_set) cannot be read back per entry. */
    res->status = chimera_nfs4_validate_getattr_request(args->num_attr_request,
                                                        args->attr_request);
    if (res->status != NFS4_OK) {
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* RFC 7530 §16.24: cookie values 1 and 2 are reserved and must never be
     * sent by a client (0 means "start of directory"). The VFS backends emit
     * only cookie 0 or values >= 3 for regular directories, so a reserved
     * cookie here is a client error. The pseudo-root uses its own
     * export-position cookie space (also >= 3), validated by the shared
     * builder against its export snapshot. */
    if (args->cookie == 1 || args->cookie == 2) {
        res->status = NFS4ERR_BAD_COOKIE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    chimera_nfs4_compound_single(thread, req);

} /* chimera_nfs4_readdir */
