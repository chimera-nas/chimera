// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <stddef.h>
#include <sys/stat.h>
#include "nfs_common.h"
#include "nfs_internal.h"

/* Static root file handle for the nfs4_root pseudo-filesystem */
static const uint8_t *nfs4_root_fh     = (const uint8_t *) "CHIMERA NFS4 ROOT FH";
static uint32_t       nfs4_root_fh_len = 21; /* strlen("CHIMERA NFS4 ROOT FH") */

/**
 * Check if a file handle corresponds to the NFSv4 root pseudo-filesystem.
 *
 * @param fh      Pointer to the file handle buffer.
 * @param fh_len  Length of the file handle buffer.
 * @return        1 if the file handle matches the NFSv4 root, 0 otherwise.
 */
static inline int
fh_is_nfs4_root(
    const uint8_t *fh,
    uint32_t       fh_len)
{
    return (fh_len == nfs4_root_fh_len) && (memcmp(fh, nfs4_root_fh, nfs4_root_fh_len) == 0);
} /* fh_is_nfs4_root */


/**
 * Retrieve the NFSv4 pseudo-root file handle.
 *
 * @param fh      Output buffer to receive the root file handle.
 * @param fh_len  Output pointer to receive the length of the file handle.
 */
static inline void
nfs4_root_get_fh(
    uint8_t  *fh,
    uint32_t *fh_len)
{
    memcpy(fh, nfs4_root_fh, nfs4_root_fh_len);
    *fh_len = nfs4_root_fh_len;
} /* nfs4_root_get_fh */

static inline int
chimera_nfs4_cred_has_mode_access(
    const struct chimera_vfs_attrs *attr,
    const struct chimera_vfs_cred  *cred,
    int                             need_read,
    int                             need_write,
    int                             need_exec)
{
    uint32_t mode = attr->va_mode;
    int      r, w, x;

    if (cred->uid == 0) {
        r = 1;
        w = 1;
        x = !!(mode & (S_IXUSR | S_IXGRP | S_IXOTH));
    } else if ((uint64_t) cred->uid == attr->va_uid) {
        r = !!(mode & S_IRUSR);
        w = !!(mode & S_IWUSR);
        x = !!(mode & S_IXUSR);
    } else {
        int in_group = ((uint64_t) cred->gid == attr->va_gid);

        if (!in_group) {
            for (uint32_t i = 0; i < cred->ngids; i++) {
                if ((uint64_t) cred->gids[i] == attr->va_gid) {
                    in_group = 1;
                    break;
                }
            }
        }

        if (in_group) {
            r = !!(mode & S_IRGRP);
            w = !!(mode & S_IWGRP);
            x = !!(mode & S_IXGRP);
        } else {
            r = !!(mode & S_IROTH);
            w = !!(mode & S_IWOTH);
            x = !!(mode & S_IXOTH);
        }
    }

    return (!need_read || r) &&
           (!need_write || w) &&
           (!need_exec || x);
} /* chimera_nfs4_cred_has_mode_access */

/**
 * Populate attributes for the NFSv4 pseudo-root directory.
 *
 * @param thread     Pointer to the NFS server thread context.
 * @param attr       Output pointer to the attribute structure to populate.
 * @param attr_mask  Attribute mask specifying which attributes to retrieve.
 */
void
nfs4_root_getattr(
    struct chimera_server_nfs_thread *thread,
    struct chimera_vfs_attrs         *attr,
    uint64_t                          attr_mask);

void
nfs4_root_lookup(
    struct chimera_server_nfs_thread *nfs_thread,
    struct nfs_request               *req);

/**
 * Populate directory entries for the NFSv4 pseudo-root directory.
 *
 * @param thread Pointer to the NFS server thread context.
 * @param req    Pointer to the NFS request structure.
 */
void
nfs4_root_readdir(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req);


void
chimera_nfs4_access(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_getfh(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_putrootfh(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_verify(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_nverify(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_putpubfh(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_renew(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_release_lockowner(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_putfh(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_savefh(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_restorefh(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_link(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_rename(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_getattr(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_setattr(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_lookup(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_lookupp(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_read(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_write(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_copy(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_commit(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_open(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

/*
 * Resume an OPEN that parked on a 4.0 CB_NULL probe (see
 * NFS4_CB_GRANT_DEFER in nfs4_callback.h).  Called by
 * nfs4_cb_null_complete on the channel's owner thread, once cb_state has
 * been written to UP or DOWN.  Re-runs the delegation grant decision and
 * completes the OPEN.
 */
void
chimera_nfs4_open_resume_after_probe(
    struct nfs_request *req);

void
chimera_nfs4_create(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_readdir(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_readlink(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_remove(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_close(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_delegreturn(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_delegpurge(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_setclientid(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_renew(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_open_confirm(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_open_downgrade(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_release_lockowner(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_setclientid_confirm(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_exchange_id(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_create_session(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_destroy_session(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_destroy_clientid(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_sequence(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_reclaim_complete(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_test_stateid(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_set_ssv(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_free_stateid(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_backchannel_ctl(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_bind_conn_to_session(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_secinfo(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_secinfo_no_name(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_allocate(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_deallocate(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_seek(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_read_plus(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_io_advise(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_write_same(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_clone(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

/* pNFS (NFSv4.1 file layout) operations — see nfs4_pnfs.c */
void
chimera_nfs4_getdeviceinfo(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_getdevicelist(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_layoutget(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_layoutreturn(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_layoutcommit(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_layoutstats(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_layouterror(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_getxattr(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_setxattr(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_listxattrs(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_removexattr(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_lock(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_lockt(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_locku(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop);

void
chimera_nfs4_compound_process(
    struct nfs_request *req,
    nfsstat4            status);

/* Validate that [p, p+len) is well-formed UTF-8 (RFC 3629): rejects stray
 * continuation bytes, overlong encodings, UTF-16 surrogates, the U+FFFE/FFFF
 * non-characters, and anything above U+10FFFF. Returns true if well-formed. */
/*
 * NFS4.1 "current stateid" (RFC 8881 §16.2.3.1.2).  The special value
 * { seqid = 1, other = all zeros } means "use the COMPOUND's current stateid",
 * which stateid-returning operations (OPEN, LOCK, OPEN_DOWNGRADE, ...) set.
 */
static inline bool
chimera_nfs4_stateid_is_current(const struct stateid4 *sid)
{
    static const uint8_t zero[NFS4_OTHER_SIZE] = { 0 };

    return sid->seqid == 1 &&
           memcmp(sid->other, zero, NFS4_OTHER_SIZE) == 0;
} /* chimera_nfs4_stateid_is_current */

/* Record the current stateid produced by a stateid-returning op (4.1+). */
static inline void
chimera_nfs4_set_current_stateid(
    struct nfs_request    *req,
    const struct stateid4 *sid)
{
    if (req->minorversion >= 1) {
        req->current_stateid       = *sid;
        req->current_stateid_valid = true;
    }
} /* chimera_nfs4_set_current_stateid */

/* Clear the COMPOUND's current stateid -- done by ops that change the current
 * filehandle (RFC 8881 §16.2.3.1.2). */
static inline void
chimera_nfs4_clear_current_stateid(struct nfs_request *req)
{
    req->current_stateid_valid = false;
} /* chimera_nfs4_clear_current_stateid */

/* If `sid` is the special current-stateid value, replace it in place with the
 * COMPOUND's current stateid (4.1+).  No-op otherwise. */
static inline void
chimera_nfs4_resolve_current_stateid(
    struct nfs_request *req,
    struct stateid4    *sid)
{
    if (req->minorversion >= 1 &&
        req->current_stateid_valid &&
        chimera_nfs4_stateid_is_current(sid)) {
        *sid = req->current_stateid;
    }
} /* chimera_nfs4_resolve_current_stateid */

static inline bool
chimera_nfs4_utf8_valid(
    const char *data,
    uint32_t    len)
{
    const unsigned char *p = (const unsigned char *) data;
    uint32_t             i;

    for (i = 0; i < len; ) {
        unsigned char c     = p[i];
        uint32_t      extra = 0;

        if (c < 0x80) {
            i++;
            continue;
        } else if (c < 0xC2) {
            /* 0x80-0xBF: stray continuation byte; 0xC0-0xC1: overlong encoding */
            return false;
        } else if (c < 0xE0) {
            extra = 1;
        } else if (c < 0xF0) {
            unsigned char c2;
            /* 3-byte sequence: need at least one more byte to range-check */
            if (i + 1 >= len || (p[i + 1] & 0xC0) != 0x80) {
                return false;
            }
            c2    = p[i + 1];
            extra = 2;
            /* 0xE0 0x80-0x9F: overlong (encodes U+0000-U+07FF in 3 bytes) */
            if (c == 0xE0 && c2 < 0xA0) {
                return false;
            }
            /* 0xED 0xA0-0xBF: surrogate (U+D800-U+DFFF) */
            if (c == 0xED && c2 >= 0xA0) {
                return false;
            }
            /* 0xEF 0xBF 0xBE/0xBF: U+FFFE and U+FFFF are non-characters */
            if (c == 0xEF && c2 == 0xBF && i + 2 < len) {
                unsigned char c3 = p[i + 2];
                if (c3 == 0xBE || c3 == 0xBF) {
                    return false;
                }
            }
        } else if (c <= 0xF4) {
            unsigned char c2;
            /* 4-byte sequence: need at least one more byte to range-check */
            if (i + 1 >= len || (p[i + 1] & 0xC0) != 0x80) {
                return false;
            }
            c2    = p[i + 1];
            extra = 3;
            /* 0xF0 0x80-0x8F: overlong (encodes U+0000-U+FFFF in 4 bytes) */
            if (c == 0xF0 && c2 < 0x90) {
                return false;
            }
            /* 0xF4 0x90-0xBF: above U+10FFFF */
            if (c == 0xF4 && c2 > 0x8F) {
                return false;
            }
        } else {
            return false;
        }

        i++;
        while (extra--) {
            if (i >= len || (p[i] & 0xC0) != 0x80) {
                return false;
            }
            i++;
        }
    }

    return true;
} /* chimera_nfs4_utf8_valid */

static inline nfsstat4
chimera_nfs4_validate_name(const xdr_opaque *name)
{
    const char *p = (const char *) name->data;
    uint32_t    i;

    if (name->len == 0) {
        return NFS4ERR_INVAL;
    }

    if (name->len > 255) {
        return NFS4ERR_NAMETOOLONG;
    }

    if ((name->len == 1 && p[0] == '.') ||
        (name->len == 2 && p[0] == '.' && p[1] == '.')) {
        return NFS4ERR_BADNAME;
    }

    /* A component name may not contain a path separator or NUL. */
    for (i = 0; i < name->len; i++) {
        if (p[i] == '/' || p[i] == '\0') {
            return NFS4ERR_BADCHAR;
        }
    }

    if (!chimera_nfs4_utf8_valid(p, name->len)) {
        return NFS4ERR_INVAL;
    }

    return NFS4_OK;
} /* chimera_nfs4_validate_name */

static inline void
chimera_nfs4_compound_complete(
    struct nfs_request *req,
    nfsstat4            status)
{
    struct chimera_server_nfs_thread *thread = req->thread;

    if (status != NFS4_OK) {
        req->res_compound.status = status;

        /* Set the status for the failed operation */
        req->res_compound.resarray[req->index].opillegal.status = status;

        /* XDR unmarshall of any WRITE4args in the compound clones the data
         * iovecs (via xdr_iovec_copy_private / evpl_iovec_clone), taking a
         * +1 refcount that is normally dropped by chimera_nfs4_write_complete.
         * When a prior op fails and the compound is truncated, those
         * WRITE ops never dispatch and their clones would leak.  Release
         * the unmarshalled iovecs of every WRITE past the failed op. */
        for (uint32_t i = req->index + 1; i < req->args_compound->num_argarray;
             i++) {
            struct nfs_argop4 *ap = &req->args_compound->argarray[i];
            if (ap->argop == OP_WRITE && ap->opwrite.data.niov) {
                evpl_iovecs_release(thread->evpl,
                                    ap->opwrite.data.iov,
                                    ap->opwrite.data.niov);
                ap->opwrite.data.niov = 0;
            }
        }

        /* Per RFC 7530 section 15.2, the response must only include
         * operations up to and including the one that failed. Truncate
         * num_resarray so that the XDR encoder does not attempt to
         * serialize subsequent entries whose resop discriminant is
         * uninitialized, which produces a malformed response. */
        req->res_compound.num_resarray = req->index + 1;
        req->index                     = req->res_compound.num_resarray;
    }

    if (thread->active) {
        thread->again = 1;
    } else {
        req->index++;
        chimera_nfs4_compound_process(req, status);
    }

} /* chimera_nfs4_compound_complete */

void
chimera_nfs4_null(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void
chimera_nfs4_compound(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct COMPOUND4args      *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);
