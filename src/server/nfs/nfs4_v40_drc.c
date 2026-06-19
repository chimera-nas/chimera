// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdbool.h>
#include <stdint.h>
#include <string.h>

#include "nfs4_v40_drc.h"
#include "nfs3_drc.h"
#include "nfs_common.h"
#include "nfs_internal.h"
#include "nfs_kv_keys.h"
#include "vfs/vfs.h"
#include "evpl/evpl.h"
#include "evpl/evpl_rpc2.h"
#include "evpl/evpl_rpc2_program.h"

/* NFSv4 RPC procedure numbers. */
#define NFS4_PROC_COMPOUND 1u

static inline uint32_t
v40_be32(const uint8_t *p)
{
    return ((uint32_t) p[0] << 24) | ((uint32_t) p[1] << 16) |
           ((uint32_t) p[2] << 8) | (uint32_t) p[3];
} /* v40_be32 */

/*
 * Peek the COMPOUND4args minorversion without decoding the whole request.  Wire
 * layout: tag<opaque> (len + bytes padded to 4) then minorversion(u32).  The
 * compound header is virtually always within the first iovec on TCP; if the tag
 * runs past it, we cannot tell -- return false and let the request pass through
 * uncached (safe: a missed cache, never a wrong reply).
 */
static bool
v40_peek_minorversion(
    const xdr_iovec *iov,
    int              niov,
    uint32_t        *out_mv)
{
    const uint8_t *p;
    uint32_t       len, tag_len, off;

    if (niov < 1) {
        return false;
    }
    p   = xdr_iovec_data(&iov[0]);
    len = xdr_iovec_len(&iov[0]);
    if (len < 4) {
        return false;
    }
    tag_len = v40_be32(p);
    off     = 4 + tag_len + ((4 - (tag_len & 3)) & 3); /* tag padded to 4 */
    if (off + 4 > len) {
        return false;  /* tag spans beyond the first iovec */
    }
    *out_mv = v40_be32(p + off);
    return true;
} /* v40_peek_minorversion */

static int
nfs4_v40_drc_dispatch(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_encoding *encoding,
    uint32_t                   proc,
    void                      *program_data,
    struct evpl_rpc2_cred     *cred,
    xdr_iovec                 *iov,
    int                        niov,
    int                        length,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs3_drc                  *drc    = &thread->shared->v40_drc;
    struct nfs3_drc_keybuf            key;
    uint32_t                          mv;

    /* Only minorversion-0 COMPOUNDs over TCP use this cache.  NULL, RDMA, and
     * 4.1+ COMPOUNDs (the session reply cache covers those) pass through.  The
     * cached reply is the TCP on-wire form, so RDMA framing would not match. */
    if (conn->rdma || proc != NFS4_PROC_COMPOUND ||
        !v40_peek_minorversion(iov, niov, &mv) || mv != 0) {
        return drc->orig_dispatch(evpl, conn, encoding, proc, program_data,
                                  cred, iov, niov, length, private_data);
    }

    memset(&key, 0, sizeof(key));
    key.addr_len = nfs3_drc_client_addr(conn, key.addr);
    key.proc     = NFS4_PROC_COMPOUND;   /* constant for 4.0; cksum disambiguates */
    key.xid      = encoding->xid;
    key.cksum    = nfs3_drc_checksum_iov(iov, niov);

    return nfs3_drc_serve(drc, &key, evpl, conn, encoding, proc, program_data,
                          cred, iov, niov, length, private_data);
} /* nfs4_v40_drc_dispatch */

void
nfs4_v40_drc_install(
    struct chimera_server_nfs_shared *shared,
    int                               persist)
{
    struct nfs3_drc *drc    = &shared->v40_drc;
    const char      *kvname = (shared->vfs && shared->vfs->kv_module) ?
        shared->vfs->kv_module->name : "";

    /* In-memory cache is always on; persist only when nfs4_drc is enabled AND
     * the backend is persistent (mirrors the 4.1 reply cache). */
    drc->persistence_disabled = !persist || (strcmp(kvname, "memkv") == 0);

    drc->orig_dispatch                     = shared->nfs_v4.rpc2.recv_call_dispatch;
    shared->nfs_v4.rpc2.recv_call_dispatch = nfs4_v40_drc_dispatch;
} /* nfs4_v40_drc_install */
