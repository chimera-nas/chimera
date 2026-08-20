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
 *
 * tag_len is attacker-controlled, so the offset arithmetic is done in 64 bits.
 * In 32-bit arithmetic `4 + tag_len + pad` wraps for tag_len near UINT32_MAX --
 * 0xFFFFFFF5 through 0xFFFFFFF8 land on off == 0xFFFFFFFC, whose `off + 4` wraps
 * to 0 and sails through a 32-bit bounds check, reading ~4GB past the buffer.
 * Widening makes the comparison exact and the guard unconditional.
 */
bool
nfs4_v40_peek_minorversion(
    const xdr_iovec *iov,
    int              niov,
    uint32_t        *out_mv)
{
    const uint8_t *p;
    uint32_t       len, tag_len;
    uint64_t       off;

    if (niov < 1) {
        return false;
    }
    p   = xdr_iovec_data(&iov[0]);
    len = xdr_iovec_len(&iov[0]);
    if (len < 4) {
        return false;
    }
    tag_len = v40_be32(p);
    off     = 4ull + tag_len + ((4u - (tag_len & 3u)) & 3u); /* tag padded to 4 */
    if (off + 4ull > (uint64_t) len) {
        return false;  /* tag spans beyond the first iovec */
    }
    *out_mv = v40_be32(p + off);
    return true;
} /* nfs4_v40_peek_minorversion */

/*
 * Which minorversion-0 operations must not be re-executed on a retransmit.
 *
 * The NFSv3 path answers the same question per procedure in
 * nfs3_drc_proc_cacheable(); a COMPOUND has to be judged by the operations it
 * carries, so this is its equivalent.  Deliberate exclusions:
 *
 *   - Idempotent ops (LOOKUP, READDIR, GETATTR, READ, ACCESS, COMMIT, ...) are
 *     safe to re-execute, so they never justify an entry.  WRITE is excluded for
 *     the same reason and to match the v3 list: replaying the same bytes at the
 *     same offset is harmless.
 *   - The seqid-sequenced state operations (OPEN, CLOSE, LOCK, LOCKU,
 *     OPEN_CONFIRM, OPEN_DOWNGRADE) have their own, better replay cache: RFC 7530
 *     Section 9.1.7 keeps the last response per state-owner, which chimera
 *     implements (struct nfs4_replay_cache).  The RFC calls that "a more reliable
 *     cache of duplicate non-idempotent requests than that of the traditional
 *     cache", so covering them here as well would add nothing and only widen the
 *     window in which a reply can be replayed.
 *
 * This is not NFS4_OP_FLAG_MUTATES from nfs4_op_matrix.h: that flag answers a
 * different question (does the op need a writable export) and includes WRITE.
 */
static bool
nfs4_v40_op_cacheable(const struct nfs_argop4 *argop)
{
    switch (argop->argop) {
        case OP_CREATE:
        case OP_REMOVE:
        case OP_RENAME:
        case OP_LINK:
        case OP_SETATTR:
        case OP_SETCLIENTID:
        case OP_SETCLIENTID_CONFIRM:
            return true;
        case OP_OPENATTR:
            /* Only creates the named-attribute directory when asked to. */
            return argop->opopenattr.createdir != 0;
        default:
            return false;
    } /* switch */
} /* nfs4_v40_op_cacheable */

bool
nfs4_v40_drc_compound_cacheable(const struct COMPOUND4args *args)
{
    uint32_t i;

    for (i = 0; i < args->num_argarray; i++) {
        if (nfs4_v40_op_cacheable(&args->argarray[i])) {
            return true;
        }
    }
    return false;
} /* nfs4_v40_drc_compound_cacheable */

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
        !nfs4_v40_peek_minorversion(iov, niov, &mv) || mv != 0) {
        return drc->orig_dispatch(evpl, conn, encoding, proc, program_data,
                                  cred, iov, niov, length, private_data);
    }

    /* The lookup below runs for every 4.0 COMPOUND, read-only ones included,
     * because the operations are not known until the arguments are decoded.
     * That is safe: only a COMPOUND carrying a non-idempotent operation is ever
     * inserted (chimera_nfs4_compound cancels the capture otherwise, keyed on
     * nfs4_v40_drc_compound_cacheable), and the key covers a checksum of the
     * whole request, so a read-only COMPOUND's bytes cannot match a cached
     * mutating one's.  A read-only request therefore always misses and executes
     * for real. */
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

    /* The caller has already checked server.nfs4_drc; persist to the KV store
     * only when the backend can actually outlive the process. */
    drc->persistence_disabled = !persist || (strcmp(kvname, "memkv") == 0);

    drc->orig_dispatch                     = shared->nfs_v4.rpc2.recv_call_dispatch;
    shared->nfs_v4.rpc2.recv_call_dispatch = nfs4_v40_drc_dispatch;
} /* nfs4_v40_drc_install */
