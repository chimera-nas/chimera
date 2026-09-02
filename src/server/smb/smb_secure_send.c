// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * The server's half of SMB3 transport encryption: deciding WHICH reply gets
 * wrapped, and snapshotting the per-session state a deferred reply needs.  The
 * cryptography itself lives in src/smb_common/smb_encrypt.c, shared with the
 * client so both sides compute identical keys, nonces and tags.
 */

#include <string.h>
#include <stdlib.h>

#include "smb_common/smb_encrypt.h"
#include "smb_common/smb_signing.h"
#include "smb_internal.h"
#include "common/evpl_iovec_cursor.h"
#include "evpl/evpl.h"

void
chimera_smb_secure_send_snapshot(
    struct chimera_smb_request     *request,
    struct chimera_smb_secure_send *snap)
{
    struct chimera_smb_session *session = request->session_handle
        ? request->session_handle->session : NULL;
    struct chimera_smb_tree    *tree = request->tree;

    memset(snap, 0, sizeof(*snap));

    /* Encryption supersedes signing: when the session encrypts (global
     * EncryptData, a per-share encrypted tree, or the request itself arrived
     * inside a TRANSFORM) every response including this async one MUST be
     * encrypted (MS-SMB2 §3.3.4.1.4) rather than signed. */
    if (session && session->enc_key_len > 0 &&
        (request->compound->received_encrypted ||
         (session->flags & CHIMERA_SMB_SESSION_ENCRYPT_DATA) ||
         (tree && tree->share && tree->share->encrypt_data))) {
        snap->encrypt     = 1;
        snap->cipher_id   = session->cipher_id;
        snap->enc_key_len = session->enc_key_len;
        snap->session_id  = session->session_id;
        snap->enc_session = session;
        memcpy(snap->enc_key, session->enc_key, session->enc_key_len);
        return;
    }

    /* Otherwise sign iff the originating request was signed (an unsigned async
     * reply on a signed session is rejected by Windows clients). */
    if ((request->smb2_hdr.flags & SMB2_FLAGS_SIGNED) && request->session_handle) {
        snap->sign = 1;
        memcpy(snap->signing_key, request->session_handle->signing_key,
               sizeof(snap->signing_key));
    }
} /* chimera_smb_secure_send_snapshot */

void
chimera_smb_secure_send(
    struct chimera_smb_conn              *conn,
    struct evpl_iovec                    *iov,
    int                                   smb2_len,
    const struct chimera_smb_secure_send *snap)
{
    struct chimera_server_smb_thread *thread = conn->thread;
    struct evpl                      *evpl   = thread->evpl;

    if (snap->encrypt) {
        struct evpl_iovec      enc_iov;
        struct netbios_header *nb;
        uint64_t               nonce;
        int                    enc_total;

        /* Strictly-monotonic per-session nonce -- never reuse a nonce for a key
         * (GCM nonce reuse is catastrophic), shared across the session's
         * channels, same source the synchronous reply path uses. */
        nonce = atomic_fetch_add(&snap->enc_session->enc_nonce_counter, 1);

        if (chimera_smb_encrypt_compound(thread->encrypt_ctx, evpl,
                                         snap->cipher_id, snap->enc_key,
                                         snap->enc_key_len, nonce,
                                         snap->session_id, iov, 1, smb2_len,
                                         (int) sizeof(struct netbios_header),
                                         &enc_iov) != 0) {
            evpl_iovec_release(evpl, iov);
            evpl_close(evpl, conn->bind);
            return;
        }

        evpl_iovec_release(evpl, iov);

        /* The NetBIOS framing length excludes itself: TRANSFORM header + ct. */
        enc_total = (int) sizeof(struct smb2_transform_header) + smb2_len;
        nb        = evpl_iovec_data(&enc_iov);
        nb->word  = __builtin_bswap32((uint32_t) enc_total);

        evpl_sendv(evpl, conn->bind, &enc_iov, 1,
                   (int) sizeof(struct netbios_header) + enc_total,
                   EVPL_SEND_FLAG_TAKE_REF);
        return;
    }

    if (snap->sign) {
        chimera_smb_sign_message(thread->signing_ctx, conn->dialect,
                                 conn->negotiated.signing_alg, snap->signing_key,
                                 (uint8_t *) evpl_iovec_data(iov) +
                                 sizeof(struct netbios_header), smb2_len);
    }

    evpl_sendv(evpl, conn->bind, iov, 1,
               (int) sizeof(struct netbios_header) + smb2_len,
               EVPL_SEND_FLAG_TAKE_REF);
} /* chimera_smb_secure_send */
