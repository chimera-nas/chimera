// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#include <openssl/evp.h>
#include <openssl/core_names.h>
#include <openssl/params.h>
#include <openssl/kdf.h>

#include "smb.h"
#include "smb_internal.h"
#include "common/macros.h"
#include "common/tcp_flavor.h"
#include "evpl/evpl.h"

static const uint8_t SMB2_PROTOCOL_ID[4] = { 0xFE, 'S', 'M', 'B' };

/* ---- SMB3 signing primitives ------------------------------------------- *
 *
 * These mirror the server's src/server/smb/smb_signing.c exactly so the two
 * sides compute identical MACs.  The client signs a single CONTIGUOUS SMB2
 * message in place (one iovec from pdu_begin) and verifies a single contiguous
 * received message, so we operate on a flat (hdr, body) buffer rather than the
 * server's compound/iovec-cursor machinery.  Same KDF, same fields zeroed.
 */

void
chimera_smb_client_preauth_extend(
    uint8_t    *hash,
    const void *msg,
    uint32_t    msg_len)
{
    EVP_MD_CTX  *md      = EVP_MD_CTX_new();
    unsigned int out_len = 0;

    chimera_smbclient_abort_if(!md, "EVP_MD_CTX_new failed");

    if (EVP_DigestInit_ex(md, EVP_sha512(), NULL) != 1 ||
        EVP_DigestUpdate(md, hash, SMB2_PREAUTH_HASH_SIZE) != 1 ||
        EVP_DigestUpdate(md, msg, msg_len) != 1 ||
        EVP_DigestFinal_ex(md, hash, &out_len) != 1) {
        chimera_smbclient_fatal("SHA-512 preauth hash update failed");
    }
    EVP_MD_CTX_free(md);
} /* chimera_smb_client_preauth_extend */

/* SP800-108 counter-mode KDF with HMAC-SHA256 (OpenSSL KBKDF).  Identical to
 * the server's kdf_counter_hmac_sha256_ossl3: USE_L + USE_SEPARATOR on, label
 * passed as SALT, context as INFO. */
static int
chimera_smb_client_kbkdf(
    const uint8_t *key,
    size_t         key_len,
    const void    *label,
    size_t         label_len,
    const uint8_t *context,
    size_t         ctx_len,
    uint8_t       *out,
    size_t         out_len)
{
    EVP_KDF     *kdf = EVP_KDF_fetch(NULL, "KBKDF", NULL);
    EVP_KDF_CTX *kctx;
    OSSL_PARAM   params[10];
    size_t       n       = 0;
    int          use_l   = 1;
    int          use_sep = 1;
    int          ok      = 0;

    if (!kdf) {
        return -1;
    }
    kctx = EVP_KDF_CTX_new(kdf);
    EVP_KDF_free(kdf);
    if (!kctx) {
        return -1;
    }

    params[n++] = OSSL_PARAM_construct_utf8_string(OSSL_KDF_PARAM_MODE, (char *) "counter", 0);
    params[n++] = OSSL_PARAM_construct_utf8_string(OSSL_KDF_PARAM_MAC, (char *) "HMAC", 0);
    params[n++] = OSSL_PARAM_construct_utf8_string(OSSL_KDF_PARAM_DIGEST, (char *) "SHA256", 0);
    params[n++] = OSSL_PARAM_construct_octet_string(OSSL_KDF_PARAM_KEY, (void *) key, key_len);
    if (label && label_len) {
        params[n++] = OSSL_PARAM_construct_octet_string(OSSL_KDF_PARAM_SALT, (void *) label, label_len);
    }
    if (context && ctx_len) {
        params[n++] = OSSL_PARAM_construct_octet_string(OSSL_KDF_PARAM_INFO, (void *) context, ctx_len);
    }
    params[n++] = OSSL_PARAM_construct_int(OSSL_KDF_PARAM_KBKDF_USE_L, &use_l);
    params[n++] = OSSL_PARAM_construct_int(OSSL_KDF_PARAM_KBKDF_USE_SEPARATOR, &use_sep);
    params[n++] = OSSL_PARAM_construct_end();

    if (EVP_KDF_derive(kctx, out, out_len, params) == 1) {
        ok = 1;
    }
    EVP_KDF_CTX_free(kctx);
    return ok ? 0 : -1;
} /* chimera_smb_client_kbkdf */

int
chimera_smb_client_derive_signing_key(
    uint16_t       dialect,
    const uint8_t *session_key,
    size_t         session_key_len,
    const uint8_t *preauth_hash,
    uint8_t       *out_key16)
{
    static const char label30[]  = "SMB2AESCMAC";   /* include NUL per spec */
    static const char ctx30[]    = "SmbSign";       /* include NUL per spec */
    static const char label311[] = "SMBSigningKey"; /* include NUL per spec */

    switch (dialect) {
        case SMB2_DIALECT_2_0_2:
        case SMB2_DIALECT_2_1:
            /* 2.x signs with the raw session key (HMAC-SHA256); no KDF. */
            if (session_key_len < 16) {
                return -1;
            }
            memcpy(out_key16, session_key, 16);
            return 0;
        case SMB2_DIALECT_3_0:
        case SMB2_DIALECT_3_0_2:
            return chimera_smb_client_kbkdf(session_key, session_key_len,
                                            label30, sizeof(label30),
                                            (const uint8_t *) ctx30, sizeof(ctx30),
                                            out_key16, 16);
        case SMB2_DIALECT_3_1_1:
            if (!preauth_hash) {
                return -1;
            }
            return chimera_smb_client_kbkdf(session_key, session_key_len,
                                            label311, sizeof(label311),
                                            preauth_hash, SMB2_PREAUTH_HASH_SIZE,
                                            out_key16, 16);
        default:
            return -1;
    } /* switch */
} /* chimera_smb_client_derive_signing_key */

/* HMAC-SHA256 (2.x / 3.1.1-HMAC) over hdr||body, first 16 bytes -> out_sig16. */
static int
chimera_smb_client_hmac_sha256(
    const struct smb2_header *hdr,
    const uint8_t            *body,
    int                       body_len,
    const uint8_t            *key,
    uint8_t                  *out_sig16)
{
    EVP_MAC      *mac;
    EVP_MAC_CTX  *mctx;
    OSSL_PARAM    params[] = {
        OSSL_PARAM_construct_utf8_string(OSSL_MAC_PARAM_DIGEST, (char *) "SHA256", 0),
        OSSL_PARAM_construct_end()
    };
    unsigned char macbuf[32];
    size_t        maclen = 0;
    int           rc     = -1;

    mac = EVP_MAC_fetch(NULL, "HMAC", NULL);
    if (!mac) {
        return -1;
    }
    mctx = EVP_MAC_CTX_new(mac);
    EVP_MAC_free(mac);
    if (!mctx) {
        return -1;
    }
    if (EVP_MAC_init(mctx, key, 16, params) == 1 &&
        EVP_MAC_update(mctx, (const uint8_t *) hdr, sizeof(*hdr)) == 1 &&
        (body_len == 0 || EVP_MAC_update(mctx, body, body_len) == 1) &&
        EVP_MAC_final(mctx, macbuf, &maclen, sizeof(macbuf)) == 1 &&
        maclen >= 16) {
        memcpy(out_sig16, macbuf, 16);
        rc = 0;
    }
    EVP_MAC_CTX_free(mctx);
    return rc;
} /* chimera_smb_client_hmac_sha256 */

/* AES-128-CMAC (3.0 / 3.0.2 / 3.1.1-CMAC) over hdr||body -> out_sig16. */
static int
chimera_smb_client_cmac_aes128(
    const struct smb2_header *hdr,
    const uint8_t            *body,
    int                       body_len,
    const uint8_t            *key,
    uint8_t                  *out_sig16)
{
    EVP_MAC     *mac;
    EVP_MAC_CTX *mctx;
    OSSL_PARAM   params[] = {
        OSSL_PARAM_construct_utf8_string(OSSL_MAC_PARAM_CIPHER, (char *) "AES-128-CBC", 0),
        OSSL_PARAM_construct_end()
    };
    size_t       maclen = 0;
    int          rc     = -1;

    mac = EVP_MAC_fetch(NULL, "CMAC", NULL);
    if (!mac) {
        return -1;
    }
    mctx = EVP_MAC_CTX_new(mac);
    EVP_MAC_free(mac);
    if (!mctx) {
        return -1;
    }
    if (EVP_MAC_init(mctx, key, 16, params) == 1 &&
        EVP_MAC_update(mctx, (const uint8_t *) hdr, sizeof(*hdr)) == 1 &&
        (body_len == 0 || EVP_MAC_update(mctx, body, body_len) == 1) &&
        EVP_MAC_final(mctx, out_sig16, &maclen, 16) == 1 &&
        maclen == 16) {
        rc = 0;
    }
    EVP_MAC_CTX_free(mctx);
    return rc;
} /* chimera_smb_client_cmac_aes128 */

/* AES-128-GMAC (3.1.1-GMAC) over hdr||body -> out_sig16 (MS-SMB2 §3.1.4.1).
 * The 12-byte IV is MessageId(8) || flags-derived(4): SERVER_TO_REDIR (+ASYNC
 * for CANCEL).  AAD = full 64-byte header (signature zeroed) followed by body;
 * the 16-byte GCM tag is the signature.  Mirrors the server exactly. */
static int
chimera_smb_client_gmac_aes128(
    const struct smb2_header *hdr,
    const uint8_t            *body,
    int                       body_len,
    const uint8_t            *key,
    uint8_t                  *out_sig16)
{
    EVP_CIPHER     *gcm;
    EVP_CIPHER_CTX *c;
    uint8_t         iv[12];
    uint32_t        high_bits;
    int             outl;
    int             rc = -1;

    high_bits = hdr->flags & SMB2_FLAGS_SERVER_TO_REDIR;
    if (hdr->command == SMB2_CANCEL) {
        high_bits |= SMB2_FLAGS_ASYNC_COMMAND;
    }

    memset(iv, 0, sizeof(iv));
    memcpy(iv, &hdr->message_id, 8);
    iv[8]  = (uint8_t) (high_bits & 0xff);
    iv[9]  = (uint8_t) ((high_bits >> 8) & 0xff);
    iv[10] = (uint8_t) ((high_bits >> 16) & 0xff);
    iv[11] = (uint8_t) ((high_bits >> 24) & 0xff);

    gcm = EVP_CIPHER_fetch(NULL, "AES-128-GCM", NULL);
    if (!gcm) {
        return -1;
    }
    c = EVP_CIPHER_CTX_new();
    if (!c) {
        EVP_CIPHER_free(gcm);
        return -1;
    }
    if (EVP_EncryptInit_ex(c, gcm, NULL, NULL, NULL) == 1 &&
        EVP_CIPHER_CTX_ctrl(c, EVP_CTRL_GCM_SET_IVLEN, sizeof(iv), NULL) == 1 &&
        EVP_EncryptInit_ex(c, NULL, NULL, key, iv) == 1 &&
        EVP_EncryptUpdate(c, NULL, &outl, (const uint8_t *) hdr, sizeof(*hdr)) == 1 &&
        (body_len == 0 || EVP_EncryptUpdate(c, NULL, &outl, body, body_len) == 1) &&
        EVP_EncryptFinal_ex(c, NULL, &outl) == 1 &&
        EVP_CIPHER_CTX_ctrl(c, EVP_CTRL_GCM_GET_TAG, 16, out_sig16) == 1) {
        rc = 0;
    }
    EVP_CIPHER_CTX_free(c);
    EVP_CIPHER_free(gcm);
    return rc;
} /* chimera_smb_client_gmac_aes128 */

/* Dispatch to the negotiated algorithm.  `dialect`/`signing_alg` come from the
 * shared session (server struct).  Computes over hdr (signature must already be
 * zeroed by the caller) plus body. */
static int
chimera_smb_client_compute_signature(
    uint16_t                  dialect,
    uint16_t                  signing_alg,
    const struct smb2_header *hdr,
    const uint8_t            *body,
    int                       body_len,
    const uint8_t            *key,
    uint8_t                  *out_sig16)
{
    switch (dialect) {
        case SMB2_DIALECT_2_0_2:
        case SMB2_DIALECT_2_1:
            return chimera_smb_client_hmac_sha256(hdr, body, body_len, key, out_sig16);
        case SMB2_DIALECT_3_0:
        case SMB2_DIALECT_3_0_2:
            return chimera_smb_client_cmac_aes128(hdr, body, body_len, key, out_sig16);
        case SMB2_DIALECT_3_1_1:
            switch (signing_alg) {
                case SMB2_SIGNING_AES_GMAC:
                    return chimera_smb_client_gmac_aes128(hdr, body, body_len, key, out_sig16);
                case SMB2_SIGNING_HMAC_SHA256:
                    return chimera_smb_client_hmac_sha256(hdr, body, body_len, key, out_sig16);
                default: /* SMB2_SIGNING_AES_CMAC */
                    return chimera_smb_client_cmac_aes128(hdr, body, body_len, key, out_sig16);
            } /* switch */
        default:
            return -1;
    } /* switch */
} /* chimera_smb_client_compute_signature */

/* Sign a contiguous SMB2 message in place: set SMB2_FLAGS_SIGNED, zero the
 * signature field, compute the MAC over the message, write it back. */
static int
chimera_smb_client_sign_inplace(
    uint16_t            dialect,
    uint16_t            signing_alg,
    const uint8_t      *key,
    struct smb2_header *hdr,
    int                 smb2_len)
{
    uint8_t signature[16];
    int     body_len = smb2_len - (int) sizeof(*hdr);
    int     rc;

    if (body_len < 0) {
        return -1;
    }
    hdr->flags |= SMB2_FLAGS_SIGNED;
    memset(hdr->signature, 0, sizeof(hdr->signature));

    rc = chimera_smb_client_compute_signature(dialect, signing_alg, hdr,
                                              (const uint8_t *) (hdr + 1), body_len,
                                              key, signature);
    if (rc != 0) {
        return rc;
    }
    memcpy(hdr->signature, signature, sizeof(signature));
    return 0;
} /* chimera_smb_client_sign_inplace */

/* Verify a contiguous received SMB2 message.  `hdr` points at a private copy of
 * the 64-byte header (so we can zero its signature without touching the wire
 * buffer); `body` is the message body.  Returns 0 if the signature matches. */
static int
chimera_smb_client_verify(
    uint16_t            dialect,
    uint16_t            signing_alg,
    const uint8_t      *key,
    struct smb2_header *hdr,
    const uint8_t      *body,
    int                 body_len)
{
    uint8_t received[16];
    uint8_t calculated[16];
    int     rc;

    memcpy(received, hdr->signature, sizeof(received));
    memset(hdr->signature, 0, sizeof(hdr->signature));

    rc = chimera_smb_client_compute_signature(dialect, signing_alg, hdr,
                                              body, body_len, key, calculated);
    if (rc != 0) {
        return rc;
    }
    return memcmp(received, calculated, sizeof(received)) == 0 ? 0 : -1;
} /* chimera_smb_client_verify */

/* ---- module lifecycle -------------------------------------------------- */

static void *
chimera_smb_client_init(
    const char                *cfgdata,
    struct prometheus_metrics *metrics)
{
    struct chimera_smb_client_shared *shared = calloc(1, sizeof(*shared));

    (void) cfgdata;
    (void) metrics;

    pthread_mutex_init(&shared->lock, NULL);

    shared->max_servers  = CHIMERA_SMB_CLIENT_MAX_SERVERS;
    shared->servers      = calloc(shared->max_servers, sizeof(*shared->servers));
    shared->tcp_protocol = EVPL_STREAM_SOCKET_TCP;

    return shared;
} /* chimera_smb_client_init */

static void
chimera_smb_client_destroy(void *private_data)
{
    struct chimera_smb_client_shared *shared = private_data;
    int                               i;

    for (i = 0; i < shared->max_servers; i++) {
        if (shared->servers[i]) {
            if (shared->servers[i]->endpoint) {
                evpl_endpoint_close(shared->servers[i]->endpoint);
            }
            free(shared->servers[i]);
        }
    }

    pthread_mutex_destroy(&shared->lock);
    free(shared->servers);
    free(shared);
} /* chimera_smb_client_destroy */

static void *
chimera_smb_client_thread_init(
    struct evpl *evpl,
    void        *private_data)
{
    struct chimera_smb_client_shared *shared = private_data;
    struct chimera_smb_client_thread *thread = calloc(1, sizeof(*thread));

    thread->evpl      = evpl;
    thread->shared    = shared;
    thread->max_conns = shared->max_servers;
    thread->conns     = calloc(thread->max_conns, sizeof(*thread->conns));

    return thread;
} /* chimera_smb_client_thread_init */

static void
chimera_smb_client_thread_destroy(void *private_data)
{
    struct chimera_smb_client_thread *thread = private_data;
    struct chimera_smb_client_conn   *conn;

    /* Detach every connection from this thread and close any still-open binds.
     * The conns are NOT freed here: their DISCONNECTED notify (which frees them)
     * is delivered later by evpl_destroy, after this thread struct is gone -- so
     * we null conn->thread to keep that notify from dereferencing freed state. */
    for (conn = thread->conns_list; conn; conn = conn->list_next) {
        conn->thread = NULL;
        if (conn->bind && !conn->closing) {
            conn->closing = 1;
            evpl_close(thread->evpl, conn->bind);
        }
    }

    free(thread->conns);
    free(thread);
} /* chimera_smb_client_thread_destroy */

/* ---- transport --------------------------------------------------------- */

void
chimera_smb_client_pdu_begin(
    struct chimera_smb_client_conn *conn,
    uint16_t                        command,
    struct evpl_iovec              *iov,
    struct evpl_iovec_cursor       *cursor,
    struct smb2_header            **hdr)
{
    struct smb2_header *h;

    evpl_iovec_alloc(conn->evpl, 65536, 8, 1, 0, iov);

    evpl_iovec_cursor_init(cursor, iov, 1);

    /* Reserve the NetBIOS framing prefix, then make the SMB2 header the origin
     * for the consumed-relative field alignment (matches the server). */
    evpl_iovec_cursor_skip(cursor, sizeof(struct smb_client_netbios_header));
    evpl_iovec_cursor_reset_consumed(cursor);

    h = evpl_iovec_cursor_data(cursor);
    evpl_iovec_cursor_skip(cursor, sizeof(struct smb2_header));

    memset(h, 0, sizeof(*h));
    memcpy(h->protocol_id, SMB2_PROTOCOL_ID, 4);
    h->struct_size             = 64;
    h->credit_charge           = 1;
    h->status                  = 0;
    h->command                 = command;
    h->credit_request_response = 256;
    h->flags                   = 0;
    h->next_command            = 0;
    h->message_id              = conn->next_message_id++;
    h->sync.process_id         = 0;
    h->sync.tree_id            = conn->server->tree_id;
    h->session_id              = conn->server->session_id;

    *hdr = h;
} /* chimera_smb_client_pdu_begin */

/* Sign (if the session has signing on), frame with the NetBIOS length, and send
 * a finished SMB2 PDU.  Shared by pdu_finish and server-initiated replies (the
 * OPLOCK_BREAK ack) that send without registering a pending reply. */
static void
chimera_smb_client_sign_frame_send(
    struct chimera_smb_client_conn *conn,
    struct evpl_iovec              *iov,
    int                             smb2_len)
{
    struct smb_client_netbios_header *netbios = evpl_iovec_data(iov);
    struct smb2_header               *hdr     = (struct smb2_header *) (netbios + 1);
    int                               total   = smb2_len + (int) sizeof(*netbios);

    /* Sign the outgoing PDU once a signing key exists for the session (3.x with
     * signing on).  NEGOTIATE and SESSION_SETUP are never signed: they run
     * before the key is derived.  The 2.x unsigned path leaves signing_active
     * clear and skips this entirely. */
    if (conn->server->signing_active &&
        hdr->command != SMB2_NEGOTIATE &&
        hdr->command != SMB2_SESSION_SETUP) {
        if (chimera_smb_client_sign_inplace(conn->server->dialect,
                                            conn->server->signing_alg,
                                            conn->server->signing_key,
                                            hdr, smb2_len) != 0) {
            chimera_smbclient_error("Failed to sign outgoing SMB2 PDU (command %u)", hdr->command);
        }
    }

    netbios->word = __builtin_bswap32((uint32_t) smb2_len);

    evpl_iovec_set_length(iov, total);

    evpl_sendv(conn->evpl, conn->bind, iov, 1, total, EVPL_SEND_FLAG_TAKE_REF);
} /* chimera_smb_client_sign_frame_send */

void
chimera_smb_client_pdu_finish(
    struct chimera_smb_client_conn *conn,
    struct evpl_iovec              *iov,
    struct evpl_iovec_cursor       *cursor,
    struct chimera_vfs_request     *request,
    chimera_smb_client_reply_cb     reply_cb,
    void                           *reply_arg)
{
    struct smb_client_netbios_header  *netbios = evpl_iovec_data(iov);
    struct smb2_header                *hdr     = (struct smb2_header *) (netbios + 1);
    struct chimera_smb_client_pending *pending;
    int                                smb2_len = evpl_iovec_cursor_consumed(cursor);

    pending             = calloc(1, sizeof(*pending));
    pending->message_id = hdr->message_id;
    pending->cb         = reply_cb;
    pending->arg        = reply_arg;
    pending->request    = request;
    pending->next       = conn->pending;
    conn->pending       = pending;

    chimera_smb_client_sign_frame_send(conn, iov, smb2_len);
} /* chimera_smb_client_pdu_finish */

/* Server-initiated SMB2 OPLOCK_BREAK (lease-break notification): acknowledge it
 * so the server can complete the conflicting open.  The client holds no client-
 * side cache yet, so it simply concedes to the server's proposed NewLeaseState.
 * (When the break does not require an ack, nothing is sent.) */
static void
chimera_smb_client_handle_oplock_break(
    struct chimera_smb_client_conn *conn,
    struct evpl_iovec_cursor       *body)
{
    struct evpl_iovec        iov;
    struct evpl_iovec_cursor cursor;
    struct smb2_header      *hdr;
    uint16_t                 struct_size, new_epoch;
    uint32_t                 flags, current_state, new_state;
    uint8_t                  lease_key[16];
    /* MS-SMB2 2.2.23.2 Flags bit (not exported in a shared header). */
    const uint32_t           ACK_REQUIRED = 0x01;

    evpl_iovec_cursor_get_uint16(body, &struct_size);

    /* Only lease-variant breaks are expected (the client requests leases, not
     * legacy oplocks); ignore anything else. */
    if (struct_size != SMB2_OPLOCK_BREAK_NOTIFY_LEASE_SIZE) {
        return;
    }

    evpl_iovec_cursor_get_uint16(body, &new_epoch);
    evpl_iovec_cursor_get_uint32(body, &flags);
    evpl_iovec_cursor_copy(body, lease_key, 16);
    evpl_iovec_cursor_get_uint32(body, &current_state);
    evpl_iovec_cursor_get_uint32(body, &new_state);

    (void) new_epoch;
    (void) current_state;

    if (!(flags & ACK_REQUIRED)) {
        return;
    }

    /* Lease-break ACK (MS-SMB2 2.2.24.2): StructureSize(36), Reserved(2),
     * Flags(4), LeaseKey(16), LeaseState(4), LeaseDuration(8). */
    chimera_smb_client_pdu_begin(conn, SMB2_OPLOCK_BREAK, &iov, &cursor, &hdr);

    evpl_iovec_cursor_append_uint16(&cursor, SMB2_OPLOCK_BREAK_ACK_LEASE_SIZE);
    evpl_iovec_cursor_append_uint16(&cursor, 0);             /* Reserved */
    evpl_iovec_cursor_append_uint32(&cursor, 0);             /* Flags */
    evpl_iovec_cursor_append_blob(&cursor, lease_key, 16);
    evpl_iovec_cursor_append_uint32(&cursor, new_state);     /* concede to NewLeaseState */
    evpl_iovec_cursor_append_uint64(&cursor, 0);             /* LeaseDuration */

    chimera_smb_client_sign_frame_send(conn, &iov, evpl_iovec_cursor_consumed(&cursor));
} /* chimera_smb_client_handle_oplock_break */

static struct chimera_smb_client_pending *
chimera_smb_client_pending_take(
    struct chimera_smb_client_conn *conn,
    uint64_t                        message_id)
{
    struct chimera_smb_client_pending **pp = &conn->pending;
    struct chimera_smb_client_pending  *p;

    while ((p = *pp)) {
        if (p->message_id == message_id) {
            *pp = p->next;
            return p;
        }
        pp = &p->next;
    }
    return NULL;
} /* chimera_smb_client_pending_take */

static void
chimera_smb_client_handle_recv(
    struct chimera_smb_client_conn *conn,
    struct evpl_iovec              *iov,
    int                             niov,
    int                             length)
{
    struct evpl_iovec_cursor           cursor;
    struct smb_client_netbios_header   netbios;
    struct smb2_header                 hdr;
    struct chimera_smb_client_pending *pending;
    int                                body_len;

    if (length < (int) (sizeof(netbios) + sizeof(hdr))) {
        chimera_smbclient_error("Received SMB2 reply too short (%d bytes)", length);
        return;
    }

    evpl_iovec_cursor_init(&cursor, iov, niov);
    evpl_iovec_cursor_copy(&cursor, &netbios, sizeof(netbios));

    evpl_iovec_cursor_reset_consumed(&cursor);
    evpl_iovec_cursor_copy(&cursor, &hdr, sizeof(hdr));

    if (memcmp(hdr.protocol_id, SMB2_PROTOCOL_ID, 4) != 0) {
        chimera_smbclient_error("Received reply with invalid SMB2 protocol id");
        return;
    }

    body_len = length - (int) sizeof(netbios) - (int) sizeof(hdr);

    /* Verify the signature on a signed reply once the session has a signing key.
     * NEGOTIATE/SESSION_SETUP replies are exempt (the final SESSION_SETUP reply
     * is signed by the server before the client commits the key; mirrors the
     * server's verify-skip for these commands).  The cursor is currently
     * positioned at the body start (consumed == sizeof(hdr)); copy the body out
     * to a contiguous buffer for the MAC, then leave a fresh cursor for the cb. */
    if (conn->server->signing_active &&
        (hdr.flags & SMB2_FLAGS_SIGNED) &&
        hdr.command != SMB2_NEGOTIATE &&
        hdr.command != SMB2_SESSION_SETUP) {
        uint8_t *body = NULL;
        int      vrc;

        if (body_len > 0) {
            struct evpl_iovec_cursor body_cursor = cursor;
            body = malloc(body_len);
            if (!body) {
                chimera_smbclient_error("Out of memory verifying SMB2 signature");
                return;
            }
            evpl_iovec_cursor_copy(&body_cursor, body, body_len);
        }

        vrc = chimera_smb_client_verify(conn->server->dialect,
                                        conn->server->signing_alg,
                                        conn->server->signing_key,
                                        &hdr, body, body_len);
        free(body);

        if (vrc != 0) {
            chimera_smbclient_error("Received SMB2 reply with invalid signature "
                                    "(command %u, message_id %lu)", hdr.command, hdr.message_id);
            chimera_smb_client_conn_fail(conn, CHIMERA_VFS_EIO);
            return;
        }
    }

    /* A server-initiated OPLOCK_BREAK (lease break) is not a reply to any
     * request -- handle it directly rather than matching a pending message_id. */
    if (hdr.command == SMB2_OPLOCK_BREAK) {
        chimera_smb_client_handle_oplock_break(conn, &cursor);
        return;
    }

    pending = chimera_smb_client_pending_take(conn, hdr.message_id);
    if (!pending) {
        chimera_smbclient_error("Received SMB2 reply for unknown message_id %lu (command %u)",
                                hdr.message_id, hdr.command);
        return;
    }

    pending->cb(conn, hdr.status, &hdr, &cursor, body_len, pending->arg);

    free(pending);
} /* chimera_smb_client_handle_recv */

/* Error-complete every request associated with a connection (in-flight,
 * deferred, and the in-progress mount).  Does not close or free the conn. */
static void
chimera_smb_client_conn_drain(
    struct chimera_smb_client_conn *conn,
    enum chimera_vfs_error          status)
{
    struct chimera_smb_client_pending  *pending;
    struct chimera_smb_client_deferred *deferred;

    while ((pending = conn->pending)) {
        conn->pending = pending->next;
        if (pending->request) {
            pending->request->status = status;
            pending->request->complete(pending->request);
        }
        free(pending);
    }

    while ((deferred = conn->deferred)) {
        conn->deferred            = deferred->next;
        deferred->request->status = status;
        deferred->request->complete(deferred->request);
        free(deferred);
    }

    if (conn->mount_request) {
        struct chimera_vfs_request *request = conn->mount_request;
        conn->mount_request = NULL;
        request->status     = status;
        request->complete(request);
    }
} /* chimera_smb_client_conn_drain */

void
chimera_smb_client_conn_fail(
    struct chimera_smb_client_conn *conn,
    enum chimera_vfs_error          status)
{
    if (conn->state == CHIMERA_SMB_CONN_FAILED) {
        return;
    }
    conn->state = CHIMERA_SMB_CONN_FAILED;

    chimera_smb_client_conn_drain(conn, status);

    if (conn->bind && !conn->closing) {
        conn->closing = 1;
        evpl_close(conn->evpl, conn->bind);
    }
} /* chimera_smb_client_conn_fail */

void
chimera_smb_client_conn_ready(struct chimera_smb_client_conn *conn)
{
    struct chimera_smb_client_deferred *deferred;

    conn->state = CHIMERA_SMB_CONN_READY;

    while ((deferred = conn->deferred)) {
        conn->deferred = deferred->next;
        deferred->start(conn, deferred->request);
        free(deferred);
    }
} /* chimera_smb_client_conn_ready */

static void
chimera_smb_client_notify(
    struct evpl        *evpl,
    struct evpl_bind   *bind,
    struct evpl_notify *notify,
    void               *private_data)
{
    struct chimera_smb_client_conn *conn = private_data;
    int                             i;

    switch (notify->notify_type) {
        case EVPL_NOTIFY_CONNECTED:
            chimera_smb_client_conn_on_connected(conn);
            break;
        case EVPL_NOTIFY_DISCONNECTED:
            /* The bind is being destroyed; error-complete anything still
             * outstanding (no further close), then unlink and free the conn. */
            conn->bind = NULL;
            chimera_smb_client_conn_drain(conn, CHIMERA_VFS_EIO);

            if (conn->thread) {
                struct chimera_smb_client_thread *thread = conn->thread;
                struct chimera_smb_client_conn  **pp     = &thread->conns_list;

                while (*pp && *pp != conn) {
                    pp = &(*pp)->list_next;
                }
                if (*pp) {
                    *pp = conn->list_next;
                }
                if (thread->conns[conn->server->index] == conn) {
                    thread->conns[conn->server->index] = NULL;
                }
            }

            free(conn);
            break;
        case EVPL_NOTIFY_RECV_MSG:
            chimera_smb_client_handle_recv(conn,
                                           notify->recv_msg.iovec,
                                           notify->recv_msg.niov,
                                           notify->recv_msg.length);

            for (i = 0; i < (int) notify->recv_msg.niov; i++) {
                evpl_iovec_release(evpl, &notify->recv_msg.iovec[i]);
            }
            break;
        case EVPL_NOTIFY_SENT:
            break;
    } /* switch */
} /* chimera_smb_client_notify */

static int
chimera_smb_client_segment(
    struct evpl      *evpl,
    struct evpl_bind *bind,
    void             *private_data)
{
    uint32_t hdr;
    int      len;

    (void) private_data;

    len = evpl_peek(evpl, bind, &hdr, 4);
    if (len < 4) {
        return -1;
    }

    hdr  = __builtin_bswap32(hdr);
    hdr &= 0x00ffffff;

    return 4 + hdr;
} /* chimera_smb_client_segment */

struct evpl_bind *
chimera_smb_client_connect(
    struct chimera_smb_client_conn *conn,
    struct evpl_endpoint           *endpoint)
{
    return evpl_connect(conn->evpl,
                        conn->thread->shared->tcp_protocol,
                        NULL,
                        endpoint,
                        chimera_smb_client_notify,
                        chimera_smb_client_segment,
                        conn);
} /* chimera_smb_client_connect */

/* ---- per-thread connection management ---------------------------------- */

struct chimera_smb_client_conn *
chimera_smb_client_get_conn(
    struct chimera_smb_client_thread *thread,
    struct chimera_smb_client_server *server)
{
    struct chimera_smb_client_conn *conn = thread->conns[server->index];

    if (conn) {
        return conn;
    }

    conn         = calloc(1, sizeof(*conn));
    conn->thread = thread;
    conn->evpl   = thread->evpl;
    conn->server = server;
    conn->state  = CHIMERA_SMB_CONN_NEW;

    conn->list_next    = thread->conns_list;
    thread->conns_list = conn;

    thread->conns[server->index] = conn;

    return conn;
} /* chimera_smb_client_get_conn */

void
chimera_smb_client_ensure_ready(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request,
    chimera_smb_client_start_fn     start)
{
    struct chimera_smb_client_deferred *deferred;

    if (conn->state == CHIMERA_SMB_CONN_READY) {
        start(conn, request);
        return;
    }

    if (conn->state == CHIMERA_SMB_CONN_FAILED) {
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }

    /* NEW or CONNECTING: defer the op until the handshake completes. */
    deferred          = calloc(1, sizeof(*deferred));
    deferred->request = request;
    deferred->start   = start;
    deferred->next    = conn->deferred;
    conn->deferred    = deferred;

    if (conn->state == CHIMERA_SMB_CONN_NEW) {
        conn->state = CHIMERA_SMB_CONN_CONNECTING;
        conn->bind  = chimera_smb_client_connect(conn, conn->server->endpoint);
    }
} /* chimera_smb_client_ensure_ready */

/* ---- dispatch ---------------------------------------------------------- */

static void
chimera_smb_client_dispatch(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_smb_client_thread *thread = private_data;
    struct chimera_smb_client_shared *shared = thread->shared;
    struct chimera_smb_client_server *server;
    struct chimera_smb_client_conn   *conn;
    chimera_smb_client_start_fn       start;
    int                               server_index;

    switch (request->opcode) {
        case CHIMERA_VFS_OP_MOUNT:
            chimera_smb_client_mount(thread, request);
            return;
        case CHIMERA_VFS_OP_UMOUNT:
            chimera_smb_client_umount(thread, request);
            return;
        case CHIMERA_VFS_OP_CLOSE:
        {
            struct chimera_smb_client_open *open_state =
                (struct chimera_smb_client_open *) request->close.vfs_private;

            if (!open_state) {
                request->status = CHIMERA_VFS_OK;
                request->complete(request);
                return;
            }

            server_index = open_state->server_index;
            server       = (server_index >= 0 && server_index < shared->max_servers)
                     ? shared->servers[server_index] : NULL;
            conn = server ? thread->conns[server_index] : NULL;

            /* Only send a real CLOSE when a live session connection exists.  If
             * the session is gone (post-umount / disconnect), the server already
             * released the handle via LOGOFF, so just drop local state -- do not
             * reconnect solely to close. */
            if (server && server->session_ready && conn &&
                conn->state == CHIMERA_SMB_CONN_READY) {
                chimera_smb_client_close(conn, request);
            } else {
                free(open_state);
                request->status = CHIMERA_VFS_OK;
                request->complete(request);
            }
            return;
        }
        case CHIMERA_VFS_OP_GETATTR:
            server_index = chimera_smb_fh_server_index(request->fh);
            start        = chimera_smb_client_getattr;
            break;
        case CHIMERA_VFS_OP_LOOKUP_AT:
            server_index = chimera_smb_fh_server_index(request->fh);
            start        = chimera_smb_client_lookup_at;
            break;
        case CHIMERA_VFS_OP_OPEN_AT:
            server_index = chimera_smb_fh_server_index(request->fh);
            start        = chimera_smb_client_open_at;
            break;
        case CHIMERA_VFS_OP_OPEN_FH:
            server_index = chimera_smb_fh_server_index(request->fh);
            start        = chimera_smb_client_open_fh;
            break;
        case CHIMERA_VFS_OP_MKDIR_AT:
            server_index = chimera_smb_fh_server_index(request->fh);
            start        = chimera_smb_client_mkdir_at;
            break;
        case CHIMERA_VFS_OP_REMOVE_AT:
            server_index = chimera_smb_fh_server_index(request->fh);
            start        = chimera_smb_client_remove_at;
            break;
        case CHIMERA_VFS_OP_SETATTR:
            server_index = chimera_smb_fh_server_index(request->fh);
            start        = chimera_smb_client_setattr;
            break;
        case CHIMERA_VFS_OP_COMMIT:
            server_index = chimera_smb_fh_server_index(request->fh);
            start        = chimera_smb_client_commit;
            break;
        case CHIMERA_VFS_OP_READ:
            server_index = chimera_smb_fh_server_index(request->fh);
            start        = chimera_smb_client_read;
            break;
        case CHIMERA_VFS_OP_WRITE:
            server_index = chimera_smb_fh_server_index(request->fh);
            start        = chimera_smb_client_write;
            break;
        case CHIMERA_VFS_OP_READDIR:
            server_index = chimera_smb_fh_server_index(request->fh);
            start        = chimera_smb_client_readdir;
            break;
        case CHIMERA_VFS_OP_RENAME_AT:
            server_index = chimera_smb_fh_server_index(request->fh);
            start        = chimera_smb_client_rename_at;
            break;
        case CHIMERA_VFS_OP_SYMLINK_AT:
            server_index = chimera_smb_fh_server_index(request->fh);
            start        = chimera_smb_client_symlink_at;
            break;
        case CHIMERA_VFS_OP_MKNOD_AT:
            server_index = chimera_smb_fh_server_index(request->fh);
            start        = chimera_smb_client_mknod_at;
            break;
        case CHIMERA_VFS_OP_READLINK:
            server_index = chimera_smb_fh_server_index(request->fh);
            start        = chimera_smb_client_readlink;
            break;
        default:
            request->status = CHIMERA_VFS_ENOTSUP;
            request->complete(request);
            return;
    } /* switch */

    if (server_index < 0 || server_index >= shared->max_servers ||
        !shared->servers[server_index]) {
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    server = shared->servers[server_index];
    conn   = chimera_smb_client_get_conn(thread, server);

    chimera_smb_client_ensure_ready(conn, request, start);
} /* chimera_smb_client_dispatch */

SYMBOL_EXPORT struct chimera_vfs_module vfs_smb = {
    .name     = "smb",
    .fh_magic = CHIMERA_VFS_FH_MAGIC_SMB,
    /* Path-only backend: full mount-relative paths, opaque per-open handle
     * tokens, no FH-relative ops (no CAP_FS_RELATIVE_OP). */
    .capabilities   = CHIMERA_VFS_CAP_FS | CHIMERA_VFS_CAP_FS_PATH_OP |
        CHIMERA_VFS_CAP_OPEN_FILE_REQUIRED,
    .init           = chimera_smb_client_init,
    .destroy        = chimera_smb_client_destroy,
    .thread_init    = chimera_smb_client_thread_init,
    .thread_destroy = chimera_smb_client_thread_destroy,
    .dispatch       = chimera_smb_client_dispatch,
};
