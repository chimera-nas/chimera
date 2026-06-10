// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <gssapi/gssapi.h>
#include <gssapi/gssapi_krb5.h>
#include <openssl/hmac.h>


#include "smb.h"
#include "server/protocol.h"
#include "vfs/vfs.h"
#include "common/macros.h"
#include "common/misc.h"
#include "common/evpl_iovec_cursor.h"
#include "server/server.h"
#include "evpl/evpl.h"
#include "smb_internal.h"
#include "smb_async_interim.h"
#include "smb_wbclient.h"
#include "smb_procs.h"
#include "smb_notify.h"
#include "smb_dump.h"
#include "smb_signing.h"
#include "smb_encrypt.h"
#include "smb_compress.h"
#include "xxhash.h"

static const uint8_t SMB2_PROTOCOL_ID[4] = { 0xFE, 'S', 'M', 'B' };

/* STATUS_NOTIFY_ENUM_DIR is a CHANGE_NOTIFY-specific warning meaning
 * "client should rescan the directory".  It is NOT a real error and
 * MUST be returned with the regular CHANGE_NOTIFY response body, not
 * the SMB2 error body.  Treat it like SUCCESS for compound dispatch
 * and reply formatting. */
static inline int
chimera_smb_is_error_status(unsigned int status)
{
    return status != SMB2_STATUS_SUCCESS &&
           status != SMB2_STATUS_MORE_PROCESSING_REQUIRED &&
           status != SMB2_STATUS_NOTIFY_ENUM_DIR;
} /* chimera_smb_is_error_status */

static void *
chimera_smb_server_init(
    const struct chimera_server_config *config,
    struct chimera_vfs                 *vfs,
    struct prometheus_metrics          *metrics)
{
    struct chimera_server_smb_shared           *shared = calloc(1, sizeof(*shared));
    const struct chimera_server_config_smb_nic *smb_nic_info;
    int                                         i, rdma = 0;
    struct sockaddr_storage                    *ss;
    struct sockaddr_in                         *sin;
    struct sockaddr_in6                        *sin6;

    if (!shared) {
        return NULL;
    }

    shared->tcp_protocol = chimera_server_config_get_tcp_stream_protocol(config);

    shared->config.port      = 445;
    shared->config.rdma_port = 445;

    shared->config.capabilities = SMB2_GLOBAL_CAP_LARGE_MTU | SMB2_GLOBAL_CAP_MULTI_CHANNEL |
        SMB2_GLOBAL_CAP_LEASING;

    shared->config.num_dialects = chimera_server_config_get_smb_num_dialects(config);

    for (i = 0; i < shared->config.num_dialects; i++) {
        shared->config.dialects[i] = chimera_server_config_get_smb_dialects(config, i);
    }

    shared->config.num_nic_info = chimera_server_config_get_smb_num_nic_info(config);

    for (i = 0; i < shared->config.num_nic_info; i++) {
        smb_nic_info = chimera_server_config_get_smb_nic_info(config, i);

        ss = &shared->config.nic_info[i].addr;

        memset(ss, 0, sizeof(*ss));

        if (strchr(smb_nic_info->address, ':')) {
            sin6              = (struct sockaddr_in6 *) ss;
            sin6->sin6_family = AF_INET6;
            inet_pton(AF_INET6, smb_nic_info->address, &sin6->sin6_addr);
        } else {
            sin             = (struct sockaddr_in *) ss;
            sin->sin_family = AF_INET;
            inet_pton(AF_INET, smb_nic_info->address, &sin->sin_addr);
        }
        shared->config.nic_info[i].speed = smb_nic_info->speed * 1000000000UL;
        shared->config.nic_info[i].rdma  = smb_nic_info->rdma;

        if (shared->config.nic_info[i].rdma) {
            rdma = 1;
        }

        chimera_smb_info("SMB Multichannel: %s, speed: %llu, rdma: %d", smb_nic_info->address, smb_nic_info->speed,
                         smb_nic_info->rdma);
    }

    snprintf(shared->config.identity, sizeof(shared->config.identity), "chimera");

    // Copy SMB auth config from server config
    shared->config.auth.winbind_enabled  = chimera_server_config_get_smb_winbind_enabled(config);
    shared->config.auth.kerberos_enabled = chimera_server_config_get_smb_kerberos_enabled(config);

    const char *winbind_domain = chimera_server_config_get_smb_winbind_domain(config);
    if (winbind_domain && winbind_domain[0]) {
        strncpy(shared->config.auth.winbind_domain, winbind_domain,
                sizeof(shared->config.auth.winbind_domain) - 1);
    }

    const char *kerberos_keytab = chimera_server_config_get_smb_kerberos_keytab(config);
    if (kerberos_keytab && kerberos_keytab[0]) {
        strncpy(shared->config.auth.kerberos_keytab, kerberos_keytab,
                sizeof(shared->config.auth.kerberos_keytab) - 1);
    }

    const char *kerberos_realm = chimera_server_config_get_smb_kerberos_realm(config);
    if (kerberos_realm && kerberos_realm[0]) {
        strncpy(shared->config.auth.kerberos_realm, kerberos_realm,
                sizeof(shared->config.auth.kerberos_realm) - 1);
    }

    if (shared->config.auth.winbind_enabled) {
        chimera_smb_info("SMB Auth: Winbind integration enabled (domain: %s)",
                         shared->config.auth.winbind_domain[0] ? shared->config.auth.winbind_domain : "(not set)");

        /* Register winbind as an identity-resolver miss handler so the VFS can
         * resolve real AD SIDs <-> uids (and uid -> real SID) on demand, behind
         * the default NSS handler. */
        chimera_vfs_identity_register_handler(vfs, smb_wbclient_identity_handler,
                                              NULL);
    }
    if (shared->config.auth.kerberos_enabled) {
        chimera_smb_info("SMB Auth: Kerberos enabled (realm: %s, keytab: %s)",
                         shared->config.auth.kerberos_realm[0] ? shared->config.auth.kerberos_realm : "(not set)",
                         shared->config.auth.kerberos_keytab[0] ? shared->config.auth.kerberos_keytab : "(default)");
    }

    shared->config.soft_fail_bad_req          = chimera_server_config_get_soft_fail_bad_req(config);
    shared->config.persistent_handles         = chimera_server_config_get_smb_persistent_handles(config);
    shared->config.named_streams              = chimera_server_config_get_smb_named_streams(config);
    shared->config.signing_required           = chimera_server_config_get_smb_signing_required(config);
    shared->config.encryption                 = chimera_server_config_get_smb_encryption(config);
    shared->config.compression                = chimera_server_config_get_smb_compression(config);
    shared->config.leases                     = chimera_server_config_get_smb_leases(config);
    shared->config.oplocks                    = chimera_server_config_get_smb_oplocks(config);
    shared->config.notify_disabled            = chimera_server_config_get_smb_notify_disabled(config);
    shared->config.acl_inherited_canonicalize = chimera_server_config_get_smb_acl_inherited_canonicalize(config);

    if (shared->config.persistent_handles) {
        chimera_smb_info("SMB3 durable/persistent handles enabled (in-memory state)");
    }

    if (shared->config.named_streams) {
        chimera_smb_info("SMB named streams (ADS) enabled");
    }

    shared->vfs     = vfs;
    shared->metrics = metrics;

    *(XXH128_hash_t *) shared->guid = XXH3_128bits(shared->config.identity, strlen(shared->config.identity));

    shared->svc      = GSS_C_NO_NAME;
    shared->srv_cred = GSS_C_NO_CREDENTIAL;

    uint32_t        min;
    gss_buffer_desc name;

    name.value  = "cifs@10.67.25.209";
    name.length = strlen(name.value);

    gss_import_name(&min, &name, GSS_C_NT_HOSTBASED_SERVICE, &shared->svc);
    //gss_acquire_cred(&min, shared->svc, 0, GSS_C_NO_OID_SET, GSS_C_ACCEPT, &shared->srv_cred, NULL, NULL);

    shared->endpoint = evpl_endpoint_create("0.0.0.0", shared->config.port);

    if (rdma) {
        shared->endpoint_rdma = evpl_endpoint_create("0.0.0.0", shared->config.rdma_port);
    }

    shared->listener = evpl_listener_create();

    pthread_mutex_init(&shared->sessions_lock, NULL);
    pthread_mutex_init(&shared->shares_lock, NULL);
    pthread_mutex_init(&shared->trees_lock, NULL);
    pthread_mutex_init(&shared->threads_lock, NULL);

    /* Seed the persistent-id allocator with a random, nonzero base so ids do
     * not restart from a fixed value across daemon restarts (a courtesy to the
     * future on-disk persistence layer; 0 is reserved for "no file id"). */
    atomic_init(&shared->next_persistent_id, (chimera_rand64() | 1));

    chimera_smb_durable_table_init(&shared->durable);

    return shared;
} /* smb_server_init */

static void
chimera_smb_server_stop(void *data)
{
    struct chimera_server_smb_shared *shared = data;

    evpl_listener_destroy(shared->listener);
} /* smb_server_stop */

static void
chimera_smb_server_destroy(void *data)
{
    struct chimera_server_smb_shared *shared = data;
    struct chimera_smb_share         *share;
    struct chimera_smb_session       *session;
    struct chimera_smb_tree          *tree;


    chimera_smb_abort_if(shared->sessions, "active sessions exist at server shutdown")

    chimera_smb_durable_table_destroy(&shared->durable);

    while (shared->free_sessions) {
        session = shared->free_sessions;
        LL_DELETE(shared->free_sessions, session);
        chimera_smb_session_destroy(session);
    }

    while (shared->free_trees) {
        tree = shared->free_trees;
        LL_DELETE(shared->free_trees, tree);
        free(tree);
    }

    while (shared->shares) {
        share = shared->shares;
        LL_DELETE(shared->shares, share);
        chimera_smb_sharemode_destroy(&share->sharemode);
        free(share);
    }

    uint32_t min;

    if (shared->svc != GSS_C_NO_NAME) {
        gss_release_name(&min, &shared->svc);
    }

    if (shared->srv_cred != GSS_C_NO_CREDENTIAL) {
        gss_release_cred(&min, &shared->srv_cred);
    }

    pthread_mutex_destroy(&shared->threads_lock);

    free(shared);
} /* smb_server_destroy */

static void
chimera_smb_server_start(void *data)
{
    struct chimera_server_smb_shared *shared = data;

    evpl_listen(shared->listener, shared->tcp_protocol, shared->endpoint);

    if (shared->endpoint_rdma) {
        evpl_listen(shared->listener, EVPL_DATAGRAM_RDMACM_RC, shared->endpoint_rdma);
    }
} /* smb_server_start */

static inline void
chimera_smb_compound_advance(
    struct chimera_smb_compound *compound);

static inline void
chimera_smb_compound_reply(struct chimera_smb_compound *compound)
{
    struct chimera_server_smb_thread *thread = compound->thread;
    struct evpl                      *evpl   = thread->evpl;
    struct chimera_smb_conn          *conn   = compound->conn;
    struct evpl_iovec_cursor          reply_cursor;
    struct evpl_iovec                 reply_iov[260];
    struct netbios_header            *netbios_hdr = NULL;
    struct smb2_header               *reply_hdr;
    struct smb_direct_hdr            *direct_hdr = NULL;
    struct chimera_smb_request       *request;
    uint32_t                         *prev_command     = NULL;
    uint16_t                          prev_hdr         = 0;
    uint32_t                          preauth_fold_len = 0;
    struct evpl_iovec                 chunk_iov[3];
    int                               i, rc, chunk_niov;
    int                               reply_hdr_len, reply_payload_length, left, chunk;

    smb_dump_compound_reply(compound);

    evpl_iovec_alloc(evpl, 8192, 8, 1, 0, &reply_iov[0]);

    evpl_iovec_cursor_init(&reply_cursor, reply_iov, 1);

    if (conn->protocol == EVPL_DATAGRAM_RDMACM_RC) {
        direct_hdr = evpl_iovec_cursor_data(&reply_cursor);
        evpl_iovec_cursor_skip(&reply_cursor, sizeof(struct smb_direct_hdr));
        evpl_iovec_cursor_zero(&reply_cursor, 4); /* pad to 24 bytes */
        reply_hdr_len = sizeof(struct smb_direct_hdr) + 4;
    } else {
        netbios_hdr = evpl_iovec_cursor_data(&reply_cursor);
        evpl_iovec_cursor_skip(&reply_cursor, sizeof(struct netbios_header));
        reply_hdr_len = sizeof(struct netbios_header);
    }

    evpl_iovec_cursor_reset_consumed(&reply_cursor);

    for (i = 0; i < compound->num_requests; i++) {
        request = compound->requests[i];

        /* Skip requests that were handled asynchronously — their interim
         * response was already sent directly by the handler. */
        if (request->status == SMB2_STATUS_PENDING) {
            continue;
        }

        if ((conn->flags & CHIMERA_SMB_CONN_FLAG_SIGNING_REQUIRED) &&
            request->session_handle) {
            request->flags |= CHIMERA_SMB_REQUEST_FLAG_SIGN;
        }

        /* The final SESSION_SETUP response that establishes an authenticated
         * session MUST be signed once a signing key exists, even when signing
         * is only enabled (not required). SMB 3.x clients verify this response
         * to confirm the server holds the session key; for SMB 3.1.1 it also
         * binds the preauth-integrity hash. */
        if (request->smb2_hdr.command == SMB2_SESSION_SETUP &&
            request->status == SMB2_STATUS_SUCCESS &&
            request->session_handle &&
            (request->session_handle->session->flags & CHIMERA_SMB_SESSION_AUTHORIZED)) {
            request->flags |= CHIMERA_SMB_REQUEST_FLAG_SIGN;
        }

        /* SMB3 multichannel session binding (MS-SMB2 3.3.5.5.3): the client
         * signs the binding SESSION_SETUP request with the existing session key
         * and requires every response on that exchange -- interim, final, or
         * error -- to be signed.  Sign with session_handle->signing_key, which
         * holds the established session's key on the interim/error legs and the
         * newly derived per-channel key on the final SUCCESS leg.  Without this
         * the client rejects the bind (an unsigned response is treated as
         * STATUS_ACCESS_DENIED). */
        if (request->smb2_hdr.command == SMB2_SESSION_SETUP &&
            (request->session_setup.flags & SMB2_SESSION_FLAG_BINDING) &&
            request->session_handle &&
            (request->session_handle->session->flags & CHIMERA_SMB_SESSION_AUTHORIZED)) {
            request->flags |= CHIMERA_SMB_REQUEST_FLAG_SIGN;
        }

        if (prev_command) {
            *prev_command = evpl_iovec_cursor_consumed(&reply_cursor) - prev_hdr;
        }

        prev_hdr = evpl_iovec_cursor_consumed(&reply_cursor);

        reply_hdr = evpl_iovec_cursor_data(&reply_cursor);

        evpl_iovec_cursor_skip(&reply_cursor, sizeof(struct smb2_header));

        prev_command = &reply_hdr->next_command;

        reply_hdr->protocol_id[0]          = 0xFE;
        reply_hdr->protocol_id[1]          = 0x53;
        reply_hdr->protocol_id[2]          = 0x4D;
        reply_hdr->protocol_id[3]          = 0x42;
        reply_hdr->struct_size             = 64;
        reply_hdr->credit_charge           = request->smb2_hdr.credit_charge;
        reply_hdr->status                  = request->status;
        reply_hdr->command                 = request->smb2_hdr.command;
        reply_hdr->credit_request_response = request->smb2_hdr.credit_request_response ?
            request->smb2_hdr.credit_request_response : 1;
        reply_hdr->flags        = request->smb2_hdr.flags | SMB2_FLAGS_SERVER_TO_REDIR;
        reply_hdr->next_command = 0;
        reply_hdr->message_id   = request->smb2_hdr.message_id;
        reply_hdr->session_id   = request->session_handle && request->session_handle->session ?
            request->session_handle->session->session_id : 0;

        if (request->async_id) {
            reply_hdr->flags         |= SMB2_FLAGS_ASYNC_COMMAND;
            reply_hdr->async.async_id = request->async_id;
        } else {
            reply_hdr->sync.process_id = request->smb2_hdr.sync.process_id;
            reply_hdr->sync.tree_id    = request->tree ? request->tree->tree_id : 0;
        }

        memset(reply_hdr->signature, 0, sizeof(reply_hdr->signature));

        if (chimera_smb_is_error_status(request->status)) {
            evpl_iovec_cursor_append_uint16(&reply_cursor, SMB2_ERROR_REPLY_SIZE);
            evpl_iovec_cursor_append_uint16(&reply_cursor, 0);
            evpl_iovec_cursor_append_uint16(&reply_cursor, 0);
            evpl_iovec_cursor_append_uint16(&reply_cursor, 0);
            evpl_iovec_cursor_append_uint8(&reply_cursor, 0);
        } else {
            switch (request->smb2_hdr.command) {
                case SMB2_NEGOTIATE:
                    chimera_smb_negotiate_reply(&reply_cursor, request);
                    break;
                case SMB2_SESSION_SETUP:
                    chimera_smb_session_setup_reply(&reply_cursor, request);
                    break;
                case SMB2_LOGOFF:
                    chimera_smb_logoff_reply(&reply_cursor, request);
                    break;
                case SMB2_TREE_CONNECT:
                    chimera_smb_tree_connect_reply(&reply_cursor, request);
                    break;
                case SMB2_TREE_DISCONNECT:
                    chimera_smb_tree_disconnect_reply(&reply_cursor, request);
                    break;
                case SMB2_CREATE:
                    chimera_smb_create_reply(&reply_cursor, request);
                    break;
                case SMB2_CLOSE:
                    chimera_smb_close_reply(&reply_cursor, request);
                    break;
                case SMB2_WRITE:
                    chimera_smb_write_reply(&reply_cursor, request);
                    break;
                case SMB2_READ:
                    chimera_smb_read_reply(&reply_cursor, request);
                    break;
                case SMB2_FLUSH:
                    chimera_smb_flush_reply(&reply_cursor, request);
                    break;
                case SMB2_IOCTL:
                    chimera_smb_ioctl_reply(&reply_cursor, request);
                    break;
                case SMB2_ECHO:
                    chimera_smb_echo_reply(&reply_cursor, request);
                    break;
                case SMB2_QUERY_INFO:
                    chimera_smb_query_info_reply(&reply_cursor, request);
                    break;
                case SMB2_QUERY_DIRECTORY:
                    chimera_smb_query_directory_reply(&reply_cursor, request);
                    break;
                case SMB2_SET_INFO:
                    chimera_smb_set_info_reply(&reply_cursor, request);
                    break;
                case SMB2_CHANGE_NOTIFY:
                    chimera_smb_change_notify_reply(&reply_cursor, request);
                    break;
                case SMB2_LOCK:
                    chimera_smb_lock_reply(&reply_cursor, request);
                    break;
                case SMB2_OPLOCK_BREAK:
                    chimera_smb_oplock_break_reply(&reply_cursor, request);
                    break;
                    /* SMB2_CANCEL never reaches this switch: chimera_smb_cancel
                     * always completes with STATUS_PENDING so the slot is
                     * skipped above. */
            } /* switch */
        }

        /* Length of this response with no trailing alignment padding, captured
         * before padding for the SMB 3.1.1 preauth-integrity fold below. */
        preauth_fold_len = evpl_iovec_cursor_consumed(&reply_cursor);

        /* Pad each response to an 8-byte boundary so the next compounded
         * response's header is aligned. NEGOTIATE and SESSION_SETUP are never
         * compounded and feed the SMB 3.1.1 preauth-integrity hash, which the
         * client computes over the message *as received* (no trailing pad):
         * emit them in canonical form so our hash matches the client's. */
        if (request->smb2_hdr.command != SMB2_NEGOTIATE &&
            request->smb2_hdr.command != SMB2_SESSION_SETUP) {
            evpl_iovec_cursor_zero(&reply_cursor, (8 - (evpl_iovec_cursor_consumed(&reply_cursor) & 7)) & 7);
        }

    }

    /* Calculate actual number of iovecs after potential inject operations.
     * reply_cursor.niov tracks remaining slots, but after inject the cursor
     * advances and niov is incremented without accounting for the split.
     * The correct count is the cursor position (index) plus 1. */
    int reply_niov = (int) (reply_cursor.iov - reply_iov) + 1;

    rc = chimera_smb_sign_compound(thread->signing_ctx, compound, reply_iov, reply_niov,
                                   evpl_iovec_cursor_consumed(&reply_cursor) + reply_hdr_len
                                   );

    if (unlikely(rc != 0)) {
        chimera_smb_error("Failed to sign compound");
        chimera_smb_compound_free(thread, compound);
        evpl_close(evpl, conn->bind);
        return;
    }

    reply_payload_length = evpl_iovec_cursor_consumed(&reply_cursor);

    /* If all requests were handled asynchronously, there is nothing to send */
    if (reply_payload_length == 0) {
        evpl_iovec_release(evpl, &reply_iov[0]);
        chimera_smb_compound_free(thread, compound);
        return;
    }

    /* SMB 3.1.1 preauth integrity: fold the NEGOTIATE / SESSION_SETUP response
    * into the running hash. The hash covers the canonical (unpadded) message,
    * so use preauth_fold_len rather than the padded payload length — Windows
    * and WPTS hash the message without trailing alignment padding. These
    * responses are not compounded and carry no injected payload, so the whole
    * SMB2 message is contiguous in reply_iov[0] after the transport header. */
    if (compound->num_requests > 0 &&
        (compound->requests[0]->smb2_hdr.command == SMB2_NEGOTIATE ||
         compound->requests[0]->smb2_hdr.command == SMB2_SESSION_SETUP)) {

        /* A SESSION_SETUP that completes with a hard error contributes nothing
         * to the preauth-integrity hash: the client folds a SESSION_SETUP
         * exchange only when the response is SUCCESS or MORE_PROCESSING_REQUIRED
         * (MS-SMB2 3.3.5.5.3), so roll the request fold back to the pre-request
         * snapshot and skip folding the error response.  Without this a failed
         * authentication leg (e.g. an invalid first channel-bind) would poison
         * the hash and the next leg's signing key would not match the client's.
         * NEGOTIATE always reaches here on success, so it is never rolled back. */
        if (compound->requests[0]->smb2_hdr.command == SMB2_SESSION_SETUP &&
            chimera_smb_is_error_status(compound->requests[0]->status)) {
            memcpy(conn->preauth_hash, conn->preauth_hash_presession,
                   sizeof(conn->preauth_hash));
        } else {
            chimera_smb_preauth_extend(conn->preauth_hash,
                                       (uint8_t *) evpl_iovec_data(&reply_iov[0]) + reply_hdr_len,
                                       preauth_fold_len);

            /* Once the NEGOTIATE response is folded in, conn->preauth_hash holds
             * the post-NEGOTIATE baseline (MS-SMB2 Connection.PreauthIntegrity
             * HashValue).  Snapshot it so each subsequent session's preauth hash
             * can restart from here (see the request-fold path). */
            if (compound->requests[0]->smb2_hdr.command == SMB2_NEGOTIATE) {
                memcpy(conn->negotiate_preauth_hash, conn->preauth_hash,
                       sizeof(conn->negotiate_preauth_hash));
            }
        }
    }

    /* Will the encryption path below wrap this reply?  A reply destined for
     * encryption that is also compressible is compressed first and then
     * encrypted (MS-SMB2 §3.1.4.4 compress-then-encrypt) by the encryption path
     * itself; the plain-compression branch below only fires when not encrypting. */
    int will_encrypt = 0;
    {
        struct chimera_smb_session *enc_session = NULL;

        if (compound->num_requests > 0 && compound->requests[0]->session_handle) {
            enc_session = compound->requests[0]->session_handle->session;
        }
        if (enc_session && (enc_session->flags & CHIMERA_SMB_SESSION_ENCRYPT_DATA)) {
            uint16_t cmd0 = compound->requests[0]->smb2_hdr.command;
            if (compound->received_encrypted ||
                (cmd0 != SMB2_NEGOTIATE && cmd0 != SMB2_SESSION_SETUP)) {
                will_encrypt = 1;
            }
        }
    }

    /* SMB3 transport compression applicability (MS-SMB2 §3.3.5.12).  When the
     * connection negotiated Plain LZ77 and a single READ asked for a compressed
     * response (SMB2_READFLAG_REQUEST_COMPRESSED), comp_buf_off marks the data
     * offset within the message; only that data buffer is compressed (the SMB2
     * headers stay as the transform's uncompressed prefix), mirroring Windows.
     * The actual compression happens either inline (plaintext send) below or, for
     * an encrypted reply, in the compress-then-encrypt path further down. */
    int comp_lz77 = 0, comp_pattern = 0, comp_chained = 0, comp_buf_off = -1, k;

    for (k = 0; k < conn->negotiated.compression_alg_count; k++) {
        if (conn->negotiated.compression_algs[k] == SMB2_COMPRESSION_LZ77) {
            comp_lz77 = 1;
        } else if (conn->negotiated.compression_algs[k] == SMB2_COMPRESSION_PATTERN_V1) {
            comp_pattern = 1;
        }
    }

    if (compound->num_requests == 1 &&
        compound->requests[0]->smb2_hdr.command == SMB2_READ &&
        (compound->requests[0]->read.flags & SMB2_READFLAG_REQUEST_COMPRESSED) &&
        compound->requests[0]->read.r_length > 0) {
        comp_buf_off = reply_payload_length - (int) compound->requests[0]->read.r_length;
    }

    /* Chained Pattern_V1 framing is used only when both sides agreed to chaining
     * and Pattern_V1 was negotiated. */
    comp_chained = comp_pattern &&
        (conn->negotiated.compression_flags & SMB2_COMPRESSION_FLAG_CHAINED);

    int comp_applicable = comp_lz77 && comp_buf_off > 0 &&
        conn->protocol != EVPL_DATAGRAM_RDMACM_RC;

    if (!will_encrypt && comp_applicable) {
        struct evpl_iovec comp_iov;
        int               comp_total;

        rc = chimera_smb_compress_message(thread->compress_ctx, evpl,
                                          SMB2_COMPRESSION_LZ77, comp_chained, comp_buf_off,
                                          reply_iov, reply_niov,
                                          reply_payload_length, reply_hdr_len,
                                          &comp_iov, &comp_total);

        if (rc == 0) {
            netbios_hdr       = evpl_iovec_data(&comp_iov);
            netbios_hdr->word = __builtin_bswap32(comp_total);

            evpl_sendv(evpl, conn->bind, &comp_iov, 1, reply_hdr_len + comp_total,
                       EVPL_SEND_FLAG_TAKE_REF);

            evpl_iovecs_release(evpl, reply_iov, reply_niov);
            chimera_smb_compound_free(thread, compound);
            return;
        }
        /* rc != 0: compression did not shrink the reply — send plaintext. */
    }

    /* SMB3 transport encryption: wrap the signed reply in a TRANSFORM header
     * when the request arrived encrypted, when the session negotiated global
     * encryption, or when it targets a per-share-encrypted tree.  The NEGOTIATE
     * / SESSION_SETUP responses themselves precede key establishment and are
     * always sent signed-but-plaintext. */
    {
        struct chimera_smb_session *enc_session = NULL;

        if (compound->num_requests > 0 && compound->requests[0]->session_handle) {
            enc_session = compound->requests[0]->session_handle->session;
        }

        if (enc_session && enc_session->enc_key_len > 0) {
            uint16_t                 cmd0         = compound->requests[0]->smb2_hdr.command;
            struct chimera_smb_tree *tree0        = compound->requests[0]->tree;
            int                      tree_encrypt = tree0 && tree0->share &&
                tree0->share->encrypt_data;

            if (compound->received_encrypted ||
                (cmd0 != SMB2_NEGOTIATE && cmd0 != SMB2_SESSION_SETUP &&
                 ((enc_session->flags & CHIMERA_SMB_SESSION_ENCRYPT_DATA) ||
                  tree_encrypt))) {
                struct evpl_iovec  enc_iov;
                struct evpl_iovec  comp_iov;
                struct evpl_iovec *enc_src_iov  = reply_iov;
                int                enc_src_niov = reply_niov;
                int                enc_src_len  = reply_payload_length;
                int                have_comp    = 0;
                uint64_t           nonce;
                int                enc_total;

                if (conn->protocol == EVPL_DATAGRAM_RDMACM_RC) {
                    /* SMB-Direct encryption is not yet implemented. */
                    chimera_smb_error("SMB3 encryption over RDMA is not supported");
                    evpl_iovecs_release(evpl, reply_iov, reply_niov);
                    evpl_close(evpl, conn->bind);
                    chimera_smb_compound_free(thread, compound);
                    return;
                }

                /* Compress-then-encrypt (MS-SMB2 §3.1.4.4): when a READ asked for
                 * a compressed response on an encrypted session, compress the data
                 * buffer into a COMPRESSION_TRANSFORM message and encrypt that.  A
                 * payload that would not shrink leaves comp unused and the original
                 * plaintext reply is encrypted as-is. */
                if (comp_applicable) {
                    int comp_total;

                    if (chimera_smb_compress_message(thread->compress_ctx, evpl,
                                                     SMB2_COMPRESSION_LZ77, comp_chained,
                                                     comp_buf_off, reply_iov, reply_niov,
                                                     reply_payload_length, reply_hdr_len,
                                                     &comp_iov, &comp_total) == 0) {
                        enc_src_iov  = &comp_iov;
                        enc_src_niov = 1;
                        enc_src_len  = comp_total;
                        have_comp    = 1;
                    }
                }

                nonce = atomic_fetch_add(&enc_session->enc_nonce_counter, 1);

                rc = chimera_smb_encrypt_compound(thread->encrypt_ctx, evpl,
                                                  enc_session->cipher_id,
                                                  enc_session->enc_key,
                                                  enc_session->enc_key_len,
                                                  nonce, enc_session->session_id,
                                                  enc_src_iov, enc_src_niov,
                                                  enc_src_len, reply_hdr_len,
                                                  &enc_iov);

                if (have_comp) {
                    evpl_iovec_release(evpl, &comp_iov);
                }

                if (rc != 0) {
                    evpl_iovecs_release(evpl, reply_iov, reply_niov);
                    evpl_close(evpl, conn->bind);
                    chimera_smb_compound_free(thread, compound);
                    return;
                }

                enc_total = reply_hdr_len + (int) sizeof(struct smb2_transform_header) +
                    enc_src_len;

                /* The encrypted message length excludes the NetBIOS framing. */
                netbios_hdr       = evpl_iovec_data(&enc_iov);
                netbios_hdr->word = __builtin_bswap32(
                    (int) sizeof(struct smb2_transform_header) + enc_src_len);

                evpl_sendv(evpl, conn->bind, &enc_iov, 1, enc_total, EVPL_SEND_FLAG_TAKE_REF);

                evpl_iovecs_release(evpl, reply_iov, reply_niov);
                chimera_smb_compound_free(thread, compound);
                return;
            }
        }
    }

    if (conn->protocol == EVPL_DATAGRAM_RDMACM_RC) {

        /* direct_hdr was set above under this same protocol check; assert the
         * invariant so it is not a bare conditional dereference. */
        chimera_smb_abort_if(direct_hdr == NULL, "RDMA reply without SMB-Direct header");

        left = reply_payload_length;

        chunk = conn->rdma_max_send;

        if (left < chunk) {
            chunk = left;
        }

        direct_hdr->credits_requested = 255;
        direct_hdr->credits_granted   = 255;
        direct_hdr->flags             = 0;
        direct_hdr->reserved          = 0;
        direct_hdr->remaining_length  = left - chunk;
        direct_hdr->data_offset       = 24;
        direct_hdr->data_length       = chunk;

        evpl_sendv(evpl, conn->bind, reply_iov, reply_niov, direct_hdr->data_length + 24,
                   EVPL_SEND_FLAG_TAKE_REF);

        left -= direct_hdr->data_length;

        evpl_iovec_cursor_init(&reply_cursor, reply_iov, reply_niov);
        evpl_iovec_cursor_skip(&reply_cursor, direct_hdr->data_length + 24);

        while (left) {

            chunk = conn->rdma_max_send;

            if (left < 4096) {
                chunk = left;
            }

            evpl_iovec_alloc(evpl, 24, 8, 1, 0, &chunk_iov[0]);

            direct_hdr = evpl_iovec_data(&chunk_iov[0]);

            direct_hdr->credits_requested = 255;
            direct_hdr->credits_granted   = 255;
            direct_hdr->flags             = 0;
            direct_hdr->reserved          = 0;
            direct_hdr->remaining_length  = left - chunk;
            direct_hdr->data_offset       = 24;
            direct_hdr->data_length       = chunk;

            chunk_niov = 1 + evpl_iovec_cursor_move(&reply_cursor, &chunk_iov[1], 2, chunk, 1);

            evpl_sendv(evpl, conn->bind, chunk_iov, chunk_niov, chunk + 24, EVPL_SEND_FLAG_TAKE_REF);

            left -= chunk;
        }

    } else {
        netbios_hdr->word = __builtin_bswap32(reply_payload_length);

        evpl_sendv(evpl, conn->bind, reply_iov, reply_niov, reply_payload_length + reply_hdr_len,
                   EVPL_SEND_FLAG_TAKE_REF);
    }

    chimera_smb_compound_free(thread, compound);
} /* chimera_smb_compound_reply */

void
chimera_smb_complete_request(
    struct chimera_smb_request *request,
    unsigned int                status)
{
    struct chimera_smb_compound *compound = request->compound;

    /* If an async-interim is pending for this request, retire it.  An interim
     * STATUS_PENDING has already gone out (request->async_id is set), so the
     * reply builder below will tag the final response with
     * SMB2_FLAGS_ASYNC_COMMAND and the matching AsyncId. */
    if (unlikely(request->async.armed)) {
        chimera_smb_async_interim_cancel(request);
    }

    request->status = status;

    compound->complete_requests++;

    /* Update saved session/tree state regardless of success/failure so that
     * subsequent related operations in the compound — including aborted
     * ones taken via the abort path — can inherit the session_handle that
     * the original first request resolved. */
    if (request->session_handle && request->session_handle->session) {
        compound->saved_session_id     = request->session_handle->session->session_id;
        compound->saved_session_handle = request->session_handle;
    }

    if (request->tree) {
        compound->saved_tree_id = request->tree->tree_id;
        compound->saved_tree    = request->tree;
    }

    /* Always advance to the next compounded request -- even after a failure.
     * MS-SMB2 processes every request in the chain: a related request inherits
     * the prior (possibly now-invalid) FileId/Session/Tree and fails naturally
     * (e.g. STATUS_FILE_CLOSED), an unrelated request is independent.  Aborting
     * the remainder with STATUS_REQUEST_ABORTED is non-conformant (no client
     * expects it) and skipped earlier requests entirely.  Every command handler
     * already completes gracefully when its FileId does not resolve. */
    chimera_smb_compound_advance(compound);
} /* chimera_smb_complete_request */

static inline void
chimera_smb_compound_advance(struct chimera_smb_compound *compound)
{
    struct chimera_smb_request *request;

    chimera_smb_abort_if(compound->complete_requests > compound->num_requests,
                         "compound_advance: complete_requests = %u num_requests = %u", compound->complete_requests,
                         compound->num_requests);

    if (compound->complete_requests >= compound->num_requests) {
        chimera_smb_compound_reply(compound);
        return;
    }

    request = compound->requests[compound->complete_requests];

    if (request->smb2_hdr.flags & SMB2_FLAGS_RELATED_OPERATIONS) {
        /* A related request inherits the session and tree from the preceding
         * request in the chain (MS-SMB2 3.3.5.2.7.2). */
        if (compound->saved_session_handle) {
            request->session_handle = compound->saved_session_handle;
        }
        if (compound->saved_tree) {
            request->tree = compound->saved_tree;
        }

        /* If the preceding request established no session -- because it failed,
         * or because the chain illegally opened with a related request -- the
         * inherited context is invalid.  Windows/Samba answer this with
         * STATUS_INVALID_PARAMETER (the chained-request session check in
         * source3/smbd/smb2_server.c), not NETWORK_NAME_DELETED. */
        if (unlikely(!request->session_handle)) {
            chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
            return;
        }
    } else {
        /* An unrelated request breaks the related chain: clear the saved
         * context so a following related request cannot inherit stale session,
         * tree, or FileId state from before this request.  This mirrors Samba
         * resetting last_session_id and compat_chain_fsp on every non-chained
         * request; it is what makes an unrelated request that fails to resolve
         * its session leave a subsequent related request with no session. */
        compound->saved_session_handle = NULL;
        compound->saved_tree           = NULL;
        compound->saved_session_id     = UINT64_MAX;
        compound->saved_tree_id        = UINT64_MAX;
        compound->saved_file_id.pid    = UINT64_MAX;
        compound->saved_file_id.vid    = UINT64_MAX;
    }

    /* A request the parser could not set up (e.g. invalid session id) carries a
     * deferred status; complete it here, in order, rather than dispatching. */
    if (unlikely(request->flags & CHIMERA_SMB_REQUEST_FLAG_PARSE_FAILED)) {
        chimera_smb_complete_request(request, request->status);
        return;
    }

    /* Reject commands that require a valid tree connection */
    if (unlikely(!request->tree &&
                 request->smb2_hdr.command != SMB2_NEGOTIATE &&
                 request->smb2_hdr.command != SMB2_SESSION_SETUP &&
                 request->smb2_hdr.command != SMB2_LOGOFF &&
                 request->smb2_hdr.command != SMB2_TREE_CONNECT &&
                 request->smb2_hdr.command != SMB2_TREE_DISCONNECT &&
                 /* SMB2_CANCEL of an async request carries an AsyncId in the
                  * header where a sync request carries the TreeId, so it never
                  * resolves a tree.  It targets a parked request on the
                  * connection (looked up by async_id), not a share, and emits
                  * no reply — let it through with a NULL tree rather than
                  * bouncing it with NETWORK_NAME_DELETED. */
                 request->smb2_hdr.command != SMB2_CANCEL &&
                 request->smb2_hdr.command != SMB2_ECHO)) {
        /* A WRITE parsed before this point cloned its payload iovecs; the
         * write handler that would normally release them is being skipped, so
         * free them here to avoid leaking the data buffer on the reject path
         * (e.g. a write against an invalid/foreign TID). */
        if (request->smb2_hdr.command == SMB2_WRITE) {
            evpl_iovecs_release(compound->thread->evpl, request->write.iov, request->write.niov);
        }
        chimera_smb_complete_request(request, SMB2_STATUS_NETWORK_NAME_DELETED);
        return;
    }

    switch (request->smb2_hdr.command) {
        case SMB2_NEGOTIATE:
            chimera_smb_negotiate(request);
            break;
        case SMB2_SESSION_SETUP:
            chimera_smb_session_setup(request);
            break;
        case SMB2_LOGOFF:
            chimera_smb_logoff(request);
            break;
        case SMB2_TREE_CONNECT:
            chimera_smb_tree_connect(request);
            break;
        case SMB2_TREE_DISCONNECT:
            chimera_smb_tree_disconnect(request);
            break;
        case SMB2_CREATE:
            chimera_smb_create(request);
            break;
        case SMB2_CLOSE:
            chimera_smb_close(request);
            break;
        case SMB2_WRITE:
            chimera_smb_write(request);
            break;
        case SMB2_READ:
            chimera_smb_read(request);
            break;
        case SMB2_FLUSH:
            chimera_smb_flush(request);
            break;
        case SMB2_IOCTL:
            chimera_smb_ioctl(request);
            break;
        case SMB2_ECHO:
            chimera_smb_echo(request);
            break;
        case SMB2_QUERY_INFO:
            chimera_smb_query_info(request);
            break;
        case SMB2_QUERY_DIRECTORY:
            chimera_smb_query_directory(request);
            break;
        case SMB2_SET_INFO:
            chimera_smb_set_info(request);
            break;
        case SMB2_CHANGE_NOTIFY:
            chimera_smb_change_notify(request);
            break;
        case SMB2_CANCEL:
            chimera_smb_cancel(request);
            break;
        case SMB2_LOCK:
            chimera_smb_lock(request);
            break;
        case SMB2_OPLOCK_BREAK:
            chimera_smb_oplock_break(request);
            break;
        default:
            /* Opcode outside the valid SMB2 range (0x00..0x12); Windows/Samba
             * fail an unknown command with STATUS_INVALID_PARAMETER. */
            chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
            break;
    } /* switch */

} /* chimera_smb_compound_advance */

static void
chimera_smb_direct_negotiate(
    struct chimera_smb_conn *conn,
    struct evpl_iovec       *iov,
    int                      niov,
    int                      length)
{
    struct evpl                         *evpl = conn->thread->evpl;
    struct smb_direct_negotiate_request *request;
    struct smb_direct_negotiate_reply   *reply;
    struct evpl_iovec                    reply_iov;

    if (length != 20 || niov != 1) {
        chimera_smb_error("Received SMB2 message with invalid length or niov");
        evpl_close(evpl, conn->bind);
        return;
    }

    evpl_iovec_alloc(evpl, sizeof(*reply), 8, 1, 0, &reply_iov);

    request = (struct smb_direct_negotiate_request *) evpl_iovec_data(&iov[0]);
    reply   = (struct smb_direct_negotiate_reply *) evpl_iovec_data(&reply_iov);

    if (request->min_version > 0x100 || request->max_version < 0x100) {
        chimera_smb_error("Received SMB2 message with invalid min or max version");
        evpl_close(evpl, conn->bind);
        return;
    }

    conn->rdma_max_send = request->max_receive_size - 24;

    reply->min_version         = 0x100;
    reply->max_version         = 0x100;
    reply->negotiated_version  = 0x100;
    reply->reserved            = 0;
    reply->credits_requested   = request->credits_requested;
    reply->credits_granted     = request->credits_requested;
    reply->status              = SMB2_STATUS_SUCCESS;
    reply->max_readwrite_size  = 8 * 1024 * 1024;
    reply->preferred_send_size = 7168;
    reply->max_receive_size    = 8192;
    reply->max_fragmented_size = 1 * 1024 * 1024;

    evpl_iovec_set_length(&reply_iov, sizeof(*reply));

    evpl_sendv(evpl, conn->bind, &reply_iov, 1, sizeof(*reply), EVPL_SEND_FLAG_TAKE_REF);

    conn->flags |= CHIMERA_SMB_CONN_FLAG_SMB_DIRECT_NEGOTIATED;
} /* chimera_smb_direct_negotiate */

static void
chimera_smb_server_handle_smb2(
    struct evpl                      *evpl,
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_conn          *conn,
    struct evpl_iovec_cursor         *request_cursor,
    int                               length,
    int                               received_encrypted)
{
    struct chimera_smb_compound       *compound;
    struct chimera_smb_request        *request;
    struct chimera_smb_session_handle *session_handle;
    struct chimera_smb_session        *session;
    struct evpl_iovec_cursor           signature_cursor;
    /* Snapshot of the cursor at the SMB2 message start, for the 3.1.1
     * preauth-integrity hash (covers the whole received message). */
    struct evpl_iovec_cursor           preauth_cursor = *request_cursor;
    int                                more_requests, rc, left = length, payload_length;

    compound = chimera_smb_compound_alloc(thread);

    compound->thread = thread;
    compound->conn   = conn;

    compound->saved_session_id     = UINT64_MAX;
    compound->saved_tree_id        = UINT64_MAX;
    compound->saved_file_id.pid    = UINT64_MAX;
    compound->saved_file_id.vid    = UINT64_MAX;
    compound->saved_session_handle = NULL;
    compound->saved_tree           = NULL;

    compound->num_requests       = 0;
    compound->complete_requests  = 0;
    compound->received_encrypted = received_encrypted;

    left = length;

    while (left) {

        /* A well-formed SMB2 message always begins with a 64-byte header
        * followed by at least the 2-byte StructureSize.  A frame too short
        * to contain that is malformed -- e.g. the 5-byte Samba "exit"+code
        * "suicide" packet that several torture tests (oplock.levelII502,
        * etc.) send to make a forked smbd drop the connection.  chimera is
        * threaded, so blindly copying the header out of a short buffer trips
        * evpl_iovec_cursor_copy's abort() and takes down the whole server.
        * Recycle the compound and close just this connection instead. */
        if (unlikely(left < (int) (sizeof(request->smb2_hdr) +
                                   sizeof(request->request_struct_size)))) {
            chimera_smb_error(
                "Received SMB2 frame too short for a header (%d bytes); "
                "closing connection", left);
            chimera_smb_compound_free(thread, compound);
            evpl_close(evpl, conn->bind);
            return;
        }

        evpl_iovec_cursor_reset_consumed(request_cursor);

        request = chimera_smb_request_alloc(thread);

        request->compound = compound;

        evpl_iovec_cursor_copy(request_cursor, &request->smb2_hdr, sizeof(request->smb2_hdr));

        left -= sizeof(request->smb2_hdr);

        /* Capture the per-request ChannelSequence (low 16 bits of the field the
         * header struct calls "status" on a request) and the replay flag for
         * MS-SMB2 §3.3.5.2.10 stale-write detection and create-context replay. */
        request->channel_sequence = (uint16_t) (request->smb2_hdr.status & 0xFFFF);
        request->is_replay        = (request->smb2_hdr.flags & SMB2_FLAGS_REPLAY_OPERATION) ? 1 : 0;

        signature_cursor = *request_cursor;

        /* We only need to validate that we are using SMB2 at this point */
        if (unlikely(memcmp(request->smb2_hdr.protocol_id, SMB2_PROTOCOL_ID, 4) != 0)) {
            chimera_smb_error("Received SMB2 message with invalid protocol header");
            chimera_smb_request_free(thread, request);
            evpl_close(evpl, conn->bind);
            return;
        }

        if (unlikely(request->smb2_hdr.struct_size != 64)) {
            chimera_smb_error("Received SMB2 message with invalid struct size");
            chimera_smb_request_free(thread, request);
            evpl_close(evpl, conn->bind);
            return;
        }

        evpl_iovec_cursor_copy(request_cursor,
                               &request->request_struct_size,
                               sizeof(request->request_struct_size));

        /* Compound related requests inherit session/tree from previous request */
        if (request->smb2_hdr.flags & SMB2_FLAGS_RELATED_OPERATIONS) {
            request->session_handle = NULL;
        } else if (request->smb2_hdr.session_id) {

            if (conn->last_session_handle &&
                conn->last_session_handle->session->session_id == request->smb2_hdr.session_id) {
                request->session_handle = conn->last_session_handle;
            } else {
                HASH_FIND(hh, conn->session_handles, &request->smb2_hdr.session_id, sizeof(uint64_t), session_handle);

                if (session_handle) {
                    request->session_handle = session_handle;
                } else {

                    session = chimera_smb_session_lookup(thread->shared, request->smb2_hdr.session_id);

                    if (session) {
                        session_handle = chimera_smb_session_handle_alloc(thread);

                        session_handle->session_id = session->session_id;
                        session_handle->session    = session;

                        HASH_ADD(hh, conn->session_handles, session_id, sizeof(uint64_t), session_handle);

                        conn->last_session_handle = session_handle;

                        request->session_handle = session_handle;

                        memcpy(request->session_handle->signing_key,
                               request->session_handle->session->signing_key,
                               sizeof(request->session_handle->signing_key));

                    } else {
                        /* The header names a session id that no longer maps to
                         * a live session (e.g. one that has been logged off, or
                         * a bogus id).  Per MS-SMB2 3.3.5.2.9 this is answered
                         * with STATUS_USER_SESSION_DELETED rather than tearing
                         * down the transport.  Defer the failure: mark the
                         * request and let the dispatcher complete it in order.
                         * Completing it here would advance complete_requests out
                         * of order (this request is not at the head of the
                         * chain), leaving an earlier compounded request never
                         * dispatched yet still replied to -- e.g. a CREATE whose
                         * response is then built over uninitialized attributes. */
                        chimera_smb_error("Received SMB2 message with invalid session id %lx", request->smb2_hdr.
                                          session_id);
                        request->session_handle                      = NULL;
                        request->status                              = SMB2_STATUS_USER_SESSION_DELETED;
                        request->flags                              |= CHIMERA_SMB_REQUEST_FLAG_PARSE_FAILED;
                        compound->requests[compound->num_requests++] = request;
                        goto next_compound_request;
                    }
                }
            }
        } else {
            request->session_handle = NULL;
        }

        /* A session closed by a PreviousSessionId reconnect lingers, still
         * referenced by its original connection, until that connection drops.
         * Requests that resolve to it are answered USER_SESSION_DELETED
         * (MS-SMB2 3.3.5.2.9).  Defer the failure exactly like the invalid-id
         * case above: completing it inline would advance complete_requests out
         * of order within a compound. */
        if (unlikely(request->session_handle &&
                     (request->session_handle->session->flags &
                      CHIMERA_SMB_SESSION_DELETED))) {
            request->session_handle                      = NULL;
            request->status                              = SMB2_STATUS_USER_SESSION_DELETED;
            request->flags                              |= CHIMERA_SMB_REQUEST_FLAG_PARSE_FAILED;
            compound->requests[compound->num_requests++] = request;
            goto next_compound_request;
        }

        /* SESSION_SETUP requests are never signature-verified at dispatch: the
         * final NTLM message is signed with a session key the server only
         * derives while *processing* this very request, so the key isn't
         * available here yet.  The NTLM exchange authenticates the request, and
         * the success reply is signed (proving the server's key).
         *
         * Messages received inside a TRANSFORM (received_encrypted) are also not
         * signature-verified: the AEAD tag already authenticated the whole
         * message, and per MS-SMB2 §3.3.5.2.9 the server skips signing
         * verification for a successfully decrypted request.  Such inner SMB2
         * headers commonly carry SMB2_FLAGS_SIGNED with a zeroed Signature
         * field, which would otherwise fail verification. */
        if ((request->smb2_hdr.flags & SMB2_FLAGS_SIGNED) &&
            !received_encrypted &&
            request->smb2_hdr.command != SMB2_SESSION_SETUP) {
            uint8_t *signing_key;

            request->flags |= CHIMERA_SMB_REQUEST_FLAG_SIGN;

            if (request->smb2_hdr.next_command) {
                payload_length = request->smb2_hdr.next_command - sizeof(request->smb2_hdr);
            } else {
                payload_length = left;
            }

            if (request->smb2_hdr.flags & SMB2_FLAGS_RELATED_OPERATIONS) {
                if (unlikely(conn->last_session_handle == NULL)) {
                    chimera_smb_error("Message contains RELATED_OPERATIONS flag but no last session handle exists");
                    chimera_smb_request_free(thread, request);
                    evpl_close(evpl, conn->bind);
                    return;
                }
                signing_key = conn->last_session_handle->signing_key;
            } else {
                if (unlikely(request->session_handle == NULL)) {
                    chimera_smb_error("Received signed SMB2 message with missing/invalid session id %x",
                                      request->smb2_hdr.session_id);
                    chimera_smb_request_free(thread, request);
                    evpl_close(evpl, conn->bind);
                    return;
                }
                signing_key = request->session_handle->signing_key;
            }

            rc = chimera_smb_verify_signature(thread->signing_ctx, request, signing_key, &signature_cursor,
                                              payload_length);

            if (unlikely(rc != 0)) {
                chimera_smb_error("Received SMB2 message with invalid signature");
                chimera_smb_request_free(thread, request);
                evpl_close(evpl, conn->bind);
                return;
            }
        } else if (!received_encrypted &&
                   !(request->smb2_hdr.flags & SMB2_FLAGS_SIGNED) &&
                   !(request->smb2_hdr.flags & SMB2_FLAGS_RELATED_OPERATIONS) &&
                   request->smb2_hdr.command != SMB2_NEGOTIATE &&
                   request->smb2_hdr.command != SMB2_SESSION_SETUP &&
                   request->smb2_hdr.command != SMB2_ECHO &&
                   ((conn->flags & CHIMERA_SMB_CONN_FLAG_SIGNING_REQUIRED) ||
                    (request->session_handle && request->session_handle->session &&
                     (request->session_handle->session->flags &
                      CHIMERA_SMB_SESSION_ENCRYPT_DATA)))) {
            /* MS-SMB2 §3.3.5.2.9 / §3.3.5.7: once a session requires protection —
             * because signing is required, or because global encryption marked
             * the session encrypt-all (Session.EncryptData) — every
             * post-authentication request must be either signed or encrypted.
             * An unsigned, unencrypted request (e.g. a bare TREE_CONNECT) is a
             * protocol violation; tear the connection down.  SESSION_SETUP is
             * exempt (its signing key is only derived while processing it), as
             * are NEGOTIATE and ECHO; compound related operations inherit the
             * lead request's protection so only the unrelated lead is checked. */
            chimera_smb_error("Unsigned, unencrypted request (cmd %u) on a protected connection; disconnecting",
                              request->smb2_hdr.command);
            chimera_smb_request_free(thread, request);
            evpl_close(evpl, conn->bind);
            return;
        }

        if (unlikely(!request->session_handle &&
                     !(request->smb2_hdr.flags & SMB2_FLAGS_RELATED_OPERATIONS) &&
                     (request->smb2_hdr.command != SMB2_NEGOTIATE &&
                      request->smb2_hdr.command != SMB2_SESSION_SETUP &&
                      request->smb2_hdr.command != SMB2_ECHO))) {
            chimera_smb_error("Received SMB2 message with invalid command and no session");
            chimera_smb_complete_request(request, SMB2_STATUS_NO_SUCH_LOGON_SESSION);
            chimera_smb_request_free(thread, request);
            return;
        }

        if (request->session_handle &&
            !(request->smb2_hdr.flags & SMB2_FLAGS_RELATED_OPERATIONS) &&
            request->smb2_hdr.sync.tree_id < request->session_handle->session->max_trees) {
            request->tree = request->session_handle->session->trees[request->smb2_hdr.sync.tree_id];
        }

        switch (request->smb2_hdr.command) {
            case SMB2_NEGOTIATE:
                rc = chimera_smb_parse_negotiate(request_cursor, request);
                break;
            case SMB2_SESSION_SETUP:
                rc = chimera_smb_parse_session_setup(request_cursor, request);
                break;
            case SMB2_LOGOFF:
                rc = chimera_smb_parse_logoff(request_cursor, request);
                break;
            case SMB2_TREE_CONNECT:
                rc = chimera_smb_parse_tree_connect(request_cursor, request);
                break;
            case SMB2_TREE_DISCONNECT:
                rc = chimera_smb_parse_tree_disconnect(request_cursor, request);
                break;
            case SMB2_CREATE:
                rc = chimera_smb_parse_create(request_cursor, request);
                break;
            case SMB2_CLOSE:
                rc = chimera_smb_parse_close(request_cursor, request);
                break;
            case SMB2_WRITE:
                rc = chimera_smb_parse_write(request_cursor, request);
                break;
            case SMB2_READ:
                rc = chimera_smb_parse_read(request_cursor, request);
                break;
            case SMB2_FLUSH:
                rc = chimera_smb_parse_flush(request_cursor, request);
                break;
            case SMB2_IOCTL:
                rc = chimera_smb_parse_ioctl(request_cursor, request);
                break;
            case SMB2_ECHO:
                rc = chimera_smb_parse_echo(request_cursor, request);
                break;
            case SMB2_QUERY_INFO:
                rc = chimera_smb_parse_query_info(request_cursor, request);
                break;
            case SMB2_QUERY_DIRECTORY:
                rc = chimera_smb_parse_query_directory(request_cursor, request);
                break;
            case SMB2_SET_INFO:
                rc = chimera_smb_parse_set_info(request_cursor, request);
                break;
            case SMB2_CHANGE_NOTIFY:
                rc = chimera_smb_parse_change_notify(request_cursor, request);
                break;
            case SMB2_CANCEL:
                rc = chimera_smb_parse_cancel(request_cursor, request);
                break;
            case SMB2_LOCK:
                rc = chimera_smb_parse_lock(request_cursor, request);
                break;
            case SMB2_OPLOCK_BREAK:
                rc = chimera_smb_parse_oplock_break(request_cursor, request);
                break;
            default:
                chimera_smb_error("Received SMB2 message with unimplemented command %u",
                                  request->smb2_hdr.command);
                request->status = SMB2_STATUS_NOT_IMPLEMENTED;
                rc              = 0;
        } /* switch */

        compound->requests[compound->num_requests++] = request;

        if (rc) {
            chimera_smb_error("smb_server_handle_msg: failed to parse command %u", request->smb2_hdr.command);
            if (request->status != SMB2_STATUS_SUCCESS) {
                chimera_smb_complete_request(request, request->status);
            } else {
                chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
            }

            if (thread->shared->config.soft_fail_bad_req == 0) {
                evpl_finish(evpl, conn->bind);
            }

            return;
        }

 next_compound_request:
        more_requests = request->smb2_hdr.next_command != 0;

        if (more_requests) {
            evpl_iovec_cursor_skip(request_cursor,
                                   request->smb2_hdr.next_command - evpl_iovec_cursor_consumed(request_cursor));
        }

        left -= evpl_iovec_cursor_consumed(request_cursor) - sizeof(request->smb2_hdr);

        if (!more_requests) {
            break;
        }

    }

    smb_dump_compound_request(compound);

    /* SMB 3.1.1 preauth integrity: fold the raw NEGOTIATE / SESSION_SETUP
     * request into the running hash before dispatch, so the SESSION_SETUP
     * handler derives the signing key over a hash that includes this request.
     * Maintained unconditionally for these commands; only consumed at 3.1.1.
     *
     * A SESSION_SETUP whose header SessionId is 0 starts a brand-new
     * authentication (the first leg of a new session, including a re-auth
     * after LOGOFF on the same connection).  Per MS-SMB2 3.3.5.5.3 that
     * session's preauth hash restarts from the post-NEGOTIATE baseline, not
     * from whatever earlier sessions accumulated -- otherwise the signing key
     * is derived over the wrong hash and the client rejects the session. */
    if (compound->num_requests > 0 &&
        (compound->requests[0]->smb2_hdr.command == SMB2_NEGOTIATE ||
         compound->requests[0]->smb2_hdr.command == SMB2_SESSION_SETUP)) {
        uint8_t *msg = malloc(length);
        if (msg) {
            if (compound->requests[0]->smb2_hdr.command == SMB2_SESSION_SETUP &&
                compound->requests[0]->smb2_hdr.session_id == 0) {
                memcpy(conn->preauth_hash, conn->negotiate_preauth_hash,
                       sizeof(conn->preauth_hash));
            }
            /* Snapshot before folding a SESSION_SETUP so a leg that fails
             * authentication can be rolled back (see the reply-fold path). */
            if (compound->requests[0]->smb2_hdr.command == SMB2_SESSION_SETUP) {
                memcpy(conn->preauth_hash_presession, conn->preauth_hash,
                       sizeof(conn->preauth_hash_presession));
            }
            evpl_iovec_cursor_copy(&preauth_cursor, msg, length);
            chimera_smb_preauth_extend(conn->preauth_hash, msg, length);
            free(msg);
        }
    }

    chimera_smb_compound_advance(compound);

} /* chimera_smb_server_handle_compound */

static void
chimera_smb_server_handle_smb1(
    struct evpl                      *evpl,
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_conn          *conn,
    struct evpl_iovec                *iov,
    int                               niov,
    int                               length)
{
    struct evpl_iovec_cursor     request_cursor;
    struct netbios_header        netbios_hdr;
    struct chimera_smb_compound *compound;
    struct chimera_smb_request  *request;
    uint8_t                      wct;
    uint16_t                     bcc;
    uint8_t                     *dialects, *bp;
    char                        *dialect;
    int                          matched = 0;
    /* Dialect the SMB1 multi-protocol negotiate resolves to: 0x02ff (the
     * SMB2 wildcard, meaning "send a real SMB2 NEGOTIATE next") when the
     * client offers "SMB 2.???", or 0x0202 when it offers the specific
     * "SMB 2.002" string (MS-SMB2 3.3.5.3.1).  The wildcard takes precedence
     * if both are present. */
    uint16_t                     smb1_dialect = 0;

    evpl_iovec_cursor_init(&request_cursor, iov, niov);
    evpl_iovec_cursor_copy(&request_cursor, &netbios_hdr, sizeof(netbios_hdr));

    request = chimera_smb_request_alloc(thread);

    evpl_iovec_cursor_copy(&request_cursor, &request->smb1_hdr, sizeof(request->smb1_hdr));

    if (request->smb1_hdr.command != SMB1_NEGOTIATE) {
        chimera_smb_error("Received SMB1 message with invalid command");
        chimera_smb_request_free(thread, request);
        evpl_close(evpl, conn->bind);
        return;
    } /* switch */

    evpl_iovec_cursor_get_uint8(&request_cursor, &wct);
    evpl_iovec_cursor_reset_consumed(&request_cursor);
    evpl_iovec_cursor_get_uint16(&request_cursor, &bcc);

    dialects = alloca(bcc + 1);

    evpl_iovec_cursor_copy(&request_cursor, dialects, bcc);

    dialects[bcc] = 0;

    bp = dialects;

    while (bp < dialects + bcc) {

        if (*bp != 0x02) {
            chimera_smb_error("Received SMB1 NEGOTIATE with buffer format that isn't dialects");
            chimera_smb_request_free(thread, request);
            evpl_close(evpl, conn->bind);
            return;
        }

        bp++;


        if (bp >= dialects + bcc) {
            chimera_smb_error("Received SMB1 NEGOTIATE with truncated dialect buffer");
            chimera_smb_request_free(thread, request);
            evpl_close(evpl, conn->bind);
            return;
        }

        dialect = (char *) bp;

        while (*bp != 0 && bp < dialects + bcc) {
            bp++;
        }

        if (*bp != 0) {
            chimera_smb_error("Received SMB1 NEGOTIATE with truncated dialect buffer");
            chimera_smb_request_free(thread, request);
            evpl_close(evpl, conn->bind);
            return;
        }

        if (strcmp(dialect, "SMB 2.???") == 0) {
            matched      = 1;
            smb1_dialect = 0x02ff;
        } else if (strcmp(dialect, "SMB 2.002") == 0) {
            matched = 1;
            /* Don't let "SMB 2.002" clobber a wildcard already seen. */
            if (smb1_dialect == 0) {
                smb1_dialect = SMB2_DIALECT_2_0_2;
            }
        }

        bp++;
    } /* chimera_smb_server_handle_smb1 */

    if (!matched) {
        chimera_smb_error("Received SMB1 NEGOTIATE with no SMB2 dialect, and we don't support SMB1");
        chimera_smb_request_free(thread, request);
        evpl_close(evpl, conn->bind);
        return;
    }

    compound = chimera_smb_compound_alloc(thread);

    compound->thread = thread;
    compound->conn   = conn;

    compound->saved_session_id     = UINT64_MAX;
    compound->saved_tree_id        = UINT64_MAX;
    compound->saved_file_id.pid    = UINT64_MAX;
    compound->saved_file_id.vid    = UINT64_MAX;
    compound->saved_session_handle = NULL;
    compound->saved_tree           = NULL;

    compound->num_requests       = 0;
    compound->complete_requests  = 0;
    compound->received_encrypted = 0;

    request->compound       = compound;
    request->session_handle = NULL;
    request->tree           = NULL;

    /* Now fabricate the SMB2 header and NEGOTIATE request structures
     * so we can proceed to handle this as though it had been SMB2 all along
     */

    request->smb2_hdr.protocol_id[0]          = 0xFE;
    request->smb2_hdr.protocol_id[1]          = 'S';
    request->smb2_hdr.protocol_id[2]          = 'M';
    request->smb2_hdr.protocol_id[3]          = 'B';
    request->smb2_hdr.struct_size             = SMB2_NEGOTIATE_REQUEST_SIZE;
    request->smb2_hdr.credit_charge           = 0;
    request->smb2_hdr.status                  = SMB2_STATUS_SUCCESS;
    request->smb2_hdr.command                 = SMB2_NEGOTIATE;
    request->smb2_hdr.credit_request_response = 0;
    request->smb2_hdr.flags                   = 0;
    request->smb2_hdr.next_command            = 0;
    request->smb2_hdr.message_id              = 0;
    request->smb2_hdr.session_id              = 0;

    request->negotiate.dialect_count = 1;
    request->negotiate.security_mode = 0;
    request->negotiate.capabilities  = 0;
    memset(request->negotiate.client_guid, 0, sizeof(request->negotiate.client_guid));
    request->negotiate.negotiate_context_offset = 0;
    request->negotiate.negotiate_context_count  = 0;
    request->negotiate.dialects[0]              = smb1_dialect;

    compound->requests[compound->num_requests++] = request;

    smb_dump_compound_request(compound);

    chimera_smb_compound_advance(compound);

} /* chimera_smb_server_handle_smb1 */

static void
chimera_smb_server_handle_rdma(
    struct evpl                      *evpl,
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_conn          *conn,
    struct evpl_iovec                *iov,
    int                               niov,
    int                               length)
{
    struct evpl_iovec_cursor request_cursor;
    struct smb_direct_hdr   *direct_hdr;
    struct evpl_iovec       *segment_iov;

    if (unlikely(!(conn->flags & CHIMERA_SMB_CONN_FLAG_SMB_DIRECT_NEGOTIATED))) {
        /* There will be a one-time smb-direct specific negotiation per connection */
        chimera_smb_direct_negotiate(conn, iov, niov, length);
        return;
    }

    chimera_smb_abort_if(niov != 1, "Received SMB2 message over RDMA with multiple iovecs");

    if (unlikely(length < sizeof(*direct_hdr))) {
        chimera_smb_error("Received SMB2 message over RDMA that is too short for header");
        evpl_close(evpl, conn->bind);
        return;
    }

    direct_hdr = (struct smb_direct_hdr *) evpl_iovec_data(&iov[0]);

    if (unlikely(direct_hdr->data_length &&
                 direct_hdr->data_offset < sizeof(*direct_hdr))) {
        chimera_smb_error("Received SMB2 message over RDMA that has data offset that is too small");
        evpl_close(evpl, conn->bind);
        return;
    }

    if (unlikely(direct_hdr->data_length &&
                 direct_hdr->data_length < sizeof(struct smb2_header))) {
        chimera_smb_error("Received SMB2 message over RDMA that has data length that is too small");
        evpl_close(evpl, conn->bind);
        return;
    }

    segment_iov = &conn->rdma_iov[conn->rdma_niov++];

    *segment_iov = iov[0];

    segment_iov->data += direct_hdr->data_offset;
    evpl_iovec_set_length(segment_iov, direct_hdr->data_length);

    conn->rdma_length += direct_hdr->data_length;

    if (direct_hdr->remaining_length == 0) {
        evpl_iovec_cursor_init(&request_cursor, conn->rdma_iov, conn->rdma_niov);
        chimera_smb_server_handle_smb2(evpl, thread, conn, &request_cursor, conn->rdma_length, 0);
        conn->rdma_niov   = 0;
        conn->rdma_length = 0;
    }

} /* chimera_smb_server_handle_rdma */


/*
 * Decrypt a TRANSFORM-wrapped (SMB3 encrypted) message and feed the plaintext
 * SMB2 message into the normal dispatch path.  request_cursor is positioned at
 * the transform header (after the 4-byte NetBIOS framing); length is the
 * transform header plus ciphertext byte count.
 */
static void
chimera_smb_server_handle_transform(
    struct evpl                      *evpl,
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_conn          *conn,
    struct evpl_iovec_cursor         *request_cursor,
    int                               length)
{
    struct smb2_transform_header th;
    struct evpl_iovec_cursor     peek_cursor = *request_cursor;
    struct evpl_iovec_cursor     plain_cursor;
    struct chimera_smb_session  *session;
    struct evpl_iovec            plain_iov;
    int                          plain_len, rc;

    if (length < (int) sizeof(th)) {
        chimera_smb_error("Truncated SMB3 transform message (%d bytes)", length);
        evpl_close(evpl, conn->bind);
        return;
    }

    /* Peek the transform header for its SessionId, then look up the session for
     * its decryption key and negotiated cipher. */
    evpl_iovec_cursor_copy(&peek_cursor, &th, sizeof(th));

    session = chimera_smb_session_lookup(thread->shared, th.session_id);

    /* Decrypt whenever the session holds derived keys.  This is broader than the
     * global ENCRYPT_DATA flag: a per-share-encrypted tree makes the client
     * encrypt even when the session is not globally encrypt-all, and we can
     * always decrypt traffic we hold keys for. */
    if (!session || session->enc_key_len == 0) {
        chimera_smb_error("Encrypted SMB2 message for unknown/non-encrypting session %lx",
                          th.session_id);
        if (session) {
            chimera_smb_session_release(thread, thread->shared, session, true);
        }
        evpl_close(evpl, conn->bind);
        return;
    }

    rc = chimera_smb_decrypt_message(thread->encrypt_ctx, evpl,
                                     session->cipher_id, session->dec_key,
                                     session->enc_key_len, request_cursor, length,
                                     &plain_iov, &plain_len);

    chimera_smb_session_release(thread, thread->shared, session, true);

    if (rc != 0) {
        evpl_close(evpl, conn->bind);
        return;
    }

    /* The decrypted plaintext is a complete (possibly compound) SMB2 message,
     * unless the peer compressed before encrypting (MS-SMB2 §3.1.4.4 mandates
     * compress-then-encrypt): then the plaintext is itself a COMPRESSION_
     * TRANSFORM (0xFC 'S' 'M' 'B').  Peek its protocol id and decompress first.
     * Either way it is dispatched as if received in the clear, flagged so its
     * reply is re-encrypted.  Handlers that outlive this call take their own
     * refs on the buffer, so releasing our initial ref here is safe. */
    {
        struct evpl_iovec_cursor inner_peek;
        uint32_t                 inner_hdr = 0;

        evpl_iovec_cursor_init(&plain_cursor, &plain_iov, 1);
        inner_peek = plain_cursor;
        if (plain_len >= 4) {
            evpl_iovec_cursor_get_uint32(&inner_peek, &inner_hdr);
        }

        if (inner_hdr == 0x424d53fc) {
            struct evpl_iovec_cursor decomp_cursor;
            struct evpl_iovec        decomp_iov;
            int                      decomp_len;

            rc = chimera_smb_decompress_message(thread->compress_ctx, evpl,
                                                &plain_cursor, plain_len,
                                                &decomp_iov, &decomp_len);

            evpl_iovec_release(evpl, &plain_iov);

            if (rc != 0) {
                evpl_close(evpl, conn->bind);
                return;
            }

            evpl_iovec_cursor_init(&decomp_cursor, &decomp_iov, 1);
            chimera_smb_server_handle_smb2(evpl, thread, conn, &decomp_cursor, decomp_len, 1);
            evpl_iovec_release(evpl, &decomp_iov);
            return;
        }

        chimera_smb_server_handle_smb2(evpl, thread, conn, &plain_cursor, plain_len, 1);
        evpl_iovec_release(evpl, &plain_iov);
    }
} /* chimera_smb_server_handle_transform */

/*
 * Handle a received SMB3 COMPRESSION_TRANSFORM message (protocol id 0xFC 'S' 'M'
 * 'B').  Unlike encryption, compression is a per-connection transform (no
 * session key), so the plaintext is simply reconstructed and re-dispatched as if
 * received in the clear.  request_cursor is positioned at the compression
 * transform header; length is its byte count (excluding NetBIOS framing).
 */
static void
chimera_smb_server_handle_compressed(
    struct evpl                      *evpl,
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_conn          *conn,
    struct evpl_iovec_cursor         *request_cursor,
    int                               length)
{
    struct evpl_iovec_cursor plain_cursor;
    struct evpl_iovec        plain_iov;
    int                      plain_len, rc;

    rc = chimera_smb_decompress_message(thread->compress_ctx, evpl,
                                        request_cursor, length,
                                        &plain_iov, &plain_len);

    if (rc != 0) {
        evpl_close(evpl, conn->bind);
        return;
    }

    evpl_iovec_cursor_init(&plain_cursor, &plain_iov, 1);

    chimera_smb_server_handle_smb2(evpl, thread, conn, &plain_cursor, plain_len, 0);

    evpl_iovec_release(evpl, &plain_iov);
} /* chimera_smb_server_handle_compressed */

static void
chimera_smb_server_handle_tcp(
    struct evpl                      *evpl,
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_conn          *conn,
    struct evpl_iovec                *iov,
    int                               niov,
    int                               length)
{
    struct evpl_iovec_cursor request_cursor, peek_cursor;
    struct netbios_header    netbios_hdr;
    uint32_t                 smb_hdr;

    evpl_iovec_cursor_init(&request_cursor, iov, niov);
    evpl_iovec_cursor_copy(&request_cursor, &netbios_hdr, sizeof(netbios_hdr));

    /* Peek the protocol id up front (even on the smbvers==2 fast path): once a
     * 3.x dialect is negotiated, encrypted TRANSFORM messages (0xFD 'S' 'M' 'B')
     * can arrive interleaved with plaintext SMB2 (0xFE 'S' 'M' 'B'). */
    peek_cursor = request_cursor;
    evpl_iovec_cursor_get_uint32(&peek_cursor, &smb_hdr);

    if (smb_hdr == 0x424d53fd) {
        chimera_smb_server_handle_transform(evpl, thread, conn, &request_cursor, length - 4);
        return;
    }

    if (smb_hdr == 0x424d53fc) {
        chimera_smb_server_handle_compressed(evpl, thread, conn, &request_cursor, length - 4);
        return;
    }

    if (likely(conn->smbvers == 2)) {
        chimera_smb_server_handle_smb2(evpl, thread, conn, &request_cursor, length - 4, 0);
        return;
    }

    if (smb_hdr == 0x424d53fe) {
        conn->smbvers = 2;
        chimera_smb_server_handle_smb2(evpl, thread, conn, &request_cursor, length - 4, 0);
    } else if (smb_hdr == 0x424d53ff) {
        chimera_smb_server_handle_smb1(evpl, thread, conn, iov, niov, length);
    } else {
        chimera_smb_error("Received SMB message with invalid protocol header");
        evpl_close(evpl, conn->bind);
        return;
    }
} /* chimera_smb_server_handle_tcp */

static void
chimera_smb_server_notify(
    struct evpl        *evpl,
    struct evpl_bind   *bind,
    struct evpl_notify *notify,
    void               *private_data)
{
    struct chimera_smb_conn          *conn   = private_data;
    struct chimera_server_smb_thread *thread = conn->thread;

    switch (notify->notify_type) {
        case EVPL_NOTIFY_CONNECTED:
            chimera_smb_info("Established %s SMB connection from %s to %s",
                             conn->protocol == EVPL_DATAGRAM_RDMACM_RC ? "RDMA" : "TCP",
                             conn->remote_addr,
                             conn->local_addr);

            break;
        case EVPL_NOTIFY_DISCONNECTED:
            chimera_smb_info("Disconnected %s SMB connection from %s to %s, handled %lu requests",
                             conn->protocol == EVPL_DATAGRAM_RDMACM_RC ? "RDMA" : "TCP",
                             conn->remote_addr, conn->local_addr, conn->requests_completed);
            chimera_smb_conn_free(thread, conn);
            break;
        case EVPL_NOTIFY_RECV_MSG:
            conn->requests_completed++;

            if (conn->protocol == EVPL_DATAGRAM_RDMACM_RC) {
                chimera_smb_server_handle_rdma(evpl, thread, conn,
                                               notify->recv_msg.iovec,
                                               notify->recv_msg.niov,
                                               notify->recv_msg.length);
            } else {
                chimera_smb_server_handle_tcp(evpl, thread, conn,
                                              notify->recv_msg.iovec,
                                              notify->recv_msg.niov,
                                              notify->recv_msg.length);
            }

            for (int i = 0; i < notify->recv_msg.niov; i++) {
                evpl_iovec_release(evpl, &notify->recv_msg.iovec[i]);
            }
            break;
        case EVPL_NOTIFY_SENT:
            break;
    } /* switch */
} /* smb_server_notify */

static int
chimera_smb_server_segment(
    struct evpl      *evpl,
    struct evpl_bind *bind,
    void             *private_data)
{
    uint32_t hdr;
    int      len;


    len = evpl_peek(evpl, bind, &hdr, 4);

    if (len < 4) {
        return -1;
    }

    hdr = __builtin_bswap32(hdr);

    hdr &= 0x00ffffff;

    return 4 + hdr;

} /* smb_server_segment */

static void
chimera_smb_server_accept(
    struct evpl             *evpl,
    struct evpl_bind        *bind,
    evpl_notify_callback_t  *notify_callback,
    evpl_segment_callback_t *segment_callback,
    void                   **conn_private_data,
    void                    *private_data)
{
    struct chimera_server_smb_thread *thread = private_data;
    struct chimera_smb_conn          *conn;

    conn = chimera_smb_conn_alloc(thread);

    conn->thread            = thread;
    conn->bind              = bind;
    conn->protocol          = evpl_bind_get_protocol(bind);
    conn->smbvers           = conn->protocol == EVPL_DATAGRAM_RDMACM_RC ? 2 : 0;
    conn->gss_flags         = 0;
    conn->gss_major         = 0;
    conn->gss_minor         = 0;
    conn->gss_output.value  = NULL;
    conn->gss_output.length = 0;
    conn->nascent_ctx       = GSS_C_NO_CONTEXT;
    conn->ntlm_output       = NULL;
    conn->ntlm_output_len   = 0;
    smb_ntlm_ctx_init(&conn->ntlm_ctx);
    memset(&conn->gssapi_ctx, 0, sizeof(conn->gssapi_ctx));
    /* SMB 3.1.1 preauth-integrity hash starts at zero for each connection. */
    memset(conn->preauth_hash, 0, sizeof(conn->preauth_hash));
    memset(conn->negotiate_preauth_hash, 0, sizeof(conn->negotiate_preauth_hash));
    conn->rdma_niov   = 0;
    conn->rdma_length = 0;
    /* Conns are pooled; clear per-connection negotiate state so the
    * "NEGOTIATE after NEGOTIATE" guard (MS-SMB2 3.3.5.4) does not trip on
    * a dialect inherited from a previously torn-down connection. */
    conn->dialect = 0;
    conn->flags   = 0;

    evpl_bind_get_local_address(bind, conn->local_addr, sizeof(conn->local_addr));
    evpl_bind_get_remote_address(bind, conn->remote_addr, sizeof(conn->remote_addr));

    /* Track this connection on its owning thread's active list so the
     * lease-break resume doorbell can walk it for parked CREATEs. */
    DL_APPEND2(thread->active_conns, conn, active_prev, active_next);

    *notify_callback   = chimera_smb_server_notify;
    *segment_callback  = chimera_smb_server_segment;
    *conn_private_data = conn;

} /* smb_server_accept */


/* Interval between durable-handle reconnect-grace sweeps. */
#define CHIMERA_SMB_DURABLE_SWEEP_INTERVAL_US (1 * 1000 * 1000)

static void
chimera_smb_durable_sweeper_fire(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    struct chimera_server_smb_thread *thread =
        container_of(timer, struct chimera_server_smb_thread, durable_sweeper);

    chimera_smb_durable_sweep(thread);
} /* chimera_smb_durable_sweeper_fire */

static void *
chimera_smb_server_thread_init(
    struct evpl               *evpl,
    struct chimera_vfs_thread *vfs_thread,
    void                      *data)
{
    struct chimera_server_smb_shared *shared = data;
    struct chimera_server_smb_thread *thread = calloc(1, sizeof(*thread));

    if (!thread) {
        return NULL;
    }

    thread->vfs_thread   = vfs_thread;
    thread->shared       = shared;
    thread->evpl         = evpl;
    thread->signing_ctx  = chimera_smb_signing_ctx_create();
    thread->encrypt_ctx  = chimera_smb_encrypt_ctx_create();
    thread->compress_ctx = chimera_smb_compress_ctx_create();

    chimera_smb_iconv_init(&thread->iconv_ctx);
    chimera_smb_notify_thread_init(thread);
    chimera_smb_lease_break_thread_init(thread);

    /* Resume doorbell: a peer thread settling a lease break rings this so this
     * thread re-scans its own connections for parked CREATEs to complete. */
    evpl_add_doorbell(evpl, &thread->lease_resume_doorbell,
                      chimera_smb_create_resume_doorbell_callback);

    /* Register in the process-global thread list so resume broadcasts reach
     * this thread. */
    pthread_mutex_lock(&shared->threads_lock);
    thread->next_thread = shared->threads;
    shared->threads     = thread;
    pthread_mutex_unlock(&shared->threads_lock);

    if (shared->config.persistent_handles) {
        evpl_add_timer(evpl, &thread->durable_sweeper,
                       chimera_smb_durable_sweeper_fire,
                       CHIMERA_SMB_DURABLE_SWEEP_INTERVAL_US);
    }

    thread->binding = evpl_listener_attach(
        evpl,
        shared->listener,
        chimera_smb_server_accept,
        thread);

    return thread;
} /* smb_server_thread_init */

static void
chimera_smb_server_thread_destroy(void *data)
{
    struct chimera_server_smb_thread  *thread = data;
    struct chimera_smb_open_file      *open_file;
    struct chimera_smb_request        *request;
    struct chimera_smb_conn           *conn;
    struct chimera_smb_compound       *compound;
    struct chimera_smb_session_handle *session_handle;
    struct chimera_server_smb_thread **tpp;

    /* Unregister from the process-global thread list first so a peer thread's
     * resume broadcast can no longer ring this thread's (about-to-be-removed)
     * resume doorbell.  evpl_remove_doorbell below then runs on this thread. */
    pthread_mutex_lock(&thread->shared->threads_lock);
    for (tpp = &thread->shared->threads; *tpp; tpp = &(*tpp)->next_thread) {
        if (*tpp == thread) {
            *tpp = thread->next_thread;
            break;
        }
    }
    pthread_mutex_unlock(&thread->shared->threads_lock);

    /* Release any durable/persistent handles still parked in the shared
     * registry while this thread's vfs_thread is alive.  Each parked open
     * pins a VFS open handle; leaving it referenced makes the VFS close
     * thread spin and chimera_vfs_destroy hang.  Must run before the
     * free_open_files drain below so the released opens are reclaimed. */
    if (thread->shared->config.persistent_handles) {
        chimera_smb_durable_drain_all(thread);
    }

    while (thread->free_compounds) {
        compound = thread->free_compounds;
        LL_DELETE(thread->free_compounds, compound);
        free(compound);
    }

    while (thread->free_open_files) {
        open_file = thread->free_open_files;
        LL_DELETE(thread->free_open_files, open_file);
        free(open_file);
    }

    while (thread->free_conns) {
        conn = thread->free_conns;
        LL_DELETE(thread->free_conns, conn);
        free(conn);
    }

    while (thread->free_requests) {
        request = thread->free_requests;
        LL_DELETE(thread->free_requests, request);
        free(request);
    }

    while (thread->free_session_handles) {
        session_handle = thread->free_session_handles;
        LL_DELETE(thread->free_session_handles, session_handle);
        free(session_handle);
    }

    if (thread->shared->config.persistent_handles) {
        evpl_remove_timer(thread->evpl, &thread->durable_sweeper);
    }

    evpl_listener_detach(thread->evpl, thread->binding);
    evpl_remove_doorbell(thread->evpl, &thread->lease_resume_doorbell);
    chimera_smb_notify_thread_destroy(thread);
    chimera_smb_lease_break_thread_destroy(thread);

    chimera_smb_iconv_destroy(&thread->iconv_ctx);
    chimera_smb_signing_ctx_destroy(thread->signing_ctx);
    chimera_smb_encrypt_ctx_destroy(thread->encrypt_ctx);
    chimera_smb_compress_ctx_destroy(thread->compress_ctx);

    free(thread);
} /* smb_server_thread_destroy */


SYMBOL_EXPORT void
chimera_smb_add_share(
    void       *smb_shared,
    const char *name,
    const char *path,
    int         continuous_availability)
{
    struct chimera_server_smb_shared *shared = smb_shared;
    struct chimera_smb_share         *share  = calloc(1, sizeof(*share));

    snprintf(share->name, sizeof(share->name), "%s", name);
    snprintf(share->path, sizeof(share->path), "%s", path);
    chimera_smb_sharemode_init(&share->sharemode);

    /* A share is continuously available (and thus eligible to grant persistent
     * handles) only when the feature is enabled AND the share opts in. */
    share->continuous_availability = shared->config.persistent_handles &&
        continuous_availability;

    pthread_mutex_lock(&shared->shares_lock);
    LL_PREPEND(shared->shares, share);
    pthread_mutex_unlock(&shared->shares_lock);

} /* chimera_smb_add_share */

SYMBOL_EXPORT int
chimera_smb_share_set_access_based_enum(
    void       *smb_shared,
    const char *name)
{
    struct chimera_server_smb_shared *shared = smb_shared;
    struct chimera_smb_share         *cur;
    int                               rc = -1;

    pthread_mutex_lock(&shared->shares_lock);
    LL_FOREACH(shared->shares, cur)
    {
        if (strcasecmp(cur->name, name) == 0) {
            cur->access_based_enum = 1;
            rc                     = 0;
            break;
        }
    }
    pthread_mutex_unlock(&shared->shares_lock);

    return rc;
} /* chimera_smb_share_set_access_based_enum */

SYMBOL_EXPORT int
chimera_smb_share_set_encrypt_data(
    void       *smb_shared,
    const char *name)
{
    struct chimera_server_smb_shared *shared = smb_shared;
    struct chimera_smb_share         *cur;
    int                               rc = -1;

    pthread_mutex_lock(&shared->shares_lock);
    LL_FOREACH(shared->shares, cur)
    {
        if (strcasecmp(cur->name, name) == 0) {
            cur->encrypt_data         = true;
            shared->any_share_encrypt = 1;
            rc                        = 0;
            break;
        }
    }
    pthread_mutex_unlock(&shared->shares_lock);

    return rc;
} /* chimera_smb_share_set_encrypt_data */


SYMBOL_EXPORT int
chimera_smb_remove_share(
    void       *smb_shared,
    const char *name)
{
    struct chimera_server_smb_shared *shared = smb_shared;
    struct chimera_smb_share         *share;
    int                               found = 0;

    pthread_mutex_lock(&shared->shares_lock);
    LL_FOREACH(shared->shares, share)
    {
        if (strcmp(share->name, name) == 0) {
            LL_DELETE(shared->shares, share);
            chimera_smb_sharemode_destroy(&share->sharemode);
            free(share);
            found = 1;
            break;
        }
    }
    pthread_mutex_unlock(&shared->shares_lock);

    return found ? 0 : -1;
} /* chimera_smb_remove_share */

SYMBOL_EXPORT const struct chimera_smb_share *
chimera_smb_get_share(
    void       *smb_shared,
    const char *name)
{
    struct chimera_server_smb_shared *shared = smb_shared;
    struct chimera_smb_share         *share;

    pthread_mutex_lock(&shared->shares_lock);
    LL_FOREACH(shared->shares, share)
    {
        if (strcmp(share->name, name) == 0) {
            pthread_mutex_unlock(&shared->shares_lock);
            return share;
        }
    }
    pthread_mutex_unlock(&shared->shares_lock);

    return NULL;
} /* chimera_smb_get_share */

SYMBOL_EXPORT void
chimera_smb_iterate_shares(
    void                        *smb_shared,
    chimera_smb_share_iterate_cb callback,
    void                        *data)
{
    struct chimera_server_smb_shared *shared = smb_shared;
    struct chimera_smb_share         *share;

    pthread_mutex_lock(&shared->shares_lock);
    LL_FOREACH(shared->shares, share)
    {
        if (callback(share, data) != 0) {
            break;
        }
    }
    pthread_mutex_unlock(&shared->shares_lock);
} /* chimera_smb_iterate_shares */

SYMBOL_EXPORT const char *
chimera_smb_share_get_name(const struct chimera_smb_share *share)
{
    return share->name;
} /* chimera_smb_share_get_name */

SYMBOL_EXPORT const char *
chimera_smb_share_get_path(const struct chimera_smb_share *share)
{
    return share->path;
} /* chimera_smb_share_get_path */

SYMBOL_EXPORT struct chimera_server_protocol smb_protocol = {
    .init           = chimera_smb_server_init,
    .destroy        = chimera_smb_server_destroy,
    .start          = chimera_smb_server_start,
    .stop           = chimera_smb_server_stop,
    .thread_init    = chimera_smb_server_thread_init,
    .thread_destroy = chimera_smb_server_thread_destroy,
};
