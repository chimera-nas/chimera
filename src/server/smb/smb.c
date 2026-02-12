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
#include "smb_procs.h"
#include "smb_dump.h"
#include "smb_signing.h"
#include "xxhash.h"

static const uint8_t SMB2_PROTOCOL_ID[4] = { 0xFE, 'S', 'M', 'B' };

static inline int
chimera_smb_is_error_status(unsigned int status)
{
    return status != SMB2_STATUS_SUCCESS &&
           status != SMB2_STATUS_MORE_PROCESSING_REQUIRED;
} /* chimera_smb_is_error_status */

static inline int
chimera_smb_status_should_abort(unsigned int status)
{
    return status != SMB2_STATUS_SUCCESS &&
           status != SMB2_STATUS_MORE_PROCESSING_REQUIRED &&
           status != SMB2_STATUS_NO_MORE_FILES;
} /* chimera_smb_status_should_abort */

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

    shared->config.port      = 445;
    shared->config.rdma_port = 445;

    shared->config.capabilities = SMB2_GLOBAL_CAP_LARGE_MTU | SMB2_GLOBAL_CAP_MULTI_CHANNEL;

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
    }
    if (shared->config.auth.kerberos_enabled) {
        chimera_smb_info("SMB Auth: Kerberos enabled (realm: %s, keytab: %s)",
                         shared->config.auth.kerberos_realm[0] ? shared->config.auth.kerberos_realm : "(not set)",
                         shared->config.auth.kerberos_keytab[0] ? shared->config.auth.kerberos_keytab : "(default)");
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
        free(share);
    }

    uint32_t min;

    if (shared->svc != GSS_C_NO_NAME) {
        gss_release_name(&min, &shared->svc);
    }

    if (shared->srv_cred != GSS_C_NO_CREDENTIAL) {
        gss_release_cred(&min, &shared->srv_cred);
    }

    free(shared);
} /* smb_server_destroy */

static void
chimera_smb_server_start(void *data)
{
    struct chimera_server_smb_shared *shared = data;

    evpl_listen(shared->listener, EVPL_STREAM_SOCKET_TCP, shared->endpoint);

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
    uint32_t                         *prev_command = NULL;
    uint16_t                          prev_hdr     = 0;
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

        if ((conn->flags & CHIMERA_SMB_CONN_FLAG_SIGNING_REQUIRED) &&
            request->session_handle) {
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
        reply_hdr->credit_request_response = 256;
        reply_hdr->flags                   = request->smb2_hdr.flags | SMB2_FLAGS_SERVER_TO_REDIR;
        reply_hdr->next_command            = 0;
        reply_hdr->message_id              = request->smb2_hdr.message_id;
        reply_hdr->session_id              = request->session_handle && request->session_handle->session ?
            request->session_handle->session->session_id : 0;
        reply_hdr->sync.process_id = request->smb2_hdr.sync.process_id;
        reply_hdr->sync.tree_id    = request->tree ? request->tree->tree_id : 0;

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
            } /* switch */
        }

        evpl_iovec_cursor_zero(&reply_cursor, (8 - (evpl_iovec_cursor_consumed(&reply_cursor) & 7)) & 7);

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

    if (conn->protocol == EVPL_DATAGRAM_RDMACM_RC) {

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

static inline void
chimera_smb_compound_abort(struct chimera_smb_compound *compound)
{
    if (compound->complete_requests < compound->num_requests) {
        chimera_smb_complete_request(compound->requests[compound->complete_requests], SMB2_STATUS_REQUEST_ABORTED);
    } else {
        chimera_smb_compound_reply(compound);
    }
} /* chimera_smb_compound_abort */

void
chimera_smb_complete_request(
    struct chimera_smb_request *request,
    unsigned int                status)
{
    struct chimera_smb_compound *compound = request->compound;

    request->status = status;

    compound->complete_requests++;

    if (chimera_smb_status_should_abort(status)) {
        chimera_smb_compound_abort(compound);
    } else {

        if (request->session_handle && request->session_handle->session) {
            compound->saved_session_id = request->session_handle->session->session_id;
        }

        if (request->tree) {
            compound->saved_tree_id = request->tree->tree_id;
        }

        chimera_smb_compound_advance(compound);
    }
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
        default:
            chimera_smb_complete_request(request, SMB2_STATUS_NOT_IMPLEMENTED);
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
    int                               length)
{
    struct chimera_smb_compound       *compound;
    struct chimera_smb_request        *request;
    struct chimera_smb_session_handle *session_handle;
    struct chimera_smb_session        *session;
    struct evpl_iovec_cursor           signature_cursor;
    int                                more_requests, rc, left = length, payload_length;

    compound = chimera_smb_compound_alloc(thread);

    compound->thread = thread;
    compound->conn   = conn;

    compound->saved_session_id  = UINT64_MAX;
    compound->saved_tree_id     = UINT64_MAX;
    compound->saved_file_id.pid = UINT64_MAX;
    compound->saved_file_id.vid = UINT64_MAX;

    compound->num_requests      = 0;
    compound->complete_requests = 0;

    left = length;

    while (left) {

        evpl_iovec_cursor_reset_consumed(request_cursor);

        request = chimera_smb_request_alloc(thread);

        request->compound = compound;

        evpl_iovec_cursor_copy(request_cursor, &request->smb2_hdr, sizeof(request->smb2_hdr));

        left -= sizeof(request->smb2_hdr);

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




        if (request->smb2_hdr.session_id) {

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
                        chimera_smb_error("Received SMB2 message with invalid session id %lx", request->smb2_hdr.
                                          session_id);
                        chimera_smb_request_free(thread, request);
                        evpl_close(evpl, conn->bind);
                        return;
                    }
                }
            }
        } else {
            request->session_handle = NULL;
        }

        if (request->smb2_hdr.flags & SMB2_FLAGS_SIGNED) {

            request->flags |= CHIMERA_SMB_REQUEST_FLAG_SIGN;

            if (request->smb2_hdr.next_command) {
                payload_length = request->smb2_hdr.next_command - sizeof(request->smb2_hdr);
            } else {
                payload_length = left;
            }

            if (unlikely(request->session_handle == NULL)) {
                chimera_smb_error("Received signed SMB2 message with missing/invalid session id %x",
                                  request->smb2_hdr.session_id);
                chimera_smb_request_free(thread, request);
                evpl_close(evpl, conn->bind);
                return;
            }

            rc = chimera_smb_verify_signature(thread->signing_ctx, request, &signature_cursor, payload_length);

            if (unlikely(rc != 0)) {
                chimera_smb_error("Received SMB2 message with invalid signature");
                chimera_smb_request_free(thread, request);
                evpl_close(evpl, conn->bind);
                return;
            }
        }

        if (unlikely(!request->session_handle && (request->smb2_hdr.command != SMB2_NEGOTIATE &&
                                                  request->smb2_hdr.command != SMB2_SESSION_SETUP &&
                                                  request->smb2_hdr.command != SMB2_ECHO))) {
            chimera_smb_error("Received SMB2 message with invalid command and no session");
            chimera_smb_complete_request(request, SMB2_STATUS_NO_SUCH_LOGON_SESSION);
            chimera_smb_request_free(thread, request);
            return;
        }

        if (request->session_handle && request->smb2_hdr.sync.tree_id < request->session_handle->session->max_trees) {
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
            default:
                chimera_smb_error("Received SMB2 message with unimplemented command %u",
                                  request->smb2_hdr.command);
                request->status = SMB2_STATUS_NOT_IMPLEMENTED;
                rc              = 0;
        } /* switch */

        if (rc) {
            chimera_smb_error("smb_server_handle_msg: failed to parse command %u", request->smb2_hdr.command);
            if (request->status != SMB2_STATUS_SUCCESS) {
                chimera_smb_complete_request(request, request->status);
            } else {
                chimera_smb_complete_request(request, SMB2_STATUS_INVALID_PARAMETER);
            }
            chimera_smb_request_free(thread, request);
            /* TODO: Should I return here, or should I try to continue parsing the rest of the compound?
             * SMB2_STATUS_INVALID_PARAMETER will cause the client to drop the connection,
             * so it may not matter either way
             */
            return;
        }

        compound->requests[compound->num_requests++] = request;

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
            matched = 1;
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

    compound->saved_session_id  = UINT64_MAX;
    compound->saved_tree_id     = UINT64_MAX;
    compound->saved_file_id.pid = UINT64_MAX;
    compound->saved_file_id.vid = UINT64_MAX;

    compound->num_requests      = 0;
    compound->complete_requests = 0;

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
    request->negotiate.dialects[0]              = 0x02ff;

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
        chimera_smb_server_handle_smb2(evpl, thread, conn, &request_cursor, conn->rdma_length);
        conn->rdma_niov   = 0;
        conn->rdma_length = 0;
    }

} /* chimera_smb_server_handle_rdma */


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

    if (likely(conn->smbvers == 2)) {
        chimera_smb_server_handle_smb2(evpl, thread, conn, &request_cursor, length - 4);
        return;
    }

    peek_cursor = request_cursor;
    evpl_iovec_cursor_get_uint32(&peek_cursor, &smb_hdr);

    if (smb_hdr == 0x424d53fe) {
        conn->smbvers = 2;
        chimera_smb_server_handle_smb2(evpl, thread, conn, &request_cursor, length - 4);
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
    conn->rdma_niov   = 0;
    conn->rdma_length = 0;

    evpl_bind_get_local_address(bind, conn->local_addr, sizeof(conn->local_addr));
    evpl_bind_get_remote_address(bind, conn->remote_addr, sizeof(conn->remote_addr));

    *notify_callback   = chimera_smb_server_notify;
    *segment_callback  = chimera_smb_server_segment;
    *conn_private_data = conn;

} /* smb_server_accept */


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

    thread->vfs_thread  = vfs_thread;
    thread->shared      = shared;
    thread->evpl        = evpl;
    thread->signing_ctx = chimera_smb_signing_ctx_create();

    chimera_smb_iconv_init(&thread->iconv_ctx);

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

    evpl_listener_detach(thread->evpl, thread->binding);

    chimera_smb_iconv_destroy(&thread->iconv_ctx);
    chimera_smb_signing_ctx_destroy(thread->signing_ctx);

    free(thread);
} /* smb_server_thread_destroy */


SYMBOL_EXPORT void
chimera_smb_add_share(
    void       *smb_shared,
    const char *name,
    const char *path)
{
    struct chimera_server_smb_shared *shared = smb_shared;
    struct chimera_smb_share         *share  = calloc(1, sizeof(*share));

    snprintf(share->name, sizeof(share->name), "%s", name);
    snprintf(share->path, sizeof(share->path), "%s", path);

    pthread_mutex_lock(&shared->shares_lock);
    LL_PREPEND(shared->shares, share);
    pthread_mutex_unlock(&shared->shares_lock);

} /* chimera_smb_add_share */


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
