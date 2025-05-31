#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

#include "smb.h"
#include "server/protocol.h"
#include "vfs/vfs.h"
#include "common/macros.h"
#include "common/misc.h"
#include "common/evpl_iovec_cursor.h"
#include "evpl/evpl.h"
#include "smb_internal.h"
#include "smb_procs.h"
#include "smb_dump.h"
#include "xxhash.h"

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
    struct chimera_server_smb_shared *shared = calloc(1, sizeof(*shared));

    if (!shared) {
        return NULL;
    }

    shared->config.port = 445;

    snprintf(shared->config.identity, sizeof(shared->config.identity), "chimera");

    shared->vfs     = vfs;
    shared->metrics = metrics;

    *(XXH128_hash_t *) shared->guid = XXH3_128bits(shared->config.identity, strlen(shared->config.identity));

    shared->endpoint = evpl_endpoint_create("0.0.0.0", shared->config.port);

    shared->listener = evpl_listener_create();

    pthread_mutex_init(&shared->sessions_lock, NULL);
    pthread_mutex_init(&shared->shares_lock, NULL);
    pthread_mutex_init(&shared->trees_lock, NULL);

    return shared;
} /* smb_server_init */

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

    evpl_listener_destroy(shared->listener);

    free(shared);
} /* smb_server_destroy */

static void
chimera_smb_server_start(void *data)
{
    struct chimera_server_smb_shared *shared = data;

    evpl_listen(shared->listener, EVPL_STREAM_SOCKET_TCP, shared->endpoint);
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
    struct evpl_iovec                 reply_iov[65];
    struct netbios_header            *netbios_hdr;
    struct smb2_header               *reply_hdr;
    struct chimera_smb_request       *request;
    uint32_t                         *prev_command = NULL;
    uint16_t                          prev_hdr     = 0;
    int                               i;

    smb_dump_compound_reply(compound);

    evpl_iovec_alloc(evpl, 4096, 8, 1, &reply_iov[0]);

    evpl_iovec_cursor_init(&reply_cursor, reply_iov, 1);

    netbios_hdr = evpl_iovec_cursor_data(&reply_cursor);
    evpl_iovec_cursor_skip(&reply_cursor, sizeof(struct netbios_header));

    evpl_iovec_cursor_reset_consumed(&reply_cursor);

    for (i = 0; i < compound->num_requests; i++) {
        request = compound->requests[i];

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
        reply_hdr->credit_request_response = request->smb2_hdr.credit_request_response;
        reply_hdr->flags                   = request->smb2_hdr.flags | SMB2_FLAGS_SERVER_TO_REDIR;
        reply_hdr->next_command            = 0;
        reply_hdr->message_id              = request->smb2_hdr.message_id;
        reply_hdr->session_id              = request->session ? request->session->session_id : 0;
        reply_hdr->sync.process_id         = request->smb2_hdr.sync.process_id;
        reply_hdr->sync.tree_id            = request->tree ? request->tree->tree_id : 0;

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

        chimera_smb_request_free(thread, request);

        evpl_iovec_cursor_zero(&reply_cursor, (8 - (evpl_iovec_cursor_consumed(&reply_cursor) & 7)) & 7);

    }

    netbios_hdr->word = __builtin_bswap32(evpl_iovec_cursor_consumed(&reply_cursor));

    evpl_sendv(evpl, conn->bind, reply_iov, reply_cursor.niov, evpl_iovec_cursor_consumed(&reply_cursor) + 4);

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

        if (request->session) {
            compound->saved_session_id = request->session->session_id;
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
            chimera_smb_abort("smb_server_handle_msg: unknown command %u", request->smb2_hdr.command);
    } /* switch */

} /* chimera_smb_compound_advance */

static void
chimera_smb_server_handle_compound(
    struct evpl                      *evpl,
    struct chimera_server_smb_thread *thread,
    struct chimera_smb_conn          *conn,
    struct evpl_iovec                *iov,
    int                               niov,
    int                               length)
{
    struct chimera_smb_compound *compound;
    struct chimera_smb_request  *request;
    struct evpl_iovec_cursor     request_cursor;
    //uint32_t                     netbios_length;
    struct netbios_header        netbios_hdr;
    int                          more_requests, rc;


    compound = chimera_smb_compound_alloc(thread);

    compound->thread = thread;
    compound->conn   = conn;

    compound->saved_session_id  = UINT64_MAX;
    compound->saved_tree_id     = UINT64_MAX;
    compound->saved_file_id.pid = UINT64_MAX;
    compound->saved_file_id.vid = UINT64_MAX;

    evpl_iovec_cursor_init(&request_cursor, iov, niov);

    evpl_iovec_cursor_copy(&request_cursor, &netbios_hdr, sizeof(netbios_hdr));

    //netbios_length = __builtin_bswap32(netbios_hdr.word) & 0x00ffffff;

    compound->num_requests      = 0;
    compound->complete_requests = 0;

    do {

        evpl_iovec_cursor_reset_consumed(&request_cursor);

        request = chimera_smb_request_alloc(thread);

        request->compound = compound;

        evpl_iovec_cursor_copy(&request_cursor, &request->smb2_hdr, sizeof(request->smb2_hdr));

        if (unlikely(request->smb2_hdr.protocol_id[0] != 0xFE ||
                     request->smb2_hdr.protocol_id[1] != 0x53 ||
                     request->smb2_hdr.protocol_id[2] != 0x4D ||
                     request->smb2_hdr.protocol_id[3] != 0x42)) {
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

        evpl_iovec_cursor_copy(&request_cursor,
                               &request->request_struct_size,
                               sizeof(request->request_struct_size));


        if (request->smb2_hdr.session_id) {

            if (conn->last_session && conn->last_session->session_id == request->smb2_hdr.session_id) {
                request->session = conn->last_session;
            } else {
                HASH_FIND(hh, conn->session_handles, &request->smb2_hdr.session_id, sizeof(uint64_t), request->session);

                if (!request->session) {
                    chimera_smb_error("Received SMB2 message with invalid session id");
                    chimera_smb_request_free(thread, request);
                    evpl_close(evpl, conn->bind);
                    return;
                }

                conn->last_session = request->session;
            }
        } else {
            request->session = NULL;
        }

        if (unlikely(!request->session && (request->smb2_hdr.command != SMB2_NEGOTIATE &&
                                           request->smb2_hdr.command != SMB2_SESSION_SETUP))) {
            chimera_smb_error("Received SMB2 message with invalid command and no session");
            chimera_smb_request_free(thread, request);
            evpl_close(evpl, conn->bind);
            return;
        }

        if (request->session && request->smb2_hdr.sync.tree_id < request->session->max_trees) {
            request->tree = request->session->trees[request->smb2_hdr.sync.tree_id];
        }

        switch (request->smb2_hdr.command) {
            case SMB2_NEGOTIATE:
                rc = chimera_smb_parse_negotiate(&request_cursor, request);
                break;
            case SMB2_SESSION_SETUP:
                rc = chimera_smb_parse_session_setup(&request_cursor, request);
                break;
            case SMB2_LOGOFF:
                rc = chimera_smb_parse_logoff(&request_cursor, request);
                break;
            case SMB2_TREE_CONNECT:
                rc = chimera_smb_parse_tree_connect(&request_cursor, request);
                break;
            case SMB2_TREE_DISCONNECT:
                rc = chimera_smb_parse_tree_disconnect(&request_cursor, request);
                break;
            case SMB2_CREATE:
                rc = chimera_smb_parse_create(&request_cursor, request);
                break;
            case SMB2_CLOSE:
                rc = chimera_smb_parse_close(&request_cursor, request);
                break;
            case SMB2_WRITE:
                rc = chimera_smb_parse_write(&request_cursor, request);
                break;
            case SMB2_READ:
                rc = chimera_smb_parse_read(&request_cursor, request);
                break;
            case SMB2_QUERY_INFO:
                rc = chimera_smb_parse_query_info(&request_cursor, request);
                break;
            case SMB2_QUERY_DIRECTORY:
                rc = chimera_smb_parse_query_directory(&request_cursor, request);
                break;
            case SMB2_SET_INFO:
                rc = chimera_smb_parse_set_info(&request_cursor, request);
                break;
            default:
                chimera_smb_error("smb_server_handle_msg: unknown command %u", request->smb2_hdr.command);
                chimera_smb_complete_request(request, SMB2_STATUS_INTERNAL_ERROR);
                rc = 0;
        } /* switch */

        if (rc) {
            chimera_smb_error("smb_server_handle_msg: failed to parse command %u", request->smb2_hdr.command);
            chimera_smb_request_free(thread, request);
            evpl_close(evpl, conn->bind);
            return;
        }

        compound->requests[compound->num_requests++] = request;

        more_requests = request->smb2_hdr.next_command != 0;

        if (more_requests) {
            evpl_iovec_cursor_skip(&request_cursor,
                                   request->smb2_hdr.next_command - evpl_iovec_cursor_consumed(&request_cursor));
        }

    } while (more_requests);

    smb_dump_compound_request(compound);

    chimera_smb_compound_advance(compound);

} /* chimera_smb_server_handle_compound */


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
            break;
        case EVPL_NOTIFY_DISCONNECTED:
            chimera_smb_conn_free(thread, conn);
            break;
        case EVPL_NOTIFY_RECV_MSG:
            chimera_smb_server_handle_compound(evpl, thread, conn,
                                               notify->recv_msg.iovec,
                                               notify->recv_msg.niov,
                                               notify->recv_msg.length);
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

    conn->thread = thread;
    conn->bind   = bind;

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

    thread->vfs_thread = vfs_thread;
    thread->shared     = shared;
    thread->evpl       = evpl;

    chimera_smb_iconv_init(&thread->iconv_ctx);

    evpl_listener_attach(evpl, shared->listener, chimera_smb_server_accept, thread);

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

    evpl_listener_detach(thread->evpl, thread->shared->listener);

    chimera_smb_iconv_destroy(&thread->iconv_ctx);

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

    strncpy(share->name, name, sizeof(share->name));
    strncpy(share->path, path, sizeof(share->path));

    pthread_mutex_lock(&shared->shares_lock);
    LL_PREPEND(shared->shares, share);
    pthread_mutex_unlock(&shared->shares_lock);

} /* chimera_s3_add_bucket */

SYMBOL_EXPORT struct chimera_server_protocol smb_protocol = {
    .init           = chimera_smb_server_init,
    .destroy        = chimera_smb_server_destroy,
    .start          = chimera_smb_server_start,
    .thread_init    = chimera_smb_server_thread_init,
    .thread_destroy = chimera_smb_server_thread_destroy,
};