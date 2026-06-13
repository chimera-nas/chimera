// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#include "smb.h"
#include "smb_internal.h"
#include "common/macros.h"
#include "common/tcp_flavor.h"
#include "evpl/evpl.h"

static const uint8_t SMB2_PROTOCOL_ID[4] = { 0xFE, 'S', 'M', 'B' };

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
    int                                total    = smb2_len + (int) sizeof(*netbios);

    pending             = calloc(1, sizeof(*pending));
    pending->message_id = hdr->message_id;
    pending->cb         = reply_cb;
    pending->arg        = reply_arg;
    pending->request    = request;
    pending->next       = conn->pending;
    conn->pending       = pending;

    netbios->word = __builtin_bswap32((uint32_t) smb2_len);

    evpl_iovec_set_length(iov, total);

    evpl_sendv(conn->evpl, conn->bind, iov, 1, total, EVPL_SEND_FLAG_TAKE_REF);
} /* chimera_smb_client_pdu_finish */

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
