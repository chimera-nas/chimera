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

    shared->max_servers = 64;
    shared->servers     = calloc(shared->max_servers, sizeof(*shared->servers));

    /* Default until the common tcp_flavor is observed at mount time. */
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

    thread->evpl   = evpl;
    thread->shared = shared;

    return thread;
} /* chimera_smb_client_thread_init */

static void
chimera_smb_client_thread_destroy(void *private_data)
{
    struct chimera_smb_client_thread *thread = private_data;

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

    evpl_iovec_alloc(conn->evpl, 8192, 8, 1, 0, iov);

    evpl_iovec_cursor_init(cursor, iov, 1);

    /* Reserve the NetBIOS framing prefix, then make the SMB2 header the origin
     * for the consumed-relative field alignment (matches the server, which
     * resets consumed at the SMB2 header). */
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
    h->sync.tree_id            = conn->tree_id;
    h->session_id              = conn->session_id;

    *hdr = h;
} /* chimera_smb_client_pdu_begin */

void
chimera_smb_client_pdu_finish(
    struct chimera_smb_client_conn *conn,
    struct evpl_iovec              *iov,
    struct evpl_iovec_cursor       *cursor,
    chimera_smb_client_reply_cb     reply_cb,
    void                           *reply_arg)
{
    struct smb_client_netbios_header *netbios  = evpl_iovec_data(iov);
    int                               smb2_len = evpl_iovec_cursor_consumed(cursor);
    int                               total    = smb2_len + (int) sizeof(*netbios);

    /* NetBIOS length prefix carries the SMB2 message length (excluding itself),
     * big-endian in the low 24 bits. */
    netbios->word = __builtin_bswap32((uint32_t) smb2_len);

    evpl_iovec_set_length(iov, total);

    conn->reply_cb  = reply_cb;
    conn->reply_arg = reply_arg;

    evpl_sendv(conn->evpl, conn->bind, iov, 1, total, EVPL_SEND_FLAG_TAKE_REF);
} /* chimera_smb_client_pdu_finish */

static void
chimera_smb_client_handle_recv(
    struct chimera_smb_client_conn *conn,
    struct evpl_iovec              *iov,
    int                             niov,
    int                             length)
{
    struct evpl_iovec_cursor         cursor;
    struct smb_client_netbios_header netbios;
    struct smb2_header               hdr;
    chimera_smb_client_reply_cb      cb;
    void                            *arg;
    int                              body_len;

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

    cb  = conn->reply_cb;
    arg = conn->reply_arg;

    conn->reply_cb  = NULL;
    conn->reply_arg = NULL;

    if (!cb) {
        chimera_smbclient_error("Received unexpected SMB2 reply (command %u)", hdr.command);
        return;
    }

    cb(conn, hdr.status, &hdr, &cursor, body_len, arg);
} /* chimera_smb_client_handle_recv */

static void
chimera_smb_client_notify(
    struct evpl        *evpl,
    struct evpl_bind   *bind,
    struct evpl_notify *notify,
    void               *private_data)
{
    struct chimera_smb_client_conn *conn = private_data;
    struct chimera_vfs_request     *request;
    int                             i;

    switch (notify->notify_type) {
        case EVPL_NOTIFY_CONNECTED:
            conn->connected = 1;
            /* MOUNT begins its handshake once the TCP connection is up. */
            chimera_smb_client_mount_on_connected(conn);
            break;
        case EVPL_NOTIFY_DISCONNECTED:
            request = conn->active_request;

            if (request) {
                /* The peer dropped while a MOUNT/UMOUNT was in flight. */
                conn->active_request = NULL;
                request->status      = CHIMERA_VFS_EIO;
                request->complete(request);
            }

            if (conn->mount) {
                free(conn->mount);
                conn->mount = NULL;
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

/* ---- dispatch ---------------------------------------------------------- */

static void
chimera_smb_client_dispatch(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_smb_client_thread *thread = private_data;

    switch (request->opcode) {
        case CHIMERA_VFS_OP_MOUNT:
            chimera_smb_client_mount(thread, request);
            break;
        case CHIMERA_VFS_OP_UMOUNT:
            chimera_smb_client_umount(thread, request);
            break;
        default:
            request->status = CHIMERA_VFS_ENOTSUP;
            request->complete(request);
            break;
    } /* switch */
} /* chimera_smb_client_dispatch */

SYMBOL_EXPORT struct chimera_vfs_module vfs_smb = {
    .name           = "smb",
    .fh_magic       = CHIMERA_VFS_FH_MAGIC_SMB,
    .capabilities   = CHIMERA_VFS_CAP_FS | CHIMERA_VFS_CAP_FS_RELATIVE_OP,
    .init           = chimera_smb_client_init,
    .destroy        = chimera_smb_client_destroy,
    .thread_init    = chimera_smb_client_thread_init,
    .thread_destroy = chimera_smb_client_thread_destroy,
    .dispatch       = chimera_smb_client_dispatch,
};
