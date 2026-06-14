// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdint.h>
#include <stdlib.h>

#include "smb_internal.h"
#include "evpl/evpl.h"

/* UMOUNT tears the shared session down gracefully on the calling thread's
 * connection: TREE_DISCONNECT -> LOGOFF -> close.  Best-effort: a server-side
 * error on either leg still completes the umount successfully.  The server
 * registration is retained (freed at module destroy) so other threads' cached
 * connections never dereference freed state. */

static void
chimera_smb_client_umount_finish(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    conn->server->session_ready = 0;

    /* Detach before closing so the DISCONNECTED notify does not re-find this
     * connection in the thread's table. */
    if (conn->thread->conns[conn->server->index] == conn) {
        conn->thread->conns[conn->server->index] = NULL;
    }

    request->status = CHIMERA_VFS_OK;
    request->complete(request);

    conn->closing = 1;
    evpl_close(conn->evpl, conn->bind);
} /* chimera_smb_client_umount_finish */

static void
chimera_smb_client_logoff_reply(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    struct chimera_vfs_request *request = arg;

    (void) hdr;
    (void) body;
    (void) body_len;

    if (status != SMB2_STATUS_SUCCESS) {
        chimera_smbclient_info("LOGOFF returned status 0x%08x (ignored)", status);
    }

    chimera_smb_client_umount_finish(conn, request);
} /* chimera_smb_client_logoff_reply */

static void
chimera_smb_client_logoff_send(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    struct evpl_iovec        iov;
    struct evpl_iovec_cursor cursor;
    struct smb2_header      *hdr;

    chimera_smb_client_pdu_begin(conn, SMB2_LOGOFF, &iov, &cursor, &hdr);

    evpl_iovec_cursor_append_uint16(&cursor, SMB2_LOGOFF_REQUEST_SIZE);
    evpl_iovec_cursor_append_uint16(&cursor, 0);

    chimera_smb_client_pdu_finish(conn, &iov, &cursor, request,
                                  chimera_smb_client_logoff_reply, request);
} /* chimera_smb_client_logoff_send */

static void
chimera_smb_client_tree_disconnect_reply(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    struct chimera_vfs_request *request = arg;

    (void) hdr;
    (void) body;
    (void) body_len;

    if (status != SMB2_STATUS_SUCCESS) {
        chimera_smbclient_info("TREE_DISCONNECT returned status 0x%08x (ignored)", status);
    }

    chimera_smb_client_logoff_send(conn, request);
} /* chimera_smb_client_tree_disconnect_reply */

void
chimera_smb_client_umount(
    struct chimera_smb_client_thread *thread,
    struct chimera_vfs_request       *request)
{
    struct chimera_smb_client_server *server = request->umount.mount_private;
    struct chimera_smb_client_conn   *conn;
    struct evpl_iovec                 iov;
    struct evpl_iovec_cursor          cursor;
    struct smb2_header               *hdr;

    if (!server) {
        request->status = CHIMERA_VFS_OK;
        request->complete(request);
        return;
    }

    conn = thread->conns[server->index];

    if (!conn || conn->state != CHIMERA_SMB_CONN_READY) {
        /* No live connection to this server on this thread; nothing to send. */
        request->status = CHIMERA_VFS_OK;
        request->complete(request);
        return;
    }

    chimera_smb_client_pdu_begin(conn, SMB2_TREE_DISCONNECT, &iov, &cursor, &hdr);

    evpl_iovec_cursor_append_uint16(&cursor, SMB2_TREE_DISCONNECT_REQUEST_SIZE);
    evpl_iovec_cursor_append_uint16(&cursor, 0);

    chimera_smb_client_pdu_finish(conn, &iov, &cursor, request,
                                  chimera_smb_client_tree_disconnect_reply, request);
} /* chimera_smb_client_umount */
