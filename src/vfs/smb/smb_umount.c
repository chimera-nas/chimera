// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdint.h>
#include <stdlib.h>

#include "smb_internal.h"
#include "evpl/evpl.h"

/* UMOUNT tears the session down gracefully: TREE_DISCONNECT -> LOGOFF ->
 * close.  It is best-effort -- a server-side error on either leg still
 * completes the umount successfully and frees local state, since there is
 * nothing the caller can do about a failed teardown. */

static void
chimera_smb_client_umount_finish(struct chimera_smb_client_conn *conn)
{
    struct chimera_vfs_request *request = conn->active_request;

    conn->active_request = NULL;

    if (conn->mount) {
        free(conn->mount);
        conn->mount = NULL;
    }

    if (request) {
        request->status = CHIMERA_VFS_OK;
        request->complete(request);
    }

    /* Closing the bind drives EVPL_NOTIFY_DISCONNECTED, which frees conn. */
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
    (void) hdr;
    (void) body;
    (void) body_len;
    (void) arg;

    if (status != SMB2_STATUS_SUCCESS) {
        chimera_smbclient_info("LOGOFF returned status 0x%08x (ignored)", status);
    }

    chimera_smb_client_umount_finish(conn);
} /* chimera_smb_client_logoff_reply */

static void
chimera_smb_client_logoff_send(struct chimera_smb_client_conn *conn)
{
    struct evpl_iovec        iov;
    struct evpl_iovec_cursor cursor;
    struct smb2_header      *hdr;

    /* LOGOFF targets the session, not a tree. */
    conn->tree_id = 0;

    chimera_smb_client_pdu_begin(conn, SMB2_LOGOFF, &iov, &cursor, &hdr);

    evpl_iovec_cursor_append_uint16(&cursor, SMB2_LOGOFF_REQUEST_SIZE);
    evpl_iovec_cursor_append_uint16(&cursor, 0); /* Reserved */

    chimera_smb_client_pdu_finish(conn, &iov, &cursor,
                                  chimera_smb_client_logoff_reply, NULL);
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
    (void) hdr;
    (void) body;
    (void) body_len;
    (void) arg;

    if (status != SMB2_STATUS_SUCCESS) {
        chimera_smbclient_info("TREE_DISCONNECT returned status 0x%08x (ignored)", status);
    }

    chimera_smb_client_logoff_send(conn);
} /* chimera_smb_client_tree_disconnect_reply */

void
chimera_smb_client_umount(
    struct chimera_smb_client_thread *thread,
    struct chimera_vfs_request       *request)
{
    struct chimera_smb_client_mount *mount = request->umount.mount_private;
    struct chimera_smb_client_conn  *conn;
    struct evpl_iovec                iov;
    struct evpl_iovec_cursor         cursor;
    struct smb2_header              *hdr;

    (void) thread;

    if (!mount || !mount->conn) {
        request->status = CHIMERA_VFS_OK;
        request->complete(request);
        return;
    }

    conn                 = mount->conn;
    conn->active_request = request;

    chimera_smb_client_pdu_begin(conn, SMB2_TREE_DISCONNECT, &iov, &cursor, &hdr);

    evpl_iovec_cursor_append_uint16(&cursor, SMB2_TREE_DISCONNECT_REQUEST_SIZE);
    evpl_iovec_cursor_append_uint16(&cursor, 0); /* Reserved */

    chimera_smb_client_pdu_finish(conn, &iov, &cursor,
                                  chimera_smb_client_tree_disconnect_reply, NULL);
} /* chimera_smb_client_umount */
