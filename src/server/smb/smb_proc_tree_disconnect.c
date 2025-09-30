// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_internal.h"
#include "smb_procs.h"
#include "common/misc.h"

void
chimera_smb_tree_disconnect(struct chimera_smb_request *request)
{
    struct chimera_server_smb_thread *thread  = request->compound->thread;
    struct chimera_smb_session       *session = request->session_handle->session;

    if (!request->tree) {
        chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
        return;
    }

    pthread_mutex_lock(&session->lock);

    request->tree->refcnt--;

    if (request->tree->refcnt == 0) {
        session->trees[request->tree->tree_id] = NULL;
        chimera_smb_tree_free(thread, thread->shared, request->tree);
    }

    pthread_mutex_unlock(&session->lock);

    chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);

} /* chimera_smb_tree_disconnect */

void
chimera_smb_tree_disconnect_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request)
{

    evpl_iovec_cursor_append_uint16(reply_cursor, SMB2_TREE_DISCONNECT_REPLY_SIZE);

} /* chimera_smb_tree_disconnect_reply */

int
chimera_smb_parse_tree_disconnect(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request)
{
    if (unlikely(request->request_struct_size != SMB2_TREE_DISCONNECT_REQUEST_SIZE)) {
        chimera_smb_error("Received SMB2 TREE_DISCONNECT request with invalid struct size (%u expected %u)",
                          request->request_struct_size,
                          SMB2_TREE_DISCONNECT_REQUEST_SIZE);
        return -1;
    }
    return 0;
} /* chimera_smb_parse_tree_disconnect */
