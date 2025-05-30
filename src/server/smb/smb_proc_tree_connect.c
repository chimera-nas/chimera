#include "smb_internal.h"
#include "smb_procs.h"
#include "common/misc.h"
#include "vfs/vfs.h"
#include "smb_string.h"

void
chimera_smb_tree_connect(struct chimera_smb_request *request)
{
    struct chimera_server_smb_thread *thread = request->compound->thread;
    struct chimera_server_smb_shared *shared = thread->shared;
    struct chimera_smb_share         *share  = NULL, *cur_share;
    struct chimera_smb_tree          *tree;
    struct chimera_smb_session       *session = request->session;
    char                             *request_pathp;
    int                               i;
    char                              path[CHIMERA_VFS_PATH_MAX];

    chimera_smb_utf16le_to_utf8(&thread->iconv_ctx,
                                request->tree_connect.path,
                                request->tree_connect.path_length,
                                path, sizeof(path));

    request_pathp = path;

    while (*request_pathp == '\\') {
        request_pathp++;
    }

    while (*request_pathp != '\0' && *request_pathp != '\\') {
        request_pathp++;
    }

    while (*request_pathp == '\\') {
        request_pathp++;
    }

    pthread_mutex_lock(&shared->shares_lock);

    LL_FOREACH(shared->shares, cur_share)
    {
        if (strcmp(cur_share->name, request_pathp) == 0) {
            share = cur_share;
            break;
        }
    }

    pthread_mutex_unlock(&shared->shares_lock);

    if (!share) {
        chimera_smb_error("Received SMB2 TREE_CONNECT request for unknown share '%s'", request->tree_connect.path);
        chimera_smb_complete_request(request, SMB2_STATUS_BAD_NETWORK_NAME);
        return;
    }

    tree = NULL;

    pthread_mutex_lock(&session->lock);

    for (i = 1; i < session->max_trees; i++) {
        if (session->trees[i] && session->trees[i]->share == share) {
            tree = session->trees[i];
            break;
        }
    }

    if (tree) {
        tree->refcnt++;
    } else {

        tree        = chimera_smb_tree_alloc(shared);
        tree->share = share;

        for (i = 1; i < session->max_trees; i++) {

            if (!session->trees[i]) {
                break;
            }
        }

        if (i < session->max_trees) {
            tree->tree_id     = i;
            session->trees[i] = tree;
        } else {

            tree->tree_id = session->max_trees;

            session->max_trees *= 2;
            session->trees      = realloc(session->trees,
                                          session->max_trees * sizeof(struct chimera_smb_tree *));

            session->trees[tree->tree_id] = tree;
        }
    }

    pthread_mutex_unlock(&session->lock);

    request->tree = tree;

    chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
} /* chimera_smb_tree_connect */

void
chimera_smb_tree_connect_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request)
{
    evpl_iovec_cursor_append_uint16(reply_cursor, SMB2_TREE_CONNECT_REPLY_SIZE);

    /* Share Type 0 = DISK*/
    evpl_iovec_cursor_append_uint8(reply_cursor, 0);

    /* Share Flags*/
    evpl_iovec_cursor_append_uint8(reply_cursor, 0);

    /* Capabilitiers */
    evpl_iovec_cursor_append_uint32(reply_cursor, 0);

    /* Maximal Access 0x001F01FF (ful RW) */
    evpl_iovec_cursor_append_uint32(reply_cursor, 0x001F01FF);

} /* chimera_smb_tree_connect_reply */

int
chimera_smb_parse_tree_connect(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request)
{
    if (unlikely(request->request_struct_size != SMB2_TREE_CONNECT_REQUEST_SIZE)) {
        chimera_smb_error("Received SMB2 TREE_CONNECT request with invalid struct size (%u expected %u)",
                          request->request_struct_size,
                          SMB2_TREE_CONNECT_REQUEST_SIZE);
        return -1;
    }

    evpl_iovec_cursor_get_uint16(request_cursor, &request->tree_connect.flags);
    evpl_iovec_cursor_get_uint16(request_cursor, &request->tree_connect.path_offset);
    evpl_iovec_cursor_get_uint16(request_cursor, &request->tree_connect.path_length);

    if (unlikely(request->tree_connect.path_length > CHIMERA_VFS_PATH_MAX)) {
        chimera_smb_error("Received SMB2 TREE_CONNECT request with invalid path length (%u max %u)",
                          request->tree_connect.path_length,
                          CHIMERA_VFS_PATH_MAX);
        return -1;
    }

    evpl_iovec_cursor_copy(request_cursor, request->tree_connect.path, request->tree_connect.path_length);

    request->tree_connect.path_length >>= 1;

    return 0;
} /* chimera_smb_parse_tree_connect */
