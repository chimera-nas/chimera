#include "smb_internal.h"
#include "smb_procs.h"
#include "common/macros.h"
#include "common/misc.h"
#include "vfs/vfs.h"
#include "vfs/vfs_release.h"

void
chimera_smb_set_info(struct chimera_smb_request *request)
{
    #if 0
    struct chimera_smb_tree      *tree = request->tree;
    struct chimera_smb_open_file *open_file;
    uint64_t                      open_file_bucket;

    if (unlikely(request->request_struct_size != SMB2_SET_INFO_REQUEST_SIZE)) {
        chimera_smb_error("Received SMB2 SET_INFO request with invalid struct size (%u expected %u)",
                          request->smb2_hdr.struct_size,
                          SMB2_SET_INFO_REQUEST_SIZE);
        chimera_smb_complete(evpl, thread, request, SMB2_STATUS_INVALID_PARAMETER);
        return;
    }

    evpl_iovec_cursor_get_uint8(&request->request_cursor, &request->set_info.info_type);
    evpl_iovec_cursor_get_uint8(&request->request_cursor, &request->set_info.info_class);
    evpl_iovec_cursor_get_uint32(&request->request_cursor, &request->set_info.buffer_length);
    evpl_iovec_cursor_get_uint16(&request->request_cursor, &request->set_info.buffer_offset);
    evpl_iovec_cursor_skip(&request->request_cursor, 2);
    evpl_iovec_cursor_get_uint32(&request->request_cursor, &request->set_info.addl_info);
    evpl_iovec_cursor_get_uint32(&request->request_cursor, &request->set_info.flags);
    evpl_iovec_cursor_get_uint64(&request->request_cursor, &request->set_info.file_id.pid);
    evpl_iovec_cursor_get_uint64(&request->request_cursor, &request->close.file_id.vbid);

    chimera_smb_debug("set_info request: %u %u %u %u %u %llu %llu", request->set_info.info_type, request->set_info.
                      info_class, request->set_info.buffer_length, request->set_info.buffer_offset, request->set_info.
                      addl_info, request->set_info.flags, request->set_info.file_id.pid, request->set_info.file_id.vbid)
    ;

    open_file_bucket = request->close.file_id.pid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;

    pthread_mutex_lock(&tree->open_files_lock[open_file_bucket]);

    HASH_FIND(hh, tree->open_files[open_file_bucket], &request->close.file_id, sizeof(struct chimera_smb_file_id),
              open_file);

    if (unlikely(!open_file)) {
        chimera_smb_error("Received SMB2 CLOSE request for unknown file id %llu", request->close.file_id.pid);
        chimera_smb_complete(evpl, thread, request, SMB2_STATUS_FILE_CLOSED);
        return;
    }

    HASH_DELETE(hh, tree->open_files[open_file_bucket], open_file);

    pthread_mutex_unlock(&tree->open_files_lock[open_file_bucket]);

    request->set_info.handle = open_file->handle;
    #endif /* if 0 */
} /* chimera_smb_set_info */

void
chimera_smb_set_info_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request)
{
} /* chimera_smb_set_info_reply */

int
chimera_smb_parse_set_info(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request)
{
    return 0;
} /* chimera_smb_parse_set_info */