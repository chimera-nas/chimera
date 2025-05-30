#include "smb_internal.h"
#include "smb_procs.h"
#include "common/macros.h"
#include "common/misc.h"
#include "vfs/vfs.h"
#include "vfs/vfs_release.h"

static void
chimera_smb_read_callback(
    enum chimera_vfs_error    error_code,
    uint32_t                  count,
    uint32_t                  eof,
    struct evpl_iovec        *iov,
    int                       niov,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_smb_request *request = private_data;

    request->read.niov     = niov;
    request->read.r_length = count;

    chimera_smb_complete_request(private_data, error_code ? SMB2_STATUS_INTERNAL_ERROR : SMB2_STATUS_SUCCESS);
} /* chimera_smb_read_callback */


void
chimera_smb_read(struct chimera_smb_request *request)
{
    struct chimera_server_smb_thread *thread = request->compound->thread;
    struct chimera_smb_open_file     *open_file;

    open_file = chimera_smb_open_file_lookup(request, &request->read.file_id);

    chimera_vfs_read(
        thread->vfs_thread,
        open_file->handle,
        request->read.offset,
        request->read.length,
        request->read.iov,
        request->read.niov,
        0,
        chimera_smb_read_callback,
        request);
} /* chimera_smb_read */


int
chimera_smb_parse_read(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request)
{
    uint16_t blob_offset, blob_length;

    evpl_iovec_cursor_get_uint8(request_cursor, &request->read.flags);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->read.length);
    evpl_iovec_cursor_get_uint64(request_cursor, &request->read.offset);
    evpl_iovec_cursor_get_uint64(request_cursor, &request->read.file_id.pid);
    evpl_iovec_cursor_get_uint64(request_cursor, &request->read.file_id.vid);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->read.minimum);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->read.channel);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->read.remaining);
    evpl_iovec_cursor_get_uint16(request_cursor, &blob_offset);
    evpl_iovec_cursor_get_uint16(request_cursor, &blob_length);

    request->read.niov = 64;

    return 0;
} /* chimera_smb_parse_write */


void
chimera_smb_read_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request)
{
    evpl_iovec_cursor_append_uint16(reply_cursor, SMB2_READ_REPLY_SIZE);
    evpl_iovec_cursor_append_uint16(reply_cursor, 64 + 16); /* data offset */
    evpl_iovec_cursor_append_uint32(reply_cursor, request->read.r_length);
    evpl_iovec_cursor_append_uint32(reply_cursor, 0); /* remaining */

    evpl_iovec_cursor_inject(reply_cursor, request->read.iov, request->read.niov, request->read.length);

} /* chimera_smb_write_reply */
