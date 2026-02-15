// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <sys/time.h>
#include <strings.h>
#include "server/smb/smb2.h"
#include "server/smb/smb_session.h"
#include "smb_internal.h"
#include "smb_procs.h"
#include "smb_string.h"
#include "common/misc.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "smb_attr.h"
#include "smb_lsarpc.h"

#define SMB2_WRITE_MASK (SMB2_FILE_WRITE_DATA | \
                         SMB2_FILE_APPEND_DATA | \
                         SMB2_FILE_WRITE_EA | \
                         SMB2_FILE_WRITE_ATTRIBUTES | \
                         SMB2_FILE_DELETE_CHILD | \
                         SMB2_FILE_ADD_FILE | \
                         SMB2_FILE_ADD_SUBDIRECTORY | \
                         SMB2_DELETE | \
                         SMB2_WRITE_DACL | \
                         SMB2_WRITE_OWNER | \
                         SMB2_GENERIC_WRITE | \
                         SMB2_GENERIC_ALL)

static inline void
chimera_smb_create_unlink_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_smb_request *request    = private_data;
    struct chimera_vfs_thread  *vfs_thread = request->compound->thread->vfs_thread;

    /* XXX We will ignore any error because there's nothing sane to do */

    chimera_vfs_release(vfs_thread, request->create.parent_handle);
    chimera_smb_open_file_release(request, request->create.r_open_file);

    chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
} /* chimera_smb_create_unlink_callback */


static inline void
chimera_smb_create_unlink(struct chimera_smb_request *request)
{
    struct chimera_vfs_thread    *vfs_thread = request->compound->thread->vfs_thread;
    struct chimera_smb_open_file *open_file  = request->create.r_open_file;

    chimera_vfs_remove_at(
        vfs_thread,
        &request->session_handle->session->cred,
        request->create.parent_handle,
        open_file->name,
        open_file->name_len,
        NULL,
        0,
        0,
        0,
        chimera_smb_create_unlink_callback,
        request);
} /* chimera_smb_create_unlink */

static inline struct chimera_smb_open_file *
chimera_smb_create_gen_open_file(
    struct chimera_smb_request     *request,
    enum chimera_smb_open_file_type type,
    chimera_smb_pipe_transceive_t   transceive,
    uint64_t                        pid,
    const void                     *parent_fh,
    int                             parent_fh_len,
    const char                     *name,
    int                             name_len,
    int                             delete_on_close,
    struct chimera_vfs_open_handle *oh)
{
    struct chimera_smb_compound      *compound = request->compound;
    struct chimera_server_smb_thread *thread   = compound->thread;
    struct chimera_smb_tree          *tree     = request->tree;
    struct chimera_smb_open_file     *open_file;
    uint64_t                          open_file_bucket;

    open_file = chimera_smb_open_file_alloc(thread);

    open_file->type = type;

    if (parent_fh_len) {
        memcpy(open_file->parent_fh, parent_fh, parent_fh_len);
    }

    open_file->parent_fh_len   = parent_fh_len;
    open_file->file_id.pid     = pid;
    open_file->file_id.vid     = chimera_rand64();
    open_file->handle          = oh;
    open_file->flags           = delete_on_close ? CHIMERA_SMB_OPEN_FILE_FLAG_DELETE_ON_CLOSE : 0;
    open_file->position        = 0;
    open_file->pipe_transceive = transceive;
    open_file->refcnt          = 2;

    open_file->name_len = name_len;
    memcpy(open_file->name, name, open_file->name_len);

    open_file_bucket = open_file->file_id.vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;

    pthread_mutex_lock(&tree->open_files_lock[open_file_bucket]);

    HASH_ADD(hh, tree->open_files[open_file_bucket], file_id, sizeof(open_file->file_id), open_file);

    pthread_mutex_unlock(&tree->open_files_lock[open_file_bucket]);

    compound->saved_file_id = open_file->file_id;

    return open_file;
} /* chimera_smb_create_gen_open_file */


static inline struct chimera_smb_open_file *
chimera_smb_create_gen_open_file_normal(
    struct chimera_smb_request     *request,
    const void                     *parent_fh,
    int                             parent_fh_len,
    const char                     *name,
    int                             name_len,
    int                             delete_on_close,
    struct chimera_vfs_open_handle *oh)
{
    return chimera_smb_create_gen_open_file(request,
                                            CHIMERA_SMB_OPEN_FILE_TYPE_FILE,
                                            NULL,
                                            ++request->tree->next_file_id,
                                            parent_fh,
                                            parent_fh_len,
                                            name,
                                            name_len,
                                            delete_on_close, oh);
} /* chimera_smb_create_gen_open_file_normal */

static inline struct chimera_smb_open_file *
chimera_smb_create_gen_open_file_pipe(
    struct chimera_smb_request   *request,
    enum chimera_smb_pipe_magic   pipe_magic,
    chimera_smb_pipe_transceive_t transceive,
    const char                   *name,
    int                           name_len)
{
    return chimera_smb_create_gen_open_file(request,
                                            CHIMERA_SMB_OPEN_FILE_TYPE_PIPE,
                                            transceive,
                                            pipe_magic,
                                            NULL,
                                            0,
                                            name,
                                            name_len,
                                            0,
                                            NULL);
} /* chimera_smb_create_gen_open_file_pipe */

static inline void
chimera_smb_create_mkdir_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_smb_request   *request    = private_data;
    struct chimera_vfs_thread    *vfs_thread = request->compound->thread->vfs_thread;
    struct chimera_smb_open_file *open_file;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_release(vfs_thread, request->create.parent_handle);
        chimera_smb_complete_request(request, SMB2_STATUS_OBJECT_NAME_NOT_FOUND);
        return;
    }

    open_file = chimera_smb_create_gen_open_file_normal(request,
                                                        request->create.parent_handle->fh,
                                                        request->create.parent_handle->fh_len,
                                                        request->create.name,
                                                        request->create.name_len,
                                                        request->create.create_options & SMB2_FILE_DELETE_ON_CLOSE,
                                                        oh);

    request->create.r_open_file = open_file;

    if (request->create.create_options & SMB2_FILE_DELETE_ON_CLOSE) {
        chimera_smb_create_unlink(request);
    } else {
        chimera_vfs_release(vfs_thread, request->create.parent_handle);
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
    }

} /* chimera_smb_create_open_parent_callback */

static inline void
chimera_smb_create_mkdir_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data)
{

    struct chimera_smb_request       *request    = private_data;
    struct chimera_smb_compound      *compound   = request->compound;
    struct chimera_server_smb_thread *thread     = compound->thread;
    struct chimera_vfs_thread        *vfs_thread = thread->vfs_thread;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_release(vfs_thread, request->create.parent_handle);
        chimera_smb_complete_request(request, error_code == CHIMERA_VFS_EEXIST ?
                                     SMB2_STATUS_OBJECT_NAME_COLLISION :
                                     SMB2_STATUS_OBJECT_NAME_NOT_FOUND);
        return;
    }

    chimera_smb_marshal_attrs(
        attr,
        &request->create.r_attrs);

    request->create.r_attrs.smb_attributes |= SMB2_FILE_ATTRIBUTE_DIRECTORY;

    chimera_vfs_open_fh(
        vfs_thread,
        &request->session_handle->session->cred,
        attr->va_fh,
        attr->va_fh_len,
        CHIMERA_VFS_OPEN_PATH,
        chimera_smb_create_mkdir_open_callback,
        request);

} /* chimera_smb_create_mkdir_callback */

static inline void
chimera_smb_create_open_at_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    struct chimera_vfs_attrs       *dir_pre_attr,
    struct chimera_vfs_attrs       *dir_post_attr,
    void                           *private_data)
{
    struct chimera_smb_request   *request    = private_data;
    struct chimera_vfs_thread    *vfs_thread = request->compound->thread->vfs_thread;
    struct chimera_smb_open_file *open_file;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_release(vfs_thread, request->create.parent_handle);
        chimera_smb_complete_request(request, error_code == CHIMERA_VFS_EEXIST ?
                                     SMB2_STATUS_OBJECT_NAME_COLLISION :
                                     SMB2_STATUS_OBJECT_NAME_NOT_FOUND);
        return;
    }

    open_file = chimera_smb_create_gen_open_file_normal(request,
                                                        request->create.parent_handle->fh,
                                                        request->create.parent_handle->fh_len,
                                                        request->create.name,
                                                        request->create.name_len,
                                                        request->create.create_options & SMB2_FILE_DELETE_ON_CLOSE, oh);

    request->create.r_open_file = open_file;

    chimera_smb_marshal_attrs(
        attr,
        &request->create.r_attrs);

    if (request->create.create_options & SMB2_FILE_DELETE_ON_CLOSE) {
        chimera_smb_create_unlink(request);
    } else {
        chimera_vfs_release(vfs_thread, request->create.parent_handle);
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
    }
} /* chimera_smb_create_open_at_callback */

static inline void
chimera_smb_create_open_getattr_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_smb_request *request = private_data;

    chimera_smb_open_file_release(request, request->create.r_open_file);

    if (error_code != CHIMERA_VFS_OK) {
        /* XXX open file */
        chimera_smb_complete_request(request, SMB2_STATUS_OBJECT_NAME_NOT_FOUND);
        return;
    }


    chimera_smb_marshal_attrs(
        attr,
        &request->create.r_attrs);

    chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);

} /* chimera_smb_create_open_getattr_callback */

static inline void
chimera_smb_create_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_smb_request       *request    = private_data;
    struct chimera_smb_compound      *compound   = request->compound;
    struct chimera_server_smb_thread *thread     = compound->thread;
    struct chimera_vfs_thread        *vfs_thread = thread->vfs_thread;
    struct chimera_smb_open_file     *open_file;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_complete_request(request, SMB2_STATUS_OBJECT_NAME_NOT_FOUND);
        return;
    }

    open_file = chimera_smb_create_gen_open_file_normal(request,
                                                        NULL, 0,
                                                        request->create.name,
                                                        request->create.name_len * 2,
                                                        request->create.create_options & SMB2_FILE_DELETE_ON_CLOSE, oh);

    request->create.r_open_file = open_file;

    chimera_vfs_getattr(vfs_thread,
                        &request->session_handle->session->cred,
                        oh,
                        CHIMERA_VFS_ATTR_FH,
                        chimera_smb_create_open_getattr_callback,
                        request);

} /* chimera_smb_create_open_at_callback */


static inline void
chimera_smb_create_open_parent_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_smb_request       *request    = private_data;
    struct chimera_server_smb_thread *thread     = request->compound->thread;
    struct chimera_vfs_thread        *vfs_thread = thread->vfs_thread;
    unsigned int                      flags      = 0;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_error("Open parent error_code %d", error_code);
        chimera_smb_complete_request(request, SMB2_STATUS_OBJECT_PATH_NOT_FOUND);
        return;
    }

    request->create.parent_handle = oh;

    if ((request->create.create_options & SMB2_FILE_DIRECTORY_FILE) &&
        request->create.create_disposition == SMB2_FILE_CREATE) {

        chimera_vfs_mkdir_at(
            vfs_thread,
            &request->session_handle->session->cred,
            oh,
            request->create.name,
            request->create.name_len,
            &request->create.set_attr,
            CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT,
            0,
            0,
            chimera_smb_create_mkdir_callback,
            request);

    } else {
        if (request->create.create_options & SMB2_FILE_DIRECTORY_FILE) {
            flags |= CHIMERA_VFS_OPEN_DIRECTORY;
        }

        if (request->create.desired_access == SMB2_FILE_READ_ATTRIBUTES) {
            flags |= CHIMERA_VFS_OPEN_PATH;
        }

        if (!(request->create.desired_access & SMB2_WRITE_MASK)) {
            flags |= CHIMERA_VFS_OPEN_READ_ONLY;
        }

        if ((request->create.create_options & SMB2_FILE_OPEN_REPARSE_POINT) &&
            request->create.create_disposition == SMB2_FILE_OPEN) {
            flags |= CHIMERA_VFS_OPEN_NOFOLLOW;
        }

        switch (request->create.create_disposition) {
            case SMB2_FILE_SUPERSEDE:
                break;
            case SMB2_FILE_OPEN:
                break;
            case SMB2_FILE_OPEN_IF:
                flags |= CHIMERA_VFS_OPEN_CREATE;
                break;
            case SMB2_FILE_CREATE:
                flags |= CHIMERA_VFS_OPEN_CREATE;
                break;
            case SMB2_FILE_OVERWRITE:
                flags |= CHIMERA_VFS_OPEN_CREATE;
                break;
            case SMB2_FILE_OVERWRITE_IF:
                flags |= CHIMERA_VFS_OPEN_CREATE;
                break;
        } /* switch */

        chimera_vfs_open_at(
            vfs_thread,
            &request->session_handle->session->cred,
            oh,
            request->create.name,
            request->create.name_len,
            flags,
            &request->create.set_attr,
            CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT,
            0,
            0,
            chimera_smb_create_open_at_callback,
            request);
    }

} /* chimera_smb_create_open_at_callback */

static inline void
chimera_smb_create_lookup_parent_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_smb_request *request    = private_data;
    struct chimera_vfs_thread  *vfs_thread = request->compound->thread->vfs_thread;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_complete_request(request, SMB2_STATUS_OBJECT_PATH_NOT_FOUND);
        return;
    }

    chimera_vfs_open_fh(
        vfs_thread,
        &request->session_handle->session->cred,
        attr->va_fh,
        attr->va_fh_len,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
        chimera_smb_create_open_parent_callback,
        request);
} /* chimera_smb_create_lookup_parent_callback */

static inline void
chimera_smb_create_process(struct chimera_smb_request *request)
{
    struct chimera_vfs_thread *vfs_thread = request->compound->thread->vfs_thread;
    struct chimera_smb_tree   *tree       = request->tree;

    if (request->create.parent_path_len) {
        chimera_vfs_lookup(
            vfs_thread,
            &request->session_handle->session->cred,
            tree->fh,
            tree->fh_len,
            request->create.parent_path,
            request->create.parent_path_len,
            CHIMERA_VFS_ATTR_FH,
            CHIMERA_VFS_LOOKUP_FOLLOW,
            chimera_smb_create_lookup_parent_callback,
            request);
    } else if (request->create.name_len) {
        chimera_vfs_open_fh(
            vfs_thread,
            &request->session_handle->session->cred,
            request->tree->fh,
            request->tree->fh_len,
            CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
            chimera_smb_create_open_parent_callback,
            request);
    } else {
        chimera_vfs_open_fh(
            vfs_thread,
            &request->session_handle->session->cred,
            request->tree->fh,
            request->tree->fh_len,
            CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED,
            chimera_smb_create_open_callback,
            request);

    }
} /* chimera_smb_create_process */


static void
chimera_smb_revalidate_tree_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_smb_request *request = private_data;
    struct chimera_smb_tree    *tree    = request->tree;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_error("Revalidate error_code %d", error_code);
        chimera_smb_complete_request(request, SMB2_STATUS_NETWORK_NAME_DELETED);
        return;
    }

    tree->fh_len = attr->va_fh_len;
    memcpy(&tree->fh, &attr->va_fh, attr->va_fh_len);

    clock_gettime(CLOCK_MONOTONIC, &tree->fh_expiration);
    tree->fh_expiration.tv_sec += 60;

    chimera_smb_create_process(request);
} /* chimera_smb_revalidate_tree_callback */

static inline void
chimera_smb_revalidate_tree(
    struct chimera_smb_tree    *tree,
    struct chimera_smb_request *request)
{
    struct chimera_vfs_thread *vfs_thread = request->compound->thread->vfs_thread;
    uint8_t                    root_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                   root_fh_len;

    chimera_vfs_get_root_fh(root_fh, &root_fh_len);

    chimera_vfs_lookup(
        vfs_thread,
        &request->session_handle->session->cred,
        root_fh,
        root_fh_len,
        tree->share->path,
        strlen(tree->share->path),
        CHIMERA_VFS_ATTR_FH,
        CHIMERA_VFS_LOOKUP_FOLLOW,
        chimera_smb_revalidate_tree_callback,
        request);

} /* chimera_smb_revalidate_tree */

void
chimera_smb_create(struct chimera_smb_request *request)
{
    struct timespec               now;
    struct chimera_smb_open_file *open_file;
    enum chimera_smb_pipe_magic   pipe_magic;
    chimera_smb_pipe_transceive_t transceive;

    if (request->tree->type == CHIMERA_SMB_TREE_TYPE_PIPE) {

        if (strcasecmp(request->create.name, "lsarpc") == 0) {
            pipe_magic = CHIMERA_SMB_OPEN_FILE_LSA_RPC;
            transceive = chimera_smb_lsarpc_transceive;
        } else {
            chimera_smb_complete_request(request, SMB2_STATUS_OBJECT_NAME_NOT_FOUND);
            return;
        }

        open_file = chimera_smb_create_gen_open_file_pipe(request,
                                                          pipe_magic,
                                                          transceive,
                                                          request->create.name,
                                                          request->create.name_len);

        request->create.r_open_file = open_file;

        request->create.r_attrs.smb_crttime    = 0;
        request->create.r_attrs.smb_atime      = 0;
        request->create.r_attrs.smb_mtime      = 0;
        request->create.r_attrs.smb_ctime      = 0;
        request->create.r_attrs.smb_alloc_size = 0;
        request->create.r_attrs.smb_size       = 0;
        request->create.r_attrs.smb_attributes = 0x80;
        request->create.r_attrs.smb_attr_mask  = SMB_ATTR_MASK_NETWORK_OPEN;

        chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);

    } else {

        clock_gettime(CLOCK_MONOTONIC, &now);

        if (chimera_timespec_cmp(&now, &request->tree->fh_expiration) > 0) {
            chimera_smb_revalidate_tree(request->tree, request);
        } else {
            chimera_smb_create_process(request);
        }
    }
} /* chimera_smb_create */

void
chimera_smb_create_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request)
{

    evpl_iovec_cursor_append_uint16(reply_cursor, SMB2_CREATE_REPLY_SIZE);

    /* Oplock level*/
    evpl_iovec_cursor_append_uint8(reply_cursor, 0);

    /* Flags */
    evpl_iovec_cursor_append_uint8(reply_cursor, 0);

    /* Create Action */

    if (request->create.create_disposition == SMB2_FILE_OPEN) {
        evpl_iovec_cursor_append_uint32(reply_cursor, SMB2_CREATE_ACTION_OPENED);
    } else {
        evpl_iovec_cursor_append_uint32(reply_cursor, SMB2_CREATE_ACTION_CREATED);
    }

    chimera_smb_append_network_open_info(reply_cursor, &request->create.r_attrs);

    /* File Id (persistent) */
    evpl_iovec_cursor_append_uint64(reply_cursor, request->create.r_open_file ?
                                    request->create.r_open_file->file_id.pid : 0);

    /* File Id (volatile) */
    evpl_iovec_cursor_append_uint64(reply_cursor, request->create.r_open_file ?
                                    request->create.r_open_file->file_id.vid : 0);

    /* Create Context Offset */
    evpl_iovec_cursor_append_uint32(reply_cursor, 0);

    /* Create Context Length */
    evpl_iovec_cursor_append_uint32(reply_cursor, 0);

    /* Ea Error Offset */
    evpl_iovec_cursor_append_uint32(reply_cursor, 0);

} /* chimera_smb_create_reply */

/*
 * Parse SMB2 CREATE request
 * Offset  Size  Field
 * ------  ----  -----------------------------------------------------------
 * 0x00    2     StructureSize = 57 (0x0039)   // fixed for request
 * 0x02    1     SecurityFlags = 0 (reserved)
 * 0x03    1     RequestedOplockLevel          // NONE/II/EXCLUSIVE/BATCH/LEASE
 * 0x04    4     ImpersonationLevel            // Anonymous/Ident./Impersonation/Delegate
 * 0x08    8     SmbCreateFlags = 0 (reserved; ignore on server)
 * 0x10    8     Reserved (ignore on server)
 * 0x18    4     DesiredAccess                 // access mask (see §2.2.13.1)
 * 0x1C    4     FileAttributes                // FILE_ATTRIBUTE_* (dirs use DIRECTORY)
 * 0x20    4     ShareAccess                   // READ/WRITE/DELETE mask
 * 0x24    4     CreateDisposition             // SUPERSEDE, OPEN, CREATE, OPEN_IF, OVERWRITE, OVERWRITE_IF
 * 0x28    4     CreateOptions                 // FILE_* options (e.g., DIRECTORY_FILE, NON_DIRECTORY_FILE)
 * 0x2C    2     NameOffset                    // from start of SMB2 header to file name
 * 0x2E    2     NameLength (bytes; UTF‑16LE; not NUL‑terminated)
 * 0x30    4     CreateContextsOffset          // 8‑byte aligned if present; 0 if none
 * 0x34    4     CreateContextsLength          // bytes of concatenated contexts
 * 0x38    ...   Buffer: FileName then SMB2_CREATE_CONTEXT blobs (if any)
 */
int
chimera_smb_parse_create(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request)
{
    uint16_t name_offset;
    uint32_t blob_offset, blob_length;
    uint16_t name16[SMB_FILENAME_MAX];
    int      name_size;
    char    *slash;

    if (unlikely(request->request_struct_size != SMB2_CREATE_REQUEST_SIZE)) {
        chimera_smb_error("Received SMB2 CREATE request with invalid struct size (%u expected %u)",
                          request->smb2_hdr.struct_size,
                          SMB2_CREATE_REQUEST_SIZE);
        request->status = SMB2_STATUS_INVALID_PARAMETER;
        return -1;
    }

    evpl_iovec_cursor_get_uint8(request_cursor, &request->create.requested_oplock_level);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->create.impersonation_level);
    evpl_iovec_cursor_get_uint64(request_cursor, &request->create.flags);
    evpl_iovec_cursor_skip(request_cursor, 8);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->create.desired_access);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->create.file_attributes);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->create.share_access);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->create.create_disposition);
    evpl_iovec_cursor_get_uint32(request_cursor, &request->create.create_options);
    evpl_iovec_cursor_get_uint16(request_cursor, &name_offset);
    evpl_iovec_cursor_get_uint16(request_cursor, &request->create.name_len);
    evpl_iovec_cursor_get_uint32(request_cursor, &blob_offset);
    evpl_iovec_cursor_get_uint32(request_cursor, &blob_length);

    if (request->create.impersonation_level > SMB2_IMPERSONATION_DELEGATE) {
        request->status = SMB2_STATUS_BAD_IMPERSONATION_LEVEL;
        return -1;
    }

    if (request->create.name_len >= SMB_FILENAME_MAX) {
        chimera_smb_error("Create request: UTF-16 name too long (%u bytes)",
                          request->create.name_len);
        request->status = SMB2_STATUS_NAME_TOO_LONG;
        return -1;
    }

    evpl_iovec_cursor_copy(request_cursor, name16, request->create.name_len);

    name_size = chimera_smb_utf16le_to_utf8(&request->compound->thread->iconv_ctx,
                                            name16,
                                            request->create.name_len,
                                            request->create.parent_path,
                                            sizeof(request->create.parent_path));
    if (name_size < 0) {
        chimera_smb_error("Failed to convert CREATE name from UTF-16LE to UTF-8");
        request->status = SMB2_STATUS_OBJECT_NAME_INVALID;
        return -1;
    }
    request->create.parent_path_len = name_size;

    /* Reject paths with a leading backslash separator */
    if (name_size > 0 && request->create.parent_path[0] == '\\') {
        request->status = SMB2_STATUS_INVALID_PARAMETER;
        return -1;
    }

    slash = rindex(request->create.parent_path, '\\');

    if (slash) {
        *slash                          = '\0';
        request->create.name            = slash + 1;
        request->create.name_len        = request->create.parent_path_len - (slash - request->create.parent_path) - 1;
        request->create.parent_path_len = slash - request->create.parent_path;

        chimera_smb_slash_back_to_forward(request->create.parent_path, request->create.parent_path_len);
    } else {
        request->create.name            = request->create.parent_path;
        request->create.name_len        = request->create.parent_path_len;
        request->create.parent_path_len = 0;
    }

    /* Initialize create-time attributes (may be populated by SD create context) */
    request->create.set_attr.va_req_mask = 0;
    request->create.set_attr.va_set_mask = 0;

    /* Parse create contexts for security descriptor (modefromsid) */
    if (blob_offset > 0 && blob_length > 0 && blob_length <= 1024) {
        uint8_t  ctx_buf[1024];
        uint32_t skip = blob_offset - evpl_iovec_cursor_consumed(request_cursor);

        evpl_iovec_cursor_skip(request_cursor, skip);

        if (evpl_iovec_cursor_get_blob(request_cursor, ctx_buf, blob_length) == 0) {
            uint32_t pos = 0;

            while (pos + 16 <= blob_length) {
                uint32_t next = ctx_buf[pos]    | (ctx_buf[pos + 1] << 8) |
                    (ctx_buf[pos + 2] << 16) | (ctx_buf[pos + 3] << 24);
                uint16_t name_off = ctx_buf[pos + 4] | (ctx_buf[pos + 5] << 8);
                uint16_t name_len = ctx_buf[pos + 6] | (ctx_buf[pos + 7] << 8);
                uint16_t data_off = ctx_buf[pos + 10] | (ctx_buf[pos + 11] << 8);
                uint32_t data_len = ctx_buf[pos + 12] | (ctx_buf[pos + 13] << 8) |
                    (ctx_buf[pos + 14] << 16) | (ctx_buf[pos + 15] << 24);

                /* Look for SMB2_CREATE_SD_BUFFER ("SecD") */
                if (name_len == 4 &&
                    pos + name_off + 4 <= blob_length &&
                    ctx_buf[pos + name_off]     == 'S' &&
                    ctx_buf[pos + name_off + 1] == 'e' &&
                    ctx_buf[pos + name_off + 2] == 'c' &&
                    ctx_buf[pos + name_off + 3] == 'D' &&
                    data_off > 0 &&
                    pos + data_off + data_len <= blob_length) {

                    chimera_smb_parse_sd_to_attrs(
                        ctx_buf + pos + data_off,
                        data_len,
                        &request->create.set_attr);
                    break;
                }

                if (next == 0) {
                    break;
                }
                pos += next;
            }
        }
    }

    return 0;
} /* chimera_smb_parse_create */ /* chimera_smb_parse_create */
