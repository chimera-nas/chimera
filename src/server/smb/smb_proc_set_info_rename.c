// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_internal.h"
#include "smb_procs.h"
#include "common/misc.h"
#include "vfs/vfs.h"

/* Forward declaration */
static void chimera_smb_set_info_rename_check_dest_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data);

static void
chimera_smb_set_info_rename_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *fromdir_pre_attr,
    struct chimera_vfs_attrs *fromdir_post_attr,
    struct chimera_vfs_attrs *todir_pre_attr,
    struct chimera_vfs_attrs *todir_post_attr,
    void                     *private_data)
{
    struct chimera_smb_request *request = private_data;

    if (request->set_info.parent_handle) {
        chimera_vfs_release(request->compound->thread->vfs_thread, request->set_info.parent_handle);
        request->set_info.parent_handle = NULL;
    }

    if (request->set_info.rename_info.new_parent_handle) {
        chimera_vfs_release(request->compound->thread->vfs_thread, request->set_info.rename_info.new_parent_handle);
        request->set_info.rename_info.new_parent_handle = NULL;
    }

    chimera_smb_open_file_release(request, request->set_info.open_file);

    chimera_smb_complete_request(request, error_code ? SMB2_STATUS_INTERNAL_ERROR : SMB2_STATUS_SUCCESS);
} /* chimera_smb_set_info_rename_callback */

static void
chimera_smb_set_info_rename_do_rename(struct chimera_smb_request *request)
{
    struct chimera_smb_open_file   *open_file      = request->set_info.open_file;
    char                           *dest_name      = request->set_info.rename_info.new_name;
    size_t                          dest_name_len  = request->set_info.rename_info.new_name_len;
    struct chimera_vfs_open_handle *dest_parent_oh = request->set_info.rename_info.new_parent_handle
                                                     ? request->set_info.rename_info.new_parent_handle
                                                     : request->set_info.parent_handle;

    chimera_vfs_rename(
        request->compound->thread->vfs_thread,
        &request->session_handle->session->cred,
        open_file->parent_fh,
        open_file->parent_fh_len,
        open_file->name,
        open_file->name_len,
        dest_parent_oh->fh,
        dest_parent_oh->fh_len,
        dest_name,
        dest_name_len,
        NULL,
        0,
        0,
        0,
        chimera_smb_set_info_rename_callback,
        request);
} /* chimera_smb_set_info_rename_do_rename */

static void
chimera_smb_set_info_rename_open_dest_dir_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_smb_request     *request     = private_data;
    struct chimera_smb_open_file   *open_file   = request->set_info.open_file;
    struct chimera_smb_rename_info *rename_info = &request->set_info.rename_info;

    if (error_code != CHIMERA_VFS_OK) {
        if (request->set_info.parent_handle) {
            chimera_vfs_release(request->compound->thread->vfs_thread,
                                request->set_info.parent_handle);
        }
        chimera_smb_open_file_release(request, request->set_info.open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INTERNAL_ERROR);
        return;
    }

    /* Store the directory handle and update the rename target */
    rename_info->new_parent_handle = oh;
    rename_info->new_name          = open_file->name;
    rename_info->new_name_len      = open_file->name_len;

    /* Check if source filename exists in this directory */
    chimera_vfs_lookup(
        request->compound->thread->vfs_thread,
        &request->session_handle->session->cred,
        oh,
        open_file->name,
        open_file->name_len,
        CHIMERA_VFS_ATTR_MODE,
        0,
        chimera_smb_set_info_rename_check_dest_callback,
        request);
} /* chimera_smb_set_info_rename_open_dest_dir_callback */

static void
chimera_smb_set_info_rename_check_dest_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data)
{
    struct chimera_smb_request     *request     = private_data;
    struct chimera_smb_rename_info *rename_info = &request->set_info.rename_info;

    if (error_code == CHIMERA_VFS_OK) {
        /* Destination exists */
        if (S_ISDIR(attr->va_mode)) {
            /* Destination is a directory - use source filename inside it */
            if (rename_info->new_parent_handle) {
                /* Already have a new parent handle from earlier lookup */
                chimera_vfs_release(request->compound->thread->vfs_thread,
                                    rename_info->new_parent_handle);
                if (request->set_info.parent_handle) {
                    chimera_vfs_release(request->compound->thread->vfs_thread,
                                        request->set_info.parent_handle);
                }
                chimera_smb_open_file_release(request, request->set_info.open_file);
                chimera_smb_complete_request(request, SMB2_STATUS_ACCESS_DENIED);
                return;
            }

            /* Open the directory so we can use it as the new parent */
            chimera_vfs_open(
                request->compound->thread->vfs_thread,
                &request->session_handle->session->cred,
                attr->va_fh,
                attr->va_fh_len,
                CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
                chimera_smb_set_info_rename_open_dest_dir_callback,
                request);
            return;
        } else {
            /* Destination is a file */
            if (!rename_info->replace_if_exist) {
                /* Cannot overwrite */
                if (request->set_info.rename_info.new_parent_handle) {
                    chimera_vfs_release(request->compound->thread->vfs_thread,
                                        request->set_info.rename_info.new_parent_handle);
                }
                if (request->set_info.parent_handle) {
                    chimera_vfs_release(request->compound->thread->vfs_thread,
                                        request->set_info.parent_handle);
                }
                chimera_smb_open_file_release(request, request->set_info.open_file);
                chimera_smb_complete_request(request, SMB2_STATUS_OBJECT_NAME_COLLISION);
                return;
            }
            /* Fall through to do rename - will overwrite */
        }
    }
    /* Destination doesn't exist or we're overwriting - proceed with rename */
    chimera_smb_set_info_rename_do_rename(request);
} /* chimera_smb_set_info_rename_check_dest_callback */ /* chimera_smb_set_info_rename_check_dest_callback */

static void
chimera_smb_set_info_rename_open_dest_parent_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_smb_request     *request       = private_data;
    struct chimera_smb_rename_info *rename_info   = &request->set_info.rename_info;
    char                           *dest_name     = rename_info->new_name;
    size_t                          dest_name_len = rename_info->new_name_len;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_open_file_release(request, request->set_info.open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_OBJECT_PATH_NOT_FOUND);
        return;
    }

    if (rename_info->new_parent_len) {
        /* We looked up a parent path, store it */
        rename_info->new_parent_handle = oh;
    } else {
        /* Simple rename - tree root */
        request->set_info.parent_handle = oh;
    }

    /* Check if destination exists */
    chimera_vfs_lookup(
        request->compound->thread->vfs_thread,
        &request->session_handle->session->cred,
        oh,
        dest_name,
        dest_name_len,
        CHIMERA_VFS_ATTR_MODE,
        0,
        chimera_smb_set_info_rename_check_dest_callback,
        request);

} /* chimera_smb_set_info_rename_open_dest_parent_callback */

static void
chimera_smb_set_info_rename_lookup_dest_parent_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_smb_request *request    = private_data;
    struct chimera_vfs_thread  *vfs_thread = request->compound->thread->vfs_thread;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_open_file_release(request, request->set_info.open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_OBJECT_PATH_NOT_FOUND);
        return;
    }

    chimera_vfs_open(
        vfs_thread,
        &request->session_handle->session->cred,
        attr->va_fh,
        attr->va_fh_len,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
        chimera_smb_set_info_rename_open_dest_parent_callback,
        request);
} /* chimera_smb_set_info_rename_lookup_dest_parent_callback */

static void
chimera_smb_set_info_rename_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_smb_request     *request       = private_data;
    struct chimera_smb_rename_info *rename_info   = &request->set_info.rename_info;
    char                           *dest_name     = rename_info->new_name;
    size_t                          dest_name_len = rename_info->new_name_len;

    request->set_info.parent_handle = oh;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_open_file_release(request, request->set_info.open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INTERNAL_ERROR);
        return;
    }

    /* Check if destination exists */
    chimera_vfs_lookup(
        request->compound->thread->vfs_thread,
        &request->session_handle->session->cred,
        oh,
        dest_name,
        dest_name_len,
        CHIMERA_VFS_ATTR_MODE,
        0,
        chimera_smb_set_info_rename_check_dest_callback,
        request);

} /* chimera_smb_set_info_rename_open_callback */

void
chimera_smb_set_info_rename_process(struct chimera_smb_request *request)
{
    struct chimera_vfs_thread      *vfs_thread  = request->compound->thread->vfs_thread;
    struct chimera_smb_tree        *tree        = request->tree;
    struct chimera_smb_rename_info *rename_info = &request->set_info.rename_info;

    if (rename_info->new_parent_len) {
        /* Moving to a different directory - lookup the new parent path */
        chimera_vfs_lookup_path(
            vfs_thread,
            &request->session_handle->session->cred,
            tree->fh,
            tree->fh_len,
            rename_info->new_parent,
            rename_info->new_parent_len,
            CHIMERA_VFS_ATTR_FH,
            0,
            chimera_smb_set_info_rename_lookup_dest_parent_callback,
            request);
    } else {
        /* Destination is in tree root (no parent path specified) */
        chimera_vfs_open(
            vfs_thread,
            &request->session_handle->session->cred,
            tree->fh,
            tree->fh_len,
            CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY,
            chimera_smb_set_info_rename_open_callback,
            request);
    }
} /* chimera_smb_set_info_rename_process */


/* Parse functions for SET_INFO SMB2_FILE_RENAME_INFO
 * request structures
 * Structure:
 *  Offset  Size  Field
 *  0       1     ReplaceIfExists (BOOLEAN)
 *  1       7     Reserved (ignored)
 *  8       8     RootDirectory (handle) -> MUST be 0 for network ops
 *  16      4     FileNameLength (bytes)
 *  20      N     FileName (UTF-16LE, NOT null-terminated)
 *  20+N    P     Padding (optional; ignored). Total size >= 24 bytes.
 */

int
chimera_smb_parse_rename_info(
    struct evpl_iovec_cursor   *cursor,
    struct chimera_smb_request *request)
{
    struct chimera_smb_rename_info *rename_info = &request->set_info.rename_info;
    uint64_t                        root_dir;
    uint16_t                        name16[SMB_FILENAME_MAX];  /* UTF-16LE bytes */
    uint32_t                        name_len;

    /* Initialize the new_parent_handle to NULL */
    rename_info->new_parent_handle = NULL;

    evpl_iovec_cursor_get_uint8(cursor, &rename_info->replace_if_exist);
    evpl_iovec_cursor_skip(cursor, 7); /* Reserved */
    evpl_iovec_cursor_get_uint64(cursor, &root_dir);
    if (root_dir != 0) {
        // Non-zero root directory not supported
        chimera_smb_error("SET_INFO RENAME_INFO with non-zero root directory not supported");
        request->status = SMB2_STATUS_INVALID_PARAMETER;
        return -1;
    }
    evpl_iovec_cursor_get_uint32(cursor, &name_len);
    if (name_len > sizeof(name16)) {
        chimera_smb_error("SET_INFO RENAME_INFO request: UTF-16 name too long (%u bytes)",
                          name_len);
        request->status = SMB2_STATUS_INFO_LENGTH_MISMATCH;
        return -1;
    }

    evpl_iovec_cursor_copy(cursor, (uint8_t *) name16, name_len);
    /* Convert UTF-16LE name to UTF-8 */
    rename_info->new_parent_len = chimera_smb_utf16le_to_utf8(&request->compound->thread->iconv_ctx,
                                                              name16,
                                                              name_len,
                                                              rename_info->new_parent,
                                                              sizeof(rename_info->new_parent));

    if (rename_info->new_parent_len < 0) {
        chimera_smb_error("SET_INFO RENAME_INFO failed to convert new name to UTF-8");
        request->status = SMB2_STATUS_INVALID_PARAMETER;
        return -1;
    }

    /* Split into parent path and name, similar to chimera_smb_parse_create */
    char *slash = rindex(rename_info->new_parent, '\\');

    if (slash) {
        *slash                      = '\0';
        rename_info->new_name       = slash + 1;
        rename_info->new_name_len   = rename_info->new_parent_len - (slash - rename_info->new_parent) - 1;
        rename_info->new_parent_len = slash - rename_info->new_parent;

        chimera_smb_slash_back_to_forward(rename_info->new_parent, rename_info->new_parent_len);
    } else {
        rename_info->new_name       = rename_info->new_parent;
        rename_info->new_name_len   = rename_info->new_parent_len;
        rename_info->new_parent_len = 0;
    }

    return 0;
} /* chimera_smb_parse_rename_info */
