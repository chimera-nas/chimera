// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_internal.h"
#include "smb_procs.h"
#include "common/misc.h"
#include "vfs/vfs.h"
#include "vfs/vfs_release.h"

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
    struct chimera_smb_request     *request     = private_data;
    struct chimera_smb_open_file   *open_file   = request->set_info.open_file;
    struct chimera_smb_rename_info *rename_info = &request->set_info.rename_info;

    if (!error_code) {
        /* Release the sharemode entry keyed by the old name before
         * updating the path, then re-acquire under the new name.
         * Without this, close would hash the new name and fail to
         * find the entry registered under the old name, leaking it. */
        chimera_smb_sharemode_release(&request->tree->share->sharemode,
                                      open_file);

        /* Update the open file's name and parent to reflect the rename,
         * so subsequent compound operations (e.g. disposition delete)
         * use the correct path. */
        struct chimera_vfs_open_handle *new_parent = rename_info->new_parent_handle
                                                     ? rename_info->new_parent_handle
                                                     : request->set_info.parent_handle;

        if (new_parent) {
            memcpy(open_file->parent_fh, new_parent->fh, new_parent->fh_len);
            open_file->parent_fh_len = new_parent->fh_len;
        }

        memcpy(open_file->name, rename_info->new_name, rename_info->new_name_len);
        open_file->name[rename_info->new_name_len] = '\0';
        open_file->name_len                        = rename_info->new_name_len;

        /* Update VFS handle DOC path so close deletes the new name */
        if ((open_file->flags & CHIMERA_SMB_OPEN_FILE_FLAG_DELETE_ON_CLOSE) &&
            open_file->handle) {
            chimera_vfs_set_delete_on_close(
                request->compound->thread->vfs_thread,
                open_file->handle,
                open_file->parent_fh,
                open_file->parent_fh_len,
                open_file->name,
                open_file->name_len,
                &request->session_handle->session->cred);
        }

        /* Re-acquire sharemode entry under the new name */
        if (open_file->type == CHIMERA_SMB_OPEN_FILE_TYPE_FILE &&
            request->tree->share &&
            (open_file->desired_access & SMB2_SHAREMODE_ACCESS_MASK)) {
            chimera_smb_sharemode_acquire(
                &request->tree->share->sharemode,
                open_file->parent_fh, open_file->parent_fh_len,
                open_file->name, open_file->name_len,
                open_file->desired_access, open_file->share_access,
                open_file);
        }
    }

    if (request->set_info.parent_handle) {
        chimera_vfs_release(request->compound->thread->vfs_thread, request->set_info.parent_handle);
        request->set_info.parent_handle = NULL;
    }

    if (rename_info->new_parent_handle) {
        chimera_vfs_release(request->compound->thread->vfs_thread, rename_info->new_parent_handle);
        rename_info->new_parent_handle = NULL;
    }

    chimera_smb_open_file_release(request, open_file);

    /* Map the rename_at failure (if any) back to the SMB status the client
    * needs.  EACCES/EPERM commonly surface from the engine's DELETE_CHILD /
    * sharing-violation gates and must be reported as ACCESS_DENIED, not
    * INTERNAL_ERROR — a STATUS_INTERNAL_ERROR makes smbtorture treat the
    * server as broken instead of asserting on the actual returned code. */
    uint32_t status;
    switch (error_code) {
        case CHIMERA_VFS_OK:        status = SMB2_STATUS_SUCCESS; break;
        case CHIMERA_VFS_EACCES:
        case CHIMERA_VFS_EPERM:     status = SMB2_STATUS_ACCESS_DENIED; break;
        case CHIMERA_VFS_EEXIST:    status = SMB2_STATUS_OBJECT_NAME_COLLISION; break;
        case CHIMERA_VFS_ENOENT:    status = SMB2_STATUS_OBJECT_NAME_NOT_FOUND; break;
        case CHIMERA_VFS_ENOTEMPTY: status = SMB2_STATUS_DIRECTORY_NOT_EMPTY; break;
        case CHIMERA_VFS_EISDIR:    status = SMB2_STATUS_FILE_IS_A_DIRECTORY; break;
        case CHIMERA_VFS_ENOTDIR:   status = SMB2_STATUS_NOT_A_DIRECTORY; break;
        default:                    status = SMB2_STATUS_INTERNAL_ERROR; break;
    } /* switch */

    chimera_smb_complete_request(request, status);
} /* chimera_smb_set_info_rename_callback */

/* Release the transient destination-parent dir-lease conflict probe (if it was
 * inserted) and drop the file-state reference taken for it. */
static void
chimera_smb_set_info_rename_dp_release(struct chimera_smb_request *request)
{
    struct chimera_vfs_thread *vfs_thread = request->compound->thread->vfs_thread;
    struct chimera_vfs_state  *vfs_state  = vfs_thread->vfs->vfs_state;

    if (request->set_info.dp_probe_active) {
        chimera_vfs_lease_release(vfs_state, request->set_info.dp_file_state,
                                  &request->set_info.dp_probe);
        request->set_info.dp_probe_active = 0;
    }
    if (request->set_info.dp_file_state) {
        chimera_vfs_state_put(vfs_state, request->set_info.dp_file_state);
        request->set_info.dp_file_state = NULL;
    }
} /* chimera_smb_set_info_rename_dp_release */

/* Issue the rename once the destination parent's directory lease (if any) has
 * yielded its HANDLE caching. */
static void
chimera_smb_set_info_rename_emit(struct chimera_smb_request *request)
{
    struct chimera_smb_open_file   *open_file      = request->set_info.open_file;
    char                           *dest_name      = request->set_info.rename_info.new_name;
    size_t                          dest_name_len  = request->set_info.rename_info.new_name_len;
    struct chimera_vfs_open_handle *dest_parent_oh = request->set_info.rename_info.new_parent_handle
                                                     ? request->set_info.rename_info.new_parent_handle
                                                     : request->set_info.parent_handle;

    chimera_vfs_rename_at(
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
        /* Self-exempt the directory lease named by the operating open's
         * ParentLeaseKey (dirlease.rename correct-parent-leaskey case). */
        open_file->parent_lease_key,
        /* ...and self-exempt the renamer's own file lease from the source
         * recall (renaming a file it holds a lease on must not break it). */
        open_file->handle,
        chimera_smb_set_info_rename_callback,
        request);
} /* chimera_smb_set_info_rename_emit */

/* Resume after the destination-parent dir-lease conflict probe resolves.
 * GRANTED: no conflicting handle-leased opener remains (none, or it closed in
 * response to the RH->R break) -> proceed with the rename.  DENIED: a holder
 * kept its handle open -> SHARING_VIOLATION (MS-SMB2 dirlease.rename_dst_parent). */
static void
chimera_smb_set_info_rename_dp_cb(
    enum chimera_vfs_lease_result result,
    struct chimera_vfs_lease     *lease,
    struct chimera_vfs_lease     *conflict,
    void                         *private_data)
{
    struct chimera_smb_request *request    = private_data;
    struct chimera_vfs_thread  *vfs_thread = request->compound->thread->vfs_thread;

    (void) lease;
    (void) conflict;

    if (result == CHIMERA_VFS_LEASE_GRANTED) {
        /* lease_acquire inserted the probe; drop it (its only purpose was to
         * break the dir lease / detect the conflict) and rename. */
        request->set_info.dp_probe_active = 1;
        chimera_smb_set_info_rename_dp_release(request);
        chimera_smb_set_info_rename_emit(request);
        return;
    }

    chimera_smb_set_info_rename_dp_release(request);

    if (request->set_info.rename_info.new_parent_handle) {
        chimera_vfs_release(vfs_thread,
                            request->set_info.rename_info.new_parent_handle);
        request->set_info.rename_info.new_parent_handle = NULL;
    }
    if (request->set_info.parent_handle) {
        chimera_vfs_release(vfs_thread, request->set_info.parent_handle);
        request->set_info.parent_handle = NULL;
    }
    chimera_smb_open_file_release(request, request->set_info.open_file);
    chimera_smb_complete_request(request, SMB2_STATUS_SHARING_VIOLATION);
} /* chimera_smb_set_info_rename_dp_cb */

static void
chimera_smb_set_info_rename_do_rename(struct chimera_smb_request *request)
{
    struct chimera_smb_open_file   *open_file      = request->set_info.open_file;
    struct chimera_vfs_thread      *vfs_thread     = request->compound->thread->vfs_thread;
    struct chimera_vfs_state       *vfs_state      = vfs_thread->vfs->vfs_state;
    struct chimera_vfs_open_handle *dest_parent_oh = request->set_info.rename_info.new_parent_handle
                                                     ? request->set_info.rename_info.new_parent_handle
                                                     : request->set_info.parent_handle;
    struct chimera_vfs_file_state  *fs;
    uint64_t                        skip_lo, skip_hi;
    bool                            has_skip;

    request->set_info.dp_probe_active = 0;
    request->set_info.dp_file_state   = NULL;

    /* A rename INTO a directory must break that directory's lease HANDLE caching
     * (RH->R): a conflicting handle-leased opener (one holding the dst parent
     * open with DELETE access) may close in response and free the rename, else
     * the rename fails SHARING_VIOLATION (MS-SMB2; dirlease.rename_dst_parent).
     * Model it as a transient SHARE probe (denied=D) on the dst parent: it
     * conflicts ONLY with a DELETE-access holder, so it is inert for ordinary
     * renames into a leased directory (dirlease.rename holders take no DELETE
     * access).  No dst-parent state => no lease => rename directly. */
    fs = dest_parent_oh ? chimera_vfs_state_get(vfs_state, dest_parent_oh->fh,
                                                dest_parent_oh->fh_len,
                                                dest_parent_oh->fh_hash, false)
                        : NULL;

    if (!fs) {
        chimera_smb_set_info_rename_emit(request);
        return;
    }

    request->set_info.dp_file_state = fs;

    /* Self-exempt the directory lease named by the operating open's
     * ParentLeaseKey: a rename issued under the dst parent's own lease must not
     * break (and then deny against) that lease. */
    has_skip = chimera_smb_parent_lease_skip(open_file->parent_lease_key,
                                             &skip_lo, &skip_hi);

    memset(&request->set_info.dp_probe, 0, sizeof(request->set_info.dp_probe));
    request->set_info.dp_probe.kind             = CHIMERA_VFS_LEASE_SHARE;
    request->set_info.dp_probe.mode.denied      = CHIMERA_VFS_LEASE_MODE_D;
    request->set_info.dp_probe.owner.protocol   = CHIMERA_VFS_LEASE_PROTO_SMB2;
    request->set_info.dp_probe.owner.client_key = request->session_handle->session->client_key;
    request->set_info.dp_probe.owner.owner_lo   = open_file->file_id.pid;
    request->set_info.dp_probe.owner.owner_hi   = open_file->file_id.vid;
    if (has_skip) {
        request->set_info.dp_probe.has_break_skip_key = 1;
        request->set_info.dp_probe.break_skip_lo      = skip_lo;
        request->set_info.dp_probe.break_skip_hi      = skip_hi;
    }

    struct chimera_server_smb_thread *thread = request->compound->thread;

    chimera_vfs_lease_acquire(thread->vfs_thread, vfs_state, fs,
                              &request->set_info.dp_probe,
                              &request->set_info.dp_ticket, true,
                              chimera_smb_set_info_rename_dp_cb, request);

    /* If the probe parked on a dir-lease break, that break targets the dst
     * parent's holder on THIS connection (the rename's own conn is mid-compound,
     * so the break was deferred for reply-before-break ordering).  The rename
     * will not reply until the break resolves, so flush the deferred break now or
     * the holder never sees it and the rename deadlocks (it never gets the chance
     * to close / ack).  Harmless if the probe resolved synchronously. */
    chimera_smb_lease_break_flush(thread);
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
    chimera_vfs_lookup_at(
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
            chimera_vfs_open_fh(
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
    chimera_vfs_lookup_at(
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

    chimera_vfs_open_fh(
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
    chimera_vfs_lookup_at(
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
    struct chimera_smb_open_file   *open_file   = request->set_info.open_file;

    /* MS-FSA 2.1.5.14.11.1 SetInfo(FileRenameInformation): if the handle was
     * not opened with DELETE access, the rename MUST fail with ACCESS_DENIED
     * (the rename removes the old name from its parent and adds a new one, so
     * the source handle's GrantedAccess must include DELETE).  Gate here, ahead
     * of the destination-existence probe — otherwise a target collision would
     * surface as OBJECT_NAME_COLLISION and mask the real authorization error. */
    if (!(open_file->granted_access & SMB2_DELETE)) {
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_ACCESS_DENIED);
        return;
    }

    if (rename_info->new_parent_len) {
        /* Moving to a different directory - lookup the new parent path */
        chimera_vfs_lookup(
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
        chimera_vfs_open_fh(
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

    int                             prc = 0;
    prc |= evpl_iovec_cursor_try_get_uint8(cursor, &rename_info->replace_if_exist);
    prc |= evpl_iovec_cursor_try_skip(cursor, 7); /* Reserved */
    prc |= evpl_iovec_cursor_try_get_uint64(cursor, &root_dir);
    prc |= evpl_iovec_cursor_try_get_uint32(cursor, &name_len);

    if (unlikely(prc)) {
        chimera_smb_error("SET_INFO RENAME_INFO request truncated in fixed body");
        request->status = SMB2_STATUS_INFO_LENGTH_MISMATCH;
        return -1;
    }

    if (root_dir != 0) {
        // Non-zero root directory not supported
        chimera_smb_error("SET_INFO RENAME_INFO with non-zero root directory not supported");
        request->status = SMB2_STATUS_INVALID_PARAMETER;
        return -1;
    }
    if (name_len > sizeof(name16)) {
        chimera_smb_error("SET_INFO RENAME_INFO request: UTF-16 name too long (%u bytes)",
                          name_len);
        request->status = SMB2_STATUS_INFO_LENGTH_MISMATCH;
        return -1;
    }

    if (unlikely(evpl_iovec_cursor_try_copy(cursor, (uint8_t *) name16, name_len) != 0)) {
        chimera_smb_error("SET_INFO RENAME_INFO name runs past the input buffer");
        request->status = SMB2_STATUS_INFO_LENGTH_MISMATCH;
        return -1;
    }
    /* Convert UTF-16LE name to UTF-8 */
    rename_info->new_parent_len = chimera_smb_utf16le_to_utf8(&request->compound->thread->iconv_ctx,
                                                              name16,
                                                              name_len,
                                                              rename_info->new_parent,
                                                              sizeof(rename_info->new_parent));

    if (rename_info->new_parent_len < 0) {
        chimera_smb_error("SET_INFO RENAME_INFO failed to convert new name to UTF-8");
        request->status = SMB2_STATUS_OBJECT_NAME_INVALID;
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
