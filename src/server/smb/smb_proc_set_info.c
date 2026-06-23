// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_internal.h"
#include "smb_procs.h"
#include "smb_ea.h"
#include "common/misc.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "vfs/vfs_notify.h"

static void
chimera_smb_set_info_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_smb_request *request = private_data;

    if (!error_code && request->set_info.open_file->parent_fh_len > 0) {
        struct chimera_server_smb_thread *thread = request->compound->thread;
        uint32_t                          mask   = request->set_info.notify_mask ?
            request->set_info.notify_mask : CHIMERA_VFS_NOTIFY_ATTRS_CHANGED;

        /* Self-exempt the parent directory lease named by the operating open's
         * ParentLeaseKey: a setinfo issued through a handle that supplied the
         * correct parent key must not break that directory's lease (MS-SMB2
         * dirlease; dirlease.set{eof,dos,*time} cases 1.1/2.1). */
        uint64_t                          skip_lo, skip_hi;
        bool                              has_skip = chimera_smb_parent_lease_skip(
            request->set_info.open_file->parent_lease_key, &skip_lo, &skip_hi);

        chimera_vfs_notify_emit_lease(thread->shared->vfs->vfs_notify,
                                      request->set_info.open_file->parent_fh,
                                      request->set_info.open_file->parent_fh_len,
                                      mask,
                                      request->set_info.open_file->name,
                                      request->set_info.open_file->name_len,
                                      NULL, 0,
                                      skip_lo, skip_hi, has_skip);
    }

    chimera_smb_open_file_release(request, request->set_info.open_file);

    chimera_smb_complete_request(request, error_code ? SMB2_STATUS_INTERNAL_ERROR : SMB2_STATUS_SUCCESS);
} /* chimera_smb_set_info_callback */

static void
chimera_smb_set_info_link_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *r_attr,
    struct chimera_vfs_attrs *r_dir_pre_attr,
    struct chimera_vfs_attrs *r_dir_post_attr,
    void                     *private_data)
{
    struct chimera_smb_request *request = private_data;

    if (request->set_info.parent_handle) {
        chimera_vfs_release(request->compound->thread->vfs_thread, request->set_info.parent_handle);
        request->set_info.parent_handle = NULL;
    }

    if (request->set_info.rename_info.new_parent_handle) {
        chimera_vfs_release(request->compound->thread->vfs_thread,
                            request->set_info.rename_info.new_parent_handle);
        request->set_info.rename_info.new_parent_handle = NULL;
    }

    chimera_smb_open_file_release(request, request->set_info.open_file);

    chimera_smb_complete_request(request, error_code ? SMB2_STATUS_INTERNAL_ERROR : SMB2_STATUS_SUCCESS);
} /* chimera_smb_set_info_link_callback */

static void
chimera_smb_set_info_link_open_dir_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_smb_request     *request     = private_data;
    struct chimera_smb_open_file   *open_file   = request->set_info.open_file;
    struct chimera_smb_rename_info *rename_info = &request->set_info.rename_info;

    if (rename_info->new_parent_len) {
        rename_info->new_parent_handle = oh;
    } else {
        request->set_info.parent_handle = oh;
    }

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_open_file_release(request, request->set_info.open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INTERNAL_ERROR);
        return;
    }

    chimera_vfs_link_at(
        request->compound->thread->vfs_thread,
        &request->session_handle->session->cred,
        open_file->handle->fh,
        open_file->handle->fh_len,
        oh->fh,
        oh->fh_len,
        rename_info->new_name,
        rename_info->new_name_len,
        rename_info->replace_if_exist,
        0,
        0,
        0,
        /* SMB rename via link: self-exempt the directory lease named by the
         * operating open's ParentLeaseKey (dirlease.rename correct-parent case). */
        open_file->parent_lease_key,
        /* ...and self-exempt the linker's own file lease from the source recall. */
        open_file->handle,
        chimera_smb_set_info_link_callback,
        request);
} /* chimera_smb_set_info_link_open_dir_callback */

static void
chimera_smb_set_info_link_lookup_parent_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_smb_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_open_file_release(request, request->set_info.open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_OBJECT_PATH_NOT_FOUND);
        return;
    }

    chimera_vfs_open_fh(
        request->compound->thread->vfs_thread,
        &request->session_handle->session->cred,
        attr->va_fh,
        attr->va_fh_len,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
        chimera_smb_set_info_link_open_dir_callback,
        request);
} /* chimera_smb_set_info_link_lookup_parent_callback */

static void
chimera_smb_set_info_link_process(struct chimera_smb_request *request)
{
    struct chimera_vfs_thread      *vfs_thread  = request->compound->thread->vfs_thread;
    struct chimera_smb_tree        *tree        = request->tree;
    struct chimera_smb_rename_info *rename_info = &request->set_info.rename_info;

    if (rename_info->new_parent_len) {
        chimera_vfs_lookup(
            vfs_thread,
            &request->session_handle->session->cred,
            tree->fh,
            tree->fh_len,
            rename_info->new_parent,
            rename_info->new_parent_len,
            CHIMERA_VFS_ATTR_FH,
            0,
            chimera_smb_set_info_link_lookup_parent_callback,
            request);
    } else {
        chimera_vfs_open_fh(
            vfs_thread,
            &request->session_handle->session->cred,
            tree->fh,
            tree->fh_len,
            CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY,
            chimera_smb_set_info_link_open_dir_callback,
            request);
    }
} /* chimera_smb_set_info_link_process */

/* Resolve a FileAllocationInformation set once the current size is known.
 * Truncate (and advance LastWriteTime) only when the requested allocation is
 * below the current EOF; otherwise leave the data/EOF alone and just touch the
 * inode so ChangeTime advances. */
static void
chimera_smb_set_info_allocation_getattr_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_smb_request *request    = private_data;
    uint64_t                    alloc_size = request->set_info.vfs_attrs.va_size;
    uint64_t                    cur_size;

    if (unlikely(error_code)) {
        chimera_smb_open_file_release(request, request->set_info.open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INTERNAL_ERROR);
        return;
    }

    cur_size = attr->va_size;

    request->set_info.vfs_attrs.va_req_mask = CHIMERA_VFS_ATTR_SIZE;
    request->set_info.vfs_attrs.va_set_mask = CHIMERA_VFS_ATTR_SIZE;

    if (alloc_size < cur_size) {
        request->set_info.vfs_attrs.va_size = alloc_size;
        if (!(request->set_info.open_file->flags & CHIMERA_SMB_OPEN_FILE_WRITE_TIME_STICKY)) {
            request->set_info.vfs_attrs.va_mtime.tv_nsec = CHIMERA_VFS_TIME_NOW;
            request->set_info.vfs_attrs.va_req_mask     |= CHIMERA_VFS_ATTR_MTIME;
            request->set_info.vfs_attrs.va_set_mask     |= CHIMERA_VFS_ATTR_MTIME;
        }
    } else {
        /* No EOF change: re-set the unchanged size so the backend advances
         * ChangeTime without disturbing LastWriteTime. */
        request->set_info.vfs_attrs.va_size = cur_size;
    }

    chimera_vfs_setattr(
        request->compound->thread->vfs_thread,
        &request->session_handle->session->cred,
        request->set_info.open_file->handle,
        &request->set_info.vfs_attrs,
        0,
        0,
        chimera_smb_set_info_callback,
        request);
} /* chimera_smb_set_info_allocation_getattr_callback */

/* Completion for the delete-on-close caching-lease recall: the recall has
 * drained (every OTHER holder's handle lease was broken and acknowledged), so the
 * peer's break is now visible to the client.  Reply to the SetInfo.  Replying
 * only after the recall drains is what makes the break deterministically precede
 * the reply (the client checks lease_break_info.count synchronously right after
 * smb2_setinfo_file -- smb2.lease.unlink). */
static void
chimera_smb_set_info_doc_recall_callback(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct chimera_smb_request *request = private_data;

    (void) error_code;

    chimera_smb_open_file_release(request, request->set_info.open_file);
    chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
} /* chimera_smb_set_info_doc_recall_callback */

/* ---- FILE_FULL_EA_INFORMATION apply engine (shared by SetInfo and CREATE
 * ExtA): applies a client EA list to the VFS xattr store one entry at a time
 * (zero-length value deletes; names canonicalized case-insensitively against the
 * existing user.* set, Samba-style).  The caller owns ea_buf for the duration
 * and is invoked via `done` with the resulting NTSTATUS.  State lives in a heap
 * context so the engine is independent of the request union. ---- */

struct chimera_smb_ea_apply {
    struct chimera_server_smb_thread *thread;
    const struct chimera_vfs_cred    *cred;
    struct chimera_vfs_open_handle   *handle;
    const uint8_t                    *ea_buf;
    uint32_t                          ea_buf_len;
    uint32_t                          ea_off;
    void                              (*done)(
        uint32_t status,
        void    *arg);
    void                             *arg;
    uint32_t                          list_len;
    uint32_t                          name_len;
    uint8_t                           list[4096];
    char                              name[CHIMERA_VFS_XATTR_NAME_MAX];
};

static void chimera_smb_ea_apply_step(
    struct chimera_smb_ea_apply *a);

static void
chimera_smb_ea_apply_finish(
    struct chimera_smb_ea_apply *a,
    uint32_t                     status)
{
    void  (*done)(
        uint32_t,
        void *) = a->done;
    void *arg = a->arg;

    free(a);
    done(status, arg);
} /* chimera_smb_ea_apply_finish */

static void
chimera_smb_ea_apply_op_cb(
    enum chimera_vfs_error          error_code,
    const struct chimera_vfs_attrs *pre_attr,
    const struct chimera_vfs_attrs *post_attr,
    void                           *private_data)
{
    struct chimera_smb_ea_apply *a = private_data;

    (void) pre_attr;
    (void) post_attr;

    /* Deleting an EA that does not exist is a no-op success. */
    if (error_code != CHIMERA_VFS_OK && error_code != CHIMERA_VFS_ENODATA) {
        chimera_smb_ea_apply_finish(a, chimera_smb_ea_status(error_code));
        return;
    }

    chimera_smb_ea_apply_step(a);
} /* chimera_smb_ea_apply_op_cb */

static void
chimera_smb_ea_apply_step(struct chimera_smb_ea_apply *a)
{
    struct chimera_smb_ea_entry entry;
    const char                 *p, *end;
    int                         found = 0;

    if (a->ea_off >= a->ea_buf_len) {
        chimera_smb_ea_apply_finish(a, SMB2_STATUS_SUCCESS);
        return;
    }

    if (chimera_smb_ea_full_parse_one(a->ea_buf, a->ea_buf_len,
                                      &a->ea_off, &entry) != 0) {
        chimera_smb_ea_apply_finish(a, SMB2_STATUS_EA_LIST_INCONSISTENT);
        return;
    }

    if (entry.name_len == 0 ||
        !chimera_smb_ea_name_valid(entry.name, entry.name_len)) {
        chimera_smb_ea_apply_finish(a, SMB2_STATUS_INVALID_EA_NAME);
        return;
    }

    /* Only Flags 0 and FILE_NEED_EA are defined (MS-FSCC 2.4.15); any other bit
     * makes the entry malformed (MS-FSA fails the set with INVALID_EA_NAME). */
    if (entry.flags & ~FILE_NEED_EA) {
        chimera_smb_ea_apply_finish(a, SMB2_STATUS_INVALID_EA_NAME);
        return;
    }

    /* Reuse an existing case-variant's stored spelling if present. */
    p   = (const char *) a->list;
    end = p + a->list_len;
    while (p < end) {
        uint32_t llen = strlen(p);

        if (chimera_vfs_xattr_is_user(p, llen) &&
            chimera_smb_ea_name_eq(p + CHIMERA_VFS_XATTR_USER_PREFIX_LEN,
                                   llen - CHIMERA_VFS_XATTR_USER_PREFIX_LEN,
                                   entry.name, entry.name_len)) {
            memcpy(a->name, p, llen);
            a->name_len = llen;
            found       = 1;
            break;
        }
        p += llen + 1;
    }

    if (!found) {
        int n = chimera_vfs_xattr_build_user(a->name, sizeof(a->name),
                                             entry.name, entry.name_len);
        if (n < 0) {
            chimera_smb_ea_apply_finish(a, SMB2_STATUS_INVALID_EA_NAME);
            return;
        }
        a->name_len = (uint32_t) n;
    }

    if (entry.value_len == 0) {
        chimera_vfs_remove_xattr(a->thread->vfs_thread, a->cred, a->handle,
                                 a->name, a->name_len,
                                 chimera_smb_ea_apply_op_cb, a);
    } else {
        chimera_vfs_set_xattr(a->thread->vfs_thread, a->cred, a->handle,
                              CHIMERA_VFS_XATTR_EITHER, a->name, a->name_len,
                              entry.value, entry.value_len,
                              chimera_smb_ea_apply_op_cb, a);
    }
} /* chimera_smb_ea_apply_step */

static void
chimera_smb_ea_apply_list_cb(
    enum chimera_vfs_error error_code,
    const char            *names,
    uint32_t               names_len,
    uint32_t               count,
    uint32_t               eof,
    uint64_t               cookie,
    void                  *private_data)
{
    struct chimera_smb_ea_apply *a = private_data;

    (void) names;       /* == a->list (the buffer we passed) */
    (void) count;
    (void) eof;
    (void) cookie;

    /* On error (e.g. ERANGE) just skip case canonicalization. */
    a->list_len = (error_code == CHIMERA_VFS_OK) ? names_len : 0;
    a->ea_off   = 0;
    chimera_smb_ea_apply_step(a);
} /* chimera_smb_ea_apply_list_cb */

void
chimera_smb_ea_apply(
    struct chimera_server_smb_thread *thread,
    const struct chimera_vfs_cred *cred,
    struct chimera_vfs_open_handle *handle,
    const uint8_t *ea_buf,
    uint32_t ea_buf_len,
    void ( *done )(uint32_t status, void *arg),
    void *arg)
{
    struct chimera_smb_ea_apply *a;

    if (!(handle->vfs_module->capabilities & CHIMERA_VFS_CAP_XATTR)) {
        done(SMB2_STATUS_EAS_NOT_SUPPORTED, arg);
        return;
    }

    if (ea_buf_len == 0) {
        done(SMB2_STATUS_SUCCESS, arg);
        return;
    }

    a             = calloc(1, sizeof(*a));
    a->thread     = thread;
    a->cred       = cred;
    a->handle     = handle;
    a->ea_buf     = ea_buf;
    a->ea_buf_len = ea_buf_len;
    a->done       = done;
    a->arg        = arg;

    /* List the existing user.* names first so each set can match case-
     * insensitively and reuse the stored spelling. */
    chimera_vfs_list_xattrs(thread->vfs_thread, cred, handle, 0,
                            a->list, sizeof(a->list),
                            chimera_smb_ea_apply_list_cb, a);
} /* chimera_smb_ea_apply */

static void
chimera_smb_set_ea_done(
    uint32_t status,
    void    *arg)
{
    struct chimera_smb_request *request = arg;

    if (request->set_info.ea_buf) {
        free(request->set_info.ea_buf);
        request->set_info.ea_buf = NULL;
    }

    /* A successful EA change fires FILE_NOTIFY_CHANGE_EA on the parent
     * (smb2.change_notify ChangeEa). */
    if (status == SMB2_STATUS_SUCCESS &&
        request->set_info.open_file->parent_fh_len > 0) {
        struct chimera_server_smb_thread *thread = request->compound->thread;

        chimera_vfs_notify_emit(thread->shared->vfs->vfs_notify,
                                request->set_info.open_file->parent_fh,
                                request->set_info.open_file->parent_fh_len,
                                CHIMERA_VFS_NOTIFY_ATTRS_CHANGED,
                                request->set_info.open_file->name,
                                request->set_info.open_file->name_len,
                                NULL, 0);
    }

    chimera_smb_open_file_release(request, request->set_info.open_file);
    chimera_smb_complete_request(request, status);
} /* chimera_smb_set_ea_done */

static void
chimera_smb_set_ea(struct chimera_smb_request *request)
{
    chimera_smb_ea_apply(request->compound->thread,
                         &request->session_handle->session->cred,
                         request->set_info.open_file->handle,
                         request->set_info.ea_buf,
                         request->set_info.ea_buf_len,
                         chimera_smb_set_ea_done, request);
} /* chimera_smb_set_ea */

void
chimera_smb_set_info(struct chimera_smb_request *request)
{
    request->set_info.open_file     = chimera_smb_open_file_resolve(request, &request->set_info.file_id);
    request->set_info.parent_handle = NULL;
    /* Default change-notify event for this SET_INFO; info classes that mutate
     * size override it below.  Cleared here since the request is pooled. */
    request->set_info.notify_mask = 0;

    if (unlikely(!request->set_info.open_file)) {
        chimera_smb_complete_request(request, SMB2_STATUS_FILE_CLOSED);
        return;
    }

    /* MS-SMB2 §3.3.5.2.10: SET_INFO is a mutating op; reject a stale
     * ChannelSequence with FILE_NOT_AVAILABLE. */
    if (chimera_smb_channel_sequence_stale(request->set_info.open_file,
                                           request->channel_sequence, 1)) {
        chimera_smb_open_file_release(request, request->set_info.open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_FILE_NOT_AVAILABLE);
        return;
    }

    switch (request->set_info.info_type) {
        case SMB2_INFO_FILE:
            switch (request->set_info.info_class) {
                case SMB2_FILE_BASIC_INFO:

                    /* FILE_ATTRIBUTE_TEMPORARY is meaningful only for a data
                     * stream; a directory has none, so setting it on a directory
                     * is STATUS_INVALID_PARAMETER (MS-FSCC 2.6 / smb2.create
                     * dosattr_tmp_dir). */
                    if ((request->set_info.open_file->flags &
                         CHIMERA_SMB_OPEN_FILE_FLAG_DIRECTORY) &&
                        (request->set_info.attrs.smb_attributes &
                         SMB2_FILE_ATTRIBUTE_TEMPORARY)) {
                        chimera_smb_open_file_release(request, request->set_info.open_file);
                        chimera_smb_complete_request(request,
                                                     SMB2_STATUS_INVALID_PARAMETER);
                        break;
                    }

                    chimera_smb_unmarshal_basic_info(&request->set_info.attrs, &request->set_info.vfs_attrs);

                    /* An explicit (non-sentinel) write-time set hands control of
                     * the LastWriteTime to this handle: its own subsequent writes
                     * and size-sets must stop advancing it (MS-FSA sticky mtime). */
                    if (request->set_info.vfs_attrs.va_set_mask & CHIMERA_VFS_ATTR_MTIME) {
                        request->set_info.open_file->flags |= CHIMERA_SMB_OPEN_FILE_WRITE_TIME_STICKY;
                    }

                    chimera_vfs_setattr(
                        request->compound->thread->vfs_thread,
                        &request->session_handle->session->cred,
                        request->set_info.open_file->handle,
                        &request->set_info.vfs_attrs,
                        0,
                        0,
                        chimera_smb_set_info_callback,
                        request);
                    break;
                case SMB2_FILE_ENDOFFILE_INFO:
                    chimera_smb_unmarshal_end_of_file_info(&request->set_info.attrs, &request->set_info.vfs_attrs);

                    /* A size change fires FILE_NOTIFY_CHANGE_SIZE (and, via the
                     * advanced LastWriteTime below, FILE_NOTIFY_CHANGE_LAST_WRITE). */
                    request->set_info.notify_mask = CHIMERA_VFS_NOTIFY_SIZE_CHANGED |
                        CHIMERA_VFS_NOTIFY_FILE_MODIFIED |
                        CHIMERA_VFS_NOTIFY_STREAM_SIZE;

                    /* Setting EndOfFile advances the LastWriteTime (it changes
                     * the file's data extent), unless this handle has taken
                     * sticky control of the write time. */
                    if (!(request->set_info.open_file->flags & CHIMERA_SMB_OPEN_FILE_WRITE_TIME_STICKY)) {
                        request->set_info.vfs_attrs.va_mtime.tv_nsec = CHIMERA_VFS_TIME_NOW;
                        request->set_info.vfs_attrs.va_req_mask     |= CHIMERA_VFS_ATTR_MTIME;
                        request->set_info.vfs_attrs.va_set_mask     |= CHIMERA_VFS_ATTR_MTIME;
                    }

                    chimera_vfs_setattr(
                        request->compound->thread->vfs_thread,
                        &request->session_handle->session->cred,
                        request->set_info.open_file->handle,
                        &request->set_info.vfs_attrs,
                        0,
                        0,
                        chimera_smb_set_info_callback,
                        request);
                    break;
                case SMB2_FILE_ALLOCATION_INFO:
                    /* AllocationInfo only changes the file when the requested
                     * allocation is below the current EOF, in which case the
                     * file is truncated to it (MS-FSCC §2.4.4).  A truncation
                     * advances LastWriteTime; a grow/hint only touches
                     * ChangeTime.  Both need the current size to decide, so this
                     * is resolved in the getattr callback. */
                    request->set_info.notify_mask = CHIMERA_VFS_NOTIFY_SIZE_CHANGED |
                        CHIMERA_VFS_NOTIFY_FILE_MODIFIED |
                        CHIMERA_VFS_NOTIFY_STREAM_SIZE;
                    chimera_smb_unmarshal_end_of_file_info(&request->set_info.attrs, &request->set_info.vfs_attrs);

                    chimera_vfs_getattr(
                        request->compound->thread->vfs_thread,
                        &request->session_handle->session->cred,
                        request->set_info.open_file->handle,
                        CHIMERA_VFS_ATTR_SIZE,
                        chimera_smb_set_info_allocation_getattr_callback,
                        request);
                    break;
                case SMB2_FILE_DISPOSITION_INFO:
                case SMB2_FILE_DISPOSITION_INFO_EX:
                    if (request->set_info.attrs.smb_disposition) {
                        request->set_info.open_file->flags |=
                            CHIMERA_SMB_OPEN_FILE_FLAG_DELETE_ON_CLOSE;
                        /* A named-stream delete-on-close removes only the stream
                         * (via chimera_vfs_remove_stream in the close handler),
                         * never the base file -- so do NOT arm the VFS doc
                         * mechanism (which would remove_at the whole file). */
                        if (!(request->set_info.open_file->flags &
                              CHIMERA_SMB_OPEN_FILE_FLAG_STREAM)) {
                            /* The file is now delete-pending: a subsequent name
                            * open is answered STATUS_DELETE_PENDING while this
                            * handle keeps it open (MS-FSA; smb2.oplock.doc). */
                            chimera_vfs_state_set_delete_pending(
                                request->set_info.open_file->share_file_state);
                            /* Propagate to VFS handle for final-close deletion */
                            chimera_vfs_set_delete_on_close(
                                request->compound->thread->vfs_thread,
                                request->set_info.open_file->handle,
                                request->set_info.open_file->parent_fh,
                                request->set_info.open_file->parent_fh_len,
                                request->set_info.open_file->name,
                                request->set_info.open_file->name_len,
                                &request->session_handle->session->cred);

                            /* Setting delete-on-close on an open file is a pending
                             * namespace mutation: recall every OTHER holder's HANDLE
                             * cache so a peer that cached the open handle re-
                             * validates against the impending removal (MS-SMB2;
                             * smb2.lease.unlink).  The operating client's own lease
                             * is spared.  This PARKS until the recall drains (the
                             * peer's break is acked), then replies -- so the break
                             * deterministically precedes the SetInfo reply. */
                            chimera_vfs_recall_handle_lease(
                                request->compound->thread->vfs_thread,
                                &request->session_handle->session->cred,
                                request->set_info.open_file->handle,
                                chimera_smb_set_info_doc_recall_callback,
                                request);
                            break;
                        }
                    } else {
                        request->set_info.open_file->flags &=
                            ~CHIMERA_SMB_OPEN_FILE_FLAG_DELETE_ON_CLOSE;
                        if (!(request->set_info.open_file->flags &
                              CHIMERA_SMB_OPEN_FILE_FLAG_STREAM)) {
                            chimera_vfs_clear_delete_on_close(
                                request->compound->thread->vfs_thread,
                                request->set_info.open_file->handle);
                            /* Disposition turned back off: no longer delete-
                             * pending, so new opens succeed again. */
                            chimera_vfs_state_clear_delete_pending(
                                request->set_info.open_file->share_file_state);
                        }
                    }
                    chimera_smb_open_file_release(request, request->set_info.open_file);
                    chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
                    break;
                case SMB2_FILE_RENAME_INFO:
                    chimera_smb_set_info_rename_process(request);
                    break;
                case SMB2_FILE_LINK_INFO:
                    chimera_smb_set_info_link_process(request);
                    break;
                case SMB2_FILE_FULL_EA_INFO:
                    chimera_smb_set_ea(request);
                    break;
                case SMB2_FILE_POSITION_INFO:
                    request->set_info.open_file->position = request->set_info.attrs.smb_position;
                    chimera_smb_open_file_release(request, request->set_info.open_file);
                    chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
                    break;
                default:
                    chimera_smb_error("SET_INFO info_class %u not implemented", request->set_info.info_class);
                    chimera_smb_open_file_release(request, request->set_info.open_file);
                    chimera_smb_complete_request(request, SMB2_STATUS_NOT_IMPLEMENTED);
            } /* switch */
            break;
        case SMB2_INFO_SECURITY:
            chimera_smb_set_security(request);
            break;
        default:
            chimera_smb_error("SET_INFO info_type %u not implemented", request->set_info.info_type);
            chimera_smb_open_file_release(request, request->set_info.open_file);
            chimera_smb_complete_request(request, SMB2_STATUS_NOT_IMPLEMENTED);
    } /* switch */
} /* chimera_smb_set_info */

void
chimera_smb_set_info_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request)
{
    evpl_iovec_cursor_append_uint16(reply_cursor, SMB2_SET_INFO_REPLY_SIZE);
} /* chimera_smb_set_info_reply */


int
chimera_smb_parse_set_info(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request)
{
    int rc = 0;

    if (unlikely(request->request_struct_size != SMB2_SET_INFO_REQUEST_SIZE)) {
        chimera_smb_error("Received SMB2 SET_INFO request with invalid struct size (%u expected %u)",
                          request->smb2_hdr.struct_size,
                          SMB2_SET_INFO_REQUEST_SIZE);
        request->status = SMB2_STATUS_INVALID_PARAMETER;
        return -1;
    }

    int prc = 0;
    prc |= evpl_iovec_cursor_try_get_uint8(request_cursor, &request->set_info.info_type);
    prc |= evpl_iovec_cursor_try_get_uint8(request_cursor, &request->set_info.info_class);
    prc |= evpl_iovec_cursor_try_get_uint32(request_cursor, &request->set_info.buffer_length);
    prc |= evpl_iovec_cursor_try_get_uint16(request_cursor, &request->set_info.buffer_offset);
    prc |= evpl_iovec_cursor_try_get_uint32(request_cursor, &request->set_info.addl_info);
    prc |= evpl_iovec_cursor_try_get_uint64(request_cursor, &request->set_info.file_id.pid);
    prc |= evpl_iovec_cursor_try_get_uint64(request_cursor, &request->set_info.file_id.vid);

    if (unlikely(prc)) {
        chimera_smb_error("Received SMB2 SET_INFO request truncated in fixed body");
        return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
    }

    /* Seek to the client-declared input buffer and fence the cursor to exactly
     * buffer_length bytes, so the per-info-class sub-parsers below can read only
     * within the declared buffer (and reject cleanly if it is too short). */
    if (unlikely(smb_cursor_seek_to(request_cursor, request->set_info.buffer_offset) != 0 ||
                 request->set_info.buffer_length > (uint32_t) evpl_iovec_cursor_remaining(request_cursor))) {
        chimera_smb_error("Received SMB2 SET_INFO with input buffer out of range");
        return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
    }
    evpl_iovec_cursor_set_limit(request_cursor, request->set_info.buffer_length);

    request->set_info.attrs.smb_attr_mask = 0;

    switch (request->set_info.info_type) {
        case SMB2_INFO_FILE:
            switch (request->set_info.info_class) {
                case SMB2_FILE_BASIC_INFO:
                    /* [MS-FSA] 2.1.5.14.2 / [MS-FSCC] 2.4.7: the input buffer
                     * must be at least sizeof(FILE_BASIC_INFORMATION) (40 bytes:
                     * four 8-byte timestamps + 4-byte FileAttributes + 4-byte
                     * Reserved).  A shorter buffer is rejected with
                     * STATUS_INFO_LENGTH_MISMATCH rather than silently accepted
                     * (WPTS MS-FSAModel SetFileBasicInformation cases). */
                    if (unlikely(request->set_info.buffer_length < SMB2_FILE_BASIC_INFO_SIZE)) {
                        return chimera_smb_parse_reject(request, SMB2_STATUS_INFO_LENGTH_MISMATCH);
                    }
                    rc = chimera_smb_parse_basic_info(request_cursor, &request->set_info.attrs);
                    break;
                case SMB2_FILE_DISPOSITION_INFO:
                    rc = chimera_smb_parse_disposition_info(request_cursor, &request->set_info.attrs);
                    break;
                case SMB2_FILE_DISPOSITION_INFO_EX:
                    rc = chimera_smb_parse_disposition_info_ex(request_cursor, &request->set_info.attrs);
                    break;
                case SMB2_FILE_ENDOFFILE_INFO:
                case SMB2_FILE_ALLOCATION_INFO:
                    rc = chimera_smb_parse_end_of_file_info(
                        request_cursor,
                        &request->set_info.attrs);
                    break;
                case SMB2_FILE_RENAME_INFO:
                case SMB2_FILE_LINK_INFO:
                    rc = chimera_smb_parse_rename_info(request_cursor, request);
                    break;
                case SMB2_FILE_FULL_EA_INFO:
                    /* Capture the client's FILE_FULL_EA_INFORMATION buffer; the
                     * process phase parses and applies it to the VFS xattr
                     * store one EA at a time. */
                    request->set_info.ea_buf     = NULL;
                    request->set_info.ea_buf_len = 0;
                    if (request->set_info.buffer_length > CHIMERA_SMB_EA_VALUE_MAX) {
                        request->status = SMB2_STATUS_EA_TOO_LARGE;
                        rc              = -1;
                        break;
                    }
                    if (request->set_info.buffer_length) {
                        request->set_info.ea_buf = malloc(request->set_info.buffer_length);
                        if (evpl_iovec_cursor_try_copy(request_cursor,
                                                       request->set_info.ea_buf,
                                                       request->set_info.buffer_length) != 0) {
                            free(request->set_info.ea_buf);
                            request->set_info.ea_buf = NULL;
                            return chimera_smb_parse_reject(request,
                                                            SMB2_STATUS_INFO_LENGTH_MISMATCH);
                        }
                        request->set_info.ea_buf_len = request->set_info.buffer_length;
                    }
                    break;
                case SMB2_FILE_POSITION_INFO:
                    rc = chimera_smb_parse_position_info(request_cursor, &request->set_info.attrs);
                    break;

                default:
                    chimera_smb_error("parse_set_info: SET_INFO info_class %u not implemented",
                                      request->set_info.info_class);
                    request->status = SMB2_STATUS_NOT_IMPLEMENTED;
                    rc              = -1;
                    break;
            } /* switch */
            break;
        case SMB2_INFO_SECURITY:
            if (request->set_info.buffer_length <= sizeof(request->set_info.sec_buf)) {
                if (unlikely(evpl_iovec_cursor_try_copy(request_cursor, request->set_info.sec_buf,
                                                        request->set_info.buffer_length) != 0)) {
                    return chimera_smb_parse_reject(request, SMB2_STATUS_INFO_LENGTH_MISMATCH);
                }
                request->set_info.sec_buf_len = request->set_info.buffer_length;
            } else {
                chimera_smb_error("parse_set_info: security descriptor too large (%u bytes)",
                                  request->set_info.buffer_length);
                request->status = SMB2_STATUS_INVALID_PARAMETER;
                rc              = -1;
            }
            break;
        default:
            chimera_smb_error("parse_set_info: SET_INFO info_type %u not implemented", request->set_info.info_type);
            request->status = SMB2_STATUS_NOT_IMPLEMENTED;
            rc              = -1;
            break;
    } /* switch */

    /* A sub-parser that ran off the end of the declared buffer returns -1
     * without setting a status; answer that cleanly as a length mismatch rather
     * than letting the dispatcher tear down the connection. */
    if (rc != 0 && request->status == SMB2_STATUS_SUCCESS) {
        return chimera_smb_parse_reject(request, SMB2_STATUS_INFO_LENGTH_MISMATCH);
    }

    return rc;
} /* chimera_smb_parse_set_info */