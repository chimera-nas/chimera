// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <sys/stat.h>
#ifdef _WIN32
#include "common/platform.h"
#endif /* ifdef _WIN32 */
#include "smb_internal.h"
#include "smb_procs.h"
#include "smb_string.h"
#include "smb_common/smb2.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"

/* ------------------------------------------------------------------ */
/* SET_REPARSE_POINT async chain                                      */
/* ------------------------------------------------------------------ */

static void
chimera_smb_set_reparse_rebind_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data);

static void
chimera_smb_set_reparse_create_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data)
{
    struct chimera_smb_request *request    = private_data;
    struct chimera_vfs_thread  *vfs_thread = request->compound->thread->vfs_thread;

    (void) set_attr;
    (void) dir_pre_attr;
    (void) dir_post_attr;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_release(vfs_thread, request->ioctl.rp_parent_handle);
        chimera_smb_open_file_release(request, request->ioctl.rp_open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INTERNAL_ERROR);
        return;
    }

    /* Re-bind the open handle to the new special-file inode (exactly as the
     * symlink path does) so the client's follow-up owner/mode SET_SECURITY
     * lands on the node rather than the removed placeholder.  Without this the
     * device/FIFO/socket keeps the server's default owner (root) and mode
     * (0666) that mknod_at laid down. */
    if (attr && (attr->va_set_mask & CHIMERA_VFS_ATTR_FH) &&
        attr->va_fh_len <= sizeof(request->ioctl.rp_new_fh)) {
        memcpy(request->ioctl.rp_new_fh, attr->va_fh, attr->va_fh_len);
        request->ioctl.rp_new_fh_len = attr->va_fh_len;

        chimera_vfs_open_fh(
            vfs_thread,
            &request->session_handle->session->cred,
            request->ioctl.rp_new_fh,
            request->ioctl.rp_new_fh_len,
            0,
            chimera_smb_set_reparse_rebind_cb,
            request);
        return;
    }

    chimera_vfs_release(vfs_thread, request->ioctl.rp_parent_handle);
    chimera_smb_open_file_release(request, request->ioctl.rp_open_file);
    chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
} /* chimera_smb_set_reparse_create_cb */

/* The SET created a new symlink inode, replacing the original object that the
 * client's open still references.  Re-bind the open's VFS handle (open_file->
 * handle) to the new inode so a following GET_REPARSE_POINT -- or any handle op
 * -- resolves the link rather than the now-orphaned original
 * (pike reparse test_set_get_reparse_point). */
static void
chimera_smb_set_reparse_rebind_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_smb_request   *request    = private_data;
    struct chimera_vfs_thread    *vfs_thread = request->compound->thread->vfs_thread;
    struct chimera_smb_open_file *open_file  = request->ioctl.rp_open_file;

    chimera_vfs_release(vfs_thread, request->ioctl.rp_parent_handle);

    if (error_code == CHIMERA_VFS_OK && oh) {
        struct chimera_vfs_open_handle *old = open_file->handle;
        open_file->handle = oh;

        /* The delete-on-close reservation was armed on the original handle (and
         * does not fire on a plain release).  Re-arm it on the new handle so the
         * close still unlinks the link by name (the pike test_set_get create
         * carries FILE_DELETE_ON_CLOSE). */
        if (open_file->flags & CHIMERA_SMB_OPEN_FILE_FLAG_DELETE_ON_CLOSE) {
            chimera_vfs_set_delete_on_close(
                vfs_thread, oh,
                open_file->parent_fh, open_file->parent_fh_len,
                open_file->name, open_file->name_len,
                &request->session_handle->session->cred);
        }

        if (old) {
            chimera_vfs_release(vfs_thread, old);
        }
    }

    chimera_smb_open_file_release(request, open_file);
    chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
} /* chimera_smb_set_reparse_rebind_cb */

static void
chimera_smb_set_reparse_symlink_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data)
{
    struct chimera_smb_request *request    = private_data;
    struct chimera_vfs_thread  *vfs_thread = request->compound->thread->vfs_thread;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_error("SET_REPARSE: symlink failed error=%d target='%s' target_len=%d",
                          error_code,
                          request->ioctl.rp_target,
                          request->ioctl.rp_target_len);
        chimera_vfs_release(vfs_thread, request->ioctl.rp_parent_handle);
        chimera_smb_open_file_release(request, request->ioctl.rp_open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INTERNAL_ERROR);
        return;
    }

    /* Re-bind the open handle to the new symlink inode (its file handle was
     * returned in attr->va_fh).  rp_parent_handle and rp_open_file are released
     * in the rebind callback. */
    if (attr && (attr->va_set_mask & CHIMERA_VFS_ATTR_FH) &&
        attr->va_fh_len <= sizeof(request->ioctl.rp_new_fh)) {
        memcpy(request->ioctl.rp_new_fh, attr->va_fh, attr->va_fh_len);
        request->ioctl.rp_new_fh_len = attr->va_fh_len;

        /* Open a real (backend) handle on the new inode -- not an INFERRED/PATH
         * handle -- so the open keeps a valid backend handle for delete-on-close
         * (the close path closes it via chimera_vfs_close). */
        chimera_vfs_open_fh(
            vfs_thread,
            &request->session_handle->session->cred,
            request->ioctl.rp_new_fh,
            request->ioctl.rp_new_fh_len,
            0,
            chimera_smb_set_reparse_rebind_cb,
            request);
        return;
    }

    /* No file handle returned -- the open keeps its (now stale) handle, but the
     * SET itself succeeded. */
    chimera_vfs_release(vfs_thread, request->ioctl.rp_parent_handle);
    chimera_smb_open_file_release(request, request->ioctl.rp_open_file);
    chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
} /* chimera_smb_set_reparse_symlink_cb */

static void
chimera_smb_set_reparse_remove_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_smb_request   *request    = private_data;
    struct chimera_vfs_thread    *vfs_thread = request->compound->thread->vfs_thread;
    struct chimera_smb_open_file *open_file  = request->ioctl.rp_open_file;
    struct chimera_vfs_attrs     *set_attr   = &request->ioctl.rp_set_attr;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_error("SET_REPARSE: remove failed error=%d name='%.*s'",
                          error_code, open_file->name_len, open_file->name);
        chimera_vfs_release(vfs_thread, request->ioctl.rp_parent_handle);
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INTERNAL_ERROR);
        return;
    }

    memset(set_attr, 0, sizeof(*set_attr));

    switch (request->ioctl.rp_nfs_type) {
        case SMB2_NFS_SPECFILE_LNK:
            chimera_vfs_symlink_at(
                vfs_thread,
                &request->session_handle->session->cred,
                request->ioctl.rp_parent_handle,
                open_file->name,
                open_file->name_len,
                request->ioctl.rp_target,
                request->ioctl.rp_target_len,
                set_attr,
                CHIMERA_VFS_ATTR_FH,
                0,
                0,
                chimera_smb_set_reparse_symlink_cb,
                request);
            break;
        case SMB2_NFS_SPECFILE_CHR:
            set_attr->va_mode = S_IFCHR | 0666;
            set_attr->va_rdev = ((uint64_t) request->ioctl.rp_device_major << 32) |
                request->ioctl.rp_device_minor;
            set_attr->va_req_mask = CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_RDEV;
            set_attr->va_set_mask = CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_RDEV;
            chimera_vfs_mknod_at(
                vfs_thread,
                &request->session_handle->session->cred,
                request->ioctl.rp_parent_handle,
                open_file->name,
                open_file->name_len,
                set_attr,
                CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_RDEV | CHIMERA_VFS_ATTR_FH,
                0,
                0,
                chimera_smb_set_reparse_create_cb,
                request);
            break;
        case SMB2_NFS_SPECFILE_BLK:
            set_attr->va_mode = S_IFBLK | 0666;
            set_attr->va_rdev = ((uint64_t) request->ioctl.rp_device_major << 32) |
                request->ioctl.rp_device_minor;
            set_attr->va_req_mask = CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_RDEV;
            set_attr->va_set_mask = CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_RDEV;
            chimera_vfs_mknod_at(
                vfs_thread,
                &request->session_handle->session->cred,
                request->ioctl.rp_parent_handle,
                open_file->name,
                open_file->name_len,
                set_attr,
                CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_RDEV | CHIMERA_VFS_ATTR_FH,
                0,
                0,
                chimera_smb_set_reparse_create_cb,
                request);
            break;
        case SMB2_NFS_SPECFILE_FIFO:
            set_attr->va_mode     = S_IFIFO | 0666;
            set_attr->va_req_mask = CHIMERA_VFS_ATTR_MODE;
            set_attr->va_set_mask = CHIMERA_VFS_ATTR_MODE;
            chimera_vfs_mknod_at(
                vfs_thread,
                &request->session_handle->session->cred,
                request->ioctl.rp_parent_handle,
                open_file->name,
                open_file->name_len,
                set_attr,
                CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_FH,
                0,
                0,
                chimera_smb_set_reparse_create_cb,
                request);
            break;
        case SMB2_NFS_SPECFILE_SOCK:
            set_attr->va_mode     = S_IFSOCK | 0666;
            set_attr->va_req_mask = CHIMERA_VFS_ATTR_MODE;
            set_attr->va_set_mask = CHIMERA_VFS_ATTR_MODE;
            chimera_vfs_mknod_at(
                vfs_thread,
                &request->session_handle->session->cred,
                request->ioctl.rp_parent_handle,
                open_file->name,
                open_file->name_len,
                set_attr,
                CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_FH,
                0,
                0,
                chimera_smb_set_reparse_create_cb,
                request);
            break;
        default:
            chimera_vfs_release(vfs_thread, request->ioctl.rp_parent_handle);
            chimera_smb_open_file_release(request, open_file);
            chimera_smb_complete_request(request, SMB2_STATUS_NOT_IMPLEMENTED);
            break;
    } /* switch */
} /* chimera_smb_set_reparse_remove_cb */

static void
chimera_smb_set_reparse_open_parent_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_smb_request   *request    = private_data;
    struct chimera_vfs_thread    *vfs_thread = request->compound->thread->vfs_thread;
    struct chimera_smb_open_file *open_file  = request->ioctl.rp_open_file;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_error("SET_REPARSE: open_parent failed error=%d", error_code);
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INTERNAL_ERROR);
        return;
    }

    request->ioctl.rp_parent_handle = oh;

    chimera_vfs_remove_at(
        vfs_thread,
        &request->session_handle->session->cred,
        oh,
        open_file->name,
        open_file->name_len,
        NULL,
        0,
        0,
        0,
        0,
        NULL,
        chimera_smb_set_reparse_remove_cb,
        request);
} /* chimera_smb_set_reparse_open_parent_cb */

/* ------------------------------------------------------------------ */
/* Reparse points of any other tag                                    */
/* ------------------------------------------------------------------ */

/* A reparse point whose tag chimera does not turn into a special file is kept
 * verbatim: the whole REPARSE_[GUID_]DATA_BUFFER is the file's chimera.reparse
 * xattr (outside the user. namespace, so it is neither an EA nor an NFS user
 * xattr), and FILE_ATTRIBUTE_REPARSE_POINT is set in the file's persisted DOS
 * attributes so every query reports it without reading the buffer.  A plain
 * open of such a file is refused with STATUS_IO_REPARSE_TAG_NOT_HANDLED (see
 * chimera_smb_create_open_at_callback), as on a Windows server, which has no
 * filter to handle the tag either. */

static void
chimera_smb_reparse_finish(
    struct chimera_smb_request *request,
    uint32_t                    status)
{
    free(request->ioctl.rp_existing);
    request->ioctl.rp_existing = NULL;
    chimera_smb_open_file_release(request, request->ioctl.rp_open_file);
    chimera_smb_complete_request(request, status);
} /* chimera_smb_reparse_finish */

static uint32_t
chimera_smb_reparse_vfs_status(enum chimera_vfs_error error_code)
{
    switch (error_code) {
        case CHIMERA_VFS_OK:
            return SMB2_STATUS_SUCCESS;
        case CHIMERA_VFS_ENOTSUP:
            return SMB2_STATUS_INVALID_DEVICE_REQUEST;
        case CHIMERA_VFS_EACCES:
        case CHIMERA_VFS_EPERM:
            return SMB2_STATUS_ACCESS_DENIED;
        default:
            return SMB2_STATUS_INTERNAL_ERROR;
    } /* switch */
} /* chimera_smb_reparse_vfs_status */

/* Compare the reparse point already on the file (rp_existing, len bytes) with
 * the tag and GUID of this request: SMB2_STATUS_SUCCESS if they agree. */
static uint32_t
chimera_smb_reparse_match_existing(
    struct chimera_smb_request *request,
    uint32_t                    len)
{
    const uint8_t *cur = request->ioctl.rp_existing;
    uint32_t       tag = request->ioctl.rp_reparse_tag;
    uint32_t       cur_tag;

    if (len < SMB2_REPARSE_DATA_HEADER_SIZE) {
        return SMB2_STATUS_SUCCESS;
    }
    memcpy(&cur_tag, cur, 4);
    if (cur_tag != tag) {
        return SMB2_STATUS_IO_REPARSE_TAG_MISMATCH;
    }
    if (!SMB2_IO_REPARSE_TAG_IS_MICROSOFT(tag) &&
        len >= SMB2_REPARSE_GUID_DATA_HEADER_SIZE &&
        memcmp(cur + 8, request->ioctl.rp_guid, 16) != 0) {
        return SMB2_STATUS_REPARSE_ATTRIBUTE_CONFLICT;
    }
    return SMB2_STATUS_SUCCESS;
} /* chimera_smb_reparse_match_existing */

static void
chimera_smb_reparse_setattr_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    (void) pre_attr;
    (void) set_attr;
    (void) post_attr;

    chimera_smb_reparse_finish(private_data, chimera_smb_reparse_vfs_status(error_code));
} /* chimera_smb_reparse_setattr_cb */

/* Set or clear FILE_ATTRIBUTE_REPARSE_POINT in the persisted DOS attributes; a
 * data file is also marked ARCHIVE (MS-FSA 2.1.5.10.37 / 2.1.5.10.3). */
static void
chimera_smb_reparse_set_dos(
    struct chimera_smb_request *request,
    int                         reparse)
{
    struct chimera_vfs_attrs *attr = &request->ioctl.rp_set_attr;
    uint32_t                  dos  = request->ioctl.rp_dos & ~SMB2_FILE_ATTRIBUTE_NORMAL;

    if (reparse) {
        dos |= SMB2_FILE_ATTRIBUTE_REPARSE_POINT;
    } else {
        dos &= ~SMB2_FILE_ATTRIBUTE_REPARSE_POINT;
    }
    if (!request->ioctl.rp_isdir) {
        dos |= SMB2_FILE_ATTRIBUTE_ARCHIVE;
    }

    attr->va_req_mask       = CHIMERA_VFS_ATTR_DOS_ATTRIBUTES;
    attr->va_set_mask       = CHIMERA_VFS_ATTR_DOS_ATTRIBUTES;
    attr->va_dos_attributes = dos;

    chimera_vfs_setattr(request->compound->thread->vfs_thread,
                        &request->session_handle->session->cred,
                        request->ioctl.rp_open_file->handle,
                        attr,
                        0,
                        0,
                        chimera_smb_reparse_setattr_cb,
                        request);
} /* chimera_smb_reparse_set_dos */

static void
chimera_smb_reparse_set_xattr_cb(
    enum chimera_vfs_error          error_code,
    const struct chimera_vfs_attrs *pre_attr,
    const struct chimera_vfs_attrs *post_attr,
    void                           *private_data)
{
    struct chimera_smb_request *request = private_data;

    (void) pre_attr;
    (void) post_attr;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_reparse_finish(request, chimera_smb_reparse_vfs_status(error_code));
        return;
    }
    chimera_smb_reparse_set_dos(request, 1);
} /* chimera_smb_reparse_set_xattr_cb */

static void
chimera_smb_reparse_set_store(struct chimera_smb_request *request)
{
    chimera_vfs_set_xattr(request->compound->thread->vfs_thread,
                          &request->session_handle->session->cred,
                          request->ioctl.rp_open_file->handle,
                          0,
                          CHIMERA_SMB_REPARSE_XATTR,
                          CHIMERA_SMB_REPARSE_XATTR_LEN,
                          request->ioctl.rp_response,
                          request->ioctl.rp_generic_len,
                          chimera_smb_reparse_set_xattr_cb,
                          request);
} /* chimera_smb_reparse_set_store */

/* A reparse point is already on the file: it may only be replaced by one of
 * the same tag (and, for a non-Microsoft tag, the same GUID). */
static void
chimera_smb_reparse_set_existing_cb(
    enum chimera_vfs_error error_code,
    uint32_t               value_len,
    void                  *private_data)
{
    struct chimera_smb_request *request = private_data;
    uint32_t                    status;

    if (error_code == CHIMERA_VFS_OK) {
        status = chimera_smb_reparse_match_existing(request, value_len);
        if (status != SMB2_STATUS_SUCCESS) {
            chimera_smb_reparse_finish(request, status);
            return;
        }
    } else if (error_code != CHIMERA_VFS_ENODATA) {
        chimera_smb_reparse_finish(request, chimera_smb_reparse_vfs_status(error_code));
        return;
    }
    chimera_smb_reparse_set_store(request);
} /* chimera_smb_reparse_set_existing_cb */

static void
chimera_smb_reparse_set_check_existing(struct chimera_smb_request *request)
{
    if (!(request->ioctl.rp_dos & SMB2_FILE_ATTRIBUTE_REPARSE_POINT)) {
        chimera_smb_reparse_set_store(request);
        return;
    }

    request->ioctl.rp_existing = malloc(CHIMERA_SMB_REPARSE_READ_MAX);
    chimera_vfs_get_xattr(request->compound->thread->vfs_thread,
                          &request->session_handle->session->cred,
                          request->ioctl.rp_open_file->handle,
                          CHIMERA_SMB_REPARSE_XATTR,
                          CHIMERA_SMB_REPARSE_XATTR_LEN,
                          request->ioctl.rp_existing,
                          CHIMERA_SMB_REPARSE_READ_MAX,
                          chimera_smb_reparse_set_existing_cb,
                          request);
} /* chimera_smb_reparse_set_check_existing */

/* Directory emptiness: a reparse point may only be set on an empty one. */
static int
chimera_smb_reparse_dir_entry_cb(
    uint64_t                        inum,
    uint64_t                        cookie,
    const char                     *name,
    int                             namelen,
    const struct chimera_vfs_attrs *attrs,
    void                           *arg)
{
    struct chimera_smb_request *request = arg;

    (void) inum;
    (void) cookie;
    (void) attrs;

    if ((namelen == 1 && name[0] == '.') ||
        (namelen == 2 && name[0] == '.' && name[1] == '.')) {
        return 0;
    }
    request->ioctl.rp_dir_nonempty = 1;
    return 1;
} /* chimera_smb_reparse_dir_entry_cb */

static void
chimera_smb_reparse_dir_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        cookie,
    uint64_t                        verifier,
    uint32_t                        eof,
    struct chimera_vfs_attrs       *attr,
    void                           *private_data)
{
    struct chimera_smb_request *request = private_data;

    (void) handle;
    (void) cookie;
    (void) verifier;
    (void) eof;
    (void) attr;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_reparse_finish(request, chimera_smb_reparse_vfs_status(error_code));
        return;
    }
    if (request->ioctl.rp_dir_nonempty) {
        chimera_smb_reparse_finish(request, SMB2_STATUS_DIRECTORY_NOT_EMPTY);
        return;
    }
    chimera_smb_reparse_set_check_existing(request);
} /* chimera_smb_reparse_dir_complete */

static void
chimera_smb_reparse_set_getattr_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_smb_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_reparse_finish(request, chimera_smb_reparse_vfs_status(error_code));
        return;
    }

    request->ioctl.rp_isdir = S_ISDIR(attr->va_mode);
    request->ioctl.rp_dos   = (attr->va_set_mask & CHIMERA_VFS_ATTR_DOS_ATTRIBUTES) ?
        attr->va_dos_attributes : 0;

    if (request->ioctl.rp_reparse_tag == SMB2_IO_REPARSE_TAG_MOUNT_POINT &&
        !request->ioctl.rp_isdir) {
        chimera_smb_reparse_finish(request, SMB2_STATUS_NOT_A_DIRECTORY);
        return;
    }

    /* A file with EAs cannot also become a reparse point. */
    if (!(request->ioctl.rp_dos & SMB2_FILE_ATTRIBUTE_REPARSE_POINT) &&
        (attr->va_set_mask & CHIMERA_VFS_ATTR_EA_SIZE) && attr->va_ea_size) {
        chimera_smb_reparse_finish(request, SMB2_STATUS_EAS_NOT_SUPPORTED);
        return;
    }

    if (request->ioctl.rp_isdir) {
        request->ioctl.rp_dir_nonempty = 0;
        chimera_vfs_readdir(request->compound->thread->vfs_thread,
                            &request->session_handle->session->cred,
                            request->ioctl.rp_open_file->handle,
                            0, 0, 0, 0, 0, NULL, 0,
                            chimera_smb_reparse_dir_entry_cb,
                            chimera_smb_reparse_dir_complete,
                            request);
        return;
    }

    chimera_smb_reparse_set_check_existing(request);
} /* chimera_smb_reparse_set_getattr_cb */

/* MS-FSA 2.1.5.10.37 for a reparse point kept verbatim. */
static void
chimera_smb_reparse_generic_set(struct chimera_smb_request *request)
{
    struct chimera_smb_open_file *open_file = request->ioctl.rp_open_file;

    if (!(open_file->granted_access & (SMB2_FILE_WRITE_DATA | SMB2_FILE_WRITE_ATTRIBUTES))) {
        chimera_smb_reparse_finish(request, SMB2_STATUS_ACCESS_DENIED);
        return;
    }
    if (!(open_file->handle->vfs_module->capabilities & CHIMERA_VFS_CAP_XATTR)) {
        chimera_smb_reparse_finish(request, SMB2_STATUS_INVALID_DEVICE_REQUEST);
        return;
    }

    if (!SMB2_IO_REPARSE_TAG_IS_MICROSOFT(request->ioctl.rp_reparse_tag)) {
        memcpy(request->ioctl.rp_guid, request->ioctl.rp_response + 8, 16);
    }

    chimera_vfs_getattr(request->compound->thread->vfs_thread,
                        &request->session_handle->session->cred,
                        open_file->handle,
                        CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_DOS_ATTRIBUTES |
                        CHIMERA_VFS_ATTR_EA_SIZE,
                        chimera_smb_reparse_set_getattr_cb,
                        request);
} /* chimera_smb_reparse_generic_set */

/* ---- DELETE_REPARSE_POINT (MS-FSA 2.1.5.10.3) ---- */

static void
chimera_smb_reparse_delete_removed_cb(
    enum chimera_vfs_error          error_code,
    const struct chimera_vfs_attrs *pre_attr,
    const struct chimera_vfs_attrs *post_attr,
    void                           *private_data)
{
    struct chimera_smb_request *request = private_data;

    (void) pre_attr;
    (void) post_attr;

    if (error_code != CHIMERA_VFS_OK && error_code != CHIMERA_VFS_ENODATA) {
        chimera_smb_reparse_finish(request, chimera_smb_reparse_vfs_status(error_code));
        return;
    }
    chimera_smb_reparse_set_dos(request, 0);
} /* chimera_smb_reparse_delete_removed_cb */

static void
chimera_smb_reparse_delete_existing_cb(
    enum chimera_vfs_error error_code,
    uint32_t               value_len,
    void                  *private_data)
{
    struct chimera_smb_request *request = private_data;
    uint32_t                    status;

    if (error_code == CHIMERA_VFS_ENODATA) {
        chimera_smb_reparse_finish(request, SMB2_STATUS_NOT_A_REPARSE_POINT);
        return;
    }
    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_reparse_finish(request, chimera_smb_reparse_vfs_status(error_code));
        return;
    }

    status = chimera_smb_reparse_match_existing(request, value_len);
    if (status != SMB2_STATUS_SUCCESS) {
        chimera_smb_reparse_finish(request, status);
        return;
    }

    chimera_vfs_remove_xattr(request->compound->thread->vfs_thread,
                             &request->session_handle->session->cred,
                             request->ioctl.rp_open_file->handle,
                             CHIMERA_SMB_REPARSE_XATTR,
                             CHIMERA_SMB_REPARSE_XATTR_LEN,
                             chimera_smb_reparse_delete_removed_cb,
                             request);
} /* chimera_smb_reparse_delete_existing_cb */

static void
chimera_smb_reparse_delete_getattr_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_smb_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_reparse_finish(request, chimera_smb_reparse_vfs_status(error_code));
        return;
    }

    request->ioctl.rp_isdir = S_ISDIR(attr->va_mode);
    request->ioctl.rp_dos   = (attr->va_set_mask & CHIMERA_VFS_ATTR_DOS_ATTRIBUTES) ?
        attr->va_dos_attributes : 0;

    if (!(request->ioctl.rp_dos & SMB2_FILE_ATTRIBUTE_REPARSE_POINT)) {
        chimera_smb_reparse_finish(request, SMB2_STATUS_NOT_A_REPARSE_POINT);
        return;
    }

    request->ioctl.rp_existing = malloc(CHIMERA_SMB_REPARSE_READ_MAX);
    chimera_vfs_get_xattr(request->compound->thread->vfs_thread,
                          &request->session_handle->session->cred,
                          request->ioctl.rp_open_file->handle,
                          CHIMERA_SMB_REPARSE_XATTR,
                          CHIMERA_SMB_REPARSE_XATTR_LEN,
                          request->ioctl.rp_existing,
                          CHIMERA_SMB_REPARSE_READ_MAX,
                          chimera_smb_reparse_delete_existing_cb,
                          request);
} /* chimera_smb_reparse_delete_getattr_cb */

void
chimera_smb_ioctl_delete_reparse(struct chimera_smb_request *request)
{
    struct chimera_smb_open_file *open_file;
    uint32_t                      tag = request->ioctl.rp_reparse_tag;
    uint32_t                      header;

    open_file = chimera_smb_open_file_resolve(request, &request->ioctl.file_id);
    if (!open_file) {
        chimera_smb_complete_request(request, SMB2_STATUS_FILE_CLOSED);
        return;
    }
    request->ioctl.rp_open_file = open_file;
    request->ioctl.rp_existing  = NULL;

    if (!(open_file->granted_access & (SMB2_FILE_WRITE_DATA | SMB2_FILE_WRITE_ATTRIBUTES))) {
        chimera_smb_reparse_finish(request, SMB2_STATUS_ACCESS_DENIED);
        return;
    }
    if (tag == 0 || tag == 1) {
        chimera_smb_reparse_finish(request, SMB2_STATUS_IO_REPARSE_TAG_INVALID);
        return;
    }
    /* The request names the reparse point and carries no data: exactly the
     * header for its kind of tag. */
    header = SMB2_IO_REPARSE_TAG_IS_MICROSOFT(tag) ?
        SMB2_REPARSE_DATA_HEADER_SIZE : SMB2_REPARSE_GUID_DATA_HEADER_SIZE;
    if (request->ioctl.rp_data_len != 0 || request->ioctl.input_count != header) {
        chimera_smb_reparse_finish(request, SMB2_STATUS_IO_REPARSE_DATA_INVALID);
        return;
    }
    if (!(open_file->handle->vfs_module->capabilities & CHIMERA_VFS_CAP_XATTR)) {
        chimera_smb_reparse_finish(request, SMB2_STATUS_NOT_A_REPARSE_POINT);
        return;
    }

    chimera_vfs_getattr(request->compound->thread->vfs_thread,
                        &request->session_handle->session->cred,
                        open_file->handle,
                        CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_DOS_ATTRIBUTES,
                        chimera_smb_reparse_delete_getattr_cb,
                        request);
} /* chimera_smb_ioctl_delete_reparse */

/* ---- GET_REPARSE_POINT of a reparse point kept verbatim ---- */

/* Fit a built GET response to the caller's buffer (MS-FSA 2.1.5.10.14): not
 * even the header -> BUFFER_TOO_SMALL; part of the data -> BUFFER_OVERFLOW
 * with what fits, as NTFS answers. */
static void
chimera_smb_get_reparse_finish(struct chimera_smb_request *request)
{
    uint32_t tag, header, max = request->ioctl.max_output_response;
    uint32_t status = SMB2_STATUS_SUCCESS;

    memcpy(&tag, request->ioctl.rp_response, 4);
    header = SMB2_IO_REPARSE_TAG_IS_MICROSOFT(tag) ?
        SMB2_REPARSE_DATA_HEADER_SIZE : SMB2_REPARSE_GUID_DATA_HEADER_SIZE;

    if (max < header) {
        request->ioctl.rp_response_len = 0;
        status                         = SMB2_STATUS_BUFFER_TOO_SMALL;
    } else if ((uint32_t) request->ioctl.rp_response_len > max) {
        request->ioctl.rp_response_len = max;
        status                         = SMB2_STATUS_BUFFER_OVERFLOW;
    }

    chimera_smb_open_file_release(request, request->ioctl.rp_open_file);
    chimera_smb_complete_request(request, status);
} /* chimera_smb_get_reparse_finish */

static void
chimera_smb_get_reparse_xattr_cb(
    enum chimera_vfs_error error_code,
    uint32_t               value_len,
    void                  *private_data)
{
    struct chimera_smb_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK || value_len < SMB2_REPARSE_DATA_HEADER_SIZE) {
        chimera_smb_open_file_release(request, request->ioctl.rp_open_file);
        chimera_smb_complete_request(request,
                                     (error_code == CHIMERA_VFS_OK ||
                                      error_code == CHIMERA_VFS_ENODATA) ?
                                     SMB2_STATUS_NOT_A_REPARSE_POINT :
                                     chimera_smb_reparse_vfs_status(error_code));
        return;
    }
    request->ioctl.rp_response_len = value_len;
    chimera_smb_get_reparse_finish(request);
} /* chimera_smb_get_reparse_xattr_cb */

void
chimera_smb_ioctl_set_reparse(struct chimera_smb_request *request)
{
    struct chimera_vfs_thread    *vfs_thread = request->compound->thread->vfs_thread;
    struct chimera_smb_open_file *open_file;

    /* If the tag was unsupported (cleared to 0 by parser), accept and ignore */
    if (request->ioctl.rp_reparse_tag == 0) {
        chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
        return;
    }

    open_file = chimera_smb_open_file_resolve(request, &request->ioctl.file_id);

    if (!open_file) {
        chimera_smb_complete_request(request, SMB2_STATUS_FILE_CLOSED);
        return;
    }

    request->ioctl.rp_open_file = open_file;
    request->ioctl.rp_existing  = NULL;

    if (request->ioctl.rp_generic) {
        chimera_smb_reparse_generic_set(request);
        return;
    }

    chimera_vfs_open_fh(
        vfs_thread,
        &request->session_handle->session->cred,
        open_file->parent_fh,
        open_file->parent_fh_len,
        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
        chimera_smb_set_reparse_open_parent_cb,
        request);
} /* chimera_smb_ioctl_set_reparse */

/* ------------------------------------------------------------------ */
/* GET_REPARSE_POINT async chain                                      */
/* ------------------------------------------------------------------ */

static void
chimera_smb_get_reparse_readlink_cb(
    enum chimera_vfs_error    error_code,
    int                       target_length,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_smb_request       *request = private_data;
    struct chimera_server_smb_thread *thread  = request->compound->thread;
    uint8_t                          *buf     = request->ioctl.rp_response;
    int                               utf16_len;
    uint16_t                          reparse_data_length;
    uint32_t                          flags;

    chimera_smb_open_file_release(request, request->ioctl.rp_open_file);

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_complete_request(request, SMB2_STATUS_INTERNAL_ERROR);
        return;
    }

    /* A relative target carries no leading separator (test before the slash
     * conversion below). */
    flags = (target_length > 0 && request->ioctl.rp_target[0] != '/') ?
        SMB2_SYMLINK_FLAG_RELATIVE : SMB2_SYMLINK_FLAG_ABSOLUTE;

    /* Convert Unix forward slashes to Windows backslashes */
    for (int i = 0; i < target_length; i++) {
        if (request->ioctl.rp_target[i] == '/') {
            request->ioctl.rp_target[i] = '\\';
        }
    }

    /* A symlink is reported as a SYMBOLIC_LINK_REPARSE_BUFFER under
     * IO_REPARSE_TAG_SYMLINK (MS-FSCC 2.1.2.4): the Windows representation that
     * Windows-style clients (e.g. pike's get_symlink) decode.  Convert the
     * target to UTF-16LE for the Substitute name; the Print name is an identical
     * copy that immediately follows it. */
    utf16_len = chimera_smb_utf8_to_utf16le(
        &thread->iconv_ctx,
        request->ioctl.rp_target,
        target_length,
        (uint16_t *) (buf + 20),
        (CHIMERA_VFS_PATH_MAX - 1) * 2);

    if (utf16_len < 0) {
        chimera_smb_complete_request(request, SMB2_STATUS_INTERNAL_ERROR);
        return;
    }

    memcpy(buf + 20 + utf16_len, buf + 20, utf16_len); /* Print name copy */

    reparse_data_length = 12 + 2 * utf16_len;

    /* ReparseTag = IO_REPARSE_TAG_SYMLINK */
    buf[0] = (SMB2_IO_REPARSE_TAG_SYMLINK >>  0) & 0xff;
    buf[1] = (SMB2_IO_REPARSE_TAG_SYMLINK >>  8) & 0xff;
    buf[2] = (SMB2_IO_REPARSE_TAG_SYMLINK >> 16) & 0xff;
    buf[3] = (SMB2_IO_REPARSE_TAG_SYMLINK >> 24) & 0xff;
    /* ReparseDataLength */
    buf[4] = (reparse_data_length >> 0) & 0xff;
    buf[5] = (reparse_data_length >> 8) & 0xff;
    /* Reserved */
    buf[6] = 0;
    buf[7] = 0;
    /* SubstituteNameOffset = 0 */
    buf[8] = 0;
    buf[9] = 0;
    /* SubstituteNameLength */
    buf[10] = (utf16_len >> 0) & 0xff;
    buf[11] = (utf16_len >> 8) & 0xff;
    /* PrintNameOffset (immediately after the Substitute name) */
    buf[12] = (utf16_len >> 0) & 0xff;
    buf[13] = (utf16_len >> 8) & 0xff;
    /* PrintNameLength */
    buf[14] = (utf16_len >> 0) & 0xff;
    buf[15] = (utf16_len >> 8) & 0xff;
    /* Flags */
    buf[16] = (flags >>  0) & 0xff;
    buf[17] = (flags >>  8) & 0xff;
    buf[18] = (flags >> 16) & 0xff;
    buf[19] = (flags >> 24) & 0xff;
    /* PathBuffer (Substitute name + Print name) already at buf+20 */

    request->ioctl.rp_response_len = 20 + 2 * utf16_len;

    chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
} /* chimera_smb_get_reparse_readlink_cb */

static inline void
chimera_smb_get_reparse_build_simple(
    struct chimera_smb_request *request,
    uint64_t                    nfs_type)
{
    uint8_t *buf      = request->ioctl.rp_response;
    int      data_len = 8; /* InodeType only */

    /* ReparseTag */
    buf[0] = (SMB2_IO_REPARSE_TAG_NFS >>  0) & 0xff;
    buf[1] = (SMB2_IO_REPARSE_TAG_NFS >>  8) & 0xff;
    buf[2] = (SMB2_IO_REPARSE_TAG_NFS >> 16) & 0xff;
    buf[3] = (SMB2_IO_REPARSE_TAG_NFS >> 24) & 0xff;
    /* ReparseDataLength */
    buf[4] = (data_len >>  0) & 0xff;
    buf[5] = (data_len >>  8) & 0xff;
    /* Reserved */
    buf[6] = 0;
    buf[7] = 0;
    /* InodeType */
    buf[8]  = (nfs_type >>  0) & 0xff;
    buf[9]  = (nfs_type >>  8) & 0xff;
    buf[10] = (nfs_type >> 16) & 0xff;
    buf[11] = (nfs_type >> 24) & 0xff;
    buf[12] = (nfs_type >> 32) & 0xff;
    buf[13] = (nfs_type >> 40) & 0xff;
    buf[14] = (nfs_type >> 48) & 0xff;
    buf[15] = (nfs_type >> 56) & 0xff;

    request->ioctl.rp_response_len = 8 + data_len; /* header(8) + data */
} /* chimera_smb_get_reparse_build_simple */

static inline void
chimera_smb_get_reparse_build_device(
    struct chimera_smb_request *request,
    uint64_t                    nfs_type,
    uint32_t                    major,
    uint32_t                    minor)
{
    uint8_t *buf      = request->ioctl.rp_response;
    int      data_len = 8 + 8; /* InodeType(8) + major(4) + minor(4) */

    /* ReparseTag */
    buf[0] = (SMB2_IO_REPARSE_TAG_NFS >>  0) & 0xff;
    buf[1] = (SMB2_IO_REPARSE_TAG_NFS >>  8) & 0xff;
    buf[2] = (SMB2_IO_REPARSE_TAG_NFS >> 16) & 0xff;
    buf[3] = (SMB2_IO_REPARSE_TAG_NFS >> 24) & 0xff;
    /* ReparseDataLength */
    buf[4] = (data_len >>  0) & 0xff;
    buf[5] = (data_len >>  8) & 0xff;
    /* Reserved */
    buf[6] = 0;
    buf[7] = 0;
    /* InodeType */
    buf[8]  = (nfs_type >>  0) & 0xff;
    buf[9]  = (nfs_type >>  8) & 0xff;
    buf[10] = (nfs_type >> 16) & 0xff;
    buf[11] = (nfs_type >> 24) & 0xff;
    buf[12] = (nfs_type >> 32) & 0xff;
    buf[13] = (nfs_type >> 40) & 0xff;
    buf[14] = (nfs_type >> 48) & 0xff;
    buf[15] = (nfs_type >> 56) & 0xff;
    /* Major */
    buf[16] = (major >>  0) & 0xff;
    buf[17] = (major >>  8) & 0xff;
    buf[18] = (major >> 16) & 0xff;
    buf[19] = (major >> 24) & 0xff;
    /* Minor */
    buf[20] = (minor >>  0) & 0xff;
    buf[21] = (minor >>  8) & 0xff;
    buf[22] = (minor >> 16) & 0xff;
    buf[23] = (minor >> 24) & 0xff;

    request->ioctl.rp_response_len = 8 + data_len; /* header(8) + data */
} /* chimera_smb_get_reparse_build_device */

static void
chimera_smb_get_reparse_getattr_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_smb_request *request    = private_data;
    struct chimera_vfs_thread  *vfs_thread = request->compound->thread->vfs_thread;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_open_file_release(request, request->ioctl.rp_open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INTERNAL_ERROR);
        return;
    }

    switch (attr->va_mode & S_IFMT) {
        case S_IFLNK:
            chimera_vfs_readlink(
                vfs_thread,
                &request->session_handle->session->cred,
                request->ioctl.rp_open_file->handle,
                request->ioctl.rp_target,
                CHIMERA_VFS_PATH_MAX,
                0,
                chimera_smb_get_reparse_readlink_cb,
                request);
            return;
        case S_IFCHR:
            chimera_smb_get_reparse_build_device(
                request,
                SMB2_NFS_SPECFILE_CHR,
                (uint32_t) (attr->va_rdev >> 32),
                (uint32_t) (attr->va_rdev & 0xFFFFFFFF));
            break;
        case S_IFBLK:
            chimera_smb_get_reparse_build_device(
                request,
                SMB2_NFS_SPECFILE_BLK,
                (uint32_t) (attr->va_rdev >> 32),
                (uint32_t) (attr->va_rdev & 0xFFFFFFFF));
            break;
        case S_IFIFO:
            chimera_smb_get_reparse_build_simple(request, SMB2_NFS_SPECFILE_FIFO);
            break;
        case S_IFSOCK:
            chimera_smb_get_reparse_build_simple(request, SMB2_NFS_SPECFILE_SOCK);
            break;
        default:
            if ((attr->va_set_mask & CHIMERA_VFS_ATTR_DOS_ATTRIBUTES) &&
                (attr->va_dos_attributes & SMB2_FILE_ATTRIBUTE_REPARSE_POINT)) {
                chimera_vfs_get_xattr(vfs_thread,
                                      &request->session_handle->session->cred,
                                      request->ioctl.rp_open_file->handle,
                                      CHIMERA_SMB_REPARSE_XATTR,
                                      CHIMERA_SMB_REPARSE_XATTR_LEN,
                                      request->ioctl.rp_response,
                                      sizeof(request->ioctl.rp_response),
                                      chimera_smb_get_reparse_xattr_cb,
                                      request);
                return;
            }
            chimera_smb_open_file_release(request, request->ioctl.rp_open_file);
            chimera_smb_complete_request(request, SMB2_STATUS_NOT_A_REPARSE_POINT);
            return;
    } /* switch */

    chimera_smb_get_reparse_finish(request);
} /* chimera_smb_get_reparse_getattr_cb */

void
chimera_smb_ioctl_get_reparse(struct chimera_smb_request *request)
{
    struct chimera_vfs_thread    *vfs_thread = request->compound->thread->vfs_thread;
    struct chimera_smb_open_file *open_file;

    open_file = chimera_smb_open_file_resolve(request, &request->ioctl.file_id);

    if (!open_file) {
        chimera_smb_complete_request(request, SMB2_STATUS_FILE_CLOSED);
        return;
    }

    request->ioctl.rp_open_file = open_file;

    chimera_vfs_getattr(
        vfs_thread,
        &request->session_handle->session->cred,
        open_file->handle,
        CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_RDEV | CHIMERA_VFS_ATTR_DOS_ATTRIBUTES,
        chimera_smb_get_reparse_getattr_cb,
        request);
} /* chimera_smb_ioctl_get_reparse */
