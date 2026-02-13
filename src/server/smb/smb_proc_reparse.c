// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <sys/stat.h>
#include "smb_internal.h"
#include "smb_procs.h"
#include "smb_string.h"
#include "smb2.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"

/* ------------------------------------------------------------------ */
/* SET_REPARSE_POINT async chain                                      */
/* ------------------------------------------------------------------ */

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

    chimera_vfs_release(vfs_thread, request->ioctl.rp_parent_handle);
    chimera_smb_open_file_release(request, request->ioctl.rp_open_file);

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_complete_request(request, SMB2_STATUS_INTERNAL_ERROR);
        return;
    }

    chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
} /* chimera_smb_set_reparse_create_cb */

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

    chimera_vfs_release(vfs_thread, request->ioctl.rp_parent_handle);
    chimera_smb_open_file_release(request, request->ioctl.rp_open_file);

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_error("SET_REPARSE: symlink failed error=%d target='%s' target_len=%d",
                          error_code,
                          request->ioctl.rp_target,
                          request->ioctl.rp_target_len);
        chimera_smb_complete_request(request, SMB2_STATUS_INTERNAL_ERROR);
        return;
    }

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
            chimera_vfs_symlink(
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
            chimera_vfs_mknod(
                vfs_thread,
                &request->session_handle->session->cred,
                request->ioctl.rp_parent_handle,
                open_file->name,
                open_file->name_len,
                set_attr,
                CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_RDEV,
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
            chimera_vfs_mknod(
                vfs_thread,
                &request->session_handle->session->cred,
                request->ioctl.rp_parent_handle,
                open_file->name,
                open_file->name_len,
                set_attr,
                CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_RDEV,
                0,
                0,
                chimera_smb_set_reparse_create_cb,
                request);
            break;
        case SMB2_NFS_SPECFILE_FIFO:
            set_attr->va_mode     = S_IFIFO | 0666;
            set_attr->va_req_mask = CHIMERA_VFS_ATTR_MODE;
            set_attr->va_set_mask = CHIMERA_VFS_ATTR_MODE;
            chimera_vfs_mknod(
                vfs_thread,
                &request->session_handle->session->cred,
                request->ioctl.rp_parent_handle,
                open_file->name,
                open_file->name_len,
                set_attr,
                CHIMERA_VFS_ATTR_MODE,
                0,
                0,
                chimera_smb_set_reparse_create_cb,
                request);
            break;
        case SMB2_NFS_SPECFILE_SOCK:
            set_attr->va_mode     = S_IFSOCK | 0666;
            set_attr->va_req_mask = CHIMERA_VFS_ATTR_MODE;
            set_attr->va_set_mask = CHIMERA_VFS_ATTR_MODE;
            chimera_vfs_mknod(
                vfs_thread,
                &request->session_handle->session->cred,
                request->ioctl.rp_parent_handle,
                open_file->name,
                open_file->name_len,
                set_attr,
                CHIMERA_VFS_ATTR_MODE,
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

    chimera_vfs_remove(
        vfs_thread,
        &request->session_handle->session->cred,
        oh,
        open_file->name,
        open_file->name_len,
        NULL,
        0,
        0,
        0,
        chimera_smb_set_reparse_remove_cb,
        request);
} /* chimera_smb_set_reparse_open_parent_cb */

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
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_HANDLE);
        return;
    }

    request->ioctl.rp_open_file = open_file;

    chimera_vfs_open(
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
    int                               data_len;

    chimera_smb_open_file_release(request, request->ioctl.rp_open_file);

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_complete_request(request, SMB2_STATUS_INTERNAL_ERROR);
        return;
    }

    /* Convert Unix forward slashes to Windows backslashes */
    for (int i = 0; i < target_length; i++) {
        if (request->ioctl.rp_target[i] == '/') {
            request->ioctl.rp_target[i] = '\\';
        }
    }

    /* Convert UTF-8 target to UTF-16LE */
    utf16_len = chimera_smb_utf8_to_utf16le(
        &thread->iconv_ctx,
        request->ioctl.rp_target,
        target_length,
        (uint16_t *) (buf + 16),
        (CHIMERA_VFS_PATH_MAX - 1) * 2);

    if (utf16_len < 0) {
        chimera_smb_complete_request(request, SMB2_STATUS_INTERNAL_ERROR);
        return;
    }

    data_len = 8 + utf16_len; /* InodeType(8) + UTF-16LE target */

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
    /* InodeType = NFS_SPECFILE_LNK */
    buf[8]  = (SMB2_NFS_SPECFILE_LNK >>  0) & 0xff;
    buf[9]  = (SMB2_NFS_SPECFILE_LNK >>  8) & 0xff;
    buf[10] = (SMB2_NFS_SPECFILE_LNK >> 16) & 0xff;
    buf[11] = (SMB2_NFS_SPECFILE_LNK >> 24) & 0xff;
    buf[12] = (SMB2_NFS_SPECFILE_LNK >> 32) & 0xff;
    buf[13] = (SMB2_NFS_SPECFILE_LNK >> 40) & 0xff;
    buf[14] = (SMB2_NFS_SPECFILE_LNK >> 48) & 0xff;
    buf[15] = (SMB2_NFS_SPECFILE_LNK >> 56) & 0xff;
    /* UTF-16LE data is already at buf+16 */

    request->ioctl.rp_response_len = 8 + data_len; /* header(8) + data */

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
            chimera_smb_open_file_release(request, request->ioctl.rp_open_file);
            chimera_smb_complete_request(request, SMB2_STATUS_NOT_A_REPARSE_POINT);
            return;
    } /* switch */

    chimera_smb_open_file_release(request, request->ioctl.rp_open_file);
    chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
} /* chimera_smb_get_reparse_getattr_cb */

void
chimera_smb_ioctl_get_reparse(struct chimera_smb_request *request)
{
    struct chimera_vfs_thread    *vfs_thread = request->compound->thread->vfs_thread;
    struct chimera_smb_open_file *open_file;

    open_file = chimera_smb_open_file_resolve(request, &request->ioctl.file_id);

    if (!open_file) {
        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_HANDLE);
        return;
    }

    request->ioctl.rp_open_file = open_file;

    chimera_vfs_getattr(
        vfs_thread,
        &request->session_handle->session->cred,
        open_file->handle,
        CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_RDEV,
        chimera_smb_get_reparse_getattr_cb,
        request);
} /* chimera_smb_ioctl_get_reparse */
