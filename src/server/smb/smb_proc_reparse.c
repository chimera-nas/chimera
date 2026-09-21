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
#include "vfs/vfs_compound.h"
#include "vfs/vfs_release.h"

/* ------------------------------------------------------------------ */
/* SET_REPARSE_POINT                                                  */
/* ------------------------------------------------------------------ */

/*
 * PUTFH(parent), OPEN_CURRENT(dir), REMOVE(name, matching the old object),
 * CREATE(symlink | node), OPEN_CURRENT, GETHANDLE.
 *
 * One sequence, because every step addresses what the step before it resolved:
 * the name is unlinked from the parent the sequence is standing on, the
 * replacement is created in that same parent and becomes the current object,
 * and the handle the client's open is re-bound to is an open of THAT object --
 * not of a file handle carried by hand between four callbacks.
 *
 * The REMOVE matches the doomed object's file handle (op_set_remove_match), so
 * a name that has since come to mean something else is left alone rather than
 * destroyed on this open's behalf; the CREATE then collides on it, which is the
 * honest answer.
 *
 * Re-binding is what the tail is for.  The SET replaces the original inode and
 * the client's open still references it, so a following GET_REPARSE -- or any
 * handle op -- must resolve the link rather than the now-orphaned original
 * (pike reparse test_set_get_reparse_point).  The re-open takes real flags (0,
 * not INFERRED/PATH) because the close path closes it through chimera_vfs_close
 * and needs a backend handle.  Re-arming the delete-on-close reservation stays
 * out of band: it sets a flag on the new handle, addresses no object through
 * the cursors, and cannot fail.
 */
static void
chimera_smb_set_reparse_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_smb_request     *request    = private_data;
    struct chimera_vfs_thread      *vfs_thread = request->compound->thread->vfs_thread;
    struct chimera_smb_open_file   *open_file  = request->ioctl.rp_open_file;
    struct chimera_vfs_open_handle *oh         = NULL;
    struct chimera_vfs_open_handle *old;
    enum chimera_vfs_error          status;
    uint32_t                        completed;

    status    = chimera_vfs_compound_status(compound);
    completed = chimera_vfs_compound_num_completed(compound);

    if (status == CHIMERA_VFS_OK) {
        oh = chimera_vfs_compound_take_handle(
            compound, chimera_vfs_compound_num_ops(compound) - 1);
    }

    chimera_vfs_compound_free(compound);
    request->vfs_compound = NULL;

    /* The CREATE is the last op that can make the SET itself fail.  A failure
     * in the re-bind behind it leaves the open on the old inode -- which is what
     * the per-op chain did when the create reported no file handle -- but the
     * reparse point IS set, so the client is told so. */
    if (status != CHIMERA_VFS_OK &&
        completed <= request->ioctl.rp_create_index) {
        chimera_smb_error("SET_REPARSE: failed at op %u error=%d name='%.*s'",
                          completed ? completed - 1 : 0, status,
                          open_file->name_len, open_file->name);
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INTERNAL_ERROR);
        return;
    }

    if (oh) {
        old               = open_file->handle;
        open_file->handle = oh;
        /* A different object now; describe the handle that reaches it. */
        open_file->open_flags = chimera_smb_open_handle_flags(
            oh, !!(open_file->flags & CHIMERA_SMB_OPEN_FILE_FLAG_DIRECTORY));

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
} /* chimera_smb_set_reparse_sequence_complete */

void
chimera_smb_ioctl_set_reparse(struct chimera_smb_request *request)
{
    struct chimera_vfs_thread    *vfs_thread = request->compound->thread->vfs_thread;
    struct chimera_smb_open_file *open_file;
    struct chimera_vfs_attrs     *set_attr = &request->ioctl.rp_set_attr;
    uint8_t                       create_type;
    int                           idx;

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

    memset(set_attr, 0, sizeof(*set_attr));

    /* What replaces the placeholder.  A device's numbers and a node's type ride
     * in the create attributes, which is how CREATE(NODE) takes them. */
    switch (request->ioctl.rp_nfs_type) {
        case SMB2_NFS_SPECFILE_LNK:
            create_type = CHIMERA_VFS_COMPOUND_CREATE_SYMLINK;
            break;
        case SMB2_NFS_SPECFILE_CHR:
        case SMB2_NFS_SPECFILE_BLK:
            create_type       = CHIMERA_VFS_COMPOUND_CREATE_NODE;
            set_attr->va_mode = (request->ioctl.rp_nfs_type == SMB2_NFS_SPECFILE_CHR ?
                                 S_IFCHR : S_IFBLK) | 0666;
            set_attr->va_rdev = ((uint64_t) request->ioctl.rp_device_major << 32) |
                request->ioctl.rp_device_minor;
            set_attr->va_req_mask = CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_RDEV;
            set_attr->va_set_mask = CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_RDEV;
            break;
        case SMB2_NFS_SPECFILE_FIFO:
        case SMB2_NFS_SPECFILE_SOCK:
            create_type       = CHIMERA_VFS_COMPOUND_CREATE_NODE;
            set_attr->va_mode = (request->ioctl.rp_nfs_type == SMB2_NFS_SPECFILE_FIFO ?
                                 S_IFIFO : S_IFSOCK) | 0666;
            set_attr->va_req_mask = CHIMERA_VFS_ATTR_MODE;
            set_attr->va_set_mask = CHIMERA_VFS_ATTR_MODE;
            break;
        default:
            chimera_smb_open_file_release(request, open_file);
            chimera_smb_complete_request(request, SMB2_STATUS_NOT_IMPLEMENTED);
            return;
    } /* switch */

    request->ioctl.rp_open_file = open_file;

    request->vfs_compound = chimera_vfs_compound_alloc(
        vfs_thread, &request->session_handle->session->cred);

    chimera_vfs_compound_add_putfh(request->vfs_compound,
                                   open_file->parent_fh,
                                   open_file->parent_fh_len);

    chimera_vfs_compound_add_open_current(request->vfs_compound,
                                          CHIMERA_VFS_OPEN_INFERRED |
                                          CHIMERA_VFS_OPEN_PATH |
                                          CHIMERA_VFS_OPEN_DIRECTORY,
                                          0);

    idx = chimera_vfs_compound_add_remove(request->vfs_compound,
                                          open_file->name,
                                          open_file->name_len,
                                          0, 0, 0);

    if (open_file->handle) {
        chimera_vfs_compound_op_set_remove_match(request->vfs_compound, idx,
                                                 open_file->handle->fh,
                                                 open_file->handle->fh_len,
                                                 1, NULL);
    }

    request->ioctl.rp_create_index =
        (uint32_t) chimera_vfs_compound_add_create(request->vfs_compound,
                                                   create_type,
                                                   open_file->name,
                                                   open_file->name_len,
                                                   request->ioctl.rp_target,
                                                   request->ioctl.rp_target_len,
                                                   set_attr,
                                                   CHIMERA_VFS_ATTR_FH,
                                                   0, 0);

    chimera_vfs_compound_add_open_current(request->vfs_compound, 0, 0);
    chimera_vfs_compound_add_gethandle(request->vfs_compound);

    chimera_vfs_compound_submit(request->vfs_compound,
                                chimera_smb_set_reparse_sequence_complete,
                                request);
} /* chimera_smb_ioctl_set_reparse */

/* ------------------------------------------------------------------ */
/* GET_REPARSE_POINT                                                  */
/* ------------------------------------------------------------------ */

static void
chimera_smb_get_reparse_readlink_cb(
    enum chimera_vfs_error error_code,
    int                    target_length,
    void                  *private_data)
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

/*
 * PUTHANDLE, READLINK -- the second sequence, run only when the first one's
 * mode says symlink.  The target belongs to the compound, so it is copied into
 * the request before the free.
 */
static void
chimera_smb_get_reparse_readlink_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_smb_request           *request = private_data;
    const struct chimera_vfs_compound_op *op;
    enum chimera_vfs_error                status;
    int                                   target_len = 0;

    status = chimera_vfs_compound_status(compound);

    if (status == CHIMERA_VFS_OK) {
        op = chimera_vfs_compound_op(compound,
                                     chimera_vfs_compound_num_ops(compound) - 1);

        target_len = (int) op->target_len;

        if (target_len > CHIMERA_VFS_PATH_MAX - 1) {
            target_len = CHIMERA_VFS_PATH_MAX - 1;
        }

        if (target_len > 0) {
            memcpy(request->ioctl.rp_target, op->target, target_len);
        }
        request->ioctl.rp_target[target_len] = '\0';
    }

    chimera_vfs_compound_free(compound);
    request->vfs_compound = NULL;

    request->ioctl.rp_target_len = target_len;

    chimera_smb_get_reparse_readlink_cb(status, target_len, request);
} /* chimera_smb_get_reparse_readlink_complete */

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

/*
 * PUTHANDLE, GETATTR(MODE | RDEV).
 *
 * The symlink arm runs as a SECOND sequence rather than a READLINK behind this
 * GETATTR, because whether to read a link at all depends on the mode this op
 * answers with -- and an op cannot be skipped on an earlier op's result: the
 * gate can only refuse, and a refusal here would fail a GET_REPARSE of a device
 * node that is perfectly well answered from the attributes alone.
 */
static void
chimera_smb_get_reparse_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_smb_request           *request   = private_data;
    struct chimera_smb_open_file         *open_file = request->ioctl.rp_open_file;
    const struct chimera_vfs_compound_op *op;
    struct chimera_vfs_attrs              attr;
    enum chimera_vfs_error                status;

    status = chimera_vfs_compound_status(compound);

    memset(&attr, 0, sizeof(attr));

    if (status == CHIMERA_VFS_OK) {
        op = chimera_vfs_compound_op(compound,
                                     chimera_vfs_compound_num_ops(compound) - 1);
        attr = op->attr;
    }

    chimera_vfs_compound_free(compound);
    request->vfs_compound = NULL;

    if (status != CHIMERA_VFS_OK) {
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INTERNAL_ERROR);
        return;
    }

    switch (attr.va_mode & S_IFMT) {
        case S_IFLNK:
            request->vfs_compound = chimera_vfs_compound_alloc(
                request->compound->thread->vfs_thread,
                &request->session_handle->session->cred);

            chimera_vfs_compound_add_puthandle(request->vfs_compound,
                                               open_file->handle,
                                               open_file->open_flags);

            chimera_vfs_compound_add_readlink(request->vfs_compound);

            chimera_vfs_compound_submit(
                request->vfs_compound,
                chimera_smb_get_reparse_readlink_complete, request);
            return;
        case S_IFCHR:
            chimera_smb_get_reparse_build_device(
                request,
                SMB2_NFS_SPECFILE_CHR,
                (uint32_t) (attr.va_rdev >> 32),
                (uint32_t) (attr.va_rdev & 0xFFFFFFFF));
            break;
        case S_IFBLK:
            chimera_smb_get_reparse_build_device(
                request,
                SMB2_NFS_SPECFILE_BLK,
                (uint32_t) (attr.va_rdev >> 32),
                (uint32_t) (attr.va_rdev & 0xFFFFFFFF));
            break;
        case S_IFIFO:
            chimera_smb_get_reparse_build_simple(request, SMB2_NFS_SPECFILE_FIFO);
            break;
        case S_IFSOCK:
            chimera_smb_get_reparse_build_simple(request, SMB2_NFS_SPECFILE_SOCK);
            break;
        default:
            chimera_smb_open_file_release(request, open_file);
            chimera_smb_complete_request(request, SMB2_STATUS_NOT_A_REPARSE_POINT);
            return;
    } /* switch */

    chimera_smb_open_file_release(request, open_file);
    chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
} /* chimera_smb_get_reparse_sequence_complete */

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

    request->vfs_compound = chimera_vfs_compound_alloc(
        vfs_thread, &request->session_handle->session->cred);

    chimera_vfs_compound_add_puthandle(request->vfs_compound,
                                       open_file->handle,
                                       open_file->open_flags);

    chimera_vfs_compound_add_getattr(request->vfs_compound,
                                     CHIMERA_VFS_ATTR_MODE |
                                     CHIMERA_VFS_ATTR_RDEV);

    chimera_vfs_compound_submit(request->vfs_compound,
                                chimera_smb_get_reparse_sequence_complete,
                                request);
} /* chimera_smb_ioctl_get_reparse */
