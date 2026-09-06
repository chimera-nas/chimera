// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <xxhash.h>

#include "smb_internal.h"
#include "vfs/sdk/vfs_attrs.h"
#include "vfs/vfs_open_cache.h"
#include "evpl/evpl.h"

/* A namespace op retargets a path-id: after a rename the source resolves to
 * nothing and an overwritten destination is gone; after a create at a name
 * whose prior object was removed-and-forgotten the id maps to a new object.
 * Either way a cached open handle keyed by that path-id holds a dead server
 * file id, so the next open of the path would fail.  Evict it (both follow
 * variants, both open caches) so the next open re-resolves.  The path id is the
 * SMB proxy's, so this lives here rather than in the backend-agnostic core. */
void
smb_evict_pathid(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request,
    const char                     *path,
    int                             pathlen)
{
    struct chimera_vfs *vfs = request->thread->vfs;
    uint64_t            id  = chimera_smb_path_intern(conn->server, path, pathlen);
    uint8_t             fh[CHIMERA_VFS_FH_SIZE];
    int                 nf;

    for (nf = 0; nf <= 1; nf++) {
        int len = chimera_smb_encode_open_fh(request->fh, id, nf, fh);

        chimera_vfs_open_cache_evict(request->thread, vfs->vfs_open_path_cache, fh, len);
        chimera_vfs_open_cache_evict(request->thread, vfs->vfs_open_file_cache, fh, len);
    }
} /* smb_evict_pathid */

/*
 * SMB2 namespace operations for the path-only SMB client: rename, symlink, and
 * mknod.  Each addresses files by the full mount-relative path carried in
 * request->X.name (and request->rename_at.new_name for rename) and drives a
 * transient SMB2 CREATE -> SET_INFO/IOCTL -> CLOSE chain, reusing the shared
 * helpers in smb_internal.h (smb_send_create / smb_send_close /
 * smb_parse_create_reply / smb_apply_attrs / struct chimera_smb_op_state).
 */

/* SMB2 IOCTL request fixed-field size (StructureSize value, MS-SMB2 2.2.31).
 * Defined locally because smb2.h carries it for the SET_REPARSE wire format. */
#ifndef SMB2_IOCTL_REQUEST_SIZE
#define SMB2_IOCTL_REQUEST_SIZE 57
#endif /* ifndef SMB2_IOCTL_REQUEST_SIZE */

/* IOCTL Flags bit: this is an FSCTL (MS-SMB2 2.2.31 SMB2_0_IOCTL_IS_FSCTL).
 * Not defined in smb2.h (the server ignores the field), so define it here. */
#define SMB2_0_IOCTL_IS_FSCTL   0x00000001

/* ---- rename_at (SET_INFO FileRenameInformation) ------------------------ */

/*
 * rename_at chain: CREATE the source path (DELETE | READ_ATTRIBUTES) -> SET_INFO
 * FileRenameInformation with the destination path -> CLOSE.  The transient
 * source FileId is carried in request->plugin_data (struct chimera_smb_op_state)
 * across the three legs.
 */

static void
chimera_smb_rename_close_reply(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    struct chimera_vfs_request *request = arg;

    (void) conn;
    (void) hdr;
    (void) body;
    (void) body_len;
    (void) status;

    /* The rename status was already recorded by the SET_INFO reply; the CLOSE
     * is best-effort cleanup of the transient open. */
    request->complete(request);
} /* chimera_smb_rename_close_reply */

static void
chimera_smb_rename_set_info_reply(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    struct chimera_vfs_request  *request = arg;
    struct chimera_smb_op_state *state   = request->plugin_data;

    (void) hdr;
    (void) body;
    (void) body_len;

    request->status = chimera_smb_status_to_errno(status);

    if (request->status == CHIMERA_VFS_OK) {
        smb_evict_pathid(conn, request, request->rename_at.name,
                         request->rename_at.namelen);
        smb_evict_pathid(conn, request, request->rename_at.new_name,
                         request->rename_at.new_namelen);
    }

    /* Always CLOSE the transient source open, even if the rename failed. */
    smb_send_close(conn, request, &state->file_id, chimera_smb_rename_close_reply);
} /* chimera_smb_rename_set_info_reply */

static void
chimera_smb_rename_create_reply(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    struct chimera_vfs_request  *request = arg;
    struct chimera_smb_op_state *state   = request->plugin_data;
    struct smb_create_result     r;
    struct evpl_iovec            iov;
    struct evpl_iovec_cursor     cursor;
    struct smb2_header          *shdr;
    uint8_t                      name16[2 * CHIMERA_SMB_PATH_MAX];
    size_t                       name16_len;
    int                          buffer_offset;
    uint32_t                     buffer_length;

    (void) hdr;
    (void) body_len;

    if (status != SMB2_STATUS_SUCCESS) {
        request->status = chimera_smb_status_to_errno(status);
        request->complete(request);
        return;
    }

    smb_parse_create_reply(body, &r);
    state->file_id = r.file_id;

    if (request->rename_at.new_namelen > CHIMERA_SMB_PATH_MAX) {
        request->status = CHIMERA_VFS_ENAMETOOLONG;
        /* Close the transient source open before bailing. */
        smb_send_close(conn, request, &state->file_id, chimera_smb_rename_close_reply);
        return;
    }

    /* FileRenameInformation.FileName is the share-relative destination path in
    * UTF-16LE with '\\' separators (smb_utf16le_encode does the '/'->'\\'). */
    name16_len = smb_utf16le_encode(request->rename_at.new_name,
                                    request->rename_at.new_namelen, name16);

    /* FileRenameInformation buffer: ReplaceIfExists(1) + Reserved(7) +
     * RootDirectory(8) + FileNameLength(4) = 20 fixed, then FileName(UTF-16LE). */
    buffer_length = (uint32_t) (20 + name16_len);

    /* SET_INFO, FILE / FileRenameInformation, on the open source FileId. */
    chimera_smb_client_pdu_begin(conn, SMB2_SET_INFO, &iov, &cursor, &shdr);

    /* The buffer immediately follows the SET_INFO fixed fields.  The fixed part
     * the server reads is StructureSize(2) + InfoType(1) + InfoClass(1) +
     * BufferLength(4) + BufferOffset(2) + Reserved(2) + AdditionalInformation(4)
     * + FileId(16) = 32 bytes, so the buffer starts at SMB2 header + 32. */
    buffer_offset = sizeof(struct smb2_header) + 32;

    evpl_iovec_cursor_append_uint16(&cursor, SMB2_SET_INFO_REQUEST_SIZE);
    evpl_iovec_cursor_append_uint8(&cursor, SMB2_INFO_FILE);
    evpl_iovec_cursor_append_uint8(&cursor, SMB2_FILE_RENAME_INFO);
    evpl_iovec_cursor_append_uint32(&cursor, buffer_length);          /* BufferLength */
    evpl_iovec_cursor_append_uint16(&cursor, (uint16_t) buffer_offset); /* BufferOffset */
    evpl_iovec_cursor_append_uint16(&cursor, 0);                      /* Reserved */
    evpl_iovec_cursor_append_uint32(&cursor, 0);                      /* AdditionalInformation */
    evpl_iovec_cursor_append_uint64(&cursor, state->file_id.pid);
    evpl_iovec_cursor_append_uint64(&cursor, state->file_id.vid);

    /* FILE_RENAME_INFORMATION_TYPE_2 body. */
    evpl_iovec_cursor_append_uint8(&cursor, 1);                       /* ReplaceIfExists */
    evpl_iovec_cursor_append_uint8(&cursor, 0);                       /* Reserved[0..6] */
    evpl_iovec_cursor_append_uint16(&cursor, 0);
    evpl_iovec_cursor_append_uint32(&cursor, 0);
    evpl_iovec_cursor_append_uint64(&cursor, 0);                      /* RootDirectory */
    evpl_iovec_cursor_append_uint32(&cursor, (uint32_t) name16_len);  /* FileNameLength */
    if (name16_len > 0) {
        evpl_iovec_cursor_append_blob(&cursor, name16, name16_len);
    }

    chimera_smb_client_pdu_finish(conn, &iov, &cursor, request,
                                  chimera_smb_rename_set_info_reply, request);
} /* chimera_smb_rename_create_reply */

void
chimera_smb_client_rename_at(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    /* Open the source by its full mount-relative path with DELETE (required to
     * rename) + READ_ATTRIBUTES.  Cross-mount renames are already rejected by
     * the VFS layer (EXDEV), so both paths are within this one share. */
    smb_send_create(conn, request,
                    request->rename_at.name, request->rename_at.namelen,
                    SMB2_DELETE | SMB2_FILE_READ_ATTRIBUTES,
                    SMB2_FILE_SHARE_READ | SMB2_FILE_SHARE_WRITE | SMB2_FILE_SHARE_DELETE,
                    SMB2_FILE_OPEN, SMB2_FILE_OPEN_REPARSE_POINT,
                    chimera_smb_rename_create_reply);
} /* chimera_smb_client_rename_at */

/* ---- symlink_at (reparse point) ---------------------------------------- */

/*
 * symlink_at chain: CREATE a new file at the link path -> IOCTL
 * FSCTL_SET_REPARSE_POINT with an NFS LNK reparse buffer carrying the target ->
 * CLOSE.  The chimera SMB server's SET_REPARSE handler (smb_proc_reparse.c)
 * removes the just-created placeholder and replaces it with a real symlink via
 * chimera_vfs_symlink_at, so the placeholder's create attrs are irrelevant.
 */

static void
chimera_smb_symlink_close_reply(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    struct chimera_vfs_request *request = arg;

    (void) conn;
    (void) hdr;
    (void) body;
    (void) body_len;
    (void) status;

    /* The symlink status was recorded by the IOCTL reply; CLOSE is cleanup. */
    request->complete(request);
} /* chimera_smb_symlink_close_reply */

/* After the new symlink's owner stamp: close the (rebound) handle. */
static void
chimera_smb_symlink_secured_reply(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    struct chimera_vfs_request  *request = arg;
    struct chimera_smb_op_state *state   = request->plugin_data;

    (void) status;
    (void) hdr;
    (void) body;
    (void) body_len;

    smb_send_close(conn, request, &state->file_id,
                   chimera_smb_symlink_close_reply);
} /* chimera_smb_symlink_secured_reply */

static void
chimera_smb_symlink_ioctl_reply(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    struct chimera_vfs_request  *request = arg;
    struct chimera_smb_op_state *state   = request->plugin_data;

    (void) hdr;
    (void) body;
    (void) body_len;

    request->status = chimera_smb_status_to_errno(status);

    /* The server rebound this handle to the new node; stamp the creator's owner
     * (lchown semantics) before closing.  A symlink's mode is always 0777, so
     * convey owner only; a device/FIFO/socket node carries the mode the caller
     * requested -- the server's SET_REPARSE laid down a default 0666, so pass
     * the real mode too.  Only on success -- a failed reparse leaves nothing to
     * own. */
    if (status == SMB2_STATUS_SUCCESS) {
        struct chimera_vfs_attrs        owner;
        const struct chimera_vfs_attrs *mode_src =
            (request->opcode == CHIMERA_VFS_OP_MKNOD_AT) ?
            request->mknod_at.set_attr : NULL;

        if (smb_build_create_owner_attrs(request, mode_src, &owner)) {
            smb_send_set_security(conn, request, &state->file_id, &owner,
                                  chimera_smb_symlink_secured_reply);
            return;
        }
    }
    smb_send_close(conn, request, &state->file_id, chimera_smb_symlink_close_reply);
} /* chimera_smb_symlink_ioctl_reply */

static void
chimera_smb_symlink_create_reply(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    struct chimera_vfs_request  *request = arg;
    struct chimera_smb_op_state *state   = request->plugin_data;
    struct smb_create_result     r;
    struct evpl_iovec            iov;
    struct evpl_iovec_cursor     cursor;
    struct smb2_header          *shdr;
    uint8_t                      target16[2 * CHIMERA_SMB_PATH_MAX];
    size_t                       target16_len;
    int                          input_offset;
    uint32_t                     input_count;
    uint16_t                     reparse_data_len;

    (void) hdr;
    (void) body_len;

    if (status == SMB2_STATUS_STOPPED_ON_SYMLINK) {
        /* The link name already exists AS a symlink, so the exclusive CREATE
         * collides: that is EEXIST, not a link to be followed (ELOOP). */
        request->status = CHIMERA_VFS_EEXIST;
        request->complete(request);
        return;
    }

    if (status != SMB2_STATUS_SUCCESS) {
        request->status = chimera_smb_status_to_errno(status);
        request->complete(request);
        return;
    }

    smb_parse_create_reply(body, &r);
    state->file_id = r.file_id;

    if (request->symlink_at.targetlen > CHIMERA_SMB_PATH_MAX) {
        request->status = CHIMERA_VFS_ENAMETOOLONG;
        smb_send_close(conn, request, &state->file_id, chimera_smb_symlink_close_reply);
        return;
    }

    /* The reparse target is UTF-16LE with '\\' separators (the server flips
     * '\\'->'/' on parse), reusing smb_utf16le_encode's '/'->'\\' convention. */
    target16_len = smb_utf16le_encode(request->symlink_at.target,
                                      request->symlink_at.targetlen, target16);

    /* NFS reparse data buffer (matches smb_proc_reparse.c parse for TAG_NFS):
     *   ReparseTag(4) + ReparseDataLength(2) + Reserved(2) +
     *   InodeType(8 = NFS_SPECFILE_LNK) + Target(UTF-16LE).
     * ReparseDataLength covers InodeType(8) + the UTF-16LE target. */
    reparse_data_len = (uint16_t) (8 + target16_len);
    input_count      = (uint32_t) (8 + reparse_data_len);

    chimera_smb_client_pdu_begin(conn, SMB2_IOCTL, &iov, &cursor, &shdr);

    /* The input buffer follows the IOCTL fixed fields: StructureSize(2) +
     * CtlCode(4) + FileId(16) + InputOffset(4) + InputCount(4) +
     * MaxInputResponse(4) + OutputOffset(4) + OutputCount(4) +
     * MaxOutputResponse(4) + Flags(4) + Reserved2(4).  The self-aligning uint32
     * append for CtlCode (right after the 2-byte StructureSize) inserts the
     * 2-byte pad that is the spec's Reserved field, so the fixed part is 56
     * bytes and the input starts at SMB2 header + 56. */
    input_offset = sizeof(struct smb2_header) + 56;

    evpl_iovec_cursor_append_uint16(&cursor, SMB2_IOCTL_REQUEST_SIZE);
    evpl_iovec_cursor_append_uint32(&cursor, SMB2_FSCTL_SET_REPARSE_POINT);
    evpl_iovec_cursor_append_uint64(&cursor, state->file_id.pid);
    evpl_iovec_cursor_append_uint64(&cursor, state->file_id.vid);
    evpl_iovec_cursor_append_uint32(&cursor, (uint32_t) input_offset); /* InputOffset */
    evpl_iovec_cursor_append_uint32(&cursor, input_count);             /* InputCount */
    evpl_iovec_cursor_append_uint32(&cursor, 0);                       /* MaxInputResponse */
    evpl_iovec_cursor_append_uint32(&cursor, 0);                       /* OutputOffset */
    evpl_iovec_cursor_append_uint32(&cursor, 0);                       /* OutputCount */
    evpl_iovec_cursor_append_uint32(&cursor, 0);                       /* MaxOutputResponse */
    evpl_iovec_cursor_append_uint32(&cursor, SMB2_0_IOCTL_IS_FSCTL);   /* Flags */
    evpl_iovec_cursor_append_uint32(&cursor, 0);                       /* Reserved2 */

    /* REPARSE_DATA_BUFFER (NFS) input. */
    evpl_iovec_cursor_append_uint32(&cursor, SMB2_IO_REPARSE_TAG_NFS);
    evpl_iovec_cursor_append_uint16(&cursor, reparse_data_len);
    evpl_iovec_cursor_append_uint16(&cursor, 0);                       /* Reserved */
    evpl_iovec_cursor_append_uint64(&cursor, SMB2_NFS_SPECFILE_LNK);   /* InodeType */
    if (target16_len > 0) {
        evpl_iovec_cursor_append_blob(&cursor, target16, target16_len);
    }

    chimera_smb_client_pdu_finish(conn, &iov, &cursor, request,
                                  chimera_smb_symlink_ioctl_reply, request);
} /* chimera_smb_symlink_create_reply */

void
chimera_smb_client_symlink_at(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    char fullpath[CHIMERA_SMB_PATH_MAX + 1];
    int  fullpath_len;

    /* Give the new symlink a re-openable child fh for the VFS name cache. */
    chimera_smb_set_child_fh(conn, request, request->symlink_at.name,
                             request->symlink_at.namelen, 1,
                             &request->symlink_at.r_attr);

    /* CREATE a new placeholder at the link path (FILE_CREATE fails if it already
     * exists, matching symlink_at's EEXIST semantics).  The server's
     * SET_REPARSE handler then removes the placeholder and lays down the real
     * symlink, so we only need DELETE + WRITE access on the placeholder. */
    fullpath_len = smb_at_full_path(conn, request, request->symlink_at.name,
                                    request->symlink_at.namelen, fullpath,
                                    sizeof(fullpath));
    if (fullpath_len < 0) {
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    smb_send_create(conn, request,
                    fullpath, fullpath_len,
                    SMB2_DELETE | SMB2_FILE_WRITE_DATA | SMB2_FILE_WRITE_ATTRIBUTES |
                    SMB2_FILE_READ_ATTRIBUTES | SMB2_READ_CONTROL |
                    SMB2_WRITE_DACL | SMB2_WRITE_OWNER,
                    SMB2_FILE_SHARE_READ | SMB2_FILE_SHARE_WRITE | SMB2_FILE_SHARE_DELETE,
                    SMB2_FILE_CREATE, SMB2_FILE_NON_DIRECTORY_FILE,
                    chimera_smb_symlink_create_reply);
} /* chimera_smb_client_symlink_at */

/* ---- readlink (FSCTL_GET_REPARSE_POINT) -------------------------------- */

/*
 * readlink operates on an open handle.  The client genericization opens the
 * target with CHIMERA_VFS_OPEN_NOFOLLOW, which the SMB open_at maps to
 * FILE_OPEN_REPARSE_POINT, so the handle's FileId refers to the symlink itself.
 * We then issue FSCTL_GET_REPARSE_POINT and decode the NFS LNK reparse buffer
 * the server returns (mirrors chimera_smb_get_reparse_readlink_cb).
 */

static void
chimera_smb_readlink_reply(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    struct chimera_vfs_request *request = arg;
    uint16_t                    structsize, reparse_data_len, rsvd16;
    uint16_t                    sub_off, sub_len, print_off, print_len;
    uint32_t                    ctlcode, input_offset, input_count;
    uint32_t                    output_offset, output_count, flags, rsvd32;
    uint32_t                    reparse_tag;
    uint64_t                    fid_pid, fid_vid;
    int                         consumed, target16_len, i, out;
    char                       *out_buf = request->readlink.r_target;
    uint32_t                    maxlen  = request->readlink.target_maxlength;
    uint8_t                     target16[2 * CHIMERA_SMB_PATH_MAX];

    (void) hdr;
    (void) body_len;

    if (status != SMB2_STATUS_SUCCESS) {
        request->status = chimera_smb_status_to_errno(status);
        request->complete(request);
        return;
    }

    /* IOCTL response (the chimera server's field order: StructureSize then
     * CtlCode directly, no leading Reserved -- matches the request convention). */
    evpl_iovec_cursor_get_uint16(body, &structsize);
    evpl_iovec_cursor_get_uint32(body, &ctlcode);
    evpl_iovec_cursor_get_uint64(body, &fid_pid);
    evpl_iovec_cursor_get_uint64(body, &fid_vid);
    evpl_iovec_cursor_get_uint32(body, &input_offset);
    evpl_iovec_cursor_get_uint32(body, &input_count);
    evpl_iovec_cursor_get_uint32(body, &output_offset);
    evpl_iovec_cursor_get_uint32(body, &output_count);
    evpl_iovec_cursor_get_uint32(body, &flags);
    evpl_iovec_cursor_get_uint32(body, &rsvd32);

    (void) structsize;
    (void) ctlcode;
    (void) fid_pid;
    (void) fid_vid;
    (void) input_offset;
    (void) input_count;
    (void) flags;
    (void) rsvd32;

    /* OutputOffset is header-relative, as is the cursor's consumed count. */
    consumed = evpl_iovec_cursor_consumed(body);
    if ((int) output_offset > consumed) {
        evpl_iovec_cursor_skip(body, (int) output_offset - consumed);
    }

    /* SYMBOLIC_LINK_REPARSE_BUFFER (MS-FSCC 2.1.2.4), the Windows form the
     * server's GET_REPARSE emits: ReparseTag(4) + ReparseDataLength(2) +
     * Reserved(2) + SubstituteNameOffset(2) + SubstituteNameLength(2) +
     * PrintNameOffset(2) + PrintNameLength(2) + Flags(4) + PathBuffer. */
    if (output_count < 20) {
        request->status = CHIMERA_VFS_EINVAL;
        request->complete(request);
        return;
    }

    evpl_iovec_cursor_get_uint32(body, &reparse_tag);
    evpl_iovec_cursor_get_uint16(body, &reparse_data_len);
    evpl_iovec_cursor_get_uint16(body, &rsvd16);
    evpl_iovec_cursor_get_uint16(body, &sub_off);
    evpl_iovec_cursor_get_uint16(body, &sub_len);
    evpl_iovec_cursor_get_uint16(body, &print_off);
    evpl_iovec_cursor_get_uint16(body, &print_len);
    evpl_iovec_cursor_get_uint32(body, &flags);

    (void) rsvd16;
    (void) print_off;
    (void) print_len;
    (void) reparse_data_len;


    /* readlink of a non-symlink is EINVAL (POSIX). */
    if (reparse_tag != SMB2_IO_REPARSE_TAG_SYMLINK) {
        request->status = CHIMERA_VFS_EINVAL;
        request->complete(request);
        return;
    }

    /* SubstituteName lives at PathBuffer + SubstituteNameOffset. */
    if (sub_off) {
        evpl_iovec_cursor_skip(body, sub_off);
    }
    target16_len = sub_len;
    if (target16_len > (int) sizeof(target16)) {
        target16_len = sizeof(target16);
    }

    /* A truncated reply (claims sub_len bytes but the cursor is short) would
     * leave target16 partly uninitialized -- treat it as a bad reply. */
    if (target16_len > 0 &&
        evpl_iovec_cursor_get_blob(body, target16, target16_len) < 0) {
        request->status = CHIMERA_VFS_EINVAL;
        request->complete(request);
        return;
    }

    /* Decode UTF-16LE (ASCII subset) into r_target, flipping '\\' back to '/'
     * (symmetric with smb_utf16le_encode). */
    out = 0;
    for (i = 0; i + 1 < target16_len && (uint32_t) out < maxlen; i += 2) {
        char c = (char) target16[i];
        out_buf[out++] = (c == '\\') ? '/' : c;
    }

    request->readlink.r_target_length = out;
    request->status                   = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_smb_readlink_reply */

void
chimera_smb_client_readlink(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    struct chimera_smb_client_open *open_state = smb_handle_open_state(request->readlink.handle);
    struct evpl_iovec               iov;
    struct evpl_iovec_cursor        cursor;
    struct smb2_header             *hdr;
    int                             input_offset;

    if (!open_state) {
        request->status = CHIMERA_VFS_EINVAL;
        request->complete(request);
        return;
    }

    chimera_smb_client_pdu_begin(conn, SMB2_IOCTL, &iov, &cursor, &hdr);

    /* No input buffer; the server reads InputCount(0) bytes at InputOffset.
     * Fixed part is 56 bytes (see chimera_smb_client_symlink_at). */
    input_offset = sizeof(struct smb2_header) + 56;

    evpl_iovec_cursor_append_uint16(&cursor, SMB2_IOCTL_REQUEST_SIZE);
    evpl_iovec_cursor_append_uint32(&cursor, SMB2_FSCTL_GET_REPARSE_POINT);
    evpl_iovec_cursor_append_uint64(&cursor, open_state->file_id.pid);
    evpl_iovec_cursor_append_uint64(&cursor, open_state->file_id.vid);
    evpl_iovec_cursor_append_uint32(&cursor, (uint32_t) input_offset);       /* InputOffset */
    evpl_iovec_cursor_append_uint32(&cursor, 0);                             /* InputCount */
    evpl_iovec_cursor_append_uint32(&cursor, 0);                             /* MaxInputResponse */
    evpl_iovec_cursor_append_uint32(&cursor, 0);                             /* OutputOffset */
    evpl_iovec_cursor_append_uint32(&cursor, 0);                             /* OutputCount */
    evpl_iovec_cursor_append_uint32(&cursor, 16 + 2 * CHIMERA_SMB_PATH_MAX); /* MaxOutputResponse */
    evpl_iovec_cursor_append_uint32(&cursor, SMB2_0_IOCTL_IS_FSCTL);         /* Flags */
    evpl_iovec_cursor_append_uint32(&cursor, 0);                             /* Reserved2 */

    chimera_smb_client_pdu_finish(conn, &iov, &cursor, request,
                                  chimera_smb_readlink_reply, request);
} /* chimera_smb_client_readlink */

/* ---- mknod_at ---------------------------------------------------------- */

void
chimera_smb_mknod_create_reply(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    struct chimera_vfs_request  *request = arg;
    struct chimera_smb_op_state *state   = request->plugin_data;
    struct smb_create_result     r;
    struct evpl_iovec            iov;
    struct evpl_iovec_cursor     cursor;
    struct smb2_header          *shdr;
    uint64_t                     mode = request->mknod_at.set_attr->va_mode;
    uint64_t                     nfs_type;
    uint16_t                     reparse_data_len;
    uint32_t                     input_count;
    int                          input_offset;
    int                          has_dev = 0;

    (void) hdr;
    (void) body_len;

    if (status == SMB2_STATUS_STOPPED_ON_SYMLINK) {
        /* The name already exists as a symlink; the exclusive CREATE collides
         * -- EEXIST, not a link to follow. */
        request->status = CHIMERA_VFS_EEXIST;
        request->complete(request);
        return;
    }

    if (status != SMB2_STATUS_SUCCESS) {
        request->status = chimera_smb_status_to_errno(status);
        request->complete(request);
        return;
    }

    smb_parse_create_reply(body, &r);
    state->file_id = r.file_id;

    /* A regular file is fully created by the CREATE itself; nothing else to
     * lay down, so just close the transient open. */
    if (S_ISREG(mode)) {
        request->status = CHIMERA_VFS_OK;
        smb_send_close(conn, request, &state->file_id,
                       chimera_smb_symlink_close_reply);
        return;
    }

    /* Special files ride the same FSCTL_SET_REPARSE_POINT NFS specfile
     * mechanism as symlinks: the server's SET_REPARSE handler replaces the
     * placeholder with a real device/FIFO/socket node via chimera_vfs_mknod_at. */
    switch (mode & S_IFMT) {
        case S_IFIFO:  nfs_type = SMB2_NFS_SPECFILE_FIFO; break;
        case S_IFSOCK: nfs_type = SMB2_NFS_SPECFILE_SOCK; break;
        case S_IFCHR:  nfs_type = SMB2_NFS_SPECFILE_CHR; has_dev = 1; break;
        case S_IFBLK:  nfs_type = SMB2_NFS_SPECFILE_BLK; has_dev = 1; break;
        default:
            request->status = CHIMERA_VFS_ENOTSUP;
            smb_send_close(conn, request, &state->file_id,
                           chimera_smb_symlink_close_reply);
            return;
    } /* switch */

    /* reparse data = InodeType(8) [+ major(4) + minor(4) for CHR/BLK]. */
    reparse_data_len = (uint16_t) (has_dev ? 16 : 8);
    input_count      = (uint32_t) (8 + reparse_data_len);

    chimera_smb_client_pdu_begin(conn, SMB2_IOCTL, &iov, &cursor, &shdr);

    input_offset = sizeof(struct smb2_header) + 56;

    evpl_iovec_cursor_append_uint16(&cursor, SMB2_IOCTL_REQUEST_SIZE);
    evpl_iovec_cursor_append_uint32(&cursor, SMB2_FSCTL_SET_REPARSE_POINT);
    evpl_iovec_cursor_append_uint64(&cursor, state->file_id.pid);
    evpl_iovec_cursor_append_uint64(&cursor, state->file_id.vid);
    evpl_iovec_cursor_append_uint32(&cursor, (uint32_t) input_offset);
    evpl_iovec_cursor_append_uint32(&cursor, input_count);
    evpl_iovec_cursor_append_uint32(&cursor, 0);                      /* MaxInputResponse */
    evpl_iovec_cursor_append_uint32(&cursor, 0);                      /* OutputOffset */
    evpl_iovec_cursor_append_uint32(&cursor, 0);                      /* OutputCount */
    evpl_iovec_cursor_append_uint32(&cursor, 0);                      /* MaxOutputResponse */
    evpl_iovec_cursor_append_uint32(&cursor, SMB2_0_IOCTL_IS_FSCTL);   /* Flags */
    evpl_iovec_cursor_append_uint32(&cursor, 0);                      /* Reserved2 */

    /* REPARSE_DATA_BUFFER (NFS) input. */
    evpl_iovec_cursor_append_uint32(&cursor, SMB2_IO_REPARSE_TAG_NFS);
    evpl_iovec_cursor_append_uint16(&cursor, reparse_data_len);
    evpl_iovec_cursor_append_uint16(&cursor, 0);                      /* Reserved */
    evpl_iovec_cursor_append_uint64(&cursor, nfs_type);               /* InodeType */
    if (has_dev) {
        /* The server reads major = va_rdev >> 32, minor = va_rdev low 32. */
        evpl_iovec_cursor_append_uint32(&cursor,
                                        (uint32_t) (request->mknod_at.set_attr->va_rdev >> 32));
        evpl_iovec_cursor_append_uint32(&cursor,
                                        (uint32_t) (request->mknod_at.set_attr->va_rdev & 0xffffffffULL));
    }

    chimera_smb_client_pdu_finish(conn, &iov, &cursor, request,
                                  chimera_smb_symlink_ioctl_reply, request);
} /* chimera_smb_mknod_create_reply */

void
chimera_smb_client_mknod_at(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    uint64_t mode = request->mknod_at.set_attr->va_mode;
    char     fullpath[CHIMERA_SMB_PATH_MAX + 1];
    int      fullpath_len;

    /* Give the new node a re-openable child fh for the VFS name cache. */
    chimera_smb_set_child_fh(conn, request, request->mknod_at.name,
                             request->mknod_at.name_len, 0,
                             &request->mknod_at.r_attr);

    /* mknod of a regular file is a plain exclusive CREATE; a device/FIFO/socket
     * node is a placeholder CREATE the server then re-lays as the real node
     * from an FSCTL_SET_REPARSE_POINT NFS specfile buffer (see the reply). */
    if (!(S_ISREG(mode) || S_ISFIFO(mode) || S_ISSOCK(mode) ||
          S_ISCHR(mode) || S_ISBLK(mode))) {
        request->status = CHIMERA_VFS_ENOTSUP;
        request->complete(request);
        return;
    }

    /* Resolve the leaf against the parent handle (a dirfd-relative create);
     * for a root-relative create this is just the name. */
    fullpath_len = smb_at_full_path(conn, request, request->mknod_at.name,
                                    request->mknod_at.name_len, fullpath,
                                    sizeof(fullpath));
    if (fullpath_len < 0) {
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    /* WRITE_DACL|WRITE_OWNER are needed for the post-reparse owner/mode stamp
     * (modefromsid SET_SECURITY), exactly as the symlink placeholder requests. */
    smb_send_create(conn, request,
                    fullpath, fullpath_len,
                    SMB2_DELETE | SMB2_FILE_WRITE_DATA |
                    SMB2_FILE_WRITE_ATTRIBUTES | SMB2_FILE_READ_ATTRIBUTES |
                    SMB2_READ_CONTROL | SMB2_WRITE_DACL | SMB2_WRITE_OWNER,
                    SMB2_FILE_SHARE_READ | SMB2_FILE_SHARE_WRITE | SMB2_FILE_SHARE_DELETE,
                    SMB2_FILE_CREATE, SMB2_FILE_NON_DIRECTORY_FILE,
                    chimera_smb_mknod_create_reply);
} /* chimera_smb_client_mknod_at */

/* ---- link_at (hard link: SET_INFO FileLinkInformation) ----------------- */

/*
 * A hard link opens the existing inode (request->fh) and issues SET_INFO
 * FileLinkInformation naming the new path (dir_fh + name); the server creates
 * the new directory entry pointing at the same inode.  Mirrors rename_at, minus
 * the DELETE access (the source name stays) and with FILE_LINK_INFO.
 */

/* Resolve dir_fh's path + '/' + name into a full mount-relative dest path. */
static int
smb_child_path(
    struct chimera_smb_client_server *server,
    const uint8_t                    *dir_fh,
    int                               dir_fh_len,
    const char                       *name,
    int                               namelen,
    char                             *out,
    int                               out_max)
{
    const char *ppath;
    int         plen = 0, n;

    if (chimera_smb_fh_is_root(dir_fh_len)) {
        if (namelen >= out_max) {
            return -1;
        }
        memcpy(out, name, namelen);
        out[namelen] = '\0';
        return namelen;
    }

    ppath = chimera_smb_path_resolve(server, chimera_smb_fh_path_id(dir_fh),
                                     &plen);
    if (!ppath) {
        return -1;
    }
    n = plen + 1 + namelen;
    if (n >= out_max) {
        return -1;
    }
    memcpy(out, ppath, plen);
    out[plen] = '/';
    memcpy(out + plen + 1, name, namelen);
    out[n] = '\0';
    return n;
} /* smb_child_path */

static void
chimera_smb_link_close_reply(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    struct chimera_vfs_request *request = arg;

    (void) conn;
    (void) hdr;
    (void) body;
    (void) body_len;
    (void) status;

    request->complete(request);
} /* chimera_smb_link_close_reply */

static void
chimera_smb_link_set_info_reply(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    struct chimera_vfs_request  *request = arg;
    struct chimera_smb_op_state *state   = request->plugin_data;

    (void) hdr;
    (void) body;
    (void) body_len;

    request->status = chimera_smb_status_to_errno(status);
    smb_send_close(conn, request, &state->file_id, chimera_smb_link_close_reply);
} /* chimera_smb_link_set_info_reply */

static void
chimera_smb_link_create_reply(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    struct chimera_vfs_request  *request = arg;
    struct chimera_smb_op_state *state   = request->plugin_data;
    struct smb_create_result     r;
    struct evpl_iovec            iov;
    struct evpl_iovec_cursor     cursor;
    struct smb2_header          *shdr;
    char                         dest[CHIMERA_SMB_PATH_MAX + 1];
    uint8_t                      name16[2 * CHIMERA_SMB_PATH_MAX];
    size_t                       name16_len;
    int                          destlen, buffer_offset;
    uint32_t                     buffer_length;

    (void) hdr;
    (void) body_len;

    if (status != SMB2_STATUS_SUCCESS) {
        request->status = chimera_smb_status_to_errno(status);
        request->complete(request);
        return;
    }

    smb_parse_create_reply(body, &r);
    state->file_id = r.file_id;

    destlen = smb_child_path(conn->server, request->link_at.dir_fh,
                             request->link_at.dir_fhlen, request->link_at.name,
                             request->link_at.namelen, dest, sizeof(dest));
    if (destlen < 0) {
        request->status = CHIMERA_VFS_ENOENT;
        smb_send_close(conn, request, &state->file_id,
                       chimera_smb_link_close_reply);
        return;
    }

    name16_len = smb_utf16le_encode(dest, destlen, name16);

    /* FileLinkInformation body: same layout as FileRenameInformation --
     * ReplaceIfExists(1) + Reserved(7) + RootDirectory(8) + FileNameLength(4)
     * = 20 fixed, then FileName(UTF-16LE). */
    buffer_length = (uint32_t) (20 + name16_len);

    chimera_smb_client_pdu_begin(conn, SMB2_SET_INFO, &iov, &cursor, &shdr);

    buffer_offset = sizeof(struct smb2_header) + 32;

    evpl_iovec_cursor_append_uint16(&cursor, SMB2_SET_INFO_REQUEST_SIZE);
    evpl_iovec_cursor_append_uint8(&cursor, SMB2_INFO_FILE);
    evpl_iovec_cursor_append_uint8(&cursor, SMB2_FILE_LINK_INFO);
    evpl_iovec_cursor_append_uint32(&cursor, buffer_length);
    evpl_iovec_cursor_append_uint16(&cursor, (uint16_t) buffer_offset);
    evpl_iovec_cursor_append_uint16(&cursor, 0);
    evpl_iovec_cursor_append_uint32(&cursor, 0);
    evpl_iovec_cursor_append_uint64(&cursor, state->file_id.pid);
    evpl_iovec_cursor_append_uint64(&cursor, state->file_id.vid);

    evpl_iovec_cursor_append_uint8(&cursor, request->link_at.replace ? 1 : 0);
    evpl_iovec_cursor_append_uint8(&cursor, 0);
    evpl_iovec_cursor_append_uint16(&cursor, 0);
    evpl_iovec_cursor_append_uint32(&cursor, 0);
    evpl_iovec_cursor_append_uint64(&cursor, 0);                       /* RootDirectory */
    evpl_iovec_cursor_append_uint32(&cursor, (uint32_t) name16_len);
    if (name16_len > 0) {
        evpl_iovec_cursor_append_blob(&cursor, name16, name16_len);
    }

    chimera_smb_client_pdu_finish(conn, &iov, &cursor, request,
                                  chimera_smb_link_set_info_reply, request);
} /* chimera_smb_link_create_reply */

void
chimera_smb_client_link_at(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    const char *src;
    int         srclen = 0, destlen;
    uint32_t    link_options;
    char        dest[CHIMERA_SMB_PATH_MAX + 1];

    /* Give the new name a re-openable child fh for the VFS name cache. */
    destlen = smb_child_path(conn->server, request->link_at.dir_fh,
                             request->link_at.dir_fhlen, request->link_at.name,
                             request->link_at.namelen, dest, sizeof(dest));
    if (destlen >= 0) {
        uint64_t id = chimera_smb_path_intern(conn->server, dest, destlen);

        request->link_at.r_attr.va_fh_len = chimera_smb_encode_open_fh(
            request->link_at.dir_fh, id, 0, request->link_at.r_attr.va_fh);
        request->link_at.r_attr.va_ino      = id | 1;
        request->link_at.r_attr.va_set_mask = CHIMERA_VFS_ATTR_FH |
            CHIMERA_VFS_ATTR_INUM;
    } else {
        request->link_at.r_attr.va_fh_len   = 0;
        request->link_at.r_attr.va_set_mask = 0;
    }

    /* The existing inode being linked: addressed by request->fh (its path id). */
    if (chimera_smb_fh_is_root(request->fh_len)) {
        src    = "";
        srclen = 0;
    } else {
        src = chimera_smb_path_resolve(conn->server,
                                       chimera_smb_fh_path_id(request->fh),
                                       &srclen);
    }
    if (!src) {
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    /* POSIX link() hard-links the symlink itself, never its target (only
     * linkat(AT_SYMLINK_FOLLOW) follows, and there the caller hands us the
     * already-resolved target).  A nofollow source fh names the link node, so
     * open the reparse point directly rather than following it -- otherwise a
     * link to a dangling symlink would spuriously fail ENOENT. */
    link_options = (!chimera_smb_fh_is_root(request->fh_len) &&
                    chimera_smb_fh_is_nofollow(request->fh))
                   ? SMB2_FILE_OPEN_REPARSE_POINT : 0;

    smb_send_create(conn, request, src, srclen,
                    SMB2_FILE_READ_ATTRIBUTES,
                    SMB2_FILE_SHARE_READ | SMB2_FILE_SHARE_WRITE | SMB2_FILE_SHARE_DELETE,
                    SMB2_FILE_OPEN, link_options,
                    chimera_smb_link_create_reply);
} /* chimera_smb_client_link_at */
