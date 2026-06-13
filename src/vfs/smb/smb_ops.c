// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <xxhash.h>

#include "smb_internal.h"
#include "vfs/vfs_attrs.h"
#include "evpl/evpl.h"

/*
 * File operations for the SMB2 client -- a PATH-ONLY VFS backend.
 *
 * Metadata ops (lookup/mkdir/remove/open) address files by the full
 * mount-relative path carried in request->X.name and do a transient SMB2 CREATE
 * on that path.  An open file's VFS handle carries an opaque token fh
 * [mount_id][server_index][FileId]; ops on an open handle
 * (read/write/getattr/setattr/commit/close) recover the server + FileId from the
 * handle's vfs_private (struct chimera_smb_client_open), never from a path.
 */

/* Shared op helpers (smb_send_create/close, parse, attrs, op-state struct, the
 * network-open-info / create-result structs, and smb_handle_open_state) are
 * declared in smb_internal.h so smb_io.c and smb_namespace.c can reuse them. */

/* ---- small helpers ----------------------------------------------------- */

size_t
smb_utf16le_encode(
    const char *s,
    int         len,
    uint8_t    *out)
{
    int i;

    for (i = 0; i < len; i++) {
        /* SMB path separators are backslashes. */
        uint8_t c = (uint8_t) (s[i] == '/' ? '\\' : s[i]);
        out[i * 2]     = c;
        out[i * 2 + 1] = 0;
    }
    return (size_t) len * 2;
} /* smb_utf16le_encode */

void
smb_parse_open_info(
    struct evpl_iovec_cursor *body,
    struct smb_open_info     *r)
{
    uint32_t reserved;

    evpl_iovec_cursor_get_uint64(body, &r->crttime);
    evpl_iovec_cursor_get_uint64(body, &r->atime);
    evpl_iovec_cursor_get_uint64(body, &r->mtime);
    evpl_iovec_cursor_get_uint64(body, &r->ctime);
    evpl_iovec_cursor_get_uint64(body, &r->alloc_size);
    evpl_iovec_cursor_get_uint64(body, &r->end_of_file);
    evpl_iovec_cursor_get_uint32(body, &r->file_attributes);
    evpl_iovec_cursor_get_uint32(body, &reserved);
} /* smb_parse_open_info */

void
smb_parse_create_reply(
    struct evpl_iovec_cursor *body,
    struct smb_create_result *r)
{
    uint16_t structsize;
    uint8_t  oplock, flags;

    evpl_iovec_cursor_get_uint16(body, &structsize);
    evpl_iovec_cursor_get_uint8(body, &oplock);
    evpl_iovec_cursor_get_uint8(body, &flags);
    evpl_iovec_cursor_get_uint32(body, &r->create_action);
    smb_parse_open_info(body, &r->info);
    evpl_iovec_cursor_get_uint64(body, &r->file_id.pid);
    evpl_iovec_cursor_get_uint64(body, &r->file_id.vid);

    (void) structsize;
    (void) oplock;
    (void) flags;
} /* smb_parse_create_reply */

/* Map SMB attrs into a chimera_vfs_attrs.  SMB exposes no POSIX owner; report
 * the requesting credential as the owner (correct for a caller inspecting files
 * it created), and a caller-supplied stable inode number. */
void
smb_apply_attrs(
    const struct chimera_vfs_request *request,
    struct chimera_vfs_attrs         *attr,
    const struct smb_open_info       *info,
    uint64_t                          ino)
{
    smb_fill_attrs_from_network_open(attr, info->crttime, info->atime,
                                     info->mtime, info->ctime, info->alloc_size,
                                     info->end_of_file, info->file_attributes);

    attr->va_ino       = ino | 1;
    attr->va_set_mask |= CHIMERA_VFS_ATTR_INUM;

    if (request->cred) {
        attr->va_uid       = request->cred->uid;
        attr->va_gid       = request->cred->gid;
        attr->va_set_mask |= CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_GID;
    }
} /* smb_apply_attrs */

/* Send an SMB2 CREATE on `path` (full mount-relative path; "" for the root). */
void
smb_send_create(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request,
    const char                     *path,
    int                             path_len,
    uint32_t                        desired_access,
    uint32_t                        share_access,
    uint32_t                        disposition,
    uint32_t                        options,
    chimera_smb_client_reply_cb     reply_cb)
{
    struct evpl_iovec        iov;
    struct evpl_iovec_cursor cursor;
    struct smb2_header      *hdr;
    uint8_t                  name16[2 * CHIMERA_SMB_PATH_MAX];
    size_t                   name16_len;

    if (path_len > CHIMERA_SMB_PATH_MAX) {
        request->status = CHIMERA_VFS_ENAMETOOLONG;
        request->complete(request);
        return;
    }

    name16_len = smb_utf16le_encode(path, path_len, name16);

    chimera_smb_client_pdu_begin(conn, SMB2_CREATE, &iov, &cursor, &hdr);

    evpl_iovec_cursor_append_uint16(&cursor, SMB2_CREATE_REQUEST_SIZE);
    evpl_iovec_cursor_append_uint8(&cursor, 0);                             /* SecurityFlags */
    evpl_iovec_cursor_append_uint8(&cursor, 0);                             /* RequestedOplockLevel */
    evpl_iovec_cursor_append_uint32(&cursor, SMB2_IMPERSONATION_IMPERSONATION);
    evpl_iovec_cursor_append_uint64(&cursor, 0);                            /* SmbCreateFlags */
    evpl_iovec_cursor_append_uint64(&cursor, 0);                            /* Reserved */
    evpl_iovec_cursor_append_uint32(&cursor, desired_access);
    evpl_iovec_cursor_append_uint32(&cursor, 0);                            /* FileAttributes */
    evpl_iovec_cursor_append_uint32(&cursor, share_access);
    evpl_iovec_cursor_append_uint32(&cursor, disposition);
    evpl_iovec_cursor_append_uint32(&cursor, options);
    evpl_iovec_cursor_append_uint16(&cursor, sizeof(struct smb2_header) + 56); /* NameOffset */
    evpl_iovec_cursor_append_uint16(&cursor, (uint16_t) name16_len);        /* NameLength */
    evpl_iovec_cursor_append_uint32(&cursor, 0);                            /* CreateContextsOffset */
    evpl_iovec_cursor_append_uint32(&cursor, 0);                            /* CreateContextsLength */
    if (name16_len > 0) {
        evpl_iovec_cursor_append_blob(&cursor, name16, name16_len);
    }

    chimera_smb_client_pdu_finish(conn, &iov, &cursor, request, reply_cb, request);
} /* smb_send_create */

void
smb_send_close(
    struct chimera_smb_client_conn          *conn,
    struct chimera_vfs_request              *request,
    const struct chimera_smb_client_file_id *file_id,
    chimera_smb_client_reply_cb              reply_cb)
{
    struct evpl_iovec        iov;
    struct evpl_iovec_cursor cursor;
    struct smb2_header      *hdr;

    chimera_smb_client_pdu_begin(conn, SMB2_CLOSE, &iov, &cursor, &hdr);

    evpl_iovec_cursor_append_uint16(&cursor, SMB2_CLOSE_REQUEST_SIZE);
    evpl_iovec_cursor_append_uint16(&cursor, 0);             /* Flags */
    evpl_iovec_cursor_append_uint32(&cursor, 0);             /* Reserved */
    evpl_iovec_cursor_append_uint64(&cursor, file_id->pid);
    evpl_iovec_cursor_append_uint64(&cursor, file_id->vid);

    chimera_smb_client_pdu_finish(conn, &iov, &cursor, request, reply_cb, request);
} /* smb_send_close */

/* ---- CLOSE (VFS op) ---------------------------------------------------- */

static void
chimera_smb_close_reply(
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

    request->status = chimera_smb_status_to_errno(status);
    request->complete(request);
} /* chimera_smb_close_reply */

void
chimera_smb_client_close(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    struct chimera_smb_client_open   *open_state =
        (struct chimera_smb_client_open *) request->close.vfs_private;
    struct chimera_smb_client_file_id file_id = open_state->file_id;

    free(open_state);

    smb_send_close(conn, request, &file_id, chimera_smb_close_reply);
} /* chimera_smb_client_close */

/* ---- getattr (handle-based QUERY_INFO) --------------------------------- */

static void
chimera_smb_getattr_reply(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    struct chimera_vfs_request     *request    = arg;
    struct chimera_smb_client_open *open_state = smb_handle_open_state(request->getattr.handle);
    struct smb_open_info            info;
    uint16_t                        structsize, out_offset;
    uint32_t                        out_length;
    int                             consumed;

    (void) conn;
    (void) hdr;
    (void) body_len;

    if (status != SMB2_STATUS_SUCCESS) {
        request->status = chimera_smb_status_to_errno(status);
        request->complete(request);
        return;
    }

    evpl_iovec_cursor_get_uint16(body, &structsize);
    evpl_iovec_cursor_get_uint16(body, &out_offset);
    evpl_iovec_cursor_get_uint32(body, &out_length);

    consumed = evpl_iovec_cursor_consumed(body);
    if (out_offset > consumed) {
        evpl_iovec_cursor_skip(body, out_offset - consumed);
    }

    smb_parse_open_info(body, &info);

    smb_apply_attrs(request, &request->getattr.r_attr, &info,
                    open_state->file_id.pid);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_smb_getattr_reply */

void
chimera_smb_client_getattr(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    struct chimera_smb_client_open *open_state = smb_handle_open_state(request->getattr.handle);
    struct evpl_iovec               iov;
    struct evpl_iovec_cursor        cursor;
    struct smb2_header             *hdr;

    if (!open_state) {
        request->status = CHIMERA_VFS_EINVAL;
        request->complete(request);
        return;
    }

    /* SMB2 QUERY_INFO, FILE / FileNetworkOpenInformation, on the open FileId. */
    chimera_smb_client_pdu_begin(conn, SMB2_QUERY_INFO, &iov, &cursor, &hdr);

    evpl_iovec_cursor_append_uint16(&cursor, SMB2_QUERY_INFO_REQUEST_SIZE);
    evpl_iovec_cursor_append_uint8(&cursor, SMB2_INFO_FILE);
    evpl_iovec_cursor_append_uint8(&cursor, SMB2_FILE_NETWORK_OPEN_INFO);
    evpl_iovec_cursor_append_uint32(&cursor, SMB2_FILE_NETWORK_OPEN_INFO_SIZE); /* OutputBufferLength */
    evpl_iovec_cursor_append_uint16(&cursor, 0);             /* InputBufferOffset */
    evpl_iovec_cursor_append_uint16(&cursor, 0);             /* Reserved */
    evpl_iovec_cursor_append_uint32(&cursor, 0);             /* InputBufferLength */
    evpl_iovec_cursor_append_uint32(&cursor, 0);             /* AdditionalInformation */
    evpl_iovec_cursor_append_uint32(&cursor, 0);             /* Flags */
    evpl_iovec_cursor_append_uint64(&cursor, open_state->file_id.pid);
    evpl_iovec_cursor_append_uint64(&cursor, open_state->file_id.vid);

    chimera_smb_client_pdu_finish(conn, &iov, &cursor, request,
                                  chimera_smb_getattr_reply, request);
} /* chimera_smb_client_getattr */

/* ---- lookup_at (full path, transient open) ----------------------------- */

static void
chimera_smb_lookup_close_reply(
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
} /* chimera_smb_lookup_close_reply */

static void
chimera_smb_lookup_create_reply(
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

    (void) hdr;
    (void) body_len;

    if (status != SMB2_STATUS_SUCCESS) {
        request->status = chimera_smb_status_to_errno(status);
        request->complete(request);
        return;
    }

    smb_parse_create_reply(body, &r);

    /* Path-only: return attrs but NO child fh (va_fh stays unset). */
    smb_apply_attrs(request, &request->lookup_at.r_attr, &r.info,
                    XXH3_64bits(request->lookup_at.component,
                                request->lookup_at.component_len));

    request->status = CHIMERA_VFS_OK;
    state->file_id  = r.file_id;

    smb_send_close(conn, request, &state->file_id, chimera_smb_lookup_close_reply);
} /* chimera_smb_lookup_create_reply */

void
chimera_smb_client_lookup_at(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    smb_send_create(conn, request,
                    request->lookup_at.component, request->lookup_at.component_len,
                    SMB2_FILE_READ_ATTRIBUTES,
                    SMB2_FILE_SHARE_READ | SMB2_FILE_SHARE_WRITE | SMB2_FILE_SHARE_DELETE,
                    SMB2_FILE_OPEN, 0,
                    chimera_smb_lookup_create_reply);
} /* chimera_smb_client_lookup_at */

/* ---- open_at / open_fh (persistent open) ------------------------------- */

static void
chimera_smb_open_at_reply(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    struct chimera_vfs_request     *request = arg;
    struct chimera_smb_client_open *open_state;
    struct smb_create_result        r;

    (void) hdr;
    (void) body_len;

    if (status != SMB2_STATUS_SUCCESS) {
        request->status = chimera_smb_status_to_errno(status);
        request->complete(request);
        return;
    }

    smb_parse_create_reply(body, &r);

    open_state               = calloc(1, sizeof(*open_state));
    open_state->file_id      = r.file_id;
    open_state->server_index = (uint8_t) conn->server->index;
    open_state->is_directory = (r.info.file_attributes & SMB2_FILE_ATTRIBUTE_DIRECTORY) != 0;

    smb_apply_attrs(request, &request->open_at.r_attr, &r.info,
                    XXH3_64bits(request->open_at.name, request->open_at.namelen));

    /* The handle's identity is the opaque open token built from the FileId. */
    request->open_at.r_attr.va_fh_len = chimera_smb_encode_open_fh(
        request->fh, &r.file_id, request->open_at.r_attr.va_fh);
    request->open_at.r_attr.va_set_mask |= CHIMERA_VFS_ATTR_FH;

    request->open_at.r_vfs_private = (uint64_t) (uintptr_t) open_state;
    request->open_at.r_created     = (r.create_action == SMB2_CREATE_ACTION_CREATED);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_smb_open_at_reply */

void
chimera_smb_client_open_at(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    uint32_t disposition;
    uint32_t desired_access;

    if (request->open_at.flags & CHIMERA_VFS_OPEN_CREATE) {
        disposition = (request->open_at.flags & CHIMERA_VFS_OPEN_EXCLUSIVE)
                      ? SMB2_FILE_CREATE : SMB2_FILE_OPEN_IF;
    } else {
        disposition = SMB2_FILE_OPEN;
    }

    desired_access = SMB2_FILE_READ_DATA | SMB2_FILE_WRITE_DATA |
        SMB2_FILE_READ_ATTRIBUTES | SMB2_FILE_WRITE_ATTRIBUTES | SMB2_DELETE;

    smb_send_create(conn, request,
                    request->open_at.name, request->open_at.namelen,
                    desired_access,
                    SMB2_FILE_SHARE_READ | SMB2_FILE_SHARE_WRITE | SMB2_FILE_SHARE_DELETE,
                    disposition, SMB2_FILE_NON_DIRECTORY_FILE,
                    chimera_smb_open_at_reply);
} /* chimera_smb_client_open_at */

static void
chimera_smb_open_fh_reply(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    struct chimera_vfs_request     *request = arg;
    struct chimera_smb_client_open *open_state;
    struct smb_create_result        r;

    (void) hdr;
    (void) body_len;

    if (status != SMB2_STATUS_SUCCESS) {
        request->status = chimera_smb_status_to_errno(status);
        request->complete(request);
        return;
    }

    smb_parse_create_reply(body, &r);

    open_state               = calloc(1, sizeof(*open_state));
    open_state->file_id      = r.file_id;
    open_state->server_index = (uint8_t) conn->server->index;
    open_state->is_directory = (r.info.file_attributes & SMB2_FILE_ATTRIBUTE_DIRECTORY) != 0;

    request->open_fh.r_vfs_private = (uint64_t) (uintptr_t) open_state;

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_smb_open_fh_reply */

void
chimera_smb_client_open_fh(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    uint32_t options;

    /* Only the mount root is re-openable by fh; an opaque open token cannot be
     * re-derived to a path (path-only contract) -- reject with ESTALE. */
    if (!chimera_smb_fh_is_root(request->fh_len)) {
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    options = (request->open_fh.flags & CHIMERA_VFS_OPEN_DIRECTORY)
              ? SMB2_FILE_DIRECTORY_FILE : 0;

    /* CREATE the share root (empty path). */
    smb_send_create(conn, request, "", 0,
                    SMB2_FILE_READ_DATA | SMB2_FILE_READ_ATTRIBUTES |
                    SMB2_FILE_LIST_DIRECTORY,
                    SMB2_FILE_SHARE_READ | SMB2_FILE_SHARE_WRITE | SMB2_FILE_SHARE_DELETE,
                    SMB2_FILE_OPEN, options,
                    chimera_smb_open_fh_reply);
} /* chimera_smb_client_open_fh */

/* ---- mkdir_at (full path, transient open) ------------------------------ */

static void
chimera_smb_mkdir_close_reply(
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
} /* chimera_smb_mkdir_close_reply */

static void
chimera_smb_mkdir_create_reply(
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

    (void) hdr;
    (void) body_len;

    if (status != SMB2_STATUS_SUCCESS) {
        request->status = chimera_smb_status_to_errno(status);
        request->complete(request);
        return;
    }

    smb_parse_create_reply(body, &r);

    /* Transient open of a directory: return attrs, no persistent fh. */
    smb_apply_attrs(request, &request->mkdir_at.r_attr, &r.info,
                    XXH3_64bits(request->mkdir_at.name, request->mkdir_at.name_len));

    request->status = CHIMERA_VFS_OK;
    state->file_id  = r.file_id;

    smb_send_close(conn, request, &state->file_id, chimera_smb_mkdir_close_reply);
} /* chimera_smb_mkdir_create_reply */

void
chimera_smb_client_mkdir_at(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    smb_send_create(conn, request,
                    request->mkdir_at.name, request->mkdir_at.name_len,
                    SMB2_FILE_READ_ATTRIBUTES,
                    SMB2_FILE_SHARE_READ | SMB2_FILE_SHARE_WRITE | SMB2_FILE_SHARE_DELETE,
                    SMB2_FILE_CREATE, SMB2_FILE_DIRECTORY_FILE,
                    chimera_smb_mkdir_create_reply);
} /* chimera_smb_client_mkdir_at */

/* ---- remove_at (full path, delete-on-close) ---------------------------- */

static void
chimera_smb_remove_close_reply(
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

    request->status = chimera_smb_status_to_errno(status);
    request->complete(request);
} /* chimera_smb_remove_close_reply */

static void
chimera_smb_remove_create_reply(
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

    (void) hdr;
    (void) body_len;

    if (status != SMB2_STATUS_SUCCESS) {
        request->status = chimera_smb_status_to_errno(status);
        request->complete(request);
        return;
    }

    smb_parse_create_reply(body, &r);
    state->file_id = r.file_id;

    /* FILE_DELETE_ON_CLOSE was set on the open, so CLOSE removes the file. */
    smb_send_close(conn, request, &state->file_id, chimera_smb_remove_close_reply);
} /* chimera_smb_remove_create_reply */

void
chimera_smb_client_remove_at(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    smb_send_create(conn, request,
                    request->remove_at.name, request->remove_at.namelen,
                    SMB2_DELETE | SMB2_FILE_READ_ATTRIBUTES,
                    SMB2_FILE_SHARE_READ | SMB2_FILE_SHARE_WRITE | SMB2_FILE_SHARE_DELETE,
                    SMB2_FILE_OPEN, SMB2_FILE_DELETE_ON_CLOSE,
                    chimera_smb_remove_create_reply);
} /* chimera_smb_client_remove_at */

/* ---- setattr (SET_INFO on the open handle) ----------------------------- */

static void
chimera_smb_setattr_reply(
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

    request->status = chimera_smb_status_to_errno(status);
    request->complete(request);
} /* chimera_smb_setattr_reply */

void
chimera_smb_client_setattr(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    struct chimera_smb_client_open *open_state = smb_handle_open_state(request->setattr.handle);
    struct chimera_vfs_attrs       *set_attr   = request->setattr.set_attr;
    struct evpl_iovec               iov;
    struct evpl_iovec_cursor        cursor;
    struct smb2_header             *hdr;

    if (!open_state) {
        request->status = CHIMERA_VFS_ENOTSUP;
        request->complete(request);
        return;
    }

    if (!(set_attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE)) {
        /* Only size changes (ftruncate) are wired up; others succeed as no-ops. */
        request->status = CHIMERA_VFS_OK;
        request->complete(request);
        return;
    }

    /* SET_INFO, FileEndOfFileInformation (8-byte EOF). */
    chimera_smb_client_pdu_begin(conn, SMB2_SET_INFO, &iov, &cursor, &hdr);

    evpl_iovec_cursor_append_uint16(&cursor, SMB2_SET_INFO_REQUEST_SIZE);
    evpl_iovec_cursor_append_uint8(&cursor, SMB2_INFO_FILE);
    evpl_iovec_cursor_append_uint8(&cursor, SMB2_FILE_ENDOFFILE_INFO);
    evpl_iovec_cursor_append_uint32(&cursor, 8);                             /* BufferLength */
    evpl_iovec_cursor_append_uint16(&cursor, sizeof(struct smb2_header) + 32); /* BufferOffset */
    evpl_iovec_cursor_append_uint16(&cursor, 0);                             /* Reserved */
    evpl_iovec_cursor_append_uint32(&cursor, 0);                             /* AdditionalInformation */
    evpl_iovec_cursor_append_uint64(&cursor, open_state->file_id.pid);
    evpl_iovec_cursor_append_uint64(&cursor, open_state->file_id.vid);
    evpl_iovec_cursor_append_uint64(&cursor, set_attr->va_size);

    chimera_smb_client_pdu_finish(conn, &iov, &cursor, request,
                                  chimera_smb_setattr_reply, request);
} /* chimera_smb_client_setattr */

/* ---- commit (FLUSH) ---------------------------------------------------- */

static void
chimera_smb_commit_reply(
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

    request->status = chimera_smb_status_to_errno(status);
    request->complete(request);
} /* chimera_smb_commit_reply */

void
chimera_smb_client_commit(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    struct chimera_smb_client_open *open_state = smb_handle_open_state(request->commit.handle);
    struct evpl_iovec               iov;
    struct evpl_iovec_cursor        cursor;
    struct smb2_header             *hdr;

    if (!open_state) {
        request->status = CHIMERA_VFS_OK;
        request->complete(request);
        return;
    }

    chimera_smb_client_pdu_begin(conn, SMB2_FLUSH, &iov, &cursor, &hdr);

    evpl_iovec_cursor_append_uint16(&cursor, SMB2_FLUSH_REQUEST_SIZE);
    evpl_iovec_cursor_append_uint16(&cursor, 0);             /* Reserved1 */
    evpl_iovec_cursor_append_uint32(&cursor, 0);             /* Reserved2 */
    evpl_iovec_cursor_append_uint64(&cursor, open_state->file_id.pid);
    evpl_iovec_cursor_append_uint64(&cursor, open_state->file_id.vid);

    chimera_smb_client_pdu_finish(conn, &iov, &cursor, request,
                                  chimera_smb_commit_reply, request);
} /* chimera_smb_client_commit */

/* read / write / readdir live in smb_io.c; rename/symlink/mknod in
 * smb_namespace.c -- they reuse the shared helpers declared in smb_internal.h. */
