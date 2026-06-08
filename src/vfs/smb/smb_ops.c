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
 * File operations for the SMB2 client.  SMB has no persistent file handle; the
 * VFS file handle encodes the share-relative path inline, and every op that
 * needs server-side identity does an SMB2 CREATE (open) on that path.  Ops on an
 * already-open VFS handle reuse the SMB FileId stashed in vfs_private, which is
 * valid on any connection bound to the shared session.
 */

/* Transient per-op state kept in request->plugin_data across a CREATE -> op ->
 * CLOSE chain. */
struct chimera_smb_op_state {
    struct chimera_smb_client_file_id file_id;
    uint8_t                           child_fh[CHIMERA_VFS_FH_SIZE];
    int                               child_fh_len;
    enum chimera_vfs_error status;
};

/* ---- small helpers ----------------------------------------------------- */

static size_t
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

/* Build a share-relative child path (parent path + "\\" + name) and the child
 * file handle.  Returns the child path length, or -1 if it would not fit. */
static int
smb_build_child(
    const uint8_t *parent_fh,
    int            parent_fh_len,
    const char    *name,
    int            namelen,
    char          *out_path,
    int            out_path_max,
    uint8_t       *out_fh,
    int           *out_fh_len)
{
    int         parent_len;
    const char *parent_path = chimera_smb_fh_path(parent_fh, parent_fh_len, &parent_len);
    int         child_len;
    uint8_t     fragment[1 + CHIMERA_SMB_FH_PATH_MAX];

    if (parent_len > 0) {
        child_len = parent_len + 1 + namelen;
    } else {
        child_len = namelen;
    }

    if (child_len > out_path_max || child_len > CHIMERA_SMB_FH_PATH_MAX) {
        return -1;
    }

    if (parent_len > 0) {
        memcpy(out_path, parent_path, parent_len);
        out_path[parent_len] = '\\';
        memcpy(out_path + parent_len + 1, name, namelen);
    } else {
        memcpy(out_path, name, namelen);
    }

    fragment[0] = (uint8_t) chimera_smb_fh_server_index(parent_fh);
    memcpy(fragment + 1, out_path, child_len);

    *out_fh_len = chimera_vfs_encode_fh_parent(parent_fh, fragment, 1 + child_len, out_fh);

    return child_len;
} /* smb_build_child */

/* Result of parsing a CREATE response. */
struct smb_create_result {
    struct chimera_smb_client_file_id file_id;
    uint32_t                          create_action;
    uint64_t                          crttime, atime, mtime, ctime;
    uint64_t                          alloc_size, end_of_file;
    uint32_t                          file_attributes;
};

static void
smb_parse_create_reply(
    struct evpl_iovec_cursor *body,
    struct smb_create_result *r)
{
    uint16_t structsize;
    uint8_t  oplock, flags;
    uint32_t reserved;

    evpl_iovec_cursor_get_uint16(body, &structsize);
    evpl_iovec_cursor_get_uint8(body, &oplock);
    evpl_iovec_cursor_get_uint8(body, &flags);
    evpl_iovec_cursor_get_uint32(body, &r->create_action);
    evpl_iovec_cursor_get_uint64(body, &r->crttime);
    evpl_iovec_cursor_get_uint64(body, &r->atime);
    evpl_iovec_cursor_get_uint64(body, &r->mtime);
    evpl_iovec_cursor_get_uint64(body, &r->ctime);
    evpl_iovec_cursor_get_uint64(body, &r->alloc_size);
    evpl_iovec_cursor_get_uint64(body, &r->end_of_file);
    evpl_iovec_cursor_get_uint32(body, &r->file_attributes);
    evpl_iovec_cursor_get_uint32(body, &reserved);
    evpl_iovec_cursor_get_uint64(body, &r->file_id.pid);
    evpl_iovec_cursor_get_uint64(body, &r->file_id.vid);

    (void) structsize;
    (void) oplock;
    (void) flags;
} /* smb_parse_create_reply */

static void
smb_apply_create_attrs(
    const struct chimera_vfs_request *request,
    struct chimera_vfs_attrs         *attr,
    const struct smb_create_result   *r,
    const char                       *path,
    int                               path_len)
{
    smb_fill_attrs_from_network_open(attr, r->crttime, r->atime, r->mtime,
                                     r->ctime, r->alloc_size, r->end_of_file,
                                     r->file_attributes);

    /* SMB CREATE does not return an inode number; synthesize a stable, nonzero
     * one from the path so callers that key on st_ino behave. */
    attr->va_ino       = XXH3_64bits(path, path_len) | 1;
    attr->va_set_mask |= CHIMERA_VFS_ATTR_INUM;

    /* SMB2 exposes no POSIX owner; report the requesting credential as the
     * owner.  This is correct for the common case of a caller inspecting files
     * it created (the chimera SMB server creates them as the session user). */
    if (request->cred) {
        attr->va_uid       = request->cred->uid;
        attr->va_gid       = request->cred->gid;
        attr->va_set_mask |= CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_GID;
    }
} /* smb_apply_create_attrs */

/* Send an SMB2 CREATE on `path` (share-relative, may be empty for the root). */
static void
smb_send_create(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request,
    const char                     *path,
    int                             path_len,
    uint32_t                        desired_access,
    uint32_t                        file_attributes,
    uint32_t                        share_access,
    uint32_t                        disposition,
    uint32_t                        options,
    chimera_smb_client_reply_cb     reply_cb)
{
    struct evpl_iovec        iov;
    struct evpl_iovec_cursor cursor;
    struct smb2_header      *hdr;
    uint8_t                  name16[2 * CHIMERA_SMB_FH_PATH_MAX];
    size_t                   name16_len = smb_utf16le_encode(path, path_len, name16);

    chimera_smb_client_pdu_begin(conn, SMB2_CREATE, &iov, &cursor, &hdr);

    evpl_iovec_cursor_append_uint16(&cursor, SMB2_CREATE_REQUEST_SIZE);
    evpl_iovec_cursor_append_uint8(&cursor, 0);                             /* SecurityFlags */
    evpl_iovec_cursor_append_uint8(&cursor, 0);                             /* RequestedOplockLevel */
    evpl_iovec_cursor_append_uint32(&cursor, SMB2_IMPERSONATION_IMPERSONATION);
    evpl_iovec_cursor_append_uint64(&cursor, 0);                            /* SmbCreateFlags */
    evpl_iovec_cursor_append_uint64(&cursor, 0);                            /* Reserved */
    evpl_iovec_cursor_append_uint32(&cursor, desired_access);
    evpl_iovec_cursor_append_uint32(&cursor, file_attributes);
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

static void
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

/* ---- getattr (transient open) ------------------------------------------ */

static void
chimera_smb_getattr_close_reply(
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
} /* chimera_smb_getattr_close_reply */

static void
chimera_smb_getattr_create_reply(
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
    const char                  *path;
    int                          path_len;

    (void) hdr;
    (void) body_len;

    if (status != SMB2_STATUS_SUCCESS) {
        request->status = chimera_smb_status_to_errno(status);
        request->complete(request);
        return;
    }

    smb_parse_create_reply(body, &r);

    path = chimera_smb_fh_path(request->fh, request->fh_len, &path_len);
    smb_apply_create_attrs(request, &request->getattr.r_attr, &r, path, path_len);

    request->status = CHIMERA_VFS_OK;
    state->file_id  = r.file_id;

    smb_send_close(conn, request, &state->file_id, chimera_smb_getattr_close_reply);
} /* chimera_smb_getattr_create_reply */

void
chimera_smb_client_getattr(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    const char *path;
    int         path_len;

    path = chimera_smb_fh_path(request->fh, request->fh_len, &path_len);

    smb_send_create(conn, request, path, path_len,
                    SMB2_FILE_READ_ATTRIBUTES, 0,
                    SMB2_FILE_SHARE_READ | SMB2_FILE_SHARE_WRITE | SMB2_FILE_SHARE_DELETE,
                    SMB2_FILE_OPEN, 0,
                    chimera_smb_getattr_create_reply);
} /* chimera_smb_client_getattr */

/* ---- lookup_at (transient open) ---------------------------------------- */

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

    smb_apply_create_attrs(request, &request->lookup_at.r_attr, &r,
                           (const char *) state->child_fh + CHIMERA_SMB_FH_PATH_OFFSET,
                           state->child_fh_len - CHIMERA_SMB_FH_PATH_OFFSET);

    request->lookup_at.r_attr.va_set_mask |= CHIMERA_VFS_ATTR_FH;
    request->lookup_at.r_attr.va_fh_len    = state->child_fh_len;
    memcpy(request->lookup_at.r_attr.va_fh, state->child_fh, state->child_fh_len);

    request->status = CHIMERA_VFS_OK;
    state->file_id  = r.file_id;

    smb_send_close(conn, request, &state->file_id, chimera_smb_lookup_close_reply);
} /* chimera_smb_lookup_create_reply */

void
chimera_smb_client_lookup_at(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    struct chimera_smb_op_state *state = request->plugin_data;
    char                         path[CHIMERA_SMB_FH_PATH_MAX];
    int                          path_len;

    path_len = smb_build_child(request->fh, request->fh_len,
                               request->lookup_at.component,
                               request->lookup_at.component_len,
                               path, sizeof(path),
                               state->child_fh, &state->child_fh_len);
    if (path_len < 0) {
        request->status = CHIMERA_VFS_ENAMETOOLONG;
        request->complete(request);
        return;
    }

    smb_send_create(conn, request, path, path_len,
                    SMB2_FILE_READ_ATTRIBUTES, 0,
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
    struct chimera_smb_op_state    *state   = request->plugin_data;
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
    open_state->is_directory = (r.file_attributes & SMB2_FILE_ATTRIBUTE_DIRECTORY) != 0;

    smb_apply_create_attrs(request, &request->open_at.r_attr, &r,
                           (const char *) state->child_fh + CHIMERA_SMB_FH_PATH_OFFSET,
                           state->child_fh_len - CHIMERA_SMB_FH_PATH_OFFSET);
    request->open_at.r_attr.va_set_mask |= CHIMERA_VFS_ATTR_FH;
    request->open_at.r_attr.va_fh_len    = state->child_fh_len;
    memcpy(request->open_at.r_attr.va_fh, state->child_fh, state->child_fh_len);

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
    struct chimera_smb_op_state *state = request->plugin_data;
    char                         path[CHIMERA_SMB_FH_PATH_MAX];
    int                          path_len;
    uint32_t                     disposition;
    uint32_t                     desired_access;

    path_len = smb_build_child(request->fh, request->fh_len,
                               request->open_at.name, request->open_at.namelen,
                               path, sizeof(path),
                               state->child_fh, &state->child_fh_len);
    if (path_len < 0) {
        request->status = CHIMERA_VFS_ENAMETOOLONG;
        request->complete(request);
        return;
    }

    if (request->open_at.flags & CHIMERA_VFS_OPEN_CREATE) {
        disposition = (request->open_at.flags & CHIMERA_VFS_OPEN_EXCLUSIVE)
                      ? SMB2_FILE_CREATE : SMB2_FILE_OPEN_IF;
    } else {
        disposition = SMB2_FILE_OPEN;
    }

    desired_access = SMB2_FILE_READ_DATA | SMB2_FILE_WRITE_DATA |
        SMB2_FILE_READ_ATTRIBUTES | SMB2_FILE_WRITE_ATTRIBUTES | SMB2_DELETE;

    smb_send_create(conn, request, path, path_len,
                    desired_access, 0,
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
    open_state->is_directory = (r.file_attributes & SMB2_FILE_ATTRIBUTE_DIRECTORY) != 0;

    request->open_fh.r_vfs_private = (uint64_t) (uintptr_t) open_state;

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_smb_open_fh_reply */

void
chimera_smb_client_open_fh(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    const char *path;
    int         path_len;
    uint32_t    options;

    path = chimera_smb_fh_path(request->fh, request->fh_len, &path_len);

    options = (request->open_fh.flags & CHIMERA_VFS_OPEN_DIRECTORY)
              ? SMB2_FILE_DIRECTORY_FILE : 0;

    smb_send_create(conn, request, path, path_len,
                    SMB2_FILE_READ_DATA | SMB2_FILE_WRITE_DATA |
                    SMB2_FILE_READ_ATTRIBUTES | SMB2_FILE_WRITE_ATTRIBUTES | SMB2_DELETE,
                    0,
                    SMB2_FILE_SHARE_READ | SMB2_FILE_SHARE_WRITE | SMB2_FILE_SHARE_DELETE,
                    SMB2_FILE_OPEN, options,
                    chimera_smb_open_fh_reply);
} /* chimera_smb_client_open_fh */

/* ---- mkdir_at (transient open) ----------------------------------------- */

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

    smb_apply_create_attrs(request, &request->mkdir_at.r_attr, &r,
                           (const char *) state->child_fh + CHIMERA_SMB_FH_PATH_OFFSET,
                           state->child_fh_len - CHIMERA_SMB_FH_PATH_OFFSET);
    request->mkdir_at.r_attr.va_set_mask |= CHIMERA_VFS_ATTR_FH;
    request->mkdir_at.r_attr.va_fh_len    = state->child_fh_len;
    memcpy(request->mkdir_at.r_attr.va_fh, state->child_fh, state->child_fh_len);

    request->status = CHIMERA_VFS_OK;
    state->file_id  = r.file_id;

    smb_send_close(conn, request, &state->file_id, chimera_smb_mkdir_close_reply);
} /* chimera_smb_mkdir_create_reply */

void
chimera_smb_client_mkdir_at(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    struct chimera_smb_op_state *state = request->plugin_data;
    char                         path[CHIMERA_SMB_FH_PATH_MAX];
    int                          path_len;

    path_len = smb_build_child(request->fh, request->fh_len,
                               request->mkdir_at.name, request->mkdir_at.name_len,
                               path, sizeof(path),
                               state->child_fh, &state->child_fh_len);
    if (path_len < 0) {
        request->status = CHIMERA_VFS_ENAMETOOLONG;
        request->complete(request);
        return;
    }

    smb_send_create(conn, request, path, path_len,
                    SMB2_FILE_READ_ATTRIBUTES, 0,
                    SMB2_FILE_SHARE_READ | SMB2_FILE_SHARE_WRITE | SMB2_FILE_SHARE_DELETE,
                    SMB2_FILE_CREATE, SMB2_FILE_DIRECTORY_FILE,
                    chimera_smb_mkdir_create_reply);
} /* chimera_smb_client_mkdir_at */

/* ---- remove_at (delete-on-close) --------------------------------------- */

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
    struct chimera_smb_op_state *state = request->plugin_data;
    char                         path[CHIMERA_SMB_FH_PATH_MAX];
    int                          path_len;
    uint8_t                      child_fh[CHIMERA_VFS_FH_SIZE];
    int                          child_fh_len;

    path_len = smb_build_child(request->fh, request->fh_len,
                               request->remove_at.name, request->remove_at.namelen,
                               path, sizeof(path),
                               child_fh, &child_fh_len);
    if (path_len < 0) {
        request->status = CHIMERA_VFS_ENAMETOOLONG;
        request->complete(request);
        return;
    }

    (void) state;

    smb_send_create(conn, request, path, path_len,
                    SMB2_DELETE | SMB2_FILE_READ_ATTRIBUTES, 0,
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
    struct chimera_smb_client_open *open_state =
        (struct chimera_smb_client_open *) (request->setattr.handle
                                            ? request->setattr.handle->vfs_private : 0);
    struct chimera_vfs_attrs       *set_attr = request->setattr.set_attr;
    struct evpl_iovec               iov;
    struct evpl_iovec_cursor        cursor;
    struct smb2_header             *hdr;

    if (!open_state) {
        /* setattr without an open handle is not yet supported. */
        request->status = CHIMERA_VFS_ENOTSUP;
        request->complete(request);
        return;
    }

    if (!(set_attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE)) {
        /* Only size changes (ftruncate) are wired up for now; other attribute
         * changes succeed as a no-op. */
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
    struct chimera_smb_client_open *open_state =
        (struct chimera_smb_client_open *) (request->commit.handle
                                            ? request->commit.handle->vfs_private : 0);
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

/* ---- read / write / readdir (not yet implemented) ---------------------- */

void
chimera_smb_client_read(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    (void) conn;
    request->status = CHIMERA_VFS_ENOTSUP;
    request->complete(request);
} /* chimera_smb_client_read */

void
chimera_smb_client_write(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    (void) conn;
    request->status = CHIMERA_VFS_ENOTSUP;
    request->complete(request);
} /* chimera_smb_client_write */

void
chimera_smb_client_readdir(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    (void) conn;
    request->status = CHIMERA_VFS_ENOTSUP;
    request->complete(request);
} /* chimera_smb_client_readdir */
