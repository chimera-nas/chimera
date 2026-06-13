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
#include "common/misc.h"

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

/* Send an SMB2 CREATE on `path`, optionally carrying `ctx` create contexts (a
 * pre-built, already-chained context blob) after the name. */
void
smb_send_create_ex(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request,
    const char                     *path,
    int                             path_len,
    uint32_t                        desired_access,
    uint32_t                        share_access,
    uint32_t                        disposition,
    uint32_t                        options,
    const uint8_t                  *ctx,
    uint32_t                        ctx_len,
    chimera_smb_client_reply_cb     reply_cb)
{
    struct evpl_iovec        iov;
    struct evpl_iovec_cursor cursor;
    struct smb2_header      *hdr;
    uint8_t                  name16[2 * CHIMERA_SMB_PATH_MAX];
    size_t                   name16_len;
    uint32_t                 name_end, ctx_off = 0, pad = 0;

    if (path_len > CHIMERA_SMB_PATH_MAX) {
        request->status = CHIMERA_VFS_ENAMETOOLONG;
        request->complete(request);
        return;
    }

    name16_len = smb_utf16le_encode(path, path_len, name16);

    /* Offsets are relative to the SMB2 header start: NameOffset = header(64) +
     * fixed body(56).  Create contexts (if any) follow the name, 8-byte aligned. */
    if (ctx_len > 0) {
        name_end = sizeof(struct smb2_header) + 56 + (uint32_t) name16_len;
        ctx_off  = (name_end + 7) & ~7u;
        pad      = ctx_off - name_end;
    }

    chimera_smb_client_pdu_begin(conn, SMB2_CREATE, &iov, &cursor, &hdr);

    evpl_iovec_cursor_append_uint16(&cursor, SMB2_CREATE_REQUEST_SIZE);
    evpl_iovec_cursor_append_uint8(&cursor, 0);                             /* SecurityFlags */
    evpl_iovec_cursor_append_uint8(&cursor, ctx_len ? SMB2_OPLOCK_LEVEL_LEASE : 0); /* RequestedOplockLevel */
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
    evpl_iovec_cursor_append_uint32(&cursor, ctx_off);                      /* CreateContextsOffset */
    evpl_iovec_cursor_append_uint32(&cursor, ctx_len);                      /* CreateContextsLength */
    if (name16_len > 0) {
        evpl_iovec_cursor_append_blob(&cursor, name16, name16_len);
    }

    if (ctx_len > 0) {
        /* Unaligned: append_blob would re-align the cursor and shift these past
         * the CreateContextsOffset we declared above (the name already left the
         * cursor at name_end). */
        static uint8_t zero[8] = { 0 };
        if (pad > 0) {
            evpl_iovec_cursor_append_blob_unaligned(&cursor, zero, pad);
        }
        evpl_iovec_cursor_append_blob_unaligned(&cursor, (uint8_t *) ctx, ctx_len);
    }

    chimera_smb_client_pdu_finish(conn, &iov, &cursor, request, reply_cb, request);
} /* smb_send_create_ex */

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
    smb_send_create_ex(conn, request, path, path_len, desired_access,
                       share_access, disposition, options, NULL, 0, reply_cb);
} /* smb_send_create */

/* Build an RqLs (lease request v1) create context into `buf` (>= 56 bytes);
 * returns its length.  Header(16) + name "RqLs"(4) + pad(4) + data(32). */
uint32_t
smb_build_lease_ctx(
    uint8_t       *buf,
    const uint8_t *lease_key,
    uint32_t       lease_state)
{
    memset(buf, 0, CHIMERA_SMB_LEASE_CTX_SIZE);

    /* Context header. */
    smb_wire_set_le16(buf + 4, 16);      /* NameOffset (from context start) */
    smb_wire_set_le16(buf + 6, 4);       /* NameLength */
    smb_wire_set_le16(buf + 10, 24);     /* DataOffset */
    smb_wire_set_le32(buf + 12, 32);     /* DataLength (RqLs v1) */
    memcpy(buf + 16, "RqLs", 4);

    /* RqLs v1 data: LeaseKey(16), LeaseState(4), LeaseFlags(4), Duration(8). */
    memcpy(buf + 24, lease_key, 16);
    smb_wire_set_le32(buf + 40, lease_state);

    return CHIMERA_SMB_LEASE_CTX_SIZE;
} /* smb_build_lease_ctx */

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

#define CHIMERA_SMB_STATFS_ATTRS \
        (CHIMERA_VFS_ATTR_SPACE_TOTAL | CHIMERA_VFS_ATTR_SPACE_FREE | \
         CHIMERA_VFS_ATTR_SPACE_AVAIL)

/* statfs is funnelled through getattr (the client opens a handle and requests
 * the SPACE_* attrs); answer it with QUERY_INFO FILESYSTEM /
 * FileFsFullSizeInformation instead of the per-file network-open info. */
static void
chimera_smb_statfs_reply(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    struct chimera_vfs_request *request = arg;
    uint16_t                    structsize, out_offset;
    uint32_t                    out_length, sectors_per_unit, bytes_per_sector;
    uint64_t                    total_units, caller_avail_units, actual_avail_units;
    uint64_t                    bytes_per_unit;
    int                         consumed;

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

    /* FileFsFullSizeInformation (MS-FSCC 2.5.4): TotalAllocationUnits(8),
     * CallerAvailableAllocationUnits(8), ActualAvailableAllocationUnits(8),
     * SectorsPerAllocationUnit(4), BytesPerSector(4). */
    evpl_iovec_cursor_get_uint64(body, &total_units);
    evpl_iovec_cursor_get_uint64(body, &caller_avail_units);
    evpl_iovec_cursor_get_uint64(body, &actual_avail_units);
    evpl_iovec_cursor_get_uint32(body, &sectors_per_unit);
    evpl_iovec_cursor_get_uint32(body, &bytes_per_sector);

    (void) structsize;
    (void) out_length;

    bytes_per_unit = (uint64_t) sectors_per_unit * bytes_per_sector;

    request->getattr.r_attr.va_fs_space_total = total_units * bytes_per_unit;
    request->getattr.r_attr.va_fs_space_avail = caller_avail_units * bytes_per_unit;
    request->getattr.r_attr.va_fs_space_free  = actual_avail_units * bytes_per_unit;
    request->getattr.r_attr.va_set_mask      |= CHIMERA_SMB_STATFS_ATTRS;

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_smb_statfs_reply */

static void
chimera_smb_client_statfs(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request,
    struct chimera_smb_client_open *open_state)
{
    struct evpl_iovec        iov;
    struct evpl_iovec_cursor cursor;
    struct smb2_header      *hdr;

    /* QUERY_INFO, FILESYSTEM / FileFsFullSizeInformation, on the open FileId. */
    chimera_smb_client_pdu_begin(conn, SMB2_QUERY_INFO, &iov, &cursor, &hdr);

    evpl_iovec_cursor_append_uint16(&cursor, SMB2_QUERY_INFO_REQUEST_SIZE);
    evpl_iovec_cursor_append_uint8(&cursor, SMB2_INFO_FILESYSTEM);
    evpl_iovec_cursor_append_uint8(&cursor, SMB2_FILE_FS_FULL_SIZE_INFO);
    evpl_iovec_cursor_append_uint32(&cursor, 32);            /* OutputBufferLength */
    evpl_iovec_cursor_append_uint16(&cursor, 0);             /* InputBufferOffset */
    evpl_iovec_cursor_append_uint16(&cursor, 0);             /* Reserved */
    evpl_iovec_cursor_append_uint32(&cursor, 0);             /* InputBufferLength */
    evpl_iovec_cursor_append_uint32(&cursor, 0);             /* AdditionalInformation */
    evpl_iovec_cursor_append_uint32(&cursor, 0);             /* Flags */
    evpl_iovec_cursor_append_uint64(&cursor, open_state->file_id.pid);
    evpl_iovec_cursor_append_uint64(&cursor, open_state->file_id.vid);

    chimera_smb_client_pdu_finish(conn, &iov, &cursor, request,
                                  chimera_smb_statfs_reply, request);
} /* chimera_smb_client_statfs */

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

    /* A statfs request (SPACE_* attrs) goes to the filesystem-info query. */
    if (request->getattr.r_attr.va_req_mask & CHIMERA_SMB_STATFS_ATTRS) {
        chimera_smb_client_statfs(conn, request, open_state);
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
    uint32_t       disposition;
    uint32_t       desired_access;
    uint32_t       options = SMB2_FILE_NON_DIRECTORY_FILE;
    uint8_t        lease_ctx[CHIMERA_SMB_LEASE_CTX_SIZE];
    uint8_t        lease_key[16];
    const uint8_t *ctx     = NULL;
    uint32_t       ctx_len = 0;

    if (request->open_at.flags & CHIMERA_VFS_OPEN_CREATE) {
        disposition = (request->open_at.flags & CHIMERA_VFS_OPEN_EXCLUSIVE)
                      ? SMB2_FILE_CREATE : SMB2_FILE_OPEN_IF;
    } else {
        disposition = SMB2_FILE_OPEN;
    }

    /*
     * NOFOLLOW: open the symlink (reparse point) itself rather than following
     * it to its target, so callers like readlink see the link node.
     */
    if (request->open_at.flags & CHIMERA_VFS_OPEN_NOFOLLOW) {
        options |= SMB2_FILE_OPEN_REPARSE_POINT;
    }

    desired_access = SMB2_FILE_READ_DATA | SMB2_FILE_WRITE_DATA |
        SMB2_FILE_READ_ATTRIBUTES | SMB2_FILE_WRITE_ATTRIBUTES | SMB2_DELETE;

    /* Request a read+handle-caching lease on file opens.  HANDLE caching is the
     * prerequisite the server requires before it will grant a durable handle
     * (the next increment), and the client acks any later break.  Directory
     * opens skip it. */
    if (!(request->open_at.flags & CHIMERA_VFS_OPEN_DIRECTORY)) {
        uint64_t *k = (uint64_t *) lease_key;

        k[0]    = chimera_rand64();
        k[1]    = chimera_rand64();
        ctx_len = smb_build_lease_ctx(lease_ctx, lease_key,
                                      SMB2_LEASE_READ_CACHING | SMB2_LEASE_HANDLE_CACHING);
        ctx = lease_ctx;
    }

    smb_send_create_ex(conn, request,
                       request->open_at.name, request->open_at.namelen,
                       desired_access,
                       SMB2_FILE_SHARE_READ | SMB2_FILE_SHARE_WRITE | SMB2_FILE_SHARE_DELETE,
                       disposition, options, ctx, ctx_len,
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

#define CHIMERA_SMB_SETATTR_TIMES \
        (CHIMERA_VFS_ATTR_ATIME | CHIMERA_VFS_ATTR_MTIME | \
         CHIMERA_VFS_ATTR_CTIME | CHIMERA_VFS_ATTR_BTIME)

/* Final SET_INFO reply (size-only, times-only, or the times leg of size+times). */
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

/* SET_INFO FileBasicInformation: set the requested timestamps (a zero FILETIME
 * means "leave unchanged"; FileAttributes 0 likewise). */
static void
chimera_smb_setattr_send_basic(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    struct chimera_smb_client_open *open_state = smb_handle_open_state(request->setattr.handle);
    struct chimera_vfs_attrs       *set_attr   = request->setattr.set_attr;
    struct evpl_iovec               iov;
    struct evpl_iovec_cursor        cursor;
    struct smb2_header             *hdr;
    uint64_t                        crttime, atime, mtime, ctime;

    crttime = (set_attr->va_set_mask & CHIMERA_VFS_ATTR_BTIME) ?
        smb_timespec_to_filetime(&set_attr->va_btime) : 0;
    atime = (set_attr->va_set_mask & CHIMERA_VFS_ATTR_ATIME) ?
        smb_timespec_to_filetime(&set_attr->va_atime) : 0;
    mtime = (set_attr->va_set_mask & CHIMERA_VFS_ATTR_MTIME) ?
        smb_timespec_to_filetime(&set_attr->va_mtime) : 0;
    ctime = (set_attr->va_set_mask & CHIMERA_VFS_ATTR_CTIME) ?
        smb_timespec_to_filetime(&set_attr->va_ctime) : 0;

    chimera_smb_client_pdu_begin(conn, SMB2_SET_INFO, &iov, &cursor, &hdr);

    evpl_iovec_cursor_append_uint16(&cursor, SMB2_SET_INFO_REQUEST_SIZE);
    evpl_iovec_cursor_append_uint8(&cursor, SMB2_INFO_FILE);
    evpl_iovec_cursor_append_uint8(&cursor, SMB2_FILE_BASIC_INFO);
    evpl_iovec_cursor_append_uint32(&cursor, 40);                            /* BufferLength */
    evpl_iovec_cursor_append_uint16(&cursor, sizeof(struct smb2_header) + 32); /* BufferOffset */
    evpl_iovec_cursor_append_uint16(&cursor, 0);                             /* Reserved */
    evpl_iovec_cursor_append_uint32(&cursor, 0);                             /* AdditionalInformation */
    evpl_iovec_cursor_append_uint64(&cursor, open_state->file_id.pid);
    evpl_iovec_cursor_append_uint64(&cursor, open_state->file_id.vid);
    /* FILE_BASIC_INFORMATION: Creation/LastAccess/LastWrite/ChangeTime + attrs. */
    evpl_iovec_cursor_append_uint64(&cursor, crttime);
    evpl_iovec_cursor_append_uint64(&cursor, atime);
    evpl_iovec_cursor_append_uint64(&cursor, mtime);
    evpl_iovec_cursor_append_uint64(&cursor, ctime);
    evpl_iovec_cursor_append_uint32(&cursor, 0);                             /* FileAttributes (no change) */
    evpl_iovec_cursor_append_uint32(&cursor, 0);                             /* Reserved */

    chimera_smb_client_pdu_finish(conn, &iov, &cursor, request,
                                  chimera_smb_setattr_reply, request);
} /* chimera_smb_setattr_send_basic */

/* After the size leg: chain the times leg if any timestamps were also set. */
static void
chimera_smb_setattr_size_reply(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    struct chimera_vfs_request *request = arg;

    (void) hdr;
    (void) body;
    (void) body_len;

    if (status != SMB2_STATUS_SUCCESS) {
        request->status = chimera_smb_status_to_errno(status);
        request->complete(request);
        return;
    }

    if (request->setattr.set_attr->va_set_mask & CHIMERA_SMB_SETATTR_TIMES) {
        chimera_smb_setattr_send_basic(conn, request);
        return;
    }

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_smb_setattr_size_reply */

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

    /* SMB can set size (FileEndOfFileInformation) and timestamps (FileBasic-
     * Information).  POSIX mode/owner have no SMB2 equivalent against this
     * server, so those bits are accepted but not applied. */
    if (set_attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE) {
        /* Set size first, then chain the times leg from its reply. */
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
                                      chimera_smb_setattr_size_reply, request);
        return;
    }

    if (set_attr->va_set_mask & CHIMERA_SMB_SETATTR_TIMES) {
        chimera_smb_setattr_send_basic(conn, request);
        return;
    }

    /* Nothing SMB can apply (e.g. mode/owner only) -- accept as a no-op. */
    request->status = CHIMERA_VFS_OK;
    request->complete(request);
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
