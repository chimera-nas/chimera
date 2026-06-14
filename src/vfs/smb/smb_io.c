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
 * SMB2 data-path operations for the path-only SMB client: READ, WRITE, and
 * directory enumeration (QUERY_DIRECTORY).  All three operate on an already-open
 * VFS handle whose vfs_private carries the SMB FileId (smb_handle_open_state).
 */

/* SMB2 READ request StructureSize (MS-SMB2 2.2.19): 48 fixed + 1 variable.  The
 * sibling WRITE/QUERY_DIRECTORY sizes are exposed by smb2.h, but the READ
 * request size is only defined as a reply size there, so define it locally. */
#ifndef SMB2_READ_REQUEST_SIZE
#define SMB2_READ_REQUEST_SIZE      49
#endif /* ifndef SMB2_READ_REQUEST_SIZE */

/* SMB2 READ/WRITE are chunked to the server-negotiated maxima (and bounded so a
 * WRITE always fits one send PDU).  Per-transfer progress lives in plugin_data. */
#define CHIMERA_SMB_WRITE_CHUNK_MAX 65280u      /* leaves room for the 64 KiB PDU */
#define CHIMERA_SMB_READ_CHUNK_MAX  (1u << 20)  /* bound a single READ response   */

struct smb_io_chunk {
    uint64_t base_offset;   /* file offset of transfer byte 0                 */
    uint32_t total;         /* total bytes to transfer                        */
    uint32_t done;          /* bytes transferred so far                       */
    uint32_t chunk_max;     /* per-request cap                                */
    uint32_t last_req;      /* bytes asked for in the in-flight request       */
};

static inline uint32_t
smb_write_chunk_cap(const struct chimera_smb_client_server *server)
{
    uint32_t cap = CHIMERA_SMB_WRITE_CHUNK_MAX;

    if (server->max_write && server->max_write < cap) {
        cap = server->max_write;
    }
    return cap;
} /* smb_write_chunk_cap */

static inline uint32_t
smb_read_chunk_cap(const struct chimera_smb_client_server *server)
{
    uint32_t cap = CHIMERA_SMB_READ_CHUNK_MAX;

    if (server->max_read && server->max_read < cap) {
        cap = server->max_read;
    }
    return cap;
} /* smb_read_chunk_cap */

/* ---- read (SMB2 READ) -------------------------------------------------- */

/* Scatter `length` bytes from the reply `body` cursor into the VFS-core read
 * iovecs, starting at byte offset `skip` into the iovec array. */
static void
smb_scatter_into_iovecs(
    struct evpl_iovec_cursor *body,
    struct evpl_iovec        *iov,
    int                       niov,
    uint32_t                  skip,
    uint32_t                  length)
{
    int      i;
    uint32_t left = length;

    for (i = 0; i < niov && left > 0; i++) {
        uint32_t avail = iov[i].length;
        uint32_t off;
        uint32_t chunk;

        if (skip >= avail) {
            skip -= avail;
            continue;
        }
        off  = skip;
        skip = 0;

        chunk = avail - off;
        if (chunk > left) {
            chunk = left;
        }

        evpl_iovec_cursor_get_blob(body, (char *) iov[i].data + off, chunk);
        left -= chunk;
    }
} /* smb_scatter_into_iovecs */

static void chimera_smb_read_send_chunk(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request);

static void
chimera_smb_read_finish(struct chimera_vfs_request *request)
{
    struct smb_io_chunk *st     = (struct smb_io_chunk *) request->plugin_data;
    uint32_t             prefix = request->read.aligned_prefix;
    uint32_t             avail  = (st->done > prefix) ? (st->done - prefix) : 0;
    uint32_t             want   = request->read.length;

    request->read.r_length = (avail < want) ? avail : want;
    request->read.r_eof    = (request->read.r_length < want) ? 1 : 0;
    request->status        = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_smb_read_finish */

static void
chimera_smb_read_reply(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    struct chimera_vfs_request *request = arg;
    struct smb_io_chunk        *st      = (struct smb_io_chunk *) request->plugin_data;
    uint16_t                    structsize;
    uint8_t                     data_offset, reserved;
    uint32_t                    data_length, data_remaining, reserved2;
    int                         consumed;

    (void) hdr;
    (void) body_len;

    /* STATUS_END_OF_FILE maps to OK but signals a zero-length short read. */
    request->status = chimera_smb_status_to_errno(status);

    if (request->status != CHIMERA_VFS_OK) {
        request->complete(request);
        return;
    }

    if (status == SMB2_STATUS_END_OF_FILE) {
        chimera_smb_read_finish(request);
        return;
    }

    /* MS-SMB2 2.2.20 READ response: StructureSize(2), DataOffset(1),
     * Reserved(1), DataLength(4), DataRemaining(4), Reserved2(4), Buffer. */
    evpl_iovec_cursor_get_uint16(body, &structsize);
    evpl_iovec_cursor_get_uint8(body, &data_offset);
    evpl_iovec_cursor_get_uint8(body, &reserved);
    evpl_iovec_cursor_get_uint32(body, &data_length);
    evpl_iovec_cursor_get_uint32(body, &data_remaining);
    evpl_iovec_cursor_get_uint32(body, &reserved2);

    (void) structsize;
    (void) reserved;
    (void) data_remaining;
    (void) reserved2;

    /* DataOffset is header-relative, as is the cursor's consumed count. */
    consumed = evpl_iovec_cursor_consumed(body);
    if ((int) data_offset > consumed) {
        evpl_iovec_cursor_skip(body, (int) data_offset - consumed);
    }

    if (data_length > 0) {
        smb_scatter_into_iovecs(body, request->read.iov,
                                request->read.buffers_provided,
                                st->done, data_length);
        st->done += data_length;
    }

    /* A short read (fewer bytes than asked) is EOF; otherwise keep going until
     * the whole aligned range is read. */
    if (data_length < st->last_req || st->done >= st->total) {
        chimera_smb_read_finish(request);
        return;
    }

    chimera_smb_read_send_chunk(conn, request);
} /* chimera_smb_read_reply */

static void
chimera_smb_read_send_chunk(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    struct chimera_smb_client_open *open_state = smb_handle_open_state(request->read.handle);
    struct smb_io_chunk            *st         = (struct smb_io_chunk *) request->plugin_data;
    struct evpl_iovec               iov;
    struct evpl_iovec_cursor        cursor;
    struct smb2_header             *hdr;
    uint32_t                        len = st->total - st->done;

    if (len > st->chunk_max) {
        len = st->chunk_max;
    }
    st->last_req = len;

    chimera_smb_client_pdu_begin(conn, SMB2_READ, &iov, &cursor, &hdr);

    evpl_iovec_cursor_append_uint16(&cursor, SMB2_READ_REQUEST_SIZE);
    evpl_iovec_cursor_append_uint8(&cursor, 0);              /* Padding */
    evpl_iovec_cursor_append_uint8(&cursor, 0);              /* Flags */
    evpl_iovec_cursor_append_uint32(&cursor, len);           /* Length */
    evpl_iovec_cursor_append_uint64(&cursor, st->base_offset + st->done);
    evpl_iovec_cursor_append_uint64(&cursor, open_state->file_id.pid);
    evpl_iovec_cursor_append_uint64(&cursor, open_state->file_id.vid);
    evpl_iovec_cursor_append_uint32(&cursor, 0);             /* MinimumCount */
    evpl_iovec_cursor_append_uint32(&cursor, 0);             /* Channel */
    evpl_iovec_cursor_append_uint32(&cursor, 0);             /* RemainingBytes */
    evpl_iovec_cursor_append_uint16(&cursor, 0);             /* ReadChannelInfoOffset */
    evpl_iovec_cursor_append_uint16(&cursor, 0);             /* ReadChannelInfoLength */
    evpl_iovec_cursor_append_uint8(&cursor, 0);              /* Buffer (1 byte min) */

    chimera_smb_client_pdu_finish(conn, &iov, &cursor, request,
                                  chimera_smb_read_reply, request);
} /* chimera_smb_read_send_chunk */

void
chimera_smb_client_read(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    struct chimera_smb_client_open *open_state = smb_handle_open_state(request->read.handle);
    struct smb_io_chunk            *st         = (struct smb_io_chunk *) request->plugin_data;
    uint64_t                        aligned_offset;

    if (!open_state) {
        request->status = CHIMERA_VFS_EINVAL;
        request->complete(request);
        return;
    }

    if (request->read.length == 0) {
        request->read.r_length = 0;
        request->read.r_eof    = 0;
        request->status        = CHIMERA_VFS_OK;
        request->complete(request);
        return;
    }

    /* The VFS core (no CAP_READ_PROVIDES_BUFFERS) pre-allocated read.iov padded
     * to a 4 KiB boundary on both sides; read the whole aligned range so the
     * data lands at iovec byte 0, where the core's finalize step expects it. */
    aligned_offset  = request->read.offset - request->read.aligned_prefix;
    st->base_offset = aligned_offset;
    st->total       = (uint32_t) (((request->read.offset + request->read.length + 4095ULL) & ~4095ULL)
                                  - aligned_offset);
    st->done      = 0;
    st->chunk_max = smb_read_chunk_cap(conn->server);

    chimera_smb_read_send_chunk(conn, request);
} /* chimera_smb_client_read */

/* ---- write (SMB2 WRITE) ------------------------------------------------ */

/* Append [skip, skip+length) bytes from the caller's write iovecs to `cursor`. */
static void
smb_gather_from_iovecs(
    struct evpl_iovec_cursor *cursor,
    const struct evpl_iovec  *iov,
    int                       niov,
    uint32_t                  skip,
    uint32_t                  length)
{
    int      i;
    uint32_t left = length;

    for (i = 0; i < niov && left > 0; i++) {
        uint32_t avail = iov[i].length;
        uint32_t off;
        uint32_t chunk;

        if (skip >= avail) {
            skip -= avail;
            continue;
        }
        off  = skip;
        skip = 0;

        chunk = avail - off;
        if (chunk > left) {
            chunk = left;
        }

        evpl_iovec_cursor_append_blob_unaligned(cursor,
                                                (char *) iov[i].data + off, chunk);
        left -= chunk;
    }
} /* smb_gather_from_iovecs */

static void chimera_smb_write_send_chunk(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request);

static void
chimera_smb_write_reply(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    struct chimera_vfs_request *request = arg;
    struct smb_io_chunk        *st      = (struct smb_io_chunk *) request->plugin_data;
    uint16_t                    structsize, channel_offset, channel_length;
    uint32_t                    count, remaining;

    (void) hdr;
    (void) body_len;

    request->status = chimera_smb_status_to_errno(status);

    if (request->status != CHIMERA_VFS_OK) {
        request->complete(request);
        return;
    }

    /* MS-SMB2 2.2.22 WRITE response: StructureSize(2), Reserved(2), Count(4),
     * Remaining(4), WriteChannelInfoOffset(2), WriteChannelInfoLength(2). */
    evpl_iovec_cursor_get_uint16(body, &structsize);
    evpl_iovec_cursor_get_uint16(body, &channel_offset); /* Reserved (2) */
    evpl_iovec_cursor_get_uint32(body, &count);
    evpl_iovec_cursor_get_uint32(body, &remaining);
    evpl_iovec_cursor_get_uint16(body, &channel_offset);
    evpl_iovec_cursor_get_uint16(body, &channel_length);

    (void) structsize;
    (void) remaining;
    (void) channel_offset;
    (void) channel_length;

    st->done += count;

    /* A short write (server accepted fewer bytes than asked) ends the transfer;
     * otherwise continue until the whole buffer is written. */
    if (count < st->last_req || st->done >= st->total) {
        request->write.r_length = st->done;
        request->write.r_sync   = request->write.sync ? 1 : 0;
        request->complete(request);
        return;
    }

    chimera_smb_write_send_chunk(conn, request);
} /* chimera_smb_write_reply */

static void
chimera_smb_write_send_chunk(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    struct chimera_smb_client_open *open_state = smb_handle_open_state(request->write.handle);
    struct smb_io_chunk            *st         = (struct smb_io_chunk *) request->plugin_data;
    struct evpl_iovec               iov;
    struct evpl_iovec_cursor        cursor;
    struct smb2_header             *hdr;
    uint32_t                        flags = request->write.sync ? SMB2_WRITEFLAG_WRITE_THROUGH : 0;
    uint32_t                        len   = st->total - st->done;

    if (len > st->chunk_max) {
        len = st->chunk_max;
    }
    st->last_req = len;

    /* MS-SMB2 2.2.21 WRITE request: fixed part is 48 bytes -> data begins at
     * header(64) + 48 = 112. */
    chimera_smb_client_pdu_begin(conn, SMB2_WRITE, &iov, &cursor, &hdr);

    evpl_iovec_cursor_append_uint16(&cursor, SMB2_WRITE_REQUEST_SIZE);
    evpl_iovec_cursor_append_uint16(&cursor, sizeof(struct smb2_header) + 48); /* DataOffset */
    evpl_iovec_cursor_append_uint32(&cursor, len);
    evpl_iovec_cursor_append_uint64(&cursor, request->write.offset + st->done);
    evpl_iovec_cursor_append_uint64(&cursor, open_state->file_id.pid);
    evpl_iovec_cursor_append_uint64(&cursor, open_state->file_id.vid);
    evpl_iovec_cursor_append_uint32(&cursor, 0);             /* Channel */
    evpl_iovec_cursor_append_uint32(&cursor, 0);             /* RemainingBytes */
    evpl_iovec_cursor_append_uint16(&cursor, 0);             /* WriteChannelInfoOffset */
    evpl_iovec_cursor_append_uint16(&cursor, 0);             /* WriteChannelInfoLength */
    evpl_iovec_cursor_append_uint32(&cursor, flags);

    smb_gather_from_iovecs(&cursor, request->write.iov, request->write.niov,
                           st->done, len);

    chimera_smb_client_pdu_finish(conn, &iov, &cursor, request,
                                  chimera_smb_write_reply, request);
} /* chimera_smb_write_send_chunk */

void
chimera_smb_client_write(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    struct chimera_smb_client_open *open_state = smb_handle_open_state(request->write.handle);
    struct smb_io_chunk            *st         = (struct smb_io_chunk *) request->plugin_data;

    if (!open_state) {
        request->status = CHIMERA_VFS_EINVAL;
        request->complete(request);
        return;
    }

    if (request->write.length == 0) {
        request->write.r_length = 0;
        request->write.r_sync   = request->write.sync ? 1 : 0;
        request->status         = CHIMERA_VFS_OK;
        request->complete(request);
        return;
    }

    st->base_offset = request->write.offset;
    st->total       = request->write.length;
    st->done        = 0;
    st->chunk_max   = smb_write_chunk_cap(conn->server);

    chimera_smb_write_send_chunk(conn, request);
} /* chimera_smb_client_write */

/* ---- readdir (SMB2 QUERY_DIRECTORY) ------------------------------------ */

/* QUERY_DIRECTORY uses FileFullDirectoryInformation (0x02): a clean layout with
 * no short-name region.  Per-entry fixed fields (after NextEntryOffset):
 *   FileIndex(4), CreationTime(8), LastAccessTime(8), LastWriteTime(8),
 *   ChangeTime(8), EndOfFile(8), AllocationSize(8), FileAttributes(4),
 *   FileNameLength(4), EaSize(4), then FileName (UTF-16LE). */
#define SMB_DIRINFO_FULL          SMB2_FILE_FULL_DIRECTORY_INFORMATION

/* Output buffer the client advertises per QUERY_DIRECTORY round. */
#define SMB_READDIR_OUTPUT_LENGTH 65536

/* Per-readdir state kept in request->plugin_data across the QUERY_DIRECTORY
 * loop (one VFS readdir call may span several QUERY_DIRECTORY round-trips). */
struct smb_readdir_ctx {
    struct chimera_smb_client_open *open_state;
    uint64_t                        next_cookie; /* monotonic emit index */
    int                             first;       /* send SMB2_RESTART_SCANS once */
    int                             full;        /* emit callback asked us to stop */
};

static void chimera_smb_readdir_query(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request);

/* Decode UTF-16LE (ASCII subset) into a UTF-8 buffer.  The client encodes paths
 * with smb_utf16le_encode (ASCII-only), so a symmetric ASCII decode is adequate
 * for the names this client round-trips.  TODO: full UTF-16 -> UTF-8. */
static int
smb_utf16le_decode_ascii(
    const uint8_t *in,
    int            in_bytes,
    char          *out,
    int            out_max)
{
    int i, n = in_bytes / 2;

    if (n >= out_max) {
        n = out_max - 1;
    }

    for (i = 0; i < n; i++) {
        out[i] = (char) in[i * 2];
    }
    out[n] = '\0';
    return n;
} /* smb_utf16le_decode_ascii */

static void
chimera_smb_readdir_reply(
    struct chimera_smb_client_conn *conn,
    uint32_t                        status,
    const struct smb2_header       *hdr,
    struct evpl_iovec_cursor       *body,
    int                             body_len,
    void                           *arg)
{
    struct chimera_vfs_request *request = arg;
    struct smb_readdir_ctx     *ctx     = request->plugin_data;
    uint16_t                    structsize, out_offset;
    uint32_t                    out_length;
    int                         consumed, base, off;
    uint8_t                    *buf;

    (void) hdr;
    (void) body_len;

    /* STATUS_NO_MORE_FILES terminates a normal enumeration. */
    if (status == SMB2_STATUS_NO_MORE_FILES) {
        request->readdir.r_eof    = 1;
        request->readdir.r_cookie = ctx->next_cookie;
        request->status           = CHIMERA_VFS_OK;
        request->complete(request);
        return;
    }

    request->status = chimera_smb_status_to_errno(status);
    if (request->status != CHIMERA_VFS_OK) {
        request->complete(request);
        return;
    }

    /* MS-SMB2 2.2.34 QUERY_DIRECTORY response: StructureSize(2),
     * OutputBufferOffset(2), OutputBufferLength(4), then the buffer.  Offset is
     * relative to the SMB2 header start (cursor consumed is header-relative). */
    evpl_iovec_cursor_get_uint16(body, &structsize);
    evpl_iovec_cursor_get_uint16(body, &out_offset);
    evpl_iovec_cursor_get_uint32(body, &out_length);
    (void) structsize;

    consumed = evpl_iovec_cursor_consumed(body);
    if ((int) out_offset > consumed) {
        evpl_iovec_cursor_skip(body, (int) out_offset - consumed);
    }

    /* Pull the whole entry buffer into a contiguous scratch so we can walk it by
     * NextEntryOffset.  out_length is bounded by SMB_READDIR_OUTPUT_LENGTH. */
    if (out_length == 0) {
        request->readdir.r_eof    = 1;
        request->readdir.r_cookie = ctx->next_cookie;
        request->status           = CHIMERA_VFS_OK;
        request->complete(request);
        return;
    }

    buf = malloc(out_length);
    if (evpl_iovec_cursor_get_blob(body, buf, out_length) < 0) {
        free(buf);
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }

    off  = 0;
    base = 0;
    while (off + 4 <= (int) out_length) {
        uint32_t                 next_offset, file_attributes, name_length;
        uint64_t                 crttime, atime, mtime, ctime, end_of_file, alloc_size;
        const uint8_t           *e = buf + off;
        char                     name[CHIMERA_SMB_PATH_MAX];
        int                      namelen;
        struct chimera_vfs_attrs attrs;
        int                      rc;

        next_offset = smb_wire_le32(e + 0);
        /* FileIndex at e+4 (unused) */
        crttime         = smb_wire_le64(e + 8);
        atime           = smb_wire_le64(e + 16);
        mtime           = smb_wire_le64(e + 24);
        ctime           = smb_wire_le64(e + 32);
        end_of_file     = smb_wire_le64(e + 40);
        alloc_size      = smb_wire_le64(e + 48);
        file_attributes = smb_wire_le32(e + 56);
        name_length     = smb_wire_le32(e + 60);
        /* EaSize at e+64 (4 bytes, unused); name begins at e+68. */

        if ((int) (68 + name_length) > (int) out_length - off) {
            break;
        }

        namelen = smb_utf16le_decode_ascii(e + 68, (int) name_length,
                                           name, sizeof(name));

        /* Skip "." and ".." -- the VFS readdir layer does not expect them (it
         * requested EMIT_DOT only at the server's discretion; vfs_nfs/readdir
         * consumers filter them, so drop them here). */
        if (!((namelen == 1 && name[0] == '.') ||
              (namelen == 2 && name[0] == '.' && name[1] == '.'))) {

            attrs.va_req_mask = request->readdir.attr_mask;
            attrs.va_set_mask = 0;

            smb_fill_attrs_from_network_open(&attrs, crttime, atime,
                                             mtime, ctime,
                                             alloc_size, end_of_file,
                                             file_attributes);

            attrs.va_ino       = XXH3_64bits(name, namelen) | 1;
            attrs.va_set_mask |= CHIMERA_VFS_ATTR_INUM;

            ctx->next_cookie++;

            rc = request->readdir.callback(attrs.va_ino,
                                           ctx->next_cookie,
                                           name, namelen,
                                           &attrs,
                                           request->proto_private_data);

            request->readdir.r_cookie = ctx->next_cookie;

            if (rc) {
                /* The emit buffer is full: stop, leaving more entries on the
                 * server.  Report not-EOF; the caller re-issues with the
                 * resume cookie (we keep the server-side scan position by NOT
                 * sending RESTART_SCANS on the follow-up). */
                ctx->full = 1;
                break;
            }
        }

        base++;

        if (next_offset == 0) {
            break;          /* last entry in this buffer */
        }
        off += (int) next_offset;
    }

    (void) base;

    free(buf);

    if (ctx->full) {
        request->readdir.r_eof = 0;
        request->status        = CHIMERA_VFS_OK;
        request->complete(request);
        return;
    }

    /* This buffer is drained; ask the server for the next batch (continuing the
     * open's scan position -- no RESTART_SCANS). */
    chimera_smb_readdir_query(conn, request);
} /* chimera_smb_readdir_reply */

static void
chimera_smb_readdir_query(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    struct smb_readdir_ctx  *ctx = request->plugin_data;
    struct evpl_iovec        iov;
    struct evpl_iovec_cursor cursor;
    struct smb2_header      *hdr;
    uint8_t                  flags;
    uint8_t                  pattern16[2];

    /* On the first round of a readdir starting at cookie 0, restart the scan to
     * the top of the directory; otherwise continue from the open's position. */
    flags      = ctx->first ? SMB2_RESTART_SCANS : 0;
    ctx->first = 0;

    /* MS-SMB2 2.2.33 QUERY_DIRECTORY request: StructureSize(2),
     * FileInformationClass(1), Flags(1), FileIndex(4), FileId(16),
     * FileNameOffset(2), FileNameLength(2), OutputBufferLength(4), then the
     * search pattern (UTF-16LE).  We always pass "*". */
    chimera_smb_client_pdu_begin(conn, SMB2_QUERY_DIRECTORY, &iov, &cursor, &hdr);

    pattern16[0] = (uint8_t) '*';
    pattern16[1] = 0;

    evpl_iovec_cursor_append_uint16(&cursor, SMB2_QUERY_DIRECTORY_REQUEST_SIZE);
    evpl_iovec_cursor_append_uint8(&cursor, SMB_DIRINFO_FULL);
    evpl_iovec_cursor_append_uint8(&cursor, flags);
    evpl_iovec_cursor_append_uint32(&cursor, 0);             /* FileIndex */
    evpl_iovec_cursor_append_uint64(&cursor, ctx->open_state->file_id.pid);
    evpl_iovec_cursor_append_uint64(&cursor, ctx->open_state->file_id.vid);
    evpl_iovec_cursor_append_uint16(&cursor, sizeof(struct smb2_header) + 32); /* FileNameOffset */
    evpl_iovec_cursor_append_uint16(&cursor, sizeof(pattern16)); /* FileNameLength */
    evpl_iovec_cursor_append_uint32(&cursor, SMB_READDIR_OUTPUT_LENGTH);
    evpl_iovec_cursor_append_blob(&cursor, pattern16, sizeof(pattern16));

    chimera_smb_client_pdu_finish(conn, &iov, &cursor, request,
                                  chimera_smb_readdir_reply, request);
} /* chimera_smb_readdir_query */

void
chimera_smb_client_readdir(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    struct chimera_smb_client_open *open_state = smb_handle_open_state(request->readdir.handle);
    struct smb_readdir_ctx         *ctx;

    if (!open_state) {
        request->status = CHIMERA_VFS_EINVAL;
        request->complete(request);
        return;
    }

    ctx              = request->plugin_data;
    ctx->open_state  = open_state;
    ctx->next_cookie = request->readdir.cookie;
    /* Restart the server-side scan only when the caller resumes from the start
     * (cookie 0).  A non-zero resume cookie continues the open's existing
     * position.  TODO: the SMB server-side scan position is per-open and is not
     * a stable cursor across separate VFS readdir calls; a precise mid-stream
     * resume would need to re-skip to request->readdir.cookie entries. */
    ctx->first = (request->readdir.cookie == 0);
    ctx->full  = 0;

    request->readdir.r_eof    = 0;
    request->readdir.r_cookie = request->readdir.cookie;

    chimera_smb_readdir_query(conn, request);
} /* chimera_smb_client_readdir */
