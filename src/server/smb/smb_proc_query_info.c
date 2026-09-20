// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "smb_internal.h"
#include "smb_procs.h"
#include "smb_string.h"
#include "smb_ea.h"
#include "common/misc.h"
#include "vfs/vfs.h"
#include "vfs/vfs_compound.h"

/*
 * Every VFS-touching QUERY_INFO level runs as a sequence that starts on the
 * object the FileId names: PUTHANDLE of the open's own handle, lent on the
 * flags it was really opened with, so the attributes come back through the
 * handle whose granted_access this query was already checked against.
 *
 * A STREAM open is the exception.  Its handle refers to the fork, and the
 * levels that enumerate (FileStreamInformation, FileFullEaInformation) are
 * about the BASE file, so those start with PUTFH of the base's file handle and
 * let the sequence open it -- which is exactly the PATH open those two used to
 * take by hand.
 */
static struct chimera_vfs_compound *
chimera_smb_query_base_sequence(struct chimera_smb_request *request)
{
    struct chimera_server_smb_thread *thread    = request->compound->thread;
    struct chimera_smb_open_file     *open_file = request->query_info.open_file;
    struct chimera_vfs_compound      *compound;

    compound = chimera_vfs_compound_alloc(
        thread->vfs_thread, &request->session_handle->session->cred);

    if (open_file->flags & CHIMERA_SMB_OPEN_FILE_FLAG_STREAM) {
        chimera_vfs_compound_add_putfh(compound, open_file->base_fh,
                                       open_file->base_fh_len);
    } else {
        chimera_vfs_compound_add_puthandle(compound, open_file->handle,
                                           open_file->open_flags);
    }

    return compound;
} /* chimera_smb_query_base_sequence */

static void
chimera_smb_query_info_getattr_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_smb_request *request = private_data;

    switch (request->query_info.info_type) {
        case SMB2_INFO_FILE:
            /* Marshal attributes based on the requested info class */
            switch (request->query_info.info_class) {
                case SMB2_FILE_BASIC_INFO:
                    chimera_smb_marshal_basic_info(attr, &request->query_info.r_attrs);
                    break;
                case SMB2_FILE_STANDARD_INFO:
                    chimera_smb_marshal_standard_info(attr, &request->query_info.r_attrs);
                    break;
                case SMB2_FILE_INTERNAL_INFO:
                    chimera_smb_marshal_internal_info(attr, &request->query_info.r_attrs);
                    break;
                case SMB2_FILE_EA_INFO:
                    chimera_smb_marshal_ea_info(attr, &request->query_info.r_attrs);
                    break;
                case SMB2_FILE_COMPRESSION_INFO:
                    chimera_smb_marshal_compression_info(attr, &request->query_info.r_attrs);
                    break;
                case SMB2_FILE_NETWORK_OPEN_INFO:
                    chimera_smb_marshal_network_open_info(attr, &request->query_info.r_attrs);
                    break;
                case SMB2_FILE_ATTRIBUTE_TAG_INFO:
                    chimera_smb_marshal_attribute_tag_info(attr, &request->query_info.r_attrs);
                    break;
                case SMB2_FILE_ALL_INFO:
                    /* For FileAllInformation, we need all attributes */
                    chimera_smb_marshal_attrs(attr, &request->query_info.r_attrs);
                    break;
                default:
                    chimera_smb_abort("Unsupported info class %d",
                                      request->query_info.info_class);
                    break;
            } /* switch */
            break;
        case SMB2_INFO_FILESYSTEM:
            switch (request->query_info.info_class) {
                case SMB2_FILE_FS_SIZE_INFO:
                case SMB2_FILE_FS_FULL_SIZE_INFO:
                    chimera_smb_marshal_fs_full_size_info(attr, &request->query_info.r_fs_attrs);
                    break;
            } /* switch */
    } /* switch */

    chimera_smb_open_file_release(request, request->query_info.open_file);

    if (unlikely(error_code)) {
        chimera_smb_complete_request(request, SMB2_STATUS_INTERNAL_ERROR);
    } else {
        chimera_smb_complete_request(request, SMB2_STATUS_SUCCESS);
    }
} /* chimera_smb_query_info_getattr_callback */

/*
 * PUTHANDLE, GETATTR.
 *
 * The attributes are copied out of the op before the sequence is freed -- a
 * freed sequence is recycled and reset.  A struct copy is enough here because
 * none of these levels asks for the ACL, the one attribute that is not a value
 * in the struct; SMB2_INFO_SECURITY, which does, is in smb_proc_security.c and
 * marshals inside the completion for that reason.
 */
static void
chimera_smb_query_info_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_smb_request           *request = private_data;
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

    chimera_smb_query_info_getattr_callback(status, &attr, request);
} /* chimera_smb_query_info_sequence_complete */

/*
 * MS-SMB2 3.3.5.20 OutputBufferLength validation, shared by the synthetic
 * named-pipe branch and the regular VFS path.
 *
 * min_length is the info level's fixed minimum; levels that did not set one
 * inherit their own output_length.  A buffer below that minimum is
 * INFO_LENGTH_MISMATCH; a buffer that merely cannot hold the whole reply gets
 * the reply truncated and BUFFER_OVERFLOW.
 */
static inline unsigned int
chimera_smb_query_info_check_length(struct chimera_smb_request *request)
{
    if (request->query_info.min_length == 0) {
        request->query_info.min_length = request->query_info.output_length;
    }

    if (request->query_info.max_response_size < request->query_info.min_length) {
        return SMB2_STATUS_INFO_LENGTH_MISMATCH;
    }

    if (request->query_info.max_response_size < request->query_info.output_length) {
        request->query_info.output_length = request->query_info.max_response_size;
        return SMB2_STATUS_BUFFER_OVERFLOW;
    }

    return SMB2_STATUS_SUCCESS;
} /* chimera_smb_query_info_check_length */

/* Walk the packed VFS list_streams records and either measure (cursor == NULL)
 * or emit MS-FSCC 2.4.43 FILE_STREAM_INFORMATION entries.  Each entry is
 *   NextEntryOffset(4) StreamNameLength(4) StreamSize(8) StreamAllocationSize(8)
 *   StreamName(UTF-16LE)
 * where the name is ":<stream>:$DATA" (the default fork is "::$DATA").  Returns
 * the total output byte length. */
static uint32_t
chimera_smb_emit_stream_info(
    struct chimera_smb_iconv_ctx *iconv,
    const uint8_t                *records,
    uint32_t                      records_len,
    uint32_t                      count,
    struct evpl_iovec_cursor     *cursor)
{
    uint32_t in      = 0;
    uint32_t out     = 0;
    uint32_t emitted = 0;

    while (in < records_len && emitted < count) {
        struct chimera_vfs_stream_entry entry;
        const char                     *sname;
        char                            disp[SMB_FILENAME_MAX + 8];
        uint16_t                        name16[SMB_FILENAME_MAX + 8];
        int                             name16_len;
        uint32_t                        disp_len, entry_size, aligned, next, p;

        memcpy(&entry, records + in, sizeof(entry));
        sname = (const char *) (records + in + sizeof(entry));

        /* ":" + stream-name + ":$DATA"  (default fork name_len == 0 -> "::$DATA") */
        disp[0] = ':';
        memcpy(disp + 1, sname, entry.name_len);
        memcpy(disp + 1 + entry.name_len, ":$DATA", 6);
        disp_len = 1 + entry.name_len + 6;

        name16_len = chimera_smb_utf8_to_utf16le(iconv, disp, disp_len,
                                                 name16, sizeof(name16));
        if (name16_len < 0) {
            name16_len = 0;
        }

        entry_size = 24 + (uint32_t) name16_len;
        aligned    = (entry_size + 7) & ~7u;

        emitted++;
        next = (emitted < count) ? aligned : 0;

        if (cursor) {
            evpl_iovec_cursor_append_uint32(cursor, next);
            evpl_iovec_cursor_append_uint32(cursor, (uint32_t) name16_len);
            evpl_iovec_cursor_append_uint64(cursor, entry.size);
            evpl_iovec_cursor_append_uint64(cursor, entry.alloc);
            if (name16_len > 0) {
                evpl_iovec_cursor_append_blob_unaligned(cursor, name16, name16_len);
            }
            /* Only an entry that is followed by another carries the pad that
             * 8-byte-aligns its successor. */
            if (next) {
                for (p = entry_size; p < aligned; p++) {
                    evpl_iovec_cursor_append_uint8(cursor, 0);
                }
            }
        }

        /* The last entry ends after its name: padding it would overstate the
         * reply against clients that size their buffer to the exact Windows
         * length (MS-FSA BVT_AlternateDataStream_ListStreams_File offers 150
         * bytes for a default fork plus two 8-character streams, which is what
         * Windows and Samba's marshall_stream_info return to the byte). */
        out += next ? aligned : entry_size;
        in  += (sizeof(entry) + entry.name_len + entry.fh_len + 7) & ~7u;
    }

    return out;
} /* chimera_smb_emit_stream_info */

/* Finish a FileStreamInformation query from `count` packed stream records held
 * in request->query_info.stream_records: measure the wire form, apply the
 * MS-SMB2 3.3.5.20.1 buffer-length rules, and complete.  The records stay put
 * for the reply builder to emit. */
static void
chimera_smb_query_stream_info_complete(
    struct chimera_smb_request *request,
    uint32_t                    records_len,
    uint32_t                    count)
{
    uint32_t status = SMB2_STATUS_SUCCESS;

    request->query_info.stream_record_len   = records_len;
    request->query_info.stream_record_count = count;

    request->query_info.output_length = chimera_smb_emit_stream_info(
        &request->compound->thread->iconv_ctx,
        request->query_info.stream_records,
        records_len,
        count,
        NULL);

    if (request->query_info.max_response_size < SMB2_FILE_STREAM_INFO_FIXED_SIZE) {
        status = SMB2_STATUS_INFO_LENGTH_MISMATCH;
    } else if (request->query_info.max_response_size < request->query_info.output_length) {
        status                            = SMB2_STATUS_BUFFER_OVERFLOW;
        request->query_info.output_length = request->query_info.max_response_size;
    }

    chimera_smb_open_file_release(request, request->query_info.open_file);
    chimera_smb_complete_request(request, status);
} /* chimera_smb_query_stream_info_complete */

/* A backend without named-stream support still has the file's unnamed data
 * fork, and MS-FSCC 2.4.43 counts that fork as a stream: Windows on NTFS and
 * Samba on a plain filesystem (vfswrap_fstreaminfo, the default VFS op used
 * when no vfs_streams_* module is loaded) both answer FileStreamInformation
 * with a single synthesized "::$DATA" entry describing the file itself rather
 * than failing the level.  Synthesize the same entry from the file's size so a
 * client probing for streams gets the conformant answer instead of an error.
 * A directory has no data fork and yields an empty list, as on both. */
static void
chimera_smb_query_stream_info_default_callback(
    enum chimera_vfs_error          error_code,
    const struct chimera_vfs_attrs *attr,
    void                           *private_data)
{
    struct chimera_smb_request     *request = private_data;
    struct chimera_vfs_stream_entry entry;
    uint32_t                        records_len = 0;
    uint32_t                        count       = 0;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_smb_open_file_release(request, request->query_info.open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_INTERNAL_ERROR);
        return;
    }

    if ((attr->va_mode & S_IFMT) != S_IFDIR) {
        /* A zero-length name is what makes the emitter spell the default fork
         * ":" + "" + ":$DATA". */
        entry.size     = attr->va_size;
        entry.alloc    = attr->va_space_used;
        entry.name_len = 0;

        memcpy(request->query_info.stream_records, &entry, sizeof(entry));

        records_len = sizeof(entry);
        count       = 1;
    }

    chimera_smb_query_stream_info_complete(request, records_len, count);
} /* chimera_smb_query_stream_info_default_callback */

/* PUTHANDLE / PUTFH(base), GETATTR -- the synthesized-fork arm above. */
static void
chimera_smb_query_stream_info_default_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_smb_request           *request = private_data;
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

    chimera_smb_query_stream_info_default_callback(status, &attr, request);
} /* chimera_smb_query_stream_info_default_complete */

/*
 * PUTHANDLE / PUTFH(base), LIST_STREAMS.
 *
 * The page the op reports lives in the compound's buffer, so it is copied into
 * the request's own record store before the free: the reply builder emits it
 * long after this callback has returned.
 */
static void
chimera_smb_query_stream_info_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_smb_request           *request = private_data;
    const struct chimera_vfs_compound_op *op;
    enum chimera_vfs_error                status;
    uint32_t                              records_len = 0, count = 0;

    status = chimera_vfs_compound_status(compound);

    if (status == CHIMERA_VFS_OK) {
        op = chimera_vfs_compound_op(compound,
                                     chimera_vfs_compound_num_ops(compound) - 1);

        records_len = op->buffer_len;
        count       = op->buffer_count;

        if (records_len) {
            memcpy(request->query_info.stream_records, op->buffer, records_len);
        }
    }

    chimera_vfs_compound_free(compound);
    request->vfs_compound = NULL;

    if (status != CHIMERA_VFS_OK) {
        chimera_smb_open_file_release(request, request->query_info.open_file);
        chimera_smb_complete_request(request,
                                     status == CHIMERA_VFS_ERANGE ?
                                     SMB2_STATUS_INFO_LENGTH_MISMATCH :
                                     SMB2_STATUS_INTERNAL_ERROR);
        return;
    }

    chimera_smb_query_stream_info_complete(request, records_len, count);
} /* chimera_smb_query_stream_info_sequence_complete */

static void
chimera_smb_query_stream_info(struct chimera_smb_request *request)
{
    struct chimera_server_smb_thread *thread    = request->compound->thread;
    struct chimera_smb_open_file     *open_file = request->query_info.open_file;

    /* Gate: named streams must be enabled and the backend must support them.
     * Without them the object still has its default data fork, so report that
     * one synthesized "::$DATA" stream instead of failing the level.  The
     * synthesis describes the OPEN's own object, not the base, so this arm
     * always addresses the open's handle. */
    if (!chimera_smb_named_streams_enabled(open_file->handle->vfs_module->capabilities,
                                           thread->shared->config.named_streams)) {
        request->vfs_compound = chimera_vfs_compound_alloc(
            thread->vfs_thread, &request->session_handle->session->cred);

        chimera_vfs_compound_add_puthandle(request->vfs_compound,
                                           open_file->handle,
                                           open_file->open_flags);

        chimera_vfs_compound_add_getattr(request->vfs_compound,
                                         CHIMERA_VFS_ATTR_MASK_STAT);

        chimera_vfs_compound_submit(
            request->vfs_compound,
            chimera_smb_query_stream_info_default_complete, request);
        return;
    }

    /* Enumerate the streams of the BASE file -- which is what the sequence
     * starts on, handle or file handle (chimera_smb_query_base_sequence). */
    request->vfs_compound = chimera_smb_query_base_sequence(request);

    chimera_vfs_compound_add_list_streams(
        request->vfs_compound,
        0,
        sizeof(request->query_info.stream_records),
        0 /* SMB FILE_STREAM_INFORMATION does not need per-stream handles */);

    chimera_vfs_compound_submit(
        request->vfs_compound,
        chimera_smb_query_stream_info_sequence_complete, request);
} /* chimera_smb_query_stream_info */

/* ---- FILE_FULL_EA_INFORMATION query: enumerate the object's user.* xattrs and
 * build the wire EA list, fetching the values with GETXATTR fan-outs. ---- */

#define CHIMERA_SMB_EA_QUERY_CAP_MAX (1u << 20)

/* How many names one GETXATTR fan-out sequence carries.
 *
 * The name list is unbounded, so the fan-out is CONSECUTIVE SEQUENCES whatever
 * this number is (the "one request, one sequence" corollary: a request whose
 * second half depends on an unbounded answer from its first half runs as
 * several).  All this decides is how many of them there are.
 *
 * It is small rather than the executor's 31 because each op's value buffer has
 * to be sized before any of them runs, and a value's real length is not known
 * until it has been fetched -- so every op in a batch is sized against the
 * whole remaining reply, and a full batch would allocate a reply's worth of
 * scratch per name to fill one of them.
 */
#define CHIMERA_SMB_EA_QUERY_BATCH   8

static void chimera_smb_query_ea_next(
    struct chimera_smb_request *request);

static void
chimera_smb_query_ea_finish(
    struct chimera_smb_request *request,
    uint32_t                    status)
{
    /* On success the reply emitter owns ea_out (frees it after emitting); on
     * error free it here. */
    if (status != SMB2_STATUS_SUCCESS && request->query_info.ea_out) {
        free(request->query_info.ea_out);
        request->query_info.ea_out = NULL;
    }
    chimera_smb_open_file_release(request, request->query_info.open_file);
    chimera_smb_complete_request(request, status);
} /* chimera_smb_query_ea_finish */

/*
 * Append one wire FILE_FULL_EA_INFORMATION entry (MS-FSCC 2.4.15) for the
 * fully-qualified xattr name `fullname` and the value a GETXATTR answered with.
 * Returns -1 when the entry does not fit the client's buffer, which is the
 * level's BUFFER_OVERFLOW.
 */
static int
chimera_smb_query_ea_append(
    struct chimera_smb_request *request,
    const char                 *fullname,
    uint32_t                    flen,
    const void                 *value,
    uint32_t                    value_len)
{
    uint8_t    *out      = request->query_info.ea_out;
    uint32_t    start    = request->query_info.ea_out_len;
    uint32_t    cnamelen = flen - CHIMERA_VFS_XATTR_USER_PREFIX_LEN;
    const char *cname    = fullname + CHIMERA_VFS_XATTR_USER_PREFIX_LEN;
    uint32_t    entry_size, aligned, next, pz;

    if (value_len > 65535) {
        value_len = 65535;            /* EaValueLength is a uint16 */
    }

    entry_size = 8 + cnamelen + 1 + value_len;
    aligned    = (entry_size + 3) & ~3u;

    if (start + aligned > request->query_info.ea_out_cap) {
        return -1;
    }

    next = aligned;                   /* the last entry is patched to 0 later */

    memcpy(out + start, &next, 4);
    out[start + 4] = 0;               /* Flags (we do not surface NEED_EA) */
    out[start + 5] = (uint8_t) cnamelen;
    out[start + 6] = (uint8_t) (value_len & 0xff);
    out[start + 7] = (uint8_t) (value_len >> 8);
    memcpy(out + start + 8, cname, cnamelen);
    out[start + 8 + cnamelen] = '\0';
    if (value_len) {
        memcpy(out + start + 8 + cnamelen + 1, value, value_len);
    }
    for (pz = entry_size; pz < aligned; pz++) {
        out[start + pz] = 0;
    }

    request->query_info.ea_last_off = start;
    request->query_info.ea_out_len  = start + aligned;

    return 0;
} /* chimera_smb_query_ea_append */

/* Step the name cursor past `count` user.* names -- the ones a sequence's
 * GETXATTRs consumed, whether they answered or not. */
static void
chimera_smb_query_ea_advance(
    struct chimera_smb_request *request,
    uint32_t                    count)
{
    const char *names_end =
        (const char *) request->query_info.stream_records +
        request->query_info.stream_record_len;
    const char *cur = request->query_info.ea_cursor;
    uint32_t    flen;

    while (cur < names_end && count) {
        flen = strlen(cur);
        if (chimera_vfs_xattr_is_user(cur, flen)) {
            count--;
        }
        cur += flen + 1;
    }

    request->query_info.ea_cursor = cur;
} /* chimera_smb_query_ea_advance */

/*
 * PUTHANDLE / PUTFH(base), GETXATTR x N.
 *
 * A sequence stops at its first failure, so everything before the failing op
 * answered and its values are taken; the failing op's own name is consumed
 * either way, because an EA removed between the LISTXATTRS and its GETXATTR is
 * not an error -- it is a name to skip, and the enumeration resumes after it
 * with another sequence.
 */
static void
chimera_smb_query_ea_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_smb_request           *request = private_data;
    const struct chimera_vfs_compound_op *op;
    enum chimera_vfs_error                status;
    uint32_t                              completed, consumed, last, i;
    int                                   overflow = 0;

    status    = chimera_vfs_compound_status(compound);
    completed = chimera_vfs_compound_num_completed(compound);

    /* ops[0] addressed the base; ops[1..] are the values.  On a failure the
     * last op that ran is the one that failed, and it has no value to take. */
    last = (status == CHIMERA_VFS_OK) ? completed :
        (completed ? completed - 1 : 0);

    for (i = 1; i < last; i++) {
        op = chimera_vfs_compound_op(compound, i);

        if (chimera_smb_query_ea_append(request, op->name, op->name_len,
                                        op->buffer, op->buffer_len) < 0) {
            overflow = 1;
            break;
        }
    }

    consumed = (completed > 1) ? completed - 1 : 0;

    chimera_vfs_compound_free(compound);
    request->vfs_compound = NULL;

    if (overflow) {
        chimera_smb_query_ea_finish(request, SMB2_STATUS_BUFFER_OVERFLOW);
        return;
    }

    if (status != CHIMERA_VFS_OK) {
        if (consumed == 0) {
            /* The base itself could not be addressed. */
            chimera_smb_query_ea_finish(request, SMB2_STATUS_INTERNAL_ERROR);
            return;
        }

        if (status != CHIMERA_VFS_ENODATA) {
            chimera_smb_query_ea_finish(request,
                                        status == CHIMERA_VFS_ERANGE ?
                                        SMB2_STATUS_BUFFER_OVERFLOW :
                                        chimera_smb_ea_status(status));
            return;
        }
    }

    chimera_smb_query_ea_advance(request, consumed);
    chimera_smb_query_ea_next(request);
} /* chimera_smb_query_ea_sequence_complete */

static void
chimera_smb_query_ea_next(struct chimera_smb_request *request)
{
    const char *names_end =
        (const char *) request->query_info.stream_records +
        request->query_info.stream_record_len;
    const char *cur;
    uint32_t    flen, vmax, n = 0;

    /* Every op in the batch is sized against the whole remaining reply: what
     * each value will cost is not known until it arrives, and the entries in
     * front of it in this batch have not arrived either. */
    vmax = request->query_info.ea_out_cap;
    if (vmax > 65535) {
        vmax = 65535;
    }

    request->vfs_compound = chimera_smb_query_base_sequence(request);

    cur = request->query_info.ea_cursor;

    while (cur < names_end && n < CHIMERA_SMB_EA_QUERY_BATCH) {
        flen = strlen(cur);
        if (chimera_vfs_xattr_is_user(cur, flen)) {
            chimera_vfs_compound_add_getxattr(request->vfs_compound, cur,
                                              (int) flen, vmax);
            n++;
        }
        cur += flen + 1;
    }

    if (n == 0) {
        chimera_vfs_compound_free(request->vfs_compound);
        request->vfs_compound = NULL;

        /* Done: terminate the chain (NextEntryOffset 0 on the last entry). */
        if (request->query_info.ea_out_len > 0) {
            uint32_t zero = 0;
            memcpy(request->query_info.ea_out + request->query_info.ea_last_off,
                   &zero, 4);
        }
        request->query_info.output_length = request->query_info.ea_out_len;
        chimera_smb_query_ea_finish(request, SMB2_STATUS_SUCCESS);
        return;
    }

    chimera_vfs_compound_submit(request->vfs_compound,
                                chimera_smb_query_ea_sequence_complete,
                                request);
} /* chimera_smb_query_ea_next */

/* PUTHANDLE / PUTFH(base), LISTXATTRS.  The names are copied out of the
 * compound's buffer because the fan-out sequences that follow walk them long
 * after this one has been freed. */
static void
chimera_smb_query_ea_list_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_smb_request           *request = private_data;
    const struct chimera_vfs_compound_op *op;
    enum chimera_vfs_error                status;
    uint32_t                              names_len = 0;

    status = chimera_vfs_compound_status(compound);

    if (status == CHIMERA_VFS_OK) {
        op = chimera_vfs_compound_op(compound,
                                     chimera_vfs_compound_num_ops(compound) - 1);

        names_len = op->buffer_len;

        if (names_len) {
            memcpy(request->query_info.stream_records, op->buffer, names_len);
        }
    }

    chimera_vfs_compound_free(compound);
    request->vfs_compound = NULL;

    if (status != CHIMERA_VFS_OK) {
        chimera_smb_query_ea_finish(request,
                                    status == CHIMERA_VFS_ERANGE ?
                                    SMB2_STATUS_BUFFER_OVERFLOW :
                                    chimera_smb_ea_status(status));
        return;
    }

    request->query_info.stream_record_len = names_len;
    request->query_info.ea_cursor         =
        (const char *) request->query_info.stream_records;

    chimera_smb_query_ea_next(request);
} /* chimera_smb_query_ea_list_sequence_complete */

static void
chimera_smb_query_full_ea_info(struct chimera_smb_request *request)
{
    struct chimera_smb_open_file *open_file = request->query_info.open_file;
    uint32_t                      cap;

    if (!(open_file->handle->vfs_module->capabilities & CHIMERA_VFS_CAP_XATTR)) {
        chimera_smb_open_file_release(request, open_file);
        chimera_smb_complete_request(request, SMB2_STATUS_EAS_NOT_SUPPORTED);
        return;
    }

    cap = request->query_info.max_response_size;
    if (cap < 64) {
        cap = 64;
    }
    if (cap > CHIMERA_SMB_EA_QUERY_CAP_MAX) {
        cap = CHIMERA_SMB_EA_QUERY_CAP_MAX;
    }

    request->query_info.ea_out      = malloc(cap);
    request->query_info.ea_out_len  = 0;
    request->query_info.ea_out_cap  = cap;
    request->query_info.ea_last_off = 0;

    /* EAs live on the file; for a stream open the base file is what the
     * sequence starts on (chimera_smb_query_base_sequence). */
    request->vfs_compound = chimera_smb_query_base_sequence(request);

    chimera_vfs_compound_add_listxattrs(
        request->vfs_compound, 0,
        sizeof(request->query_info.stream_records));

    chimera_vfs_compound_submit(request->vfs_compound,
                                chimera_smb_query_ea_list_sequence_complete,
                                request);
} /* chimera_smb_query_full_ea_info */

void
chimera_smb_query_info(struct chimera_smb_request *request)
{
    struct chimera_server_smb_thread *thread       = request->compound->thread;
    uint32_t                          getattr_mask = 0;
    uint32_t                          status       = SMB2_STATUS_SUCCESS;


    request->query_info.open_file             = chimera_smb_open_file_resolve(request, &request->query_info.file_id);
    request->query_info.r_attrs.smb_attr_mask = 0;

    if (unlikely(!request->query_info.open_file)) {
        chimera_smb_complete_request(request, SMB2_STATUS_FILE_CLOSED);
        return;
    }

    /* Named-pipe FIDs on IPC$ (srvsvc / lsarpc / samr / wkssvc) carry
     * open_file->handle == NULL -- see chimera_smb_create_gen_open_file_pipe.
     * Dispatching chimera_vfs_getattr through the normal path would deref that
     * NULL and crash the server (Windows Explorer hits this when browsing a
     * share: after TREE_CONNECT it opens srvsvc and issues
     * FILE_STANDARD_INFO / FILE_BASIC_INFO on it before starting RPC).
     *
     * Serve QUERY_INFO on a pipe FID synthetically: reasonable regular-file
     * defaults are enough for the info classes Windows queries on named
     * pipes (STANDARD_INFO, BASIC_INFO, NETWORK_OPEN_INFO, ALL_INFO).  Route
     * through the existing getattr callback so all supported info classes
     * marshal uniformly; unsupported classes still hit the callback's abort
     * as before (no regression). */
    if (request->query_info.open_file->type == CHIMERA_SMB_OPEN_FILE_TYPE_PIPE) {
        struct chimera_vfs_attrs pipe_attrs;

        /* Requests are recycled, so seed min_length here rather than relying on
         * the regular path's initialization further down -- the fixed-size
         * levels below inherit output_length from it, ALL_INFO overrides it. */
        request->query_info.min_length = 0;

        memset(&pipe_attrs, 0, sizeof(pipe_attrs));
        pipe_attrs.va_mode  = S_IFREG | 0666;
        pipe_attrs.va_nlink = 1;
        /* FileInternalInformation (and FileAllInformation) marshal
         * IndexNumber directly from va_ino (smb_attr.h); leaving it 0 would
         * report every pipe FID with the same all-zero index number.  There
         * is no real inode backing a pipe FID, so use the open's persistent
         * id (unique and stable for the life of this open) as a synthetic
         * one instead. */
        pipe_attrs.va_ino      = request->query_info.open_file->file_id.pid;
        pipe_attrs.va_set_mask = CHIMERA_VFS_ATTR_MASK_STAT;

        /* Set output_length based on info class before completing (the reply
         * path reads it verbatim into the wire header). */
        switch (request->query_info.info_type) {
            case SMB2_INFO_FILE:
                switch (request->query_info.info_class) {
                    case SMB2_FILE_BASIC_INFO:
                        request->query_info.output_length = SMB2_FILE_BASIC_INFO_SIZE;
                        break;
                    case SMB2_FILE_STANDARD_INFO:
                        request->query_info.output_length = SMB2_FILE_STANDARD_INFO_SIZE;
                        break;
                    case SMB2_FILE_INTERNAL_INFO:
                        request->query_info.output_length = SMB2_FILE_INTERNAL_INFO_SIZE;
                        break;
                    case SMB2_FILE_EA_INFO:
                        request->query_info.output_length = SMB2_FILE_EA_INFO_SIZE;
                        break;
                    case SMB2_FILE_NETWORK_OPEN_INFO:
                        request->query_info.output_length = SMB2_FILE_NETWORK_OPEN_INFO_SIZE;
                        break;
                    case SMB2_FILE_ATTRIBUTE_TAG_INFO:
                        request->query_info.output_length = SMB2_FILE_ATTRIBUTE_TAG_INFO_SIZE;
                        break;
                    case SMB2_FILE_ALL_INFO:
                        /* Windows queries FileAllInformation on a pipe FID, and
                         * this branch's comment above already counts it among
                         * the classes it serves -- but the case was missing, so
                         * it fell through to INVALID_INFO_CLASS.  Size it the
                         * same way the VFS path does: the fixed 100 bytes plus
                         * the share-relative name as UTF-16LE.  A pipe open
                         * carries the pipe name in full_path (set by
                         * chimera_smb_create_gen_open_file), so the reply names
                         * the pipe, as Windows reports it. */
                        request->query_info.output_length = SMB2_FILE_ALL_INFO_FIXED_SIZE +
                            (request->query_info.open_file->full_path_len
                             ? request->query_info.open_file->full_path_len * 2 : 2);
                        /* Variable-length level: the fixed portion ends after
                         * the FileNameLength field, so that -- not the whole
                         * reply -- is the buffer minimum. */
                        request->query_info.min_length = SMB2_FILE_ALL_INFO_FIXED_SIZE + 4;
                        break;
                    default:
                        /* Info classes we don't synthesize for pipes: fail
                         * cleanly rather than falling through to the VFS path. */
                        chimera_smb_open_file_release(request, request->query_info.open_file);
                        chimera_smb_complete_request(request, SMB2_STATUS_INVALID_INFO_CLASS);
                        return;
                } /* switch */
                break;
            default:
                /* FILESYSTEM / SECURITY info on a pipe FID: not supported. */
                chimera_smb_open_file_release(request, request->query_info.open_file);
                chimera_smb_complete_request(request, SMB2_STATUS_INVALID_DEVICE_REQUEST);
                return;
        } /* switch */

        /* The pipe branch answers without touching the VFS, so it has to run
         * the same OutputBufferLength validation the regular path runs below.
         * Without it a QUERY_INFO on a pipe FID with OutputBufferLength=2
         * returned STATUS_SUCCESS carrying a full fixed-size payload, which
         * MS-SMB2 3.3.5.20.1 requires to be INFO_LENGTH_MISMATCH. */
        status = chimera_smb_query_info_check_length(request);

        if (status != SMB2_STATUS_SUCCESS) {
            chimera_smb_open_file_release(request, request->query_info.open_file);
            chimera_smb_complete_request(request, status);
            return;
        }

        chimera_smb_query_info_getattr_callback(CHIMERA_VFS_OK, &pipe_attrs, request);
        return;
    }

    /* min_length is the info level's fixed minimum size: a request whose
     * OutputBufferLength is smaller than this must fail INFO_LENGTH_MISMATCH
     * (MS-SMB2 3.3.5.20.1 / smb2.getinfo.q*_buffercheck).  For fixed-size
     * levels it equals output_length; variable-size levels override it below. */
    request->query_info.min_length = 0;

    switch (request->query_info.info_type) {
        case SMB2_INFO_FILE:
            /* The attribute-bearing FILE info classes require the handle to
             * hold FILE_READ_ATTRIBUTES (MS-FSA 2.1.5.11): a handle opened with
             * only FILE_READ_DATA cannot query FileBasicInformation and friends
             * -> STATUS_ACCESS_DENIED (smb2.streams.attributes1).  Classes that
             * carry no file attributes (standard sizes, internal id, EA size,
             * position, mode, alignment, access, name) are not gated. */
            switch (request->query_info.info_class) {
                case SMB2_FILE_BASIC_INFO:
                case SMB2_FILE_NETWORK_OPEN_INFO:
                case SMB2_FILE_ATTRIBUTE_TAG_INFO:
                case SMB2_FILE_ALL_INFO:
                    /* FileCompressionInformation is intentionally NOT gated:
                     * MS-SMB2 3.3.5.20.1's READ_ATTRIBUTES list does not include
                     * it (the always-NONE compression state is readable). */
                    if (!(request->query_info.open_file->granted_access &
                          SMB2_FILE_READ_ATTRIBUTES)) {
                        status = SMB2_STATUS_ACCESS_DENIED;
                    }
                    break;
                default:
                    break;
            } /* switch */

            if (status != SMB2_STATUS_SUCCESS) {
                break;
            }

            /* Calculate the output buffer length based on the info class */
            switch (request->query_info.info_class) {
                case SMB2_FILE_BASIC_INFO:
                    request->query_info.output_length = SMB2_FILE_BASIC_INFO_SIZE;
                    getattr_mask                      = CHIMERA_VFS_ATTR_MASK_STAT;
                    break;
                case SMB2_FILE_STANDARD_INFO:
                    request->query_info.output_length = SMB2_FILE_STANDARD_INFO_SIZE;
                    getattr_mask                      = CHIMERA_VFS_ATTR_MASK_STAT;
                    break;
                case SMB2_FILE_INTERNAL_INFO:
                    request->query_info.output_length = SMB2_FILE_INTERNAL_INFO_SIZE;
                    getattr_mask                      = CHIMERA_VFS_ATTR_MASK_STAT;
                    break;
                case SMB2_FILE_EA_INFO:
                    request->query_info.output_length = SMB2_FILE_EA_INFO_SIZE;
                    getattr_mask                      = CHIMERA_VFS_ATTR_MASK_STAT |
                        CHIMERA_VFS_ATTR_EA_SIZE;
                    break;
                case SMB2_FILE_ACCESS_INFO:
                    /* GrantedAccess of this handle; no backend attrs needed. */
                    request->query_info.output_length = SMB2_FILE_ACCESS_INFO_SIZE;
                    break;
                case SMB2_FILE_POSITION_INFO:
                    /* CurrentByteOffset is per-handle state; no backend attrs. */
                    request->query_info.output_length = SMB2_FILE_POSITION_INFO_SIZE;
                    break;
                case SMB2_FILE_MODE_INFO:
                    /* FileModeInformation (MS-FSCC 2.4.26): a single Mode DWORD
                    * of open-mode flags; per-handle state, no backend attrs. */
                    request->query_info.output_length = SMB2_FILE_MODE_INFO_SIZE;
                    break;
                case SMB2_FILE_ALIGNMENT_INFO:
                    /* FileAlignmentInformation (MS-FSCC 2.4.3): a single
                     * AlignmentRequirement DWORD; no backend attrs. */
                    request->query_info.output_length = SMB2_FILE_ALIGNMENT_INFO_SIZE;
                    break;
                case SMB2_FILE_COMPRESSION_INFO:
                    request->query_info.output_length = SMB2_FILE_COMPRESSION_INFO_SIZE;
                    getattr_mask                      = CHIMERA_VFS_ATTR_MASK_STAT;
                    break;
                case SMB2_FILE_NETWORK_OPEN_INFO:
                    request->query_info.output_length = SMB2_FILE_NETWORK_OPEN_INFO_SIZE;
                    getattr_mask                      = CHIMERA_VFS_ATTR_MASK_STAT;
                    break;
                case SMB2_FILE_ATTRIBUTE_TAG_INFO:
                    request->query_info.output_length = SMB2_FILE_ATTRIBUTE_TAG_INFO_SIZE;
                    getattr_mask                      = CHIMERA_VFS_ATTR_MASK_STAT;
                    break;
                case SMB2_FILE_ALL_INFO:
                    /* FileAllInformation (MS-FSCC 2.4.2): the fixed 100 bytes
                     * (FileNameLength field included) followed by the
                     * share-relative path as UTF-16LE (full_path_len*2).  The
                     * share root has an empty path; Windows reports its name as
                     * "\" (2 bytes) so the reply clears the Linux cifs client's
                     * 101-byte FILE_ALL_INFO minimum (see chimera_smb_append_all_info). */
                    request->query_info.output_length = SMB2_FILE_ALL_INFO_FIXED_SIZE +
                        (request->query_info.open_file->full_path_len
                         ? request->query_info.open_file->full_path_len * 2 : 2);
                    /* The fixed portion ends after the FileNameLength field; a
                     * shorter buffer is INFO_LENGTH_MISMATCH (smbtorture uses
                     * fixed=104 here). */
                    request->query_info.min_length = SMB2_FILE_ALL_INFO_FIXED_SIZE + 4;
                    getattr_mask                   = CHIMERA_VFS_ATTR_MASK_STAT |
                        CHIMERA_VFS_ATTR_EA_SIZE;
                    break;
                case SMB2_FILE_NORMALIZED_NAME_INFO:
                    /* MS-FSCC 2.4.30 FILE_NAME_INFORMATION layout (FileNameLength
                     * + FileName in UTF-16LE).  The normalized name is the open's
                     * share-relative path, which we already hold (as UTF-8) — no
                     * backend attrs. */
                    request->query_info.output_length = 4 + request->query_info.open_file->full_path_len * 2;
                    request->query_info.min_length    = 4;
                    break;
                case SMB2_FILE_FULL_EA_INFO:
                    /* Output length depends on the enumerated EA set, so this
                     * class drives its own async list+get flow. */
                    chimera_smb_query_full_ea_info(request);
                    return;
                case SMB2_FILE_STREAM_INFO:
                    /* Output length depends on the enumerated stream set, so
                     * this class drives its own async list_streams flow. */
                    chimera_smb_query_stream_info(request);
                    return;
                default:
                    /* A FILE info class we do not handle: keep NOT_IMPLEMENTED.
                     * Samba's smb2.getinfo.qfile_buffercheck enumerates every
                     * file level and treats NOT_IMPLEMENTED as "level
                     * unsupported, skip" while asserting OK otherwise -- so a
                     * valid-but-unsupported level must NOT be reported as
                     * INVALID_INFO_CLASS.  Distinguishing a genuinely invalid
                     * class value from a valid-but-unsupported one needs a
                     * file-info-class validity table (out of scope here). */
                    status = SMB2_STATUS_NOT_IMPLEMENTED;
                    break;
            } /* switch */
            break;
        case SMB2_INFO_FILESYSTEM:
            switch (request->query_info.info_class) {
                case SMB2_FILE_FS_VOLUME_INFO:
                    /* 18-byte fixed header + 6-byte VolumeLabel ("fs\0" in
                     * UTF-16LE).  smbtorture.qfs_buffercheck hardcodes fixed=24
                     * for this level, so the total must be >= 24. */
                    request->query_info.output_length = 24;
                    break;
                case SMB2_FILE_FS_SIZE_INFO:
                    request->query_info.output_length = 24;
                    getattr_mask                      = CHIMERA_VFS_ATTR_MASK_STATFS;
                    break;
                case SMB2_FILE_FS_DEVICE_INFO:
                    request->query_info.output_length = 8;
                    break;
                case SMB2_FILE_FS_ATTRIBUTE_INFO:
                    /* The flag word follows the serving module, and the open
                     * is released before the reply is marshalled, so derive
                     * it here while the handle is still ours. */
                    request->query_info.r_fs_attrs.smb_fs_attributes =
                        chimera_smb_fs_attributes(
                            request->query_info.open_file->handle->vfs_module->capabilities,
                            thread->shared->config.named_streams);
                    request->query_info.output_length = 16;
                    break;
                case SMB2_FILE_FS_CONTROL_INFO:
                    /* FileFsControlInformation (MS-FSCC 2.5.2): quota control.
                     * We do not enforce quotas; report "no quota tracking"
                     * (all-zero thresholds, no control flags) like a volume with
                     * quotas disabled. */
                    request->query_info.output_length = SMB2_FILE_FS_CONTROL_INFO_SIZE;
                    break;
                case SMB2_FILE_FS_FULL_SIZE_INFO:
                    request->query_info.output_length = 32;
                    getattr_mask                      = CHIMERA_VFS_ATTR_MASK_STATFS;
                    break;
                case SMB2_FILE_FS_OBJECTID_INFO:
                    /* FileFsObjectIdInformation (MS-FSCC 2.5.6): a volume object
                    * id.  We have no persistent volume GUID; report all-zero. */
                    request->query_info.output_length = SMB2_FILE_FS_OBJECTID_INFO_SIZE;
                    break;
                case SMB2_FILE_FS_SECTOR_SIZE_INFO:
                    /* FileFsSectorSizeInformation (MS-FSCC 2.5.7). */
                    request->query_info.output_length = SMB2_FILE_FS_SECTOR_SIZE_INFO_SIZE;
                    break;
                default:
                    /* Unhandled FS info class = invalid class for QUERY_INFO. */
                    status = SMB2_STATUS_INVALID_INFO_CLASS;
                    break;
            } /* switch */
            break;
        case SMB2_INFO_SECURITY:
            chimera_smb_query_security(request);
            return;
        default:
            status = SMB2_STATUS_NOT_IMPLEMENTED;
            break;
    } /* switch */

    /* Buffer-length validation (MS-SMB2 3.3.5.20).  Only for levels we accept;
     * NOT_IMPLEMENTED levels keep their status. */
    if (status == SMB2_STATUS_SUCCESS) {
        status = chimera_smb_query_info_check_length(request);
    }

    if (status != SMB2_STATUS_SUCCESS) {
        chimera_smb_open_file_release(request, request->query_info.open_file);
        chimera_smb_complete_request(request, status);
        return;
    }

    if (getattr_mask) {
        /* PUTHANDLE, GETATTR: the attributes come through the FileId's own
         * handle, which is the one this query's access check ran against. */
        request->vfs_compound = chimera_vfs_compound_alloc(
            thread->vfs_thread, &request->session_handle->session->cred);

        chimera_vfs_compound_add_puthandle(
            request->vfs_compound,
            request->query_info.open_file->handle,
            request->query_info.open_file->open_flags);

        chimera_vfs_compound_add_getattr(request->vfs_compound, getattr_mask);

        chimera_vfs_compound_submit(request->vfs_compound,
                                    chimera_smb_query_info_sequence_complete,
                                    request);
    } else {
        chimera_smb_open_file_release(request, request->query_info.open_file);
        chimera_smb_complete_request(request, status);
    }

} /* chimera_smb_query_info */

void
chimera_smb_query_info_reply(
    struct evpl_iovec_cursor   *reply_cursor,
    struct chimera_smb_request *request)
{
    struct chimera_server_smb_thread *thread = request->compound->thread;
    uint16_t                          namebuf[8];

    if (request->query_info.info_type == SMB2_INFO_SECURITY) {
        chimera_smb_query_security_reply(reply_cursor, request);
        return;
    }

    /* Append the query info reply header */
    evpl_iovec_cursor_append_uint16(reply_cursor, SMB2_QUERY_INFO_REPLY_SIZE);
    evpl_iovec_cursor_append_uint16(reply_cursor, 64 + 8);  /* Fixed offset from SMB protocol */
    evpl_iovec_cursor_append_uint32(reply_cursor, request->query_info.output_length);

    switch (request->query_info.info_type) {
        case SMB2_INFO_FILE:
            /* Append the attributes based on the info class */
            switch (request->query_info.info_class) {
                case SMB2_FILE_BASIC_INFO:
                    chimera_smb_append_basic_info(reply_cursor, &request->query_info.r_attrs);
                    break;
                case SMB2_FILE_STANDARD_INFO:
                    chimera_smb_append_standard_info(reply_cursor, request->query_info.open_file, &request->query_info.
                                                     r_attrs);
                    break;
                case SMB2_FILE_INTERNAL_INFO:
                    chimera_smb_append_internal_info(reply_cursor, &request->query_info.r_attrs);
                    break;
                case SMB2_FILE_EA_INFO:
                    chimera_smb_append_ea_info(reply_cursor, &request->query_info.r_attrs);
                    break;
                case SMB2_FILE_COMPRESSION_INFO:
                    chimera_smb_append_compression_info(reply_cursor, &request->query_info.r_attrs);
                    break;
                case SMB2_FILE_NETWORK_OPEN_INFO:
                    chimera_smb_append_network_open_info(reply_cursor, &request->query_info.r_attrs);
                    break;
                case SMB2_FILE_ATTRIBUTE_TAG_INFO:
                    chimera_smb_append_attribute_tag_info(reply_cursor, &request->query_info.r_attrs);
                    break;
                case SMB2_FILE_ACCESS_INFO:
                    evpl_iovec_cursor_append_uint32(
                        reply_cursor,
                        request->query_info.open_file->granted_access);
                    break;
                case SMB2_FILE_POSITION_INFO:
                    evpl_iovec_cursor_append_uint64(
                        reply_cursor,
                        request->query_info.open_file->position);
                    break;
                case SMB2_FILE_MODE_INFO:
                    /* FileModeInformation (MS-FSCC 2.4.26): the Mode flags the
                     * file was opened with (WRITE_THROUGH / SEQUENTIAL_ONLY /
                     * NO_INTERMEDIATE_BUFFERING / DELETE_ON_CLOSE...).  We do not
                     * track these per-open yet; report 0 (no special modes),
                     * which is what Windows returns for a plain open. */
                    evpl_iovec_cursor_append_uint32(reply_cursor, 0);
                    break;
                case SMB2_FILE_ALIGNMENT_INFO:
                    /* FileAlignmentInformation: FILE_BYTE_ALIGNMENT (0) — no
                     * alignment requirement for buffered I/O. */
                    evpl_iovec_cursor_append_uint32(reply_cursor, 0);
                    break;
                case SMB2_FILE_NORMALIZED_NAME_INFO:
                {
                    /* FILE_NAME_INFORMATION: FileNameLength (UTF-16LE bytes)
                     * followed by the share-relative path converted to UTF-16LE. */
                    struct chimera_smb_open_file *of = request->query_info.open_file;
                    uint16_t                     *nb;

                    evpl_iovec_cursor_append_uint32(reply_cursor, of->full_path_len * 2);
                    nb = evpl_iovec_cursor_data(reply_cursor);
                    chimera_smb_utf8_to_utf16le(&request->compound->thread->iconv_ctx,
                                                of->full_path, of->full_path_len,
                                                nb, SMB_PATH_MAX * 2);
                    evpl_iovec_cursor_skip(reply_cursor, of->full_path_len * 2);
                    break;
                }
                case SMB2_FILE_ALL_INFO:
                    chimera_smb_append_all_info(&request->compound->thread->iconv_ctx,
                                                reply_cursor, request->query_info.open_file, &request->query_info.
                                                r_attrs);
                    break;
                case SMB2_FILE_FULL_EA_INFO:
                    /* The wire FULL_EA list was built into ea_out by the async
                     * enumeration; emit it verbatim (empty list -> no bytes). */
                    if (request->query_info.ea_out_len) {
                        evpl_iovec_cursor_append_blob_unaligned(
                            reply_cursor,
                            request->query_info.ea_out,
                            request->query_info.ea_out_len);
                    }
                    if (request->query_info.ea_out) {
                        free(request->query_info.ea_out);
                        request->query_info.ea_out = NULL;
                    }
                    break;
                case SMB2_FILE_STREAM_INFO:
                    chimera_smb_emit_stream_info(
                        &request->compound->thread->iconv_ctx,
                        request->query_info.stream_records,
                        request->query_info.stream_record_len,
                        request->query_info.stream_record_count,
                        reply_cursor);
                    break;
                default:
                    chimera_smb_abort("%s: unsupported file information class: %d",
                                      __FUNCTION__, request->query_info.info_class);
                    break;
            } /* switch */
            break;
        case SMB2_INFO_FILESYSTEM:
            switch (request->query_info.info_class) {
                case SMB2_FILE_FS_VOLUME_INFO:
                    evpl_iovec_cursor_append_uint64(reply_cursor, 0); /* Create time */
                    evpl_iovec_cursor_append_uint32(reply_cursor, 0x12345678); /* Serial number */
                    evpl_iovec_cursor_append_uint32(reply_cursor, 6); /* Label length (bytes) */
                    evpl_iovec_cursor_append_uint8(reply_cursor, 0); /* SupportsObjects */
                    evpl_iovec_cursor_append_uint8(reply_cursor, 0); /* Reserved */
                    /* VolumeLabel "fs\0" in UTF-16LE = 6 bytes (18-byte fixed
                     * header + 6 => 24-byte total). */
                    chimera_smb_utf8_to_utf16le(
                        &request->compound->thread->iconv_ctx,
                        "fs",
                        2,
                        namebuf,
                        8);
                    namebuf[2] = 0;
                    evpl_iovec_cursor_append_blob_unaligned(reply_cursor, namebuf, 6);
                    break;
                case SMB2_FILE_FS_SIZE_INFO:
                    evpl_iovec_cursor_append_uint64(reply_cursor, request->query_info.r_fs_attrs.
                                                    smb_total_allocation_units);
                    evpl_iovec_cursor_append_uint64(reply_cursor, request->query_info.r_fs_attrs.
                                                    smb_caller_available_allocation_units);
                    evpl_iovec_cursor_append_uint32(reply_cursor, request->query_info.r_fs_attrs.
                                                    smb_sectors_per_allocation_unit);
                    evpl_iovec_cursor_append_uint32(reply_cursor, request->query_info.r_fs_attrs.smb_bytes_per_sector);
                    break;
                case SMB2_FILE_FS_DEVICE_INFO:
                    evpl_iovec_cursor_append_uint32(reply_cursor, 0x14); /* Network File System */
                    evpl_iovec_cursor_append_uint32(reply_cursor, 0x10); /* FILE_REMOTE_DEVICE */
                    break;
                case SMB2_FILE_FS_ATTRIBUTE_INFO:
                    evpl_iovec_cursor_append_uint32(reply_cursor,
                                                    request->query_info.r_fs_attrs.smb_fs_attributes);
                    evpl_iovec_cursor_append_uint32(reply_cursor, 255);
                    evpl_iovec_cursor_append_uint32(reply_cursor, 4);

                    chimera_smb_utf8_to_utf16le(
                        &request->compound->thread->iconv_ctx,
                        "fs",
                        2,
                        namebuf,
                        8);
                    evpl_iovec_cursor_append_blob(reply_cursor, namebuf, 4);
                    break;
                case SMB2_FILE_FS_FULL_SIZE_INFO:
                    evpl_iovec_cursor_append_uint64(reply_cursor, request->query_info.r_fs_attrs.
                                                    smb_total_allocation_units);
                    evpl_iovec_cursor_append_uint64(reply_cursor, request->query_info.r_fs_attrs.
                                                    smb_caller_available_allocation_units);
                    evpl_iovec_cursor_append_uint64(reply_cursor, request->query_info.r_fs_attrs.
                                                    smb_actual_available_allocation_units);
                    evpl_iovec_cursor_append_uint32(reply_cursor, request->query_info.r_fs_attrs.
                                                    smb_sectors_per_allocation_unit);
                    evpl_iovec_cursor_append_uint32(reply_cursor, request->query_info.r_fs_attrs.smb_bytes_per_sector);
                    break;
                case SMB2_FILE_FS_CONTROL_INFO:
                    /* MS-FSCC 2.5.2 FileFsControlInformation.  No quota tracking:
                     * the FreeSpace* quota-event byte counts are 0 (filtering
                     * disabled), DefaultQuotaThreshold/Limit are -1 (UINT64_MAX,
                     * no limit), and no control flags are set. */
                    evpl_iovec_cursor_append_uint64(reply_cursor, 0); /* FreeSpaceStartFiltering */
                    evpl_iovec_cursor_append_uint64(reply_cursor, 0); /* FreeSpaceThreshold */
                    evpl_iovec_cursor_append_uint64(reply_cursor, 0); /* FreeSpaceStopFiltering */
                    evpl_iovec_cursor_append_uint64(reply_cursor, UINT64_MAX); /* DefaultQuotaThreshold */
                    evpl_iovec_cursor_append_uint64(reply_cursor, UINT64_MAX); /* DefaultQuotaLimit */
                    evpl_iovec_cursor_append_uint32(reply_cursor, 0); /* FileSystemControlFlags */
                    evpl_iovec_cursor_append_uint32(reply_cursor, 0); /* Padding */
                    break;
                case SMB2_FILE_FS_OBJECTID_INFO:
                    /* MS-FSCC 2.5.6 FileFsObjectIdInformation: 16-byte ObjectId +
                    * 48-byte ExtendedInfo.  We have no persistent volume GUID. */
                    evpl_iovec_cursor_zero(reply_cursor, SMB2_FILE_FS_OBJECTID_INFO_SIZE);
                    break;
                case SMB2_FILE_FS_SECTOR_SIZE_INFO:
                    /* MS-FSCC 2.5.7 FileFsSectorSizeInformation. */
                    evpl_iovec_cursor_append_uint32(reply_cursor, 512); /* LogicalBytesPerSector */
                    evpl_iovec_cursor_append_uint32(reply_cursor, thread->shared->config.fs_physical_bytes_per_sector); /* PhysicalBytesPerSectorForAtomicity */
                    evpl_iovec_cursor_append_uint32(reply_cursor, thread->shared->config.fs_physical_bytes_per_sector); /* PhysicalBytesPerSectorForPerformance */
                    evpl_iovec_cursor_append_uint32(reply_cursor, thread->shared->config.fs_physical_bytes_per_sector); /* FSEffPhysicalBytesPerSectorForAtomicity */
                    evpl_iovec_cursor_append_uint32(reply_cursor, thread->shared->config.fs_sector_size_flags); /* Flags: aligned + partition aligned */
                    evpl_iovec_cursor_append_uint32(reply_cursor, 0); /* ByteOffsetForSectorAlignment */
                    evpl_iovec_cursor_append_uint32(reply_cursor, 0); /* ByteOffsetForPartitionAlignment */
                    break;
                default:
                    chimera_smb_abort("%s: unsupported filesystem information class: %d",
                                      __FUNCTION__, request->query_info.info_class);
                    break;
            } /* switch */
            break;
        default:
            chimera_smb_abort("Unsupported information type: %d", request->query_info.info_type);
            break;
    } /* switch */

} /* chimera_smb_query_info_reply */

int
chimera_smb_parse_query_info(
    struct evpl_iovec_cursor   *request_cursor,
    struct chimera_smb_request *request)
{
    uint32_t max_response_size = 0, input_size = 0;
    uint16_t input_offset = 0;

    if (unlikely(request->request_struct_size != SMB2_QUERY_INFO_REQUEST_SIZE)) {
        chimera_smb_error("Received SMB2 QUERY_INFO request with invalid struct size (%u expected %u)",
                          request->smb2_hdr.struct_size,
                          SMB2_QUERY_INFO_REQUEST_SIZE);
        request->status = SMB2_STATUS_INVALID_PARAMETER;
        return -1;
    }

    int prc = 0;
    prc |= evpl_iovec_cursor_try_get_uint8(request_cursor, &request->query_info.
                                           info_type);
    prc |= evpl_iovec_cursor_try_get_uint8(request_cursor, &request->query_info.
                                           info_class);
    prc                                  |= evpl_iovec_cursor_try_get_uint32(request_cursor, &max_response_size);
    request->query_info.max_response_size = max_response_size;
    prc                                  |= evpl_iovec_cursor_try_get_uint16(request_cursor, &input_offset);
    prc                                  |= evpl_iovec_cursor_try_get_uint32(request_cursor, &input_size);
    prc                                  |= evpl_iovec_cursor_try_get_uint32(request_cursor, &request->query_info.
                                                                             addl_info);
    prc |= evpl_iovec_cursor_try_get_uint32(request_cursor, &request->query_info.flags)
    ;
    prc |= evpl_iovec_cursor_try_get_uint64(request_cursor, &request->query_info.
                                            file_id.pid);
    prc |= evpl_iovec_cursor_try_get_uint64(request_cursor, &request->query_info.
                                            file_id.vid);

    if (unlikely(prc)) {
        chimera_smb_error("Received SMB2 QUERY_INFO request truncated in fixed body");
        return chimera_smb_parse_reject(request, SMB2_STATUS_INVALID_PARAMETER);
    }

    return 0;
} /* chimera_smb_parse_query_info */
