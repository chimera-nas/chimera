// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * SMB2 span tracing (see smb_trace.h).  Runs only for a recording span.
 */

#include "smb_trace.h"

#if CHIMERA_HAVE_OTEL

#include <stdio.h>
#include <string.h>

#include "common/format.h"

static const char *
smb_trace_command_name(uint32_t command)
{
    switch (command) {
        case SMB2_NEGOTIATE:       return "Negotiate";
        case SMB2_SESSION_SETUP:   return "SessionSetup";
        case SMB2_LOGOFF:          return "Logoff";
        case SMB2_TREE_CONNECT:    return "TreeConnect";
        case SMB2_TREE_DISCONNECT: return "TreeDisconnect";
        case SMB2_CREATE:          return "Create";
        case SMB2_CLOSE:           return "Close";
        case SMB2_FLUSH:           return "Flush";
        case SMB2_READ:            return "Read";
        case SMB2_WRITE:           return "Write";
        case SMB2_LOCK:            return "Lock";
        case SMB2_IOCTL:           return "Ioctl";
        case SMB2_CANCEL:          return "Cancel";
        case SMB2_ECHO:            return "Echo";
        case SMB2_QUERY_DIRECTORY: return "QueryDirectory";
        case SMB2_CHANGE_NOTIFY:   return "ChangeNotify";
        case SMB2_QUERY_INFO:      return "QueryInfo";
        case SMB2_SET_INFO:        return "SetInfo";
        case SMB2_OPLOCK_BREAK:    return "OplockBreak";
        default:                   return "Unknown";
    } /* switch */
} /* smb_trace_command_name */

/* Encode an SMB FileId (persistent + volatile, 16 bytes) as hex onto a span. */
static void
smb_trace_file_id(
    struct otel_span                 *s,
    const struct chimera_smb_file_id *file_id)
{
    char hex[2 * sizeof(*file_id) + 1];

    format_hex(hex, sizeof(hex), file_id, (int) sizeof(*file_id));
    otel_span_attr_str(s, "smb.file_id", hex);
} /* smb_trace_file_id */

void
_smb_trace_compound_request(struct chimera_smb_compound *compound)
{
    char ops[256];
    int  off = 0;
    int  i;

    if (compound->num_requests == 0) {
        return;
    }

    /* Aggregate span for the whole compound PDU; the individual requests are
     * its child spans (see _smb_trace_op_begin). */
    otel_span_set_name(&compound->otel, "smb2.COMPOUND");
    otel_span_attr_u64(&compound->otel, "smb.numreq",
                       (uint64_t) compound->num_requests);

    /* Command sequence, e.g. "Create,QueryInfo,Close". */
    ops[0] = '\0';
    for (i = 0; i < compound->num_requests; i++) {
        if (off < (int) sizeof(ops) - 1) {
            off += snprintf(ops + off, sizeof(ops) - off, "%s%s",
                            off ? "," : "",
                            smb_trace_command_name(compound->requests[i]->smb2_hdr.command));
        }
    }
    if (off > (int) sizeof(ops) - 1) {
        off = (int) sizeof(ops) - 1;
    }
    otel_span_attr_strn(&compound->otel, "smb.ops", ops, (size_t) off);
} /* _smb_trace_compound_request */

/* Begin a span for one request inside the compound, as a child of the aggregate
 * span.  Names it "smb2.<Command>", attaches the command's key attributes, and
 * re-points the VFS parent at this span so the request's VFS work nests under it
 * rather than the whole compound. */
void
_smb_trace_op_begin(
    struct chimera_smb_compound *compound,
    struct chimera_smb_request  *request)
{
    struct otel_span *s = &compound->op_otel;
    char              name[48];

    snprintf(name, sizeof(name), "smb2.%s",
             smb_trace_command_name(request->smb2_hdr.command));
    otel_span_start_child(s, name, OTEL_SPAN_INTERNAL, &compound->otel);

    switch (request->smb2_hdr.command) {
        case SMB2_CREATE:
            if (request->create.name && request->create.name_len > 0) {
                otel_span_attr_strn(s, "smb.name",
                                    request->create.name, request->create.name_len);
            }
            otel_span_attr_u64(s, "smb.desired_access", request->create.desired_access);
            otel_span_attr_u64(s, "smb.disposition", request->create.create_disposition);
            break;
        case SMB2_READ:
            otel_span_attr_u64(s, "smb.offset", request->read.offset);
            otel_span_attr_u64(s, "smb.length", request->read.length);
            smb_trace_file_id(s, &request->read.file_id);
            break;
        case SMB2_WRITE:
            otel_span_attr_u64(s, "smb.offset", request->write.offset);
            otel_span_attr_u64(s, "smb.length", request->write.length);
            smb_trace_file_id(s, &request->write.file_id);
            break;
        case SMB2_CLOSE:
            smb_trace_file_id(s, &request->close.file_id);
            break;
        case SMB2_FLUSH:
            smb_trace_file_id(s, &request->flush.file_id);
            break;
        case SMB2_LOCK:
            smb_trace_file_id(s, &request->lock.file_id);
            break;
        case SMB2_IOCTL:
            otel_span_attr_u64(s, "smb.ctl_code", request->ioctl.ctl_code);
            smb_trace_file_id(s, &request->ioctl.file_id);
            break;
        case SMB2_QUERY_INFO:
            otel_span_attr_u64(s, "smb.info_type", request->query_info.info_type);
            otel_span_attr_u64(s, "smb.info_class", request->query_info.info_class);
            smb_trace_file_id(s, &request->query_info.file_id);
            break;
        case SMB2_SET_INFO:
            otel_span_attr_u64(s, "smb.info_type", request->set_info.info_type);
            otel_span_attr_u64(s, "smb.info_class", request->set_info.info_class);
            smb_trace_file_id(s, &request->set_info.file_id);
            break;
        case SMB2_QUERY_DIRECTORY:
            if (request->query_directory.pattern_length > 0) {
                otel_span_attr_strn(s, "smb.pattern", request->query_directory.pattern,
                                    request->query_directory.pattern_length);
            }
            smb_trace_file_id(s, &request->query_directory.file_id);
            break;
        default:
            break;
    } /* switch */

    compound->thread->vfs_thread->otel_parent = s;
    compound->op_span_active                  = 1;
} /* _smb_trace_op_begin */

#endif /* CHIMERA_HAVE_OTEL */
