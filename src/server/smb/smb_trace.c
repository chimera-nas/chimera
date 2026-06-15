// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * SMB2 span tracing (see smb_trace.h).  Runs only for a recording span.
 */

#include <stdio.h>
#include <string.h>

#include "smb_trace.h"
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

/* Encode an SMB FileId (persistent + volatile, 16 bytes) as hex. */
static void
smb_trace_file_id(
    struct chimera_smb_compound      *compound,
    const struct chimera_smb_file_id *file_id)
{
    char hex[2 * sizeof(*file_id) + 1];

    format_hex(hex, sizeof(hex), file_id, (int) sizeof(*file_id));
    otel_span_attr_str(&compound->otel, "smb.file_id", hex);
} /* smb_trace_file_id */

void
_smb_trace_compound_request(struct chimera_smb_compound *compound)
{
    struct chimera_smb_request *r0;
    char                        name[48];
    char                        ops[256];
    int                         off = 0;
    int                         i;

    if (compound->num_requests == 0) {
        return;
    }

    r0 = compound->requests[0];

    snprintf(name, sizeof(name), "smb2.%s",
             smb_trace_command_name(r0->smb2_hdr.command));
    otel_span_set_name(&compound->otel, name);
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

    /* Per-command attributes for the leading request. */
    switch (r0->smb2_hdr.command) {
        case SMB2_CREATE:
            if (r0->create.name && r0->create.name_len > 0) {
                otel_span_attr_strn(&compound->otel, "smb.name",
                                    r0->create.name, r0->create.name_len);
            }
            break;
        case SMB2_READ:
            otel_span_attr_u64(&compound->otel, "smb.offset", r0->read.offset);
            otel_span_attr_u64(&compound->otel, "smb.length", r0->read.length);
            smb_trace_file_id(compound, &r0->read.file_id);
            break;
        case SMB2_WRITE:
            otel_span_attr_u64(&compound->otel, "smb.offset", r0->write.offset);
            otel_span_attr_u64(&compound->otel, "smb.length", r0->write.length);
            smb_trace_file_id(compound, &r0->write.file_id);
            break;
        case SMB2_CLOSE:
            smb_trace_file_id(compound, &r0->close.file_id);
            break;
        default:
            break;
    } /* switch */
} /* _smb_trace_compound_request */
