// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include "smb_dump.h"
#include "smb_string.h"
#include "smb_internal.h"
#include "common/format.h"

static const char *
smb_command_name(uint32_t command)
{
    switch (command) {
        case SMB2_NEGOTIATE:
            return "Negotiate";
        case SMB2_SESSION_SETUP:
            return "SessionSetup";
        case SMB2_LOGOFF:
            return "Logoff";
        case SMB2_TREE_CONNECT:
            return "TreeConnect";
        case SMB2_TREE_DISCONNECT:
            return "TreeDisconnect";
        case SMB2_CREATE:
            return "Create";
        case SMB2_CLOSE:
            return "Close";
        case SMB2_FLUSH:
            return "Flush";
        case SMB2_READ:
            return "Read";
        case SMB2_WRITE:
            return "Write";
        case SMB2_LOCK:
            return "Lock";
        case SMB2_IOCTL:
            return "Ioctl";
        case SMB2_CANCEL:
            return "Cancel";
        case SMB2_ECHO:
            return "Echo";
        case SMB2_QUERY_DIRECTORY:
            return "QueryDirectory";
        case SMB2_CHANGE_NOTIFY:
            return "ChangeNotify";
        case SMB2_QUERY_INFO:
            return "QueryInfo";
        case SMB2_SET_INFO:
            return "SetInfo";
        case SMB2_OPLOCK_BREAK:
            return "OplockBreak";
        case SMB1_NEGOTIATE:
            return "SMB1Negotiate";
        default:
            return "Unknown";
    } /* switch */
} /* smb_command_name */

static const char *
smb_status_name(uint32_t status)
{
    switch (status) {
        case SMB2_STATUS_SUCCESS:
            return "Success";
        case SMB2_STATUS_PENDING:
            return "Pending";
        case SMB2_STATUS_MORE_PROCESSING_REQUIRED:
            return "MoreProcessingRequired";
        case SMB2_STATUS_NO_SUCH_FILE:
            return "NoSuchFile";
        case SMB2_STATUS_INTERNAL_ERROR:
            return "InternalError";
        case SMB2_STATUS_BAD_NETWORK_NAME:
            return "BadNetworkName";
        case SMB2_STATUS_INVALID_PARAMETER:
            return "InvalidParameter";
        case SMB2_STATUS_NOT_SUPPORTED:
            return "NotSupported";
        case SMB2_STATUS_INVALID_HANDLE:
            return "InvalidHandle";
        case SMB2_STATUS_REQUEST_ABORTED:
            return "RequestAborted";
        case SMB2_STATUS_END_OF_FILE:
            return "EndOfFile";
        default:
            return "Unknown";
    } /* switch */
} /* smb_status_name */

static const char *
smb_create_disposition_name(uint32_t disposition)
{
    switch (disposition) {
        case SMB2_FILE_CREATE:
            return "Create";
        case SMB2_FILE_OPEN:
            return "Open";
        case SMB2_FILE_OPEN_IF:
            return "OpenIf";
        case SMB2_FILE_OVERWRITE:
            return "Overwrite";
        case SMB2_FILE_OVERWRITE_IF:
            return "OverwriteIf";
        default:
            return "Unknown";
    } /* switch */
} /* smb_create_disposition_name */

void
_smb_dump_request(
    int                         i,
    int                         n,
    struct chimera_smb_request *request)
{
    char  argstr[512];
    char  hdr_args[80];
    char *hdrp = hdr_args;

    *hdrp = '\0';

    if (request->smb2_hdr.session_id) {
        hdrp += sprintf(hdrp, " sessiond %lx", request->smb2_hdr.session_id);
    }

    if (request->smb2_hdr.sync.tree_id) {
        sprintf(hdrp, " tree_id %x", request->smb2_hdr.sync.tree_id);
    }

    switch (request->smb2_hdr.command) {
        case SMB2_TREE_CONNECT:
            sprintf(argstr, " path %.*s", request->tree_connect.path_length, request->tree_connect.path);
            break;
        case SMB2_CREATE:
            sprintf(argstr, " parent_path %.*s name %.*s create_disposition %s",
                    request->create.parent_path_len, request->create.parent_path,
                    request->create.name_len, request->create.name,
                    smb_create_disposition_name(request->create.create_disposition));
            break;
        case SMB2_CLOSE:
            if (request->close.file_id.pid != UINT64_MAX) {
                sprintf(argstr, " file_id %lx.%lx", request->close.file_id.pid, request->close.file_id.vid);
            }
            break;
        case SMB2_WRITE:
            sprintf(argstr, " file_id %lx.%lx offset %lu length %u write_through %d",
                    request->write.file_id.pid, request->write.file_id.vid,
                    request->write.offset, request->write.length,
                    !!(request->write.flags & SMB2_WRITEFLAG_WRITE_THROUGH));
            break;
        case SMB2_READ:
            sprintf(argstr, " file_id %lx.%lx offset %lu length %u",
                    request->read.file_id.pid, request->read.file_id.vid,
                    request->read.offset, request->read.length);
            break;
        case SMB2_IOCTL:
            sprintf(argstr, " file_id %lx.%lx ctl_code %u",
                    request->ioctl.file_id.pid, request->ioctl.file_id.vid,
                    request->ioctl.ctl_code);
            break;
        case SMB2_SET_INFO:
            sprintf(argstr, " file_id %lx.%lx info_type %u info_class %u addl_info %u",
                    request->set_info.file_id.pid, request->set_info.file_id.vid,
                    request->set_info.info_type, request->set_info.info_class,
                    request->set_info.addl_info);
            break;
        case SMB2_QUERY_INFO:
            sprintf(argstr, " file_id %lx.%lx info_type %u info_class %u addl_info %u flags %u",
                    request->query_info.file_id.pid, request->query_info.file_id.vid,
                    request->query_info.info_type, request->query_info.info_class,
                    request->query_info.addl_info, request->query_info.flags);
            break;
        case SMB2_QUERY_DIRECTORY:
            sprintf(argstr, " file_id %lx.%lx flags %x info_class %u file_index %u pattern %.*s",
                    request->query_directory.file_id.pid, request->query_directory.file_id.vid,
                    request->query_directory.flags, request->query_directory.info_class,
                    request->query_directory.file_index, request->query_directory.pattern_length,
                    request->query_directory.pattern);
            break;
        default:
            argstr[0] = '\0';
    } /* switch */

    chimera_smb_debug("SMB  Request %p: %d/%d %s%s%s", request, i, n,
                      smb_command_name(request->smb2_hdr.command),
                      hdr_args,
                      argstr);
} /* _smb_dump_request */

void
_smb_dump_reply(
    int                         i,
    int                         n,
    struct chimera_smb_request *request)
{
    char  argstr[512];
    char  hdr_args[80];
    char *hdrp = hdr_args;

    switch (request->smb2_hdr.command) {
        case SMB2_CREATE:
            if (request->status == SMB2_STATUS_SUCCESS) {
                sprintf(argstr, " file_id %lx.%lx",
                        request->create.r_open_file->file_id.pid,
                        request->create.r_open_file->file_id.vid);
            }
            break;
        default:
            argstr[0] = '\0';
    } /* switch */

    *hdrp = '\0';

    if (request->session) {
        hdrp += sprintf(hdrp, " sessiond %lx", request->session->session_id);
    }

    if (request->tree) {
        sprintf(hdrp, " tree_id %x", request->tree->tree_id);
    }

    chimera_smb_debug("SMB  Reply   %p: %d/%d %s %s%s%s",
                      request, i, n, smb_command_name(request->smb2_hdr.command),
                      smb_status_name(request->status), hdr_args, argstr);
} /* _smb_dump_request_reply */

void
_smb_dump_compound_request(struct chimera_smb_compound *compound)
{
    for (int i = 0; i < compound->num_requests; i++) {
        struct chimera_smb_request *request = compound->requests[i];
        _smb_dump_request(i + 1, compound->num_requests, request);
    }
} /* _smb_dump_compound_request */

void
_smb_dump_compound_reply(struct chimera_smb_compound *compound)
{
    for (int i = 0; i < compound->num_requests; i++) {
        struct chimera_smb_request *request = compound->requests[i];
        _smb_dump_reply(i + 1, compound->num_requests, request);
    }
} /* _smb_dump_compound_reply */