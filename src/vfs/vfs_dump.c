#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#include "vfs_dump.h"
#include "vfs.h"
#include "vfs_internal.h"
#include "common/format.h"
#include "vfs_open_cache.h"

#define CHIMERA_VFS_OP_LOOKUP_PATH 1
#define CHIMERA_VFS_OP_LOOKUP      2
#define CHIMERA_VFS_OP_GETATTR     3
#define CHIMERA_VFS_OP_READDIR     4
#define CHIMERA_VFS_OP_READLINK    5
#define CHIMERA_VFS_OP_OPEN        6
#define CHIMERA_VFS_OP_OPEN_AT     7
#define CHIMERA_VFS_OP_CLOSE       8
#define CHIMERA_VFS_OP_READ        9
#define CHIMERA_VFS_OP_WRITE       10

const char *
chimera_vfs_op_name(unsigned int opcode)
{
    switch (opcode) {
        case CHIMERA_VFS_OP_LOOKUP_PATH: return "LookupPath";
        case CHIMERA_VFS_OP_LOOKUP: return "Lookup";
        case CHIMERA_VFS_OP_GETATTR: return "GetAttr";
        case CHIMERA_VFS_OP_READDIR: return "ReadDir";
        case CHIMERA_VFS_OP_READLINK: return "ReadLink";
        case CHIMERA_VFS_OP_OPEN: return "Open";
        case CHIMERA_VFS_OP_OPEN_AT: return "OpenAt";
        case CHIMERA_VFS_OP_CLOSE: return "Close";
        case CHIMERA_VFS_OP_READ: return "Read";
        case CHIMERA_VFS_OP_WRITE: return "Write";
        case CHIMERA_VFS_OP_REMOVE: return "Remove";
        case CHIMERA_VFS_OP_MKDIR: return "Mkdir";
        case CHIMERA_VFS_OP_COMMIT: return "Commit";
        case CHIMERA_VFS_OP_ACCESS: return "Access";
        case CHIMERA_VFS_OP_SYMLINK: return "Symlink";
        case CHIMERA_VFS_OP_RENAME: return "Rename";
        case CHIMERA_VFS_OP_SETATTR: return "SetAttr";
        case CHIMERA_VFS_OP_LINK: return "Link";
        default: return "Unknown";
    } /* switch */

} /* chimera_vfs_op_name */

void
__chimera_vfs_dump_request(struct chimera_vfs_request *req)
{
    char argstr[80];
    char fhstr[80], fhstr2[80];

    switch (req->opcode) {
        case CHIMERA_VFS_OP_LOOKUP:
            snprintf(argstr, sizeof(argstr), "name %.*s",
                     req->lookup.component_len, req->lookup.component);
            break;
        case CHIMERA_VFS_OP_LOOKUP_PATH:
            snprintf(argstr, sizeof(argstr), "path %.*s",
                     req->lookup_path.pathlen, req->lookup_path.path);
            break;
        case CHIMERA_VFS_OP_SETATTR:
            snprintf(argstr, sizeof(argstr), "attrmask %lx",
                     req->setattr.attr_mask);
            break;
        case CHIMERA_VFS_OP_READDIR:
            snprintf(argstr, sizeof(argstr), "cookie %lu attrmask %lx",
                     req->readdir.cookie,
                     req->readdir.attrmask);
            break;
        case CHIMERA_VFS_OP_OPEN_AT:
            snprintf(argstr, sizeof(argstr), "name %.*s",
                     req->open_at.namelen,
                     req->open_at.name);
            break;
        case CHIMERA_VFS_OP_CLOSE:
            snprintf(argstr, sizeof(argstr), "hdl %lx",
                     req->close.vfs_private);
            break;
        case CHIMERA_VFS_OP_READ:
            snprintf(argstr, sizeof(argstr),
                     "hdl %lx offset %lu len %u",
                     req->read.handle->vfs_private,
                     req->read.offset,
                     req->read.length);
            break;
        case CHIMERA_VFS_OP_WRITE:
            snprintf(argstr, sizeof(argstr),
                     "hdl %lx offset %lu len %u",
                     req->write.handle->vfs_private,
                     req->write.offset,
                     req->write.length);
            break;
        case CHIMERA_VFS_OP_MKDIR:
            snprintf(argstr, sizeof(argstr), "name %.*s",
                     req->mkdir.name_len,
                     req->mkdir.name);
            break;
        case CHIMERA_VFS_OP_COMMIT:
            snprintf(argstr, sizeof(argstr), "hdl %lx",
                     req->commit.handle->vfs_private);
            break;
        case CHIMERA_VFS_OP_ACCESS:
            snprintf(argstr, sizeof(argstr),
                     "read %u write %u execute %u",
                     !!(req->access.access & CHIMERA_VFS_ACCESS_READ),
                     !!(req->access.access & CHIMERA_VFS_ACCESS_WRITE),
                     !!(req->access.access & CHIMERA_VFS_ACCESS_EXECUTE));
            break;
        case CHIMERA_VFS_OP_SYMLINK:
            snprintf(argstr, sizeof(argstr), "name %.*s target %.*s",
                     req->symlink.namelen,
                     req->symlink.name,
                     req->symlink.targetlen,
                     req->symlink.target);
            break;
        case CHIMERA_VFS_OP_RENAME:
            format_hex(fhstr2, sizeof(fhstr2), req->rename.new_fh, req->rename.new_fhlen)
            ;
            snprintf(argstr, sizeof(argstr),
                     "name %.*s new_fh %s newname %.*s",
                     req->rename.namelen,
                     req->rename.name,
                     fhstr2,
                     req->rename.new_namelen,
                     req->rename.new_name);
            break;
        case CHIMERA_VFS_OP_LINK:
            format_hex(fhstr2, sizeof(fhstr2),
                       req->link.dir_fh,
                       req->link.dir_fhlen);
            snprintf(argstr, sizeof(argstr),
                     "dir %s name %.*s",
                     fhstr2,
                     req->link.namelen,
                     req->link.name);
            break;
        default:
            argstr[0] = '\0';
            break;
    } /* switch */

    format_hex(fhstr, sizeof(fhstr), req->fh, req->fh_len);

    chimera_vfs_debug("VFS Request %p: %s %s/%s%s%s",
                      req,
                      chimera_vfs_op_name(req->opcode),
                      req->module->name,
                      fhstr,
                      argstr[0] ? " " : "",
                      argstr);
} /* chimera_vfs_dump_request */

void
__chimera_vfs_dump_reply(struct chimera_vfs_request *req)
{
    char argstr[80];
    char fhstr[80];

    argstr[0] = '\0';

    switch (req->opcode) {
        case CHIMERA_VFS_OP_ACCESS:
            snprintf(argstr, sizeof(argstr),
                     "read %u write %u execute %u",
                     !!(req->access.r_access & CHIMERA_VFS_ACCESS_READ),
                     !!(req->access.r_access & CHIMERA_VFS_ACCESS_WRITE),
                     !!(req->access.r_access & CHIMERA_VFS_ACCESS_EXECUTE));
            break;
        case CHIMERA_VFS_OP_LOOKUP_PATH:
            format_hex(fhstr, sizeof(fhstr), req->lookup_path.r_attr.va_fh, req->lookup_path.r_attr.va_fh_len);
            if (req->status == CHIMERA_VFS_OK) {
                snprintf(argstr, sizeof(argstr), "path %.*s r_fh %s",
                         req->lookup_path.pathlen,
                         req->lookup_path.path,
                         fhstr);
            }
            break;
        case CHIMERA_VFS_OP_LOOKUP:
            format_hex(fhstr, sizeof(fhstr), req->lookup.r_attr.va_fh, req->lookup.r_attr.va_fh_len);
            if (req->status == CHIMERA_VFS_OK) {
                snprintf(argstr, sizeof(argstr), "name %.*s r_fh %s",
                         req->lookup.component_len,
                         req->lookup.component,
                         fhstr);
            }
            break;
        case CHIMERA_VFS_OP_OPEN_AT:
            format_hex(fhstr, sizeof(fhstr), req->open_at.r_attr.va_fh, req->open_at.r_attr.va_fh_len);
            if (req->status == CHIMERA_VFS_OK) {
                snprintf(argstr, sizeof(argstr), "name %.*s r_fh %s",
                         req->open_at.namelen,
                         req->open_at.name,
                         fhstr);
            }
            break;
        case CHIMERA_VFS_OP_READDIR:
            if (req->status == CHIMERA_VFS_OK) {
                snprintf(argstr, sizeof(argstr), "cookie %lu eof %u",
                         req->readdir.r_cookie,
                         req->readdir.r_eof);
            }
            break;
        case CHIMERA_VFS_OP_READ:
            snprintf(argstr, sizeof(argstr), "r_len %u r_eof %u",
                     req->read.r_length,
                     req->read.r_eof);
            break;
        case CHIMERA_VFS_OP_WRITE:
            snprintf(argstr, sizeof(argstr), "r_len %u",
                     req->write.r_length);
            break;
        default:
            break;
    } /* switch */

    format_hex(fhstr, sizeof(fhstr), req->fh, req->fh_len);

    chimera_vfs_debug("VFS Reply   %p: %s %s/%s%s%s status %d (%s) elapsed %lu ns",
                      req,
                      chimera_vfs_op_name(req->opcode),
                      req->module->name,
                      fhstr,
                      argstr[0] ? " " : "",
                      argstr,
                      req->status,
                      req->status ? strerror(req->status) : "OK",
                      req->elapsed_ns);
} /* chimera_vfs_dump_reply */

