#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#include "vfs_dump.h"
#include "vfs.h"
#include "vfs_internal.h"
#include "common/format.h"

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
            format_hex(fhstr, sizeof(fhstr), req->lookup.fh, req->lookup.fh_len)
            ;
            snprintf(argstr, sizeof(argstr), "fh %s name %.*s",
                     fhstr,
                     req->lookup.component_len, req->lookup.component);
            break;
        case CHIMERA_VFS_OP_LOOKUP_PATH:
            snprintf(argstr, sizeof(argstr), "path %.*s",
                     req->lookup_path.pathlen, req->lookup_path.path);
            break;
        case CHIMERA_VFS_OP_GETATTR:
            format_hex(fhstr, sizeof(fhstr), req->getattr.fh, req->getattr.
                       fh_len);
            snprintf(argstr, sizeof(argstr), "fh %s", fhstr);
            break;
        case CHIMERA_VFS_OP_READDIR:
            format_hex(fhstr, sizeof(fhstr), req->readdir.fh, req->readdir.
                       fh_len);
            snprintf(argstr, sizeof(argstr), "fh %s", fhstr);
            break;
        case CHIMERA_VFS_OP_READLINK:
            format_hex(fhstr, sizeof(fhstr), req->readlink.fh, req->readlink.
                       fh_len);
            snprintf(argstr, sizeof(argstr), "fh %s", fhstr);
            break;
        case CHIMERA_VFS_OP_OPEN:
            format_hex(fhstr, sizeof(fhstr), req->open.fh, req->open.fh_len);
            snprintf(argstr, sizeof(argstr), "fh %s", fhstr);
            break;
        case CHIMERA_VFS_OP_OPEN_AT:
            format_hex(fhstr, sizeof(fhstr), req->open_at.parent_fh, req->
                       open_at.
                       parent_fh_len);
            snprintf(argstr, sizeof(argstr), "fh %s name %.*s",
                     fhstr,
                     req->open_at.namelen,
                     req->open_at.name);
            break;
        case CHIMERA_VFS_OP_CLOSE:
            format_hex(fhstr, sizeof(fhstr), req->close.handle->fh, req->close.
                       handle->fh_len);
            snprintf(argstr, sizeof(argstr), "fh %s hdl %lx",
                     fhstr,
                     req->close.handle->vfs_private);
            break;
        case CHIMERA_VFS_OP_READ:
            format_hex(fhstr, sizeof(fhstr), req->read.handle->fh, req->read.
                       handle->fh_len);
            snprintf(argstr, sizeof(argstr),
                     "fh %s hdl %lx offset %lu len %u",
                     fhstr,
                     req->read.handle->vfs_private,
                     req->read.offset,
                     req->read.length);
            break;
        case CHIMERA_VFS_OP_WRITE:
            format_hex(fhstr, sizeof(fhstr), req->write.handle->fh, req->write.
                       handle->fh_len);
            snprintf(argstr, sizeof(argstr),
                     "fh %s hdl %lx offset %lu len %u",
                     fhstr,
                     req->write.handle->vfs_private,
                     req->write.offset,
                     req->write.length);
            break;
        case CHIMERA_VFS_OP_REMOVE:
            format_hex(fhstr, sizeof(fhstr), req->remove.fh, req->remove.fh_len)
            ;
            snprintf(argstr, sizeof(argstr), "fh %s", fhstr);
            break;
        case CHIMERA_VFS_OP_MKDIR:
            format_hex(fhstr, sizeof(fhstr), req->mkdir.fh, req->mkdir.fh_len);
            snprintf(argstr, sizeof(argstr), "fh %s name %.*s",
                     fhstr,
                     req->mkdir.name_len,
                     req->mkdir.name);
            break;
        case CHIMERA_VFS_OP_COMMIT:
            format_hex(fhstr, sizeof(fhstr), req->commit.handle->fh, req->commit
                       .
                       handle->fh_len);
            snprintf(argstr, sizeof(argstr), "fh %s hdl %lx",
                     fhstr,
                     req->commit.handle->vfs_private);
            break;
        case CHIMERA_VFS_OP_ACCESS:
            format_hex(fhstr, sizeof(fhstr), req->access.fh, req->access.fh_len)
            ;
            snprintf(argstr, sizeof(argstr), "fh %s", fhstr);
            break;
        case CHIMERA_VFS_OP_SYMLINK:
            format_hex(fhstr, sizeof(fhstr), req->symlink.fh, req->symlink.
                       fh_len)
            ;
            snprintf(argstr, sizeof(argstr), "fh %s name %.*s target %.*s",
                     fhstr,
                     req->symlink.namelen,
                     req->symlink.name,
                     req->symlink.targetlen,
                     req->symlink.target);
            break;
        case CHIMERA_VFS_OP_RENAME:
            format_hex(fhstr, sizeof(fhstr), req->rename.fh, req->rename.fh_len)
            ;
            snprintf(argstr, sizeof(argstr),
                     "fh %s name %.*s new_fh %s newname %.*s",
                     fhstr,
                     req->rename.namelen,
                     req->rename.name,
                     fhstr2,
                     req->rename.new_namelen,
                     req->rename.new_name);
            break;
        case CHIMERA_VFS_OP_LINK:
            format_hex(fhstr, sizeof(fhstr), req->link.fh, req->link.fh_len);
            format_hex(fhstr2, sizeof(fhstr2), req->link.dir_fh, req->link.
                       dir_fhlen);
            snprintf(argstr, sizeof(argstr), "fh %s dir %s name %.*s",
                     fhstr,
                     fhstr2,
                     req->link.namelen,
                     req->link.name);
            break;
        default:
            argstr[0] = '\0';
            break;
    } /* switch */

    chimera_vfs_debug("VFS Request %p: %s %s",
                      req,
                      chimera_vfs_op_name(req->opcode),
                      argstr);
} /* chimera_vfs_dump_request */

void
__chimera_vfs_dump_reply(struct chimera_vfs_request *req)
{
    char argstr[80];
    char fhstr[80];

    argstr[0] = '\0';

    switch (req->opcode) {
        case CHIMERA_VFS_OP_LOOKUP:
            if (req->status == CHIMERA_VFS_OK) {
                format_hex(fhstr, sizeof(fhstr),
                           req->lookup.r_fh,
                           req->lookup.r_fh_len);
                snprintf(argstr, sizeof(argstr), "fh %s", fhstr);
            }
            break;
        case CHIMERA_VFS_OP_OPEN_AT:
            if (req->status == CHIMERA_VFS_OK) {
                format_hex(fhstr, sizeof(fhstr),
                           req->open_at.fh,
                           req->open_at.fh_len);
                snprintf(argstr, sizeof(argstr), "fh %s", fhstr);
            }
            break;
        default:
            break;
    } /* switch */

    chimera_vfs_debug("VFS Reply   %p: %s %s status %d (%s) elapsed %lu ns",
                      req,
                      chimera_vfs_op_name(req->opcode),
                      argstr,
                      req->status,
                      req->status ? strerror(req->status) : "OK",
                      req->elapsed_ns);
} /* chimera_vfs_dump_reply */

