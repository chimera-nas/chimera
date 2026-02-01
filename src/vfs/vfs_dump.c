// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#include "vfs_dump.h"
#include "vfs.h"
#include "vfs_internal.h"
#include "common/format.h"
#include "vfs_open_cache.h"
#include "common/snprintf.h"

const char *
chimera_vfs_op_name(unsigned int opcode)
{
    switch (opcode) {
        case CHIMERA_VFS_OP_MOUNT: return "Mount";
        case CHIMERA_VFS_OP_UMOUNT: return "Umount";
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
        case CHIMERA_VFS_OP_SYMLINK: return "Symlink";
        case CHIMERA_VFS_OP_RENAME: return "Rename";
        case CHIMERA_VFS_OP_SETATTR: return "SetAttr";
        case CHIMERA_VFS_OP_LINK: return "Link";
        case CHIMERA_VFS_OP_CREATE_UNLINKED: return "CreateUnlinked";
        default: return "Unknown";
    } /* switch */

} /* chimera_vfs_op_name */

void
__chimera_vfs_dump_request(struct chimera_vfs_request *req)
{
    char argstr[256];
    char fhstr[80], fhstr2[80];
    char namestr[128], namestr2[128];

    switch (req->opcode) {
        case CHIMERA_VFS_OP_MOUNT:
            chimera_snprintf(argstr, sizeof(argstr), "path %s:%s@%s attrmask %lx",
                             req->mount.module->name,
                             req->mount.path,
                             req->mount.mount_path,
                             req->mount.r_attr.va_req_mask);
            break;
        case CHIMERA_VFS_OP_UMOUNT:
            chimera_snprintf(argstr, sizeof(argstr), "private %p",
                             req->umount.mount_private);
            break;
        case CHIMERA_VFS_OP_LOOKUP:
            format_safe_name(namestr, sizeof(namestr),
                             req->lookup.component, req->lookup.component_len);
            chimera_snprintf(argstr, sizeof(argstr), "name %s attrmask %lx dir_attr_mask %lx",
                             namestr,
                             req->lookup.r_attr.va_req_mask,
                             req->lookup.r_dir_attr.va_req_mask);
            break;
        case CHIMERA_VFS_OP_GETATTR:
            chimera_snprintf(argstr, sizeof(argstr), "attrmask %lx",
                             req->getattr.r_attr.va_req_mask);
            break;
        case CHIMERA_VFS_OP_SETATTR:
            chimera_snprintf(argstr, sizeof(argstr), "attrmask %lx",
                             req->setattr.set_attr->va_req_mask);
            break;
        case CHIMERA_VFS_OP_READDIR:
            chimera_snprintf(argstr, sizeof(argstr), "cookie %lu attrmask %lx",
                             req->readdir.cookie,
                             req->readdir.attr_mask);
            break;
        case CHIMERA_VFS_OP_OPEN_AT:
            format_safe_name(namestr, sizeof(namestr),
                             req->open_at.name, req->open_at.namelen);
            chimera_snprintf(argstr, sizeof(argstr), "name %s flags %08x",
                             namestr,
                             req->open_at.flags);
            break;
        case CHIMERA_VFS_OP_CLOSE:
            chimera_snprintf(argstr, sizeof(argstr), "hdl %lx",
                             req->close.vfs_private);
            break;
        case CHIMERA_VFS_OP_READ:
            chimera_snprintf(argstr, sizeof(argstr),
                             "hdl %lx offset %lu len %u",
                             req->read.handle->vfs_private,
                             req->read.offset,
                             req->read.length);
            break;
        case CHIMERA_VFS_OP_WRITE:
            chimera_snprintf(argstr, sizeof(argstr),
                             "hdl %lx offset %lu len %u sync %d",
                             req->write.handle->vfs_private,
                             req->write.offset,
                             req->write.length,
                             req->write.sync);
            break;
        case CHIMERA_VFS_OP_MKDIR:
            format_safe_name(namestr, sizeof(namestr),
                             req->mkdir.name, req->mkdir.name_len);
            chimera_snprintf(argstr, sizeof(argstr), "name %s",
                             namestr);
            break;
        case CHIMERA_VFS_OP_REMOVE:
            format_safe_name(namestr, sizeof(namestr),
                             req->remove.name, req->remove.namelen);
            chimera_snprintf(argstr, sizeof(argstr), "name %s",
                             namestr);
            break;
        case CHIMERA_VFS_OP_COMMIT:
            chimera_snprintf(argstr, sizeof(argstr), "hdl %lx",
                             req->commit.handle->vfs_private);
            break;
        case CHIMERA_VFS_OP_SYMLINK:
            format_safe_name(namestr, sizeof(namestr),
                             req->symlink.name, req->symlink.namelen);
            format_safe_name(namestr2, sizeof(namestr2),
                             req->symlink.target, req->symlink.targetlen);
            chimera_snprintf(argstr, sizeof(argstr), "name %s target %s",
                             namestr,
                             namestr2);
            break;
        case CHIMERA_VFS_OP_RENAME:
            format_hex(fhstr2, sizeof(fhstr2), req->rename.new_fh, req->rename.new_fhlen);
            format_safe_name(namestr, sizeof(namestr),
                             req->rename.name, req->rename.namelen);
            format_safe_name(namestr2, sizeof(namestr2),
                             req->rename.new_name, req->rename.new_namelen);
            chimera_snprintf(argstr, sizeof(argstr),
                             "name %s new_fh %s newname %s",
                             namestr,
                             fhstr2,
                             namestr2);
            break;
        case CHIMERA_VFS_OP_LINK:
            format_hex(fhstr2, sizeof(fhstr2),
                       req->link.dir_fh,
                       req->link.dir_fhlen);
            format_safe_name(namestr, sizeof(namestr),
                             req->link.name, req->link.namelen);
            chimera_snprintf(argstr, sizeof(argstr),
                             "dir %s name %s",
                             fhstr2,
                             namestr);
            break;
        default:
            argstr[0] = '\0';
            break;
    } /* switch */

    format_hex(fhstr, sizeof(fhstr), req->fh, req->fh_len);

    chimera_vfs_debug("VFS  Request %p: %s %s%s%s",
                      req,
                      chimera_vfs_op_name(req->opcode),
                      fhstr,
                      argstr[0] ? " " : "",
                      argstr);
} /* chimera_vfs_dump_request */ /* chimera_vfs_dump_request */

void
__chimera_vfs_dump_reply(struct chimera_vfs_request *req)
{
    char        argstr[256];
    char        fhstr[80];
    char        namestr[128];
    const char *fhstr_ptr;

    argstr[0] = '\0';

    switch (req->opcode) {
        case CHIMERA_VFS_OP_MOUNT:
            if (req->mount.r_attr.va_set_mask & CHIMERA_VFS_ATTR_FH) {
                format_hex(fhstr, sizeof(fhstr), req->mount.r_attr.va_fh, req->mount.r_attr.va_fh_len);
                fhstr_ptr = fhstr;
            } else {
                fhstr_ptr = "UNSET";
            }
            chimera_snprintf(argstr, sizeof(argstr), "r_fh %s", fhstr_ptr);
            break;
        case CHIMERA_VFS_OP_LOOKUP:
            if (req->lookup.r_attr.va_set_mask & CHIMERA_VFS_ATTR_FH) {
                format_hex(fhstr, sizeof(fhstr), req->lookup.r_attr.va_fh, req->lookup.r_attr.va_fh_len);
                fhstr_ptr = fhstr;
            } else {
                fhstr_ptr = "UNSET";
            }
            format_safe_name(namestr, sizeof(namestr),
                             req->lookup.component, req->lookup.component_len);
            chimera_snprintf(argstr, sizeof(argstr), "name %s r_fh %s",
                             namestr,
                             fhstr_ptr);
            break;
        case CHIMERA_VFS_OP_GETATTR:
            chimera_snprintf(argstr, sizeof(argstr), "r_attr %lx", req->getattr.r_attr.va_set_mask);
            break;
        case CHIMERA_VFS_OP_OPEN_AT:
            if (req->open_at.r_attr.va_set_mask & CHIMERA_VFS_ATTR_FH) {
                format_hex(fhstr, sizeof(fhstr), req->open_at.r_attr.va_fh, req->open_at.r_attr.va_fh_len);
                fhstr_ptr = fhstr;
            } else {
                fhstr_ptr = "UNSET";
            }
            format_safe_name(namestr, sizeof(namestr),
                             req->open_at.name, req->open_at.namelen);
            chimera_snprintf(argstr, sizeof(argstr), "name %s r_fh %s",
                             namestr,
                             fhstr_ptr);
            break;
        case CHIMERA_VFS_OP_CREATE_UNLINKED:
            if (req->create_unlinked.r_attr.va_set_mask & CHIMERA_VFS_ATTR_FH) {
                format_hex(fhstr, sizeof(fhstr), req->create_unlinked.r_attr.va_fh, req->create_unlinked.r_attr.
                           va_fh_len);
                fhstr_ptr = fhstr;
            } else {
                fhstr_ptr = "UNSET";
            }
            chimera_snprintf(argstr, sizeof(argstr), "r_fh %s", fhstr_ptr);
            break;
        case CHIMERA_VFS_OP_MKDIR:
            if (req->mkdir.r_attr.va_set_mask & CHIMERA_VFS_ATTR_FH) {
                format_hex(fhstr, sizeof(fhstr), req->mkdir.r_attr.va_fh, req->mkdir.r_attr.va_fh_len);
                fhstr_ptr = fhstr;
            } else {
                fhstr_ptr = "UNSET";
            }
            format_safe_name(namestr, sizeof(namestr),
                             req->mkdir.name, req->mkdir.name_len);
            chimera_snprintf(argstr, sizeof(argstr), "name %s r_fh %s",
                             namestr,
                             fhstr_ptr);
            break;
        case CHIMERA_VFS_OP_READDIR:
            if (req->status == CHIMERA_VFS_OK) {
                chimera_snprintf(argstr, sizeof(argstr), "cookie %lu eof %u",
                                 req->readdir.r_cookie,
                                 req->readdir.r_eof);
            }
            break;
        case CHIMERA_VFS_OP_READ:
            chimera_snprintf(argstr, sizeof(argstr), "r_len %u r_eof %u",
                             req->read.r_length,
                             req->read.r_eof);
            break;
        case CHIMERA_VFS_OP_WRITE:
            chimera_snprintf(argstr, sizeof(argstr), "r_len %u",
                             req->write.r_length);
            break;
        default:
            break;
    } /* switch */

    format_hex(fhstr, sizeof(fhstr), req->fh, req->fh_len);

    chimera_vfs_debug("VFS  Reply   %p: %s %s%s%s status %d (%s) elapsed %lu ns",
                      req,
                      chimera_vfs_op_name(req->opcode),
                      fhstr,
                      argstr[0] ? " " : "",
                      argstr,
                      req->status,
                      req->status ? strerror(req->status) : "OK",
                      req->elapsed_ns);
} /* chimera_vfs_dump_reply */ /* chimera_vfs_dump_reply */

