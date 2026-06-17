// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#include "vfs_dump.h"
#include "vfs.h"
#include "vfs_internal.h"
#include "common/macros.h"
#include "common/format.h"
#include "vfs_open_cache.h"
#include "common/snprintf.h"

const char *
chimera_vfs_op_name(unsigned int opcode)
{
    switch (opcode) {
        case CHIMERA_VFS_OP_MOUNT: return "Mount";
        case CHIMERA_VFS_OP_UMOUNT: return "Umount";
        case CHIMERA_VFS_OP_LOOKUP_AT: return "Lookup";
        case CHIMERA_VFS_OP_GETATTR: return "GetAttr";
        case CHIMERA_VFS_OP_READDIR: return "ReadDir";
        case CHIMERA_VFS_OP_READLINK: return "ReadLink";
        case CHIMERA_VFS_OP_OPEN_FH: return "Open";
        case CHIMERA_VFS_OP_OPEN_AT: return "OpenAt";
        case CHIMERA_VFS_OP_CLOSE: return "Close";
        case CHIMERA_VFS_OP_READ: return "Read";
        case CHIMERA_VFS_OP_WRITE: return "Write";
        case CHIMERA_VFS_OP_REMOVE_AT: return "Remove";
        case CHIMERA_VFS_OP_MKDIR_AT: return "Mkdir";
        case CHIMERA_VFS_OP_COMMIT: return "Commit";
        case CHIMERA_VFS_OP_SYMLINK_AT: return "Symlink";
        case CHIMERA_VFS_OP_RENAME_AT: return "Rename";
        case CHIMERA_VFS_OP_SETATTR: return "SetAttr";
        case CHIMERA_VFS_OP_LINK_AT: return "Link";
        case CHIMERA_VFS_OP_CREATE_UNLINKED: return "CreateUnlinked";
        case CHIMERA_VFS_OP_ALLOCATE: return "Allocate";
        case CHIMERA_VFS_OP_SEEK: return "Seek";
        case CHIMERA_VFS_OP_LOCK: return "Lock";
        case CHIMERA_VFS_OP_COPY_RANGE: return "CopyRange";
        case CHIMERA_VFS_OP_CLONE_RANGE: return "CloneRange";
        case CHIMERA_VFS_OP_MOVE_RANGE: return "MoveRange";
        case CHIMERA_VFS_OP_GET_XATTR: return "GetXattr";
        case CHIMERA_VFS_OP_SET_XATTR: return "SetXattr";
        case CHIMERA_VFS_OP_LIST_XATTRS: return "ListXattrs";
        case CHIMERA_VFS_OP_REMOVE_XATTR: return "RemoveXattr";
        case CHIMERA_VFS_OP_GET_LAYOUT: return "GetLayout";
        case CHIMERA_VFS_OP_OPEN_STREAM: return "OpenStream";
        case CHIMERA_VFS_OP_LIST_STREAMS: return "ListStreams";
        case CHIMERA_VFS_OP_REMOVE_STREAM: return "RemoveStream";
        default: return "Unknown";
    } /* switch */

} /* chimera_vfs_op_name */

#if CHIMERA_HAVE_OTEL

/* Hex buffer big enough for any VFS file handle (incl. the +16 slack on va_fh). */
#define VFS_TRACE_FH_HEX    (2 * (CHIMERA_VFS_FH_SIZE + 16) + 1)

/* Opaque values (xattr value, symlink target) are usually short but may be large;
* cap what we copy into the span arena.  The full length is recorded alongside. */
#define VFS_TRACE_VALUE_MAX 64

/* Emit the populated scalar fields of an attrs block as "<prefix>.*" attributes,
 * gated by va_set_mask (which the backend sets for the fields it actually filled).
 * Scalars only -- never the ACL or pNFS blob -- so the span arena stays bounded. */
static void
vfs_trace_attrs(
    struct otel_span               *s,
    const char                     *prefix,
    const struct chimera_vfs_attrs *a)
{
    uint64_t m = a->va_set_mask;
    char     key[40];

    if (m & CHIMERA_VFS_ATTR_SIZE) {
        snprintf(key, sizeof(key), "%s.size", prefix);
        otel_span_attr_u64(s, key, a->va_size);
    }
    if (m & CHIMERA_VFS_ATTR_MODE) {
        snprintf(key, sizeof(key), "%s.mode", prefix);
        otel_span_attr_u64(s, key, a->va_mode);
    }
    if (m & CHIMERA_VFS_ATTR_NLINK) {
        snprintf(key, sizeof(key), "%s.nlink", prefix);
        otel_span_attr_u64(s, key, a->va_nlink);
    }
    if (m & CHIMERA_VFS_ATTR_UID) {
        snprintf(key, sizeof(key), "%s.uid", prefix);
        otel_span_attr_u64(s, key, a->va_uid);
    }
    if (m & CHIMERA_VFS_ATTR_GID) {
        snprintf(key, sizeof(key), "%s.gid", prefix);
        otel_span_attr_u64(s, key, a->va_gid);
    }
    if (m & CHIMERA_VFS_ATTR_INUM) {
        snprintf(key, sizeof(key), "%s.inum", prefix);
        otel_span_attr_u64(s, key, a->va_ino);
    }
    if (m & CHIMERA_VFS_ATTR_FH) {
        char hex[VFS_TRACE_FH_HEX];
        snprintf(key, sizeof(key), "%s.fh", prefix);
        format_hex(hex, sizeof(hex), a->va_fh, a->va_fh_len);
        otel_span_attr_str(s, key, hex);
    }
} /* vfs_trace_attrs */

/* Emit an opaque value (xattr value, symlink target) truncated to a safe length,
 * plus its full byte length under `len_key`. */
static void
vfs_trace_value(
    struct otel_span *s,
    const char       *key,
    const char       *len_key,
    const void       *val,
    uint32_t          len)
{
    if (val && len) {
        otel_span_attr_strn(s, key, val,
                            len > VFS_TRACE_VALUE_MAX ? VFS_TRACE_VALUE_MAX : len);
    }
    otel_span_attr_u64(s, len_key, len);
} /* vfs_trace_value */

SYMBOL_EXPORT void
_chimera_vfs_trace_complete(struct chimera_vfs_request *request)
{
    struct otel_span *s = &request->otel;

    char              fhhex[VFS_TRACE_FH_HEX];

    otel_span_set_name(s, chimera_vfs_op_name(request->opcode));

    format_hex(fhhex, sizeof(fhhex), request->fh, request->fh_len);
    otel_span_attr_str(s, "vfs.fh", fhhex);
    otel_span_attr_i64(s, "vfs.status", request->status);

    /* Per-op request + response (r_*) attributes.  File data (read/write iov) and
     * directory entries (readdir) are deliberately never attached; opaque values
     * (xattr/symlink) are truncated. */
    switch (request->opcode) {
        case CHIMERA_VFS_OP_LOOKUP_AT:
            otel_span_attr_strn(s, "vfs.name", request->lookup_at.component,
                                request->lookup_at.component_len);
            vfs_trace_attrs(s, "vfs", &request->lookup_at.r_attr);
            break;
        case CHIMERA_VFS_OP_GETATTR:
            vfs_trace_attrs(s, "vfs", &request->getattr.r_attr);
            break;
        case CHIMERA_VFS_OP_SETATTR:
            if (request->setattr.set_attr) {
                otel_span_attr_u64(s, "vfs.set_mask",
                                   request->setattr.set_attr->va_set_mask);
            }
            vfs_trace_attrs(s, "vfs", &request->setattr.r_post_attr);
            break;
        case CHIMERA_VFS_OP_READ:
            otel_span_attr_u64(s, "vfs.offset", request->read.offset);
            otel_span_attr_u64(s, "vfs.length", request->read.length);
            otel_span_attr_u64(s, "vfs.bytes", request->read.r_length);
            otel_span_attr_bool(s, "vfs.eof", request->read.r_eof);
            break;
        case CHIMERA_VFS_OP_WRITE:
            otel_span_attr_u64(s, "vfs.offset", request->write.offset);
            otel_span_attr_u64(s, "vfs.length", request->write.length);
            otel_span_attr_u64(s, "vfs.bytes", request->write.r_length);
            otel_span_attr_u64(s, "vfs.sync", request->write.sync);
            break;
        case CHIMERA_VFS_OP_COMMIT:
            otel_span_attr_u64(s, "vfs.offset", request->commit.offset);
            otel_span_attr_u64(s, "vfs.length", request->commit.length);
            break;
        case CHIMERA_VFS_OP_OPEN_AT:
            otel_span_attr_strn(s, "vfs.name", request->open_at.name,
                                request->open_at.namelen);
            otel_span_attr_u64(s, "vfs.flags", request->open_at.flags);
            otel_span_attr_bool(s, "vfs.created", request->open_at.r_created);
            vfs_trace_attrs(s, "vfs", &request->open_at.r_attr);
            break;
        case CHIMERA_VFS_OP_OPEN_FH:
            otel_span_attr_u64(s, "vfs.flags", request->open_fh.flags);
            break;
        case CHIMERA_VFS_OP_MKDIR_AT:
            otel_span_attr_strn(s, "vfs.name", request->mkdir_at.name,
                                request->mkdir_at.name_len);
            vfs_trace_attrs(s, "vfs", &request->mkdir_at.r_attr);
            break;
        case CHIMERA_VFS_OP_MKNOD_AT:
            otel_span_attr_strn(s, "vfs.name", request->mknod_at.name,
                                request->mknod_at.name_len);
            vfs_trace_attrs(s, "vfs", &request->mknod_at.r_attr);
            break;
        case CHIMERA_VFS_OP_SYMLINK_AT:
            otel_span_attr_strn(s, "vfs.name", request->symlink_at.name,
                                request->symlink_at.namelen);
            vfs_trace_value(s, "vfs.target", "vfs.target_len",
                            request->symlink_at.target, request->symlink_at.targetlen);
            vfs_trace_attrs(s, "vfs", &request->symlink_at.r_attr);
            break;
        case CHIMERA_VFS_OP_REMOVE_AT:
            otel_span_attr_strn(s, "vfs.name", request->remove_at.name,
                                request->remove_at.namelen);
            break;
        case CHIMERA_VFS_OP_RENAME_AT:
            otel_span_attr_strn(s, "vfs.name", request->rename_at.name,
                                request->rename_at.namelen);
            otel_span_attr_strn(s, "vfs.new_name", request->rename_at.new_name,
                                request->rename_at.new_namelen);
            break;
        case CHIMERA_VFS_OP_LINK_AT:
            otel_span_attr_strn(s, "vfs.name", request->link_at.name,
                                request->link_at.namelen);
            vfs_trace_attrs(s, "vfs", &request->link_at.r_attr);
            break;
        case CHIMERA_VFS_OP_READDIR:
            otel_span_attr_u64(s, "vfs.cookie", request->readdir.cookie);
            otel_span_attr_u64(s, "vfs.r_cookie", request->readdir.r_cookie);
            otel_span_attr_bool(s, "vfs.eof", request->readdir.r_eof);
            break;
        case CHIMERA_VFS_OP_READLINK:
            vfs_trace_value(s, "vfs.target", "vfs.target_len",
                            request->readlink.r_target,
                            request->readlink.r_target_length);
            break;
        case CHIMERA_VFS_OP_ALLOCATE:
            otel_span_attr_u64(s, "vfs.offset", request->allocate.offset);
            otel_span_attr_u64(s, "vfs.length", request->allocate.length);
            break;
        case CHIMERA_VFS_OP_SEEK:
            otel_span_attr_u64(s, "vfs.offset", request->seek.offset);
            otel_span_attr_u64(s, "vfs.what", request->seek.what);
            otel_span_attr_u64(s, "vfs.r_offset", request->seek.r_offset);
            otel_span_attr_bool(s, "vfs.eof", request->seek.r_eof);
            break;
        case CHIMERA_VFS_OP_LOCK:
            otel_span_attr_u64(s, "vfs.offset", request->lock.offset);
            otel_span_attr_u64(s, "vfs.length", request->lock.length);
            otel_span_attr_u64(s, "vfs.lock_type", request->lock.lock_type);
            break;
        case CHIMERA_VFS_OP_COPY_RANGE:
            otel_span_attr_u64(s, "vfs.src_offset", request->copy_range.src_offset);
            otel_span_attr_u64(s, "vfs.dst_offset", request->copy_range.dst_offset);
            otel_span_attr_u64(s, "vfs.length", request->copy_range.length);
            otel_span_attr_u64(s, "vfs.bytes", request->copy_range.r_length);
            break;
        case CHIMERA_VFS_OP_CLONE_RANGE:
            otel_span_attr_u64(s, "vfs.src_offset", request->clone_range.src_offset);
            otel_span_attr_u64(s, "vfs.dst_offset", request->clone_range.dst_offset);
            otel_span_attr_u64(s, "vfs.length", request->clone_range.length);
            break;
        case CHIMERA_VFS_OP_MOVE_RANGE:
            otel_span_attr_u64(s, "vfs.src_offset", request->move_range.src_offset);
            otel_span_attr_u64(s, "vfs.dst_offset", request->move_range.dst_offset);
            otel_span_attr_u64(s, "vfs.length", request->move_range.length);
            break;
        case CHIMERA_VFS_OP_GET_XATTR:
            otel_span_attr_strn(s, "vfs.name", request->get_xattr.name,
                                request->get_xattr.namelen);
            vfs_trace_value(s, "vfs.value", "vfs.value_len",
                            request->get_xattr.value, request->get_xattr.r_value_len);
            break;
        case CHIMERA_VFS_OP_SET_XATTR:
            otel_span_attr_strn(s, "vfs.name", request->set_xattr.name,
                                request->set_xattr.namelen);
            vfs_trace_value(s, "vfs.value", "vfs.value_len",
                            request->set_xattr.value, request->set_xattr.value_len);
            break;
        case CHIMERA_VFS_OP_LIST_XATTRS:
            otel_span_attr_u64(s, "vfs.count", request->list_xattrs.r_count);
            otel_span_attr_bool(s, "vfs.eof", request->list_xattrs.r_eof);
            break;
        case CHIMERA_VFS_OP_REMOVE_XATTR:
            otel_span_attr_strn(s, "vfs.name", request->remove_xattr.name,
                                request->remove_xattr.namelen);
            break;
        case CHIMERA_VFS_OP_OPEN_STREAM:
            otel_span_attr_strn(s, "vfs.name", request->open_stream.name,
                                request->open_stream.namelen);
            vfs_trace_attrs(s, "vfs", &request->open_stream.r_attr);
            break;
        case CHIMERA_VFS_OP_LIST_STREAMS:
            otel_span_attr_u64(s, "vfs.count", request->list_streams.r_count);
            otel_span_attr_bool(s, "vfs.eof", request->list_streams.r_eof);
            break;
        case CHIMERA_VFS_OP_REMOVE_STREAM:
            otel_span_attr_strn(s, "vfs.name", request->remove_stream.name,
                                request->remove_stream.namelen);
            break;
        default:
            break;
    } /* switch */

    if (request->status != CHIMERA_VFS_OK) {
        otel_span_set_status(s, OTEL_STATUS_ERROR, NULL);
    }

    otel_span_end(s);
} /* _chimera_vfs_trace_complete */
#endif /* CHIMERA_HAVE_OTEL */

void
__chimera_vfs_dump_request(struct chimera_vfs_request *req)
{
    char argstr[256];
    char fhstr[80], fhstr2[80];
    char namestr[FORMAT_SAFE_NAME_MAX], namestr2[FORMAT_SAFE_NAME_MAX];

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
        case CHIMERA_VFS_OP_LOOKUP_AT:
            format_safe_name(namestr, sizeof(namestr),
                             req->lookup_at.component, req->lookup_at.component_len);
            chimera_snprintf(argstr, sizeof(argstr), "name %s attrmask %lx dir_attr_mask %lx",
                             namestr,
                             req->lookup_at.r_attr.va_req_mask,
                             req->lookup_at.r_dir_attr.va_req_mask);
            break;
        case CHIMERA_VFS_OP_GETATTR:
            chimera_snprintf(argstr, sizeof(argstr), "attrmask %lx",
                             req->getattr.r_attr.va_req_mask);
            break;
        case CHIMERA_VFS_OP_SETATTR:
            chimera_snprintf(argstr, sizeof(argstr), "set_mask %lx",
                             req->setattr.set_attr->va_set_mask);
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
        case CHIMERA_VFS_OP_MKDIR_AT:
            format_safe_name(namestr, sizeof(namestr),
                             req->mkdir_at.name, req->mkdir_at.name_len);
            chimera_snprintf(argstr, sizeof(argstr), "name %s",
                             namestr);
            break;
        case CHIMERA_VFS_OP_REMOVE_AT:
            format_safe_name(namestr, sizeof(namestr),
                             req->remove_at.name, req->remove_at.namelen);
            chimera_snprintf(argstr, sizeof(argstr), "name %s",
                             namestr);
            break;
        case CHIMERA_VFS_OP_COMMIT:
            chimera_snprintf(argstr, sizeof(argstr), "hdl %lx",
                             req->commit.handle->vfs_private);
            break;
        case CHIMERA_VFS_OP_ALLOCATE:
            chimera_snprintf(argstr, sizeof(argstr),
                             "hdl %lx offset %lu len %lu flags %u",
                             req->allocate.handle->vfs_private,
                             req->allocate.offset,
                             req->allocate.length,
                             req->allocate.flags);
            break;
        case CHIMERA_VFS_OP_COPY_RANGE:
            chimera_snprintf(argstr, sizeof(argstr),
                             "src_hdl %lx src_off %lu dst_hdl %lx dst_off %lu len %lu",
                             req->copy_range.src_handle->vfs_private,
                             req->copy_range.src_offset,
                             req->copy_range.dst_handle->vfs_private,
                             req->copy_range.dst_offset,
                             req->copy_range.length);
            break;
        case CHIMERA_VFS_OP_CLONE_RANGE:
            chimera_snprintf(argstr, sizeof(argstr),
                             "src_hdl %lx src_off %lu dst_hdl %lx dst_off %lu len %lu",
                             req->clone_range.src_handle->vfs_private,
                             req->clone_range.src_offset,
                             req->clone_range.dst_handle->vfs_private,
                             req->clone_range.dst_offset,
                             req->clone_range.length);
            break;
        case CHIMERA_VFS_OP_MOVE_RANGE:
            chimera_snprintf(argstr, sizeof(argstr),
                             "src_hdl %lx src_off %lu dst_hdl %lx dst_off %lu len %lu",
                             req->move_range.src_handle->vfs_private,
                             req->move_range.src_offset,
                             req->move_range.dst_handle->vfs_private,
                             req->move_range.dst_offset,
                             req->move_range.length);
            break;
        case CHIMERA_VFS_OP_SYMLINK_AT:
            format_safe_name(namestr, sizeof(namestr),
                             req->symlink_at.name, req->symlink_at.namelen);
            format_safe_name(namestr2, sizeof(namestr2),
                             req->symlink_at.target, req->symlink_at.targetlen);
            chimera_snprintf(argstr, sizeof(argstr), "name %s target %s",
                             namestr,
                             namestr2);
            break;
        case CHIMERA_VFS_OP_RENAME_AT:
            format_hex(fhstr2, sizeof(fhstr2), req->rename_at.new_fh, req->rename_at.new_fhlen);
            format_safe_name(namestr, sizeof(namestr),
                             req->rename_at.name, req->rename_at.namelen);
            format_safe_name(namestr2, sizeof(namestr2),
                             req->rename_at.new_name, req->rename_at.new_namelen);
            chimera_snprintf(argstr, sizeof(argstr),
                             "name %s new_fh %s newname %s",
                             namestr,
                             fhstr2,
                             namestr2);
            break;
        case CHIMERA_VFS_OP_LINK_AT:
            format_hex(fhstr2, sizeof(fhstr2),
                       req->link_at.dir_fh,
                       req->link_at.dir_fhlen);
            format_safe_name(namestr, sizeof(namestr),
                             req->link_at.name, req->link_at.namelen);
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
} /* chimera_vfs_dump_request */

void
__chimera_vfs_dump_reply(struct chimera_vfs_request *req)
{
    char        argstr[256];
    char        fhstr[80];
    char        namestr[FORMAT_SAFE_NAME_MAX];
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
        case CHIMERA_VFS_OP_LOOKUP_AT:
            if (req->lookup_at.r_attr.va_set_mask & CHIMERA_VFS_ATTR_FH) {
                format_hex(fhstr, sizeof(fhstr), req->lookup_at.r_attr.va_fh, req->lookup_at.r_attr.va_fh_len);
                fhstr_ptr = fhstr;
            } else {
                fhstr_ptr = "UNSET";
            }
            format_safe_name(namestr, sizeof(namestr),
                             req->lookup_at.component, req->lookup_at.component_len);
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
        case CHIMERA_VFS_OP_MKDIR_AT:
            if (req->mkdir_at.r_attr.va_set_mask & CHIMERA_VFS_ATTR_FH) {
                format_hex(fhstr, sizeof(fhstr), req->mkdir_at.r_attr.va_fh, req->mkdir_at.r_attr.va_fh_len);
                fhstr_ptr = fhstr;
            } else {
                fhstr_ptr = "UNSET";
            }
            format_safe_name(namestr, sizeof(namestr),
                             req->mkdir_at.name, req->mkdir_at.name_len);
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
        case CHIMERA_VFS_OP_COPY_RANGE:
            chimera_snprintf(argstr, sizeof(argstr), "r_len %lu",
                             req->copy_range.r_length);
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
} /* chimera_vfs_dump_reply */

