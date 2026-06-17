// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * NFSv3 span tracing (see nfs3_trace.h).  Bodies run only for a recording span.
 * They set the span name and attach the operation's file handle (hex, same
 * encoding as the debug dump) plus a few useful attributes.  Field expressions
 * mirror nfs3_dump.c.
 */

#include "nfs3_trace.h"

#if CHIMERA_HAVE_OTEL

#include "common/format.h"

/* NFSv3 file handles are at most NFS3_FHSIZE (64) bytes on the wire. */
#define NFS3_TRACE_FH_HEX (2 * 64 + 1)

static inline void
nfs3_trace_fh(
    struct nfs_request *req,
    const char         *key,
    const void         *fh,
    int                 fhlen)
{
    char hex[NFS3_TRACE_FH_HEX];

    if (fh && fhlen > 0) {
        format_hex(hex, sizeof(hex), fh, fhlen);
        otel_span_attr_str(&req->otel, key, hex);
    }
} /* nfs3_trace_fh */

void
_nfs3_trace_null(struct nfs_request *req)
{
    otel_span_set_name(&req->otel, "nfs3.null");
} /* _nfs3_trace_null */

void
_nfs3_trace_getattr(
    struct nfs_request  *req,
    struct GETATTR3args *args)
{
    otel_span_set_name(&req->otel, "nfs3.getattr");
    nfs3_trace_fh(req, "nfs.fh", args->object.data.data, args->object.data.len);
} /* _nfs3_trace_getattr */

void
_nfs3_trace_setattr(
    struct nfs_request  *req,
    struct SETATTR3args *args)
{
    otel_span_set_name(&req->otel, "nfs3.setattr");
    nfs3_trace_fh(req, "nfs.fh", args->object.data.data, args->object.data.len);
} /* _nfs3_trace_setattr */

void
_nfs3_trace_lookup(
    struct nfs_request *req,
    struct LOOKUP3args *args)
{
    otel_span_set_name(&req->otel, "nfs3.lookup");
    nfs3_trace_fh(req, "nfs.dir_fh", args->what.dir.data.data, args->what.dir.data.len);
    otel_span_attr_strn(&req->otel, "nfs.name", args->what.name.str, args->what.name.len);
} /* _nfs3_trace_lookup */

void
_nfs3_trace_access(
    struct nfs_request *req,
    struct ACCESS3args *args)
{
    otel_span_set_name(&req->otel, "nfs3.access");
    nfs3_trace_fh(req, "nfs.fh", args->object.data.data, args->object.data.len);
    otel_span_attr_u64(&req->otel, "nfs.access", args->access);
} /* _nfs3_trace_access */

void
_nfs3_trace_readlink(
    struct nfs_request   *req,
    struct READLINK3args *args)
{
    otel_span_set_name(&req->otel, "nfs3.readlink");
    nfs3_trace_fh(req, "nfs.fh", args->symlink.data.data, args->symlink.data.len);
} /* _nfs3_trace_readlink */

void
_nfs3_trace_read(
    struct nfs_request *req,
    struct READ3args   *args)
{
    otel_span_set_name(&req->otel, "nfs3.read");
    nfs3_trace_fh(req, "nfs.fh", args->file.data.data, args->file.data.len);
    otel_span_attr_u64(&req->otel, "nfs.offset", args->offset);
    otel_span_attr_u64(&req->otel, "nfs.count", args->count);
} /* _nfs3_trace_read */

void
_nfs3_trace_write(
    struct nfs_request *req,
    struct WRITE3args  *args)
{
    otel_span_set_name(&req->otel, "nfs3.write");
    nfs3_trace_fh(req, "nfs.fh", args->file.data.data, args->file.data.len);
    otel_span_attr_u64(&req->otel, "nfs.offset", args->offset);
    otel_span_attr_u64(&req->otel, "nfs.count", args->count);
} /* _nfs3_trace_write */

void
_nfs3_trace_create(
    struct nfs_request *req,
    struct CREATE3args *args)
{
    otel_span_set_name(&req->otel, "nfs3.create");
    nfs3_trace_fh(req, "nfs.dir_fh", args->where.dir.data.data, args->where.dir.data.len);
    otel_span_attr_strn(&req->otel, "nfs.name", args->where.name.str, args->where.name.len);
} /* _nfs3_trace_create */

void
_nfs3_trace_mkdir(
    struct nfs_request *req,
    struct MKDIR3args  *args)
{
    otel_span_set_name(&req->otel, "nfs3.mkdir");
    nfs3_trace_fh(req, "nfs.dir_fh", args->where.dir.data.data, args->where.dir.data.len);
    otel_span_attr_strn(&req->otel, "nfs.name", args->where.name.str, args->where.name.len);
} /* _nfs3_trace_mkdir */

void
_nfs3_trace_symlink(
    struct nfs_request  *req,
    struct SYMLINK3args *args)
{
    otel_span_set_name(&req->otel, "nfs3.symlink");
    nfs3_trace_fh(req, "nfs.dir_fh", args->where.dir.data.data, args->where.dir.data.len);
    otel_span_attr_strn(&req->otel, "nfs.name", args->where.name.str, args->where.name.len);
} /* _nfs3_trace_symlink */

void
_nfs3_trace_mknod(
    struct nfs_request *req,
    struct MKNOD3args  *args)
{
    otel_span_set_name(&req->otel, "nfs3.mknod");
    nfs3_trace_fh(req, "nfs.dir_fh", args->where.dir.data.data, args->where.dir.data.len);
    otel_span_attr_strn(&req->otel, "nfs.name", args->where.name.str, args->where.name.len);
} /* _nfs3_trace_mknod */

void
_nfs3_trace_remove(
    struct nfs_request *req,
    struct REMOVE3args *args)
{
    otel_span_set_name(&req->otel, "nfs3.remove");
    nfs3_trace_fh(req, "nfs.dir_fh", args->object.dir.data.data, args->object.dir.data.len);
    otel_span_attr_strn(&req->otel, "nfs.name", args->object.name.str, args->object.name.len);
} /* _nfs3_trace_remove */

void
_nfs3_trace_rmdir(
    struct nfs_request *req,
    struct RMDIR3args  *args)
{
    otel_span_set_name(&req->otel, "nfs3.rmdir");
    nfs3_trace_fh(req, "nfs.dir_fh", args->object.dir.data.data, args->object.dir.data.len);
    otel_span_attr_strn(&req->otel, "nfs.name", args->object.name.str, args->object.name.len);
} /* _nfs3_trace_rmdir */

void
_nfs3_trace_rename(
    struct nfs_request *req,
    struct RENAME3args *args)
{
    otel_span_set_name(&req->otel, "nfs3.rename");
    nfs3_trace_fh(req, "nfs.from_dir_fh", args->from.dir.data.data, args->from.dir.data.len);
    otel_span_attr_strn(&req->otel, "nfs.from_name", args->from.name.str, args->from.name.len);
    nfs3_trace_fh(req, "nfs.to_dir_fh", args->to.dir.data.data, args->to.dir.data.len);
    otel_span_attr_strn(&req->otel, "nfs.to_name", args->to.name.str, args->to.name.len);
} /* _nfs3_trace_rename */

void
_nfs3_trace_link(
    struct nfs_request *req,
    struct LINK3args   *args)
{
    otel_span_set_name(&req->otel, "nfs3.link");
    nfs3_trace_fh(req, "nfs.fh", args->file.data.data, args->file.data.len);
    nfs3_trace_fh(req, "nfs.dir_fh", args->link.dir.data.data, args->link.dir.data.len);
    otel_span_attr_strn(&req->otel, "nfs.name", args->link.name.str, args->link.name.len);
} /* _nfs3_trace_link */

void
_nfs3_trace_readdir(
    struct nfs_request  *req,
    struct READDIR3args *args)
{
    otel_span_set_name(&req->otel, "nfs3.readdir");
    nfs3_trace_fh(req, "nfs.fh", args->dir.data.data, args->dir.data.len);
    otel_span_attr_u64(&req->otel, "nfs.cookie", args->cookie);
    otel_span_attr_u64(&req->otel, "nfs.count", args->count);
} /* _nfs3_trace_readdir */

void
_nfs3_trace_readdirplus(
    struct nfs_request      *req,
    struct READDIRPLUS3args *args)
{
    otel_span_set_name(&req->otel, "nfs3.readdirplus");
    nfs3_trace_fh(req, "nfs.fh", args->dir.data.data, args->dir.data.len);
    otel_span_attr_u64(&req->otel, "nfs.cookie", args->cookie);
    otel_span_attr_u64(&req->otel, "nfs.maxcount", args->maxcount);
} /* _nfs3_trace_readdirplus */

void
_nfs3_trace_fsstat(
    struct nfs_request *req,
    struct FSSTAT3args *args)
{
    otel_span_set_name(&req->otel, "nfs3.fsstat");
    nfs3_trace_fh(req, "nfs.fh", args->fsroot.data.data, args->fsroot.data.len);
} /* _nfs3_trace_fsstat */

void
_nfs3_trace_fsinfo(
    struct nfs_request *req,
    struct FSINFO3args *args)
{
    otel_span_set_name(&req->otel, "nfs3.fsinfo");
    nfs3_trace_fh(req, "nfs.fh", args->fsroot.data.data, args->fsroot.data.len);
} /* _nfs3_trace_fsinfo */

void
_nfs3_trace_pathconf(
    struct nfs_request   *req,
    struct PATHCONF3args *args)
{
    otel_span_set_name(&req->otel, "nfs3.pathconf");
    nfs3_trace_fh(req, "nfs.fh", args->object.data.data, args->object.data.len);
} /* _nfs3_trace_pathconf */

void
_nfs3_trace_commit(
    struct nfs_request *req,
    struct COMMIT3args *args)
{
    otel_span_set_name(&req->otel, "nfs3.commit");
    nfs3_trace_fh(req, "nfs.fh", args->file.data.data, args->file.data.len);
    otel_span_attr_u64(&req->otel, "nfs.offset", args->offset);
    otel_span_attr_u64(&req->otel, "nfs.count", args->count);
} /* _nfs3_trace_commit */

#endif /* CHIMERA_HAVE_OTEL */
