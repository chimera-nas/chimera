// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * NFSv3 span tracing, modeled on nfs3_dump.h.  Each handler calls the matching
 * nfs3_trace_<op>() right next to its nfs3_dump_<op>(); the macro short-circuits
 * (one inline flag test) unless this request's span is being recorded, so the
 * per-op span naming + attribute code stays out of the mainline and costs
 * nothing for unsampled traces.  The _nfs3_trace_<op>() bodies set the span name
 * ("nfs3.<op>") and attach the file handle (hex) plus op-specific attributes.
 */

#pragma once

#include "nfs_common.h"

void _nfs3_trace_null(
    struct nfs_request *req);
void _nfs3_trace_getattr(
    struct nfs_request  *req,
    struct GETATTR3args *args);
void _nfs3_trace_setattr(
    struct nfs_request  *req,
    struct SETATTR3args *args);
void _nfs3_trace_lookup(
    struct nfs_request *req,
    struct LOOKUP3args *args);
void _nfs3_trace_access(
    struct nfs_request *req,
    struct ACCESS3args *args);
void _nfs3_trace_readlink(
    struct nfs_request   *req,
    struct READLINK3args *args);
void _nfs3_trace_read(
    struct nfs_request *req,
    struct READ3args   *args);
void _nfs3_trace_write(
    struct nfs_request *req,
    struct WRITE3args  *args);
void _nfs3_trace_create(
    struct nfs_request *req,
    struct CREATE3args *args);
void _nfs3_trace_mkdir(
    struct nfs_request *req,
    struct MKDIR3args  *args);
void _nfs3_trace_symlink(
    struct nfs_request  *req,
    struct SYMLINK3args *args);
void _nfs3_trace_mknod(
    struct nfs_request *req,
    struct MKNOD3args  *args);
void _nfs3_trace_remove(
    struct nfs_request *req,
    struct REMOVE3args *args);
void _nfs3_trace_rmdir(
    struct nfs_request *req,
    struct RMDIR3args  *args);
void _nfs3_trace_rename(
    struct nfs_request *req,
    struct RENAME3args *args);
void _nfs3_trace_link(
    struct nfs_request *req,
    struct LINK3args   *args);
void _nfs3_trace_readdir(
    struct nfs_request  *req,
    struct READDIR3args *args);
void _nfs3_trace_readdirplus(
    struct nfs_request      *req,
    struct READDIRPLUS3args *args);
void _nfs3_trace_fsstat(
    struct nfs_request *req,
    struct FSSTAT3args *args);
void _nfs3_trace_fsinfo(
    struct nfs_request *req,
    struct FSINFO3args *args);
void _nfs3_trace_pathconf(
    struct nfs_request   *req,
    struct PATHCONF3args *args);
void _nfs3_trace_commit(
    struct nfs_request *req,
    struct COMMIT3args *args);

#define nfs3_trace_(req, fn, ...) \
        do { if (otel_span_recording(&(req)->otel)) { fn(req, ## __VA_ARGS__); } } while (0)

#define nfs3_trace_null(req)              nfs3_trace_(req, _nfs3_trace_null)
#define nfs3_trace_getattr(req, args)     nfs3_trace_(req, _nfs3_trace_getattr, args)
#define nfs3_trace_setattr(req, args)     nfs3_trace_(req, _nfs3_trace_setattr, args)
#define nfs3_trace_lookup(req, args)      nfs3_trace_(req, _nfs3_trace_lookup, args)
#define nfs3_trace_access(req, args)      nfs3_trace_(req, _nfs3_trace_access, args)
#define nfs3_trace_readlink(req, args)    nfs3_trace_(req, _nfs3_trace_readlink, args)
#define nfs3_trace_read(req, args)        nfs3_trace_(req, _nfs3_trace_read, args)
#define nfs3_trace_write(req, args)       nfs3_trace_(req, _nfs3_trace_write, args)
#define nfs3_trace_create(req, args)      nfs3_trace_(req, _nfs3_trace_create, args)
#define nfs3_trace_mkdir(req, args)       nfs3_trace_(req, _nfs3_trace_mkdir, args)
#define nfs3_trace_symlink(req, args)     nfs3_trace_(req, _nfs3_trace_symlink, args)
#define nfs3_trace_mknod(req, args)       nfs3_trace_(req, _nfs3_trace_mknod, args)
#define nfs3_trace_remove(req, args)      nfs3_trace_(req, _nfs3_trace_remove, args)
#define nfs3_trace_rmdir(req, args)       nfs3_trace_(req, _nfs3_trace_rmdir, args)
#define nfs3_trace_rename(req, args)      nfs3_trace_(req, _nfs3_trace_rename, args)
#define nfs3_trace_link(req, args)        nfs3_trace_(req, _nfs3_trace_link, args)
#define nfs3_trace_readdir(req, args)     nfs3_trace_(req, _nfs3_trace_readdir, args)
#define nfs3_trace_readdirplus(req, args) nfs3_trace_(req, _nfs3_trace_readdirplus, args)
#define nfs3_trace_fsstat(req, args)      nfs3_trace_(req, _nfs3_trace_fsstat, args)
#define nfs3_trace_fsinfo(req, args)      nfs3_trace_(req, _nfs3_trace_fsinfo, args)
#define nfs3_trace_pathconf(req, args)    nfs3_trace_(req, _nfs3_trace_pathconf, args)
#define nfs3_trace_commit(req, args)      nfs3_trace_(req, _nfs3_trace_commit, args)
