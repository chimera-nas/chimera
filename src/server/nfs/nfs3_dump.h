// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "nfs_common.h"

void _nfs3_dump_null(
    struct nfs_request *req);
void _nfs3_dump_getattr(
    struct nfs_request  *req,
    struct GETATTR3args *args);
void _nfs3_dump_setattr(
    struct nfs_request  *req,
    struct SETATTR3args *args);
void _nfs3_dump_lookup(
    struct nfs_request *req,
    struct LOOKUP3args *args);
void _nfs3_dump_access(
    struct nfs_request *req,
    struct ACCESS3args *args);
void _nfs3_dump_readlink(
    struct nfs_request   *req,
    struct READLINK3args *args);
void _nfs3_dump_read(
    struct nfs_request *req,
    struct READ3args   *args);
void _nfs3_dump_write(
    struct nfs_request *req,
    struct WRITE3args  *args);
void _nfs3_dump_create(
    struct nfs_request *req,
    struct CREATE3args *args);
void _nfs3_dump_mkdir(
    struct nfs_request *req,
    struct MKDIR3args  *args);
void _nfs3_dump_symlink(
    struct nfs_request  *req,
    struct SYMLINK3args *args);
void _nfs3_dump_mknod(
    struct nfs_request *req,
    struct MKNOD3args  *args);
void _nfs3_dump_remove(
    struct nfs_request *req,
    struct REMOVE3args *args);
void _nfs3_dump_rmdir(
    struct nfs_request *req,
    struct RMDIR3args  *args);
void _nfs3_dump_rename(
    struct nfs_request *req,
    struct RENAME3args *args);
void _nfs3_dump_link(
    struct nfs_request *req,
    struct LINK3args   *args);
void _nfs3_dump_readdir(
    struct nfs_request  *req,
    struct READDIR3args *args);
void _nfs3_dump_readdirplus(
    struct nfs_request      *req,
    struct READDIRPLUS3args *args);
void _nfs3_dump_fsstat(
    struct nfs_request *req,
    struct FSSTAT3args *args);
void _nfs3_dump_fsinfo(
    struct nfs_request *req,
    struct FSINFO3args *args);
void _nfs3_dump_pathconf(
    struct nfs_request   *req,
    struct PATHCONF3args *args);
void _nfs3_dump_commit(
    struct nfs_request *req,
    struct COMMIT3args *args);

#define nfs3_dump_null(req)              if (ChimeraLogLevel >= CHIMERA_LOG_DEBUG) { _nfs3_dump_null(req); }
#define nfs3_dump_getattr(req, args)     if (ChimeraLogLevel >= CHIMERA_LOG_DEBUG) { _nfs3_dump_getattr(req, args); }
#define nfs3_dump_setattr(req, args)     if (ChimeraLogLevel >= CHIMERA_LOG_DEBUG) { _nfs3_dump_setattr(req, args); }
#define nfs3_dump_lookup(req, args)      if (ChimeraLogLevel >= CHIMERA_LOG_DEBUG) { _nfs3_dump_lookup(req, args); }
#define nfs3_dump_access(req, args)      if (ChimeraLogLevel >= CHIMERA_LOG_DEBUG) { _nfs3_dump_access(req, args); }
#define nfs3_dump_readlink(req, args)    if (ChimeraLogLevel >= CHIMERA_LOG_DEBUG) { _nfs3_dump_readlink(req, args); }
#define nfs3_dump_read(req, args)        if (ChimeraLogLevel >= CHIMERA_LOG_DEBUG) { _nfs3_dump_read(req, args); }
#define nfs3_dump_write(req, args)       if (ChimeraLogLevel >= CHIMERA_LOG_DEBUG) { _nfs3_dump_write(req, args); }
#define nfs3_dump_create(req, args)      if (ChimeraLogLevel >= CHIMERA_LOG_DEBUG) { _nfs3_dump_create(req, args); }
#define nfs3_dump_mkdir(req, args)       if (ChimeraLogLevel >= CHIMERA_LOG_DEBUG) { _nfs3_dump_mkdir(req, args); }
#define nfs3_dump_symlink(req, args)     if (ChimeraLogLevel >= CHIMERA_LOG_DEBUG) { _nfs3_dump_symlink(req, args); }
#define nfs3_dump_mknod(req, args)       if (ChimeraLogLevel >= CHIMERA_LOG_DEBUG) { _nfs3_dump_mknod(req, args); }
#define nfs3_dump_remove(req, args)      if (ChimeraLogLevel >= CHIMERA_LOG_DEBUG) { _nfs3_dump_remove(req, args); }
#define nfs3_dump_rmdir(req, args)       if (ChimeraLogLevel >= CHIMERA_LOG_DEBUG) { _nfs3_dump_rmdir(req, args); }
#define nfs3_dump_rename(req, args)      if (ChimeraLogLevel >= CHIMERA_LOG_DEBUG) { _nfs3_dump_rename(req, args); }
#define nfs3_dump_link(req, args)        if (ChimeraLogLevel >= CHIMERA_LOG_DEBUG) { _nfs3_dump_link(req, args); }
#define nfs3_dump_readdir(req, args)     if (ChimeraLogLevel >= CHIMERA_LOG_DEBUG) { _nfs3_dump_readdir(req, args); }
#define nfs3_dump_readdirplus(req, args) if (ChimeraLogLevel >= CHIMERA_LOG_DEBUG) { _nfs3_dump_readdirplus(req, args); \
}
#define nfs3_dump_fsstat(req, args)      if (ChimeraLogLevel >= CHIMERA_LOG_DEBUG) { _nfs3_dump_fsstat(req, args); }
#define nfs3_dump_fsinfo(req, args)      if (ChimeraLogLevel >= CHIMERA_LOG_DEBUG) { _nfs3_dump_fsinfo(req, args); }
#define nfs3_dump_pathconf(req, args)    if (ChimeraLogLevel >= CHIMERA_LOG_DEBUG) { _nfs3_dump_pathconf(req, args); }
#define nfs3_dump_commit(req, args)      if (ChimeraLogLevel >= CHIMERA_LOG_DEBUG) { _nfs3_dump_commit(req, args); }



