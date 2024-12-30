#pragma once

#include "nfs_internal.h"
#include "nfs4_xdr.h"
struct nfs_request;

void _nfs4_dump_null(
    struct nfs_request *req);
void _nfs4_dump_compound(
    struct nfs_request   *req,
    struct COMPOUND4args *args);


#define nfs4_dump_null(req)           if (ChimeraLogLevel >= CHIMERA_LOG_DEBUG) { _nfs4_dump_null(req); }
#define nfs4_dump_compound(req, args) if (ChimeraLogLevel >= CHIMERA_LOG_DEBUG) { _nfs4_dump_compound(req, args); }
