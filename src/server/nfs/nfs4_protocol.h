// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#ifndef CHIMERA_NFS4_PROTOCOL_H
#define CHIMERA_NFS4_PROTOCOL_H

#include "nfs4_procs.h"

struct chimera_vfs_layout_device;

nfsstat4 chimera_nfs4_protocol_status(
    struct nfs_request      *req,
    const struct nfs_argop4 *argop);
nfsstat4 chimera_nfs4_getdeviceinfo_fill(
    struct chimera_server_nfs_thread       *thread,
    struct nfs_request                     *req,
    const struct GETDEVICEINFO4args        *args,
    struct GETDEVICEINFO4res               *res,
    const struct chimera_vfs_layout_device *private_device);

#endif /* CHIMERA_NFS4_PROTOCOL_H */
