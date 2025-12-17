// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once


#define NFS_PORT          2049
#define NFS_MOUNT_PORT    20048
#define NFS_RPC_PROGRAM   100003
#define NFS_MOUNT_PROGRAM 100005

void register_nfs_rpc_services(
    void);

void unregister_nfs_rpc_services(
    void);