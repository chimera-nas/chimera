// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once


#define NFS_PORT          2049
#define NFS_MOUNT_PORT    20048
#define NFS_RPC_PROGRAM   100003
#define NFS_MOUNT_PROGRAM 100005
#define NFS_NLM_PROGRAM   100021

void register_nfs_rpc_services(
    int lockmgr_port);

void unregister_nfs_rpc_services(
    int lockmgr_port);
