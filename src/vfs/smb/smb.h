// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "vfs/vfs.h"

/* SMB2 client VFS module.  Proxies VFS operations to a remote SMB2 server,
 * mirroring the NFS client (src/vfs/nfs).  This first increment supports only
 * MOUNT (connect + NEGOTIATE + SESSION_SETUP + TREE_CONNECT) and UMOUNT
 * (TREE_DISCONNECT + LOGOFF + disconnect); file operations are layered on
 * incrementally. */
extern struct chimera_vfs_module vfs_smb;
