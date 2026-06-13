// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "smb_internal.h"
#include "vfs/vfs_attrs.h"
#include "evpl/evpl.h"

/*
 * SMB2 data-path operations for the path-only SMB client: READ, WRITE, and
 * directory enumeration (QUERY_DIRECTORY).  All three operate on an already-open
 * VFS handle whose vfs_private carries the SMB FileId (smb_handle_open_state).
 */

/* ---- read (SMB2 READ) -------------------------------------------------- */

void
chimera_smb_client_read(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    (void) conn;
    request->status = CHIMERA_VFS_ENOTSUP;
    request->complete(request);
} /* chimera_smb_client_read */

/* ---- write (SMB2 WRITE) ------------------------------------------------ */

void
chimera_smb_client_write(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    (void) conn;
    request->status = CHIMERA_VFS_ENOTSUP;
    request->complete(request);
} /* chimera_smb_client_write */

/* ---- readdir (SMB2 QUERY_DIRECTORY) ------------------------------------ */

void
chimera_smb_client_readdir(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    (void) conn;
    request->status = CHIMERA_VFS_ENOTSUP;
    request->complete(request);
} /* chimera_smb_client_readdir */
