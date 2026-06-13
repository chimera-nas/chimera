// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <xxhash.h>

#include "smb_internal.h"
#include "vfs/vfs_attrs.h"
#include "evpl/evpl.h"

/*
 * SMB2 namespace operations for the path-only SMB client: rename, symlink, and
 * mknod.  Each addresses files by the full mount-relative path carried in
 * request->X.name (and request->X.new_name for rename) and drives a transient
 * SMB2 CREATE -> SET_INFO -> CLOSE chain, reusing the shared helpers in
 * smb_internal.h (smb_send_create / smb_send_close / smb_parse_create_reply /
 * smb_apply_attrs / struct chimera_smb_op_state).
 */

/* ---- rename_at (SET_INFO FileRenameInformation) ------------------------ */

void
chimera_smb_client_rename_at(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    (void) conn;
    request->status = CHIMERA_VFS_ENOTSUP;
    request->complete(request);
} /* chimera_smb_client_rename_at */

/* ---- symlink_at (reparse point) ---------------------------------------- */

void
chimera_smb_client_symlink_at(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    (void) conn;
    request->status = CHIMERA_VFS_ENOTSUP;
    request->complete(request);
} /* chimera_smb_client_symlink_at */

/* ---- mknod_at ---------------------------------------------------------- */

void
chimera_smb_client_mknod_at(
    struct chimera_smb_client_conn *conn,
    struct chimera_vfs_request     *request)
{
    (void) conn;
    request->status = CHIMERA_VFS_ENOTSUP;
    request->complete(request);
} /* chimera_smb_client_mknod_at */
