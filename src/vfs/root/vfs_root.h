// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "vfs/vfs.h"

extern struct chimera_vfs_module vfs_root;

/* Get the root pseudo-filesystem's file handle */
void
chimera_vfs_root_get_fh(
    uint8_t  *fh,
    uint32_t *fh_len);

/* Register the root pseudo-filesystem mount in the mount table.
 * This must be called after vfs_root module is registered and initialized. */
void
chimera_vfs_root_register_mount(
    struct chimera_vfs *vfs);

