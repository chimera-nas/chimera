// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

extern struct chimera_server_protocol nfs_protocol;

void chimera_nfs_add_export(
    void       *nfs_shared,
    const char *name,
    const char *path);