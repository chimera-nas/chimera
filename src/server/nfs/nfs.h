// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

extern struct chimera_server_protocol nfs_protocol;

struct chimera_nfs_export;

void chimera_nfs_add_export(
    void       *nfs_shared,
    const char *name,
    const char *path);

int
chimera_nfs_remove_export(
    void       *nfs_shared,
    const char *name);

const struct chimera_nfs_export *
chimera_nfs_get_export(
    void       *nfs_shared,
    const char *name);

typedef int (*chimera_nfs_export_iterate_cb)(
    const struct chimera_nfs_export *export,
    void *data);

void
chimera_nfs_iterate_exports(
    void                         *nfs_shared,
    chimera_nfs_export_iterate_cb callback,
    void                         *data);

const char *
chimera_nfs_export_get_name(
    const struct chimera_nfs_export *export);

const char *
chimera_nfs_export_get_path(
    const struct chimera_nfs_export *export);