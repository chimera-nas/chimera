// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#pragma once

#include "common/macros.h"
#include "nfs4_xdr.h"
#include "vfs/vfs.h"

#define NFS4_CHANGE_FLOOR_LIMIT 65536
struct nfs4_change_table;
struct nfs4_change_observation;

SYMBOL_EXPORT struct nfs4_change_table * nfs4_change_table_create(
    void);
SYMBOL_EXPORT void nfs4_change_table_free(
    struct nfs4_change_table *table);
/* Called only when granting accepted optional write delegation state. */
SYMBOL_EXPORT bool nfs4_change_register(
    struct nfs4_change_table *table,
    const uint8_t            *fh,
    uint32_t                  len);
/* Pure projection: captures a private observation; no published map changes. */
SYMBOL_EXPORT nfsstat4 nfs4_change_project(
    struct nfs4_change_table        *table,
    const uint8_t                   *fh,
    uint32_t                         len,
    struct chimera_vfs_attrs        *attrs,
    struct nfs4_change_observation **pending,
    struct nfs4_change_observation **observation);
SYMBOL_EXPORT void nfs4_change_observe(
    struct nfs4_change_observation *observation,
    const struct chimera_vfs_attrs *attrs);
SYMBOL_EXPORT void nfs4_change_finish(
    struct nfs4_change_table        *table,
    struct nfs4_change_observation **pending,
    bool                             accepted);
