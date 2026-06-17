// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "server/protocol.h"
#include "vfs/vfs.h"
#include "prometheus-c.h"
struct chimera_smb_share;
struct chimera_cluster;

void
chimera_smb_add_share(
    void       *smb_shared,
    const char *name,
    const char *path,
    int         continuous_availability);

/* Attach the cluster eviction registry + its query fn (post-init, from
 * chimera_server) so the durable recovery scan can tell an evicted peer's
 * handles (adoptable) from a live peer's (must not be stolen). */
void
chimera_smb_set_cluster(
    void *smb_shared,
    struct chimera_cluster *cluster,
    int ( *node_evicted )(const struct chimera_cluster *, int));

/* React to a peer eviction: re-arm the per-share durable recovery scan so the
 * next tree-connect re-adopts the evicted node's persisted handles. */
void
chimera_smb_cluster_on_evict(
    void *smb_shared);

/* Enable access-based directory enumeration on a named share. */
int
chimera_smb_share_set_access_based_enum(
    void       *smb_shared,
    const char *name);

/* Enable per-share SMB3 encryption (SMB2_SHAREFLAG_ENCRYPT_DATA) on a named
 * share. */
int
chimera_smb_share_set_encrypt_data(
    void       *smb_shared,
    const char *name);

int
chimera_smb_remove_share(
    void       *smb_shared,
    const char *name);

const struct chimera_smb_share *
chimera_smb_get_share(
    void       *smb_shared,
    const char *name);

typedef int (*chimera_smb_share_iterate_cb)(
    const struct chimera_smb_share *share,
    void                           *data);

void
chimera_smb_iterate_shares(
    void                        *smb_shared,
    chimera_smb_share_iterate_cb callback,
    void                        *data);

const char *
chimera_smb_share_get_name(
    const struct chimera_smb_share *share);

const char *
chimera_smb_share_get_path(
    const struct chimera_smb_share *share);

int
chimera_smb_add_ntlm_user(
    void       *smb_shared,
    const char *username,
    const char *password);

extern struct chimera_server_protocol smb_protocol;