// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "server/protocol.h"
#include "vfs/vfs.h"
#include "prometheus-c.h"
struct chimera_smb_share;

void
chimera_smb_add_share(
    void       *smb_shared,
    const char *name,
    const char *path);

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

extern struct chimera_server_protocol smb_protocol;