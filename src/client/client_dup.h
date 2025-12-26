// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"
#include "vfs/vfs_release.h"

static inline void
chimera_dup_handle(
    struct chimera_client_thread   *thread,
    struct chimera_vfs_open_handle *oh)
{
    chimera_vfs_dup_handle(thread->vfs_thread, oh);
} /* chimera_dup_handle */
