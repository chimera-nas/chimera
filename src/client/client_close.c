// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "client_internal.h"
#include "vfs/vfs_release.h"

SYMBOL_EXPORT void
chimera_close(
    struct chimera_client_thread   *thread,
    struct chimera_vfs_open_handle *oh)
{
    chimera_vfs_release(thread->vfs_thread, oh);

} /* chimera_close */