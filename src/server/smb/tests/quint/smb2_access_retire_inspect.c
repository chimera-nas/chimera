// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#include "server/smb/smb_session.h"

/* Isolate server protocol declarations from the intentionally independent wire
 * harness constants; the probe sees only opaque retained anchor identities. */
void *
smb_access_retire_test_handle(void *private_data)
{
    struct chimera_smb_open_file *open = private_data;

    return open->handle;
} /* smb_access_retire_test_handle */

int
smb_access_retire_test_closed(void *private_data)
{
    struct chimera_smb_open_file *open = private_data;

    return !!(open->flags & CHIMERA_SMB_OPEN_FILE_CLOSED);
} /* smb_access_retire_test_closed */

int
smb_access_retire_test_timer(
    void              *private_data,
    struct evpl_timer *timer)
{
    struct chimera_smb_open_file *open = private_data;

    return open && &open->retire_timer == timer;
} /* smb_access_retire_test_timer */
