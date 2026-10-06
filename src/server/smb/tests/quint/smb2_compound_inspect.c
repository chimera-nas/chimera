// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#include "server/smb/smb_internal.h"
#include "server/smb/smb_compound.h"

/* Keep private server layouts out of the wire harness, whose protocol macros
 * intentionally describe packets independently of the server's parser. */
bool
smb2_test_compound_matches(
    struct chimera_vfs_compound *compound,
    uint64_t                     session_id,
    uint64_t                     message_id)
{
    if (!chimera_vfs_compound_num_groups(compound)) {
        return false;
    }
    struct smb_vfs_command *command = chimera_vfs_compound_group_context(compound, 0);
    return command && command->request->smb2_hdr.session_id == session_id &&
           command->request->smb2_hdr.message_id == message_id;
} /* smb2_test_compound_matches */
