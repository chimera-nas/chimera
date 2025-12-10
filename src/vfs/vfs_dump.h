// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#ifndef __VFS_DUMP_H__
#define __VFS_DUMP_H__

#include "common/logging.h"

struct chimera_vfs_request;

const char *
chimera_vfs_op_name(
    unsigned int opcode);

void __chimera_vfs_dump_request(
    struct chimera_vfs_request *request);
void __chimera_vfs_dump_reply(
    struct chimera_vfs_request *request);

#define chimera_vfs_dump_request(request) \
        if (ChimeraLogLevel >= CHIMERA_LOG_DEBUG) { \
            __chimera_vfs_dump_request(request); \
        }

#define chimera_vfs_dump_reply(request) \
        if (ChimeraLogLevel >= CHIMERA_LOG_DEBUG) { \
            __chimera_vfs_dump_reply(request); \
        }

#endif /* ifndef __VFS_DUMP_H__ */
