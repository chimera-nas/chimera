#pragma once

#include "server/protocol.h"
#include "vfs/vfs.h"
#include "prometheus-c.h"
void
chimera_smb_add_share(
    void       *smb_shared,
    const char *name,
    const char *path);

extern struct chimera_server_protocol smb_protocol;