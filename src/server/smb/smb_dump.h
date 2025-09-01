// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

struct chimera_smb_compound;

void _smb_dump_compound_request(
    struct chimera_smb_compound *compound);

void _smb_dump_compound_reply(
    struct chimera_smb_compound *compound);

#define smb_dump_compound_request(compound) if (ChimeraLogLevel >= CHIMERA_LOG_DEBUG) { _smb_dump_compound_request( \
                                                                                            compound); }

#define smb_dump_compound_reply(compound)   if (ChimeraLogLevel >= CHIMERA_LOG_DEBUG) { _smb_dump_compound_reply( \
                                                                                            compound); }
