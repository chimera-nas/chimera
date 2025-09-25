// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>

struct smb1_header {
    uint8_t  protocol_id[4];
    uint8_t  command;
    uint32_t status;
    uint8_t  flags1;
    uint16_t flags2;
    uint16_t pid_high;
    uint64_t signature;
    uint16_t reserved;
    uint16_t tree_id;
    uint16_t pid_low;
    uint16_t user_id;
    uint16_t multiplex_id;
} __attribute__((packed));