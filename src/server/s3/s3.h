// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

void
chimera_s3_add_bucket(
    void       *s3_shared,
    const char *name,
    const char *path);

void
chimera_s3_add_bucket(
    void       *s3_shared,
    const char *name,
    const char *path);

int
chimera_s3_add_cred(
    void       *s3_shared,
    const char *access_key,
    const char *secret_key,
    int         pinned);

extern struct chimera_server_protocol s3_protocol;