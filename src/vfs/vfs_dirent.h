// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/* SMB-style directory wildcard match (MS-FSA 2.1.4.4), exposed for callers that
 * filter outside chimera_vfs_readdir.  A NULL/empty pattern matches everything. */
int
chimera_vfs_dirent_match(
    const char *name,
    int         namelen,
    const char *pattern,
    int         patternlen);
