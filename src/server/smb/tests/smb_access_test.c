// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * SMB access helpers (smb_access.h): which DesiredAccess bits make an open a
 * data-write open (and so a read-write backend open), and how a MAXIMUM_ALLOWED
 * grant is trimmed when the backend handle is read-only.
 */

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>

#include "server/smb/smb_access.h"

static int passed = 0;
static int failed = 0;

#define CHECK(cond, ...)                          \
        do {                                      \
            if (cond) {                           \
                passed++;                         \
            } else {                              \
                fprintf(stderr, "  FAIL: ");      \
                fprintf(stderr, __VA_ARGS__);     \
                fprintf(stderr, "\n");            \
                failed++;                         \
            }                                     \
        } while (0)

struct writes_case {
    const char *name;
    uint32_t    access;
    bool        writes;
};

/* *INDENT-OFF* */
static const struct writes_case writes_cases[] = {
    { "FILE_WRITE_DATA",                   SMB2_FILE_WRITE_DATA,         true  },
    { "FILE_APPEND_DATA",                  SMB2_FILE_APPEND_DATA,        true  },
    { "GENERIC_WRITE",                     SMB2_GENERIC_WRITE,           true  },
    { "GENERIC_ALL",                       SMB2_GENERIC_ALL,             true  },
    { "MAXIMUM_ALLOWED",                   SMB2_MAXIMUM_ALLOWED,         true  },
    { "FILE_READ_DATA|MAXIMUM_ALLOWED",    SMB2_FILE_READ_DATA |
                                           SMB2_MAXIMUM_ALLOWED,         true  },
    { "none",                              0,                            false },
    { "FILE_READ_DATA",                    SMB2_FILE_READ_DATA,          false },
    { "FILE_READ_ATTRIBUTES",              SMB2_FILE_READ_ATTRIBUTES,    false },
    { "FILE_WRITE_ATTRIBUTES",             SMB2_FILE_WRITE_ATTRIBUTES,   false },
    { "FILE_READ_ATTRIBUTES|WRITE_ATTRIBUTES",
                                           SMB2_FILE_READ_ATTRIBUTES |
                                           SMB2_FILE_WRITE_ATTRIBUTES,   false },
    { "FILE_WRITE_EA",                     SMB2_FILE_WRITE_EA,           false },
    { "DELETE",                            SMB2_DELETE,                  false },
    { "WRITE_DACL",                        SMB2_WRITE_DACL,              false },
    { "WRITE_OWNER",                       SMB2_WRITE_OWNER,             false },
    { "FILE_DELETE_CHILD",                 SMB2_FILE_DELETE_CHILD,       false },
    { "SYNCHRONIZE",                       SMB2_SYNCHRONIZE,             false },
    { "GENERIC_READ",                      SMB2_GENERIC_READ,            false },
    { "GENERIC_EXECUTE",                   SMB2_GENERIC_EXECUTE,         false },
};
/* *INDENT-ON* */

#define FILE_ALL 0x001f01ffu

#define MAX      SMB2_MAXIMUM_ALLOWED

/* chimera_smb_may_reopen_read_only(desired_access, create_options, disposition,
 * has_stream, already_retried, error): which refused opens CREATE re-issues
 * read-only. */
static void
check_reopen_read_only(void)
{
    CHECK(chimera_smb_may_reopen_read_only(MAX, 0, SMB2_FILE_OPEN, false, false,
                                           CHIMERA_VFS_EISDIR),
          "MAXIMUM_ALLOWED OPEN refused EISDIR is re-issued");
    CHECK(chimera_smb_may_reopen_read_only(MAX, 0, SMB2_FILE_OPEN_IF, false, false,
                                           CHIMERA_VFS_EACCES),
          "MAXIMUM_ALLOWED OPEN_IF refused EACCES is re-issued");
    CHECK(chimera_smb_may_reopen_read_only(MAX, 0, SMB2_FILE_OPEN, false, false,
                                           CHIMERA_VFS_EPERM),
          "EPERM is re-issued");
    CHECK(chimera_smb_may_reopen_read_only(MAX, 0, SMB2_FILE_OPEN, false, false,
                                           CHIMERA_VFS_EROFS),
          "EROFS is re-issued");
    CHECK(!chimera_smb_may_reopen_read_only(MAX, 0, SMB2_FILE_OPEN, false, false,
                                            CHIMERA_VFS_ENOENT),
          "ENOENT is not re-issued");
    CHECK(!chimera_smb_may_reopen_read_only(MAX, 0, SMB2_FILE_OPEN, false, true,
                                            CHIMERA_VFS_EACCES),
          "an open already re-issued is not re-issued again");
    CHECK(!chimera_smb_may_reopen_read_only(MAX | SMB2_FILE_WRITE_DATA, 0,
                                            SMB2_FILE_OPEN, false, false,
                                            CHIMERA_VFS_EACCES),
          "an explicit data-write right is not re-issued");
    CHECK(!chimera_smb_may_reopen_read_only(SMB2_FILE_READ_DATA, 0, SMB2_FILE_OPEN,
                                            false, false, CHIMERA_VFS_EACCES),
          "an open without MAXIMUM_ALLOWED is not re-issued");
    CHECK(!chimera_smb_may_reopen_read_only(MAX, 0, SMB2_FILE_CREATE, false, false,
                                            CHIMERA_VFS_EACCES),
          "FILE_CREATE is not re-issued");
    CHECK(!chimera_smb_may_reopen_read_only(MAX, 0, SMB2_FILE_OVERWRITE_IF, false,
                                            false, CHIMERA_VFS_EACCES),
          "an overwrite refused EACCES is not re-issued");
    CHECK(chimera_smb_may_reopen_read_only(MAX, 0, SMB2_FILE_CREATE, true, false,
                                           CHIMERA_VFS_EACCES),
          "a named-stream base open is re-issued whatever the disposition");
    CHECK(!chimera_smb_may_reopen_read_only(MAX, SMB2_FILE_DIRECTORY_FILE,
                                            SMB2_FILE_OPEN_IF, false, false,
                                            CHIMERA_VFS_EACCES),
          "a directory OPEN_IF (the mkdir path's open of an existing "
          "directory, already read-only) is not re-issued");
    CHECK(chimera_smb_may_reopen_read_only(MAX, 0, SMB2_FILE_OVERWRITE_IF, false,
                                           false, CHIMERA_VFS_EISDIR),
          "an overwrite refused EISDIR (a directory, never truncated) is "
          "re-issued");

} /* check_reopen_read_only */

int
main(void)
{
    check_reopen_read_only();

    for (size_t i = 0; i < sizeof(writes_cases) / sizeof(writes_cases[0]); i++) {
        const struct writes_case *wc = &writes_cases[i];

        CHECK(chimera_smb_access_writes_data(wc->access) == wc->writes,
              "writes_data(%s) != %s", wc->name, wc->writes ? "true" : "false");
    }

    CHECK(chimera_smb_max_allowed_trim(FILE_ALL, false, true, false) == 0x001f01f9u,
          "a read-only file handle loses WRITE_DATA and APPEND_DATA");
    CHECK(chimera_smb_max_allowed_trim(FILE_ALL, true, true, false) == FILE_ALL,
          "a read-only directory handle keeps its grant");
    CHECK(chimera_smb_max_allowed_trim(FILE_ALL, false, false, false) == FILE_ALL,
          "a read-write file handle keeps its grant");
    CHECK(chimera_smb_max_allowed_trim(FILE_ALL, false, false, true) == 0x001f01b9u,
          "a READONLY file loses WRITE_DATA, APPEND_DATA/ADD_SUBDIRECTORY and "
          "DELETE_CHILD");
    CHECK(chimera_smb_max_allowed_trim(FILE_ALL, true, false, true) == FILE_ALL,
          "a READONLY directory keeps its grant");
    CHECK(chimera_smb_max_allowed_trim(FILE_ALL, false, true, true) == 0x001f01b9u,
          "a READONLY file on a read-only handle loses the same four bits");

    fprintf(stderr, "smb_access_test: %d passed, %d failed\n", passed, failed);
    return failed ? 1 : 0;
} /* main */
