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
    CHECK(chimera_smb_may_reopen_read_only(SMB2_DELETE | SMB2_FILE_WRITE_ATTRIBUTES,
                                           0, SMB2_FILE_OVERWRITE_IF, false,
                                           false, CHIMERA_VFS_EISDIR),
          "an overwrite opened read-write only for its content replacement, "
          "refused EISDIR, is re-issued");
    CHECK(!chimera_smb_may_reopen_read_only(SMB2_DELETE | SMB2_FILE_WRITE_ATTRIBUTES,
                                            0, SMB2_FILE_OVERWRITE_IF, false,
                                            false, CHIMERA_VFS_EACCES),
          "an overwrite opened read-write only for its content replacement, "
          "refused EACCES, is not re-issued");
    CHECK(!chimera_smb_may_reopen_read_only(SMB2_FILE_WRITE_DATA, 0,
                                            SMB2_FILE_OVERWRITE_IF, false, false,
                                            CHIMERA_VFS_EISDIR),
          "an overwrite with an explicit data-write right is not re-issued");
    CHECK(!chimera_smb_may_reopen_read_only(SMB2_GENERIC_READ, 0, SMB2_FILE_OPEN,
                                            false, false, CHIMERA_VFS_EISDIR),
          "a read-only open is not re-issued");
    CHECK(!chimera_smb_may_reopen_read_only(SMB2_GENERIC_READ, 0,
                                            SMB2_FILE_OVERWRITE_IF, true, false,
                                            CHIMERA_VFS_EISDIR),
          "a named-stream base open, read-only, is not re-issued");

} /* check_reopen_read_only */

/* chimera_smb_overwrite_met_directory(create_options, disposition, has_stream,
 * error): an overwriting open the backend refused EISDIR met an existing
 * directory, which CREATE answers OBJECT_NAME_COLLISION (MS-FSA 2.1.5.1.2). */
static void
check_overwrite_met_directory(void)
{
    CHECK(chimera_smb_overwrite_met_directory(0, SMB2_FILE_OVERWRITE_IF, false,
                                              CHIMERA_VFS_EISDIR),
          "OVERWRITE_IF refused EISDIR met a directory");
    CHECK(chimera_smb_overwrite_met_directory(0, SMB2_FILE_OVERWRITE, false,
                                              CHIMERA_VFS_EISDIR),
          "OVERWRITE refused EISDIR met a directory");
    CHECK(chimera_smb_overwrite_met_directory(0, SMB2_FILE_SUPERSEDE, false,
                                              CHIMERA_VFS_EISDIR),
          "SUPERSEDE refused EISDIR met a directory");
    CHECK(!chimera_smb_overwrite_met_directory(0, SMB2_FILE_OPEN, false,
                                               CHIMERA_VFS_EISDIR),
          "an open that does not overwrite keeps its EISDIR answer");
    CHECK(!chimera_smb_overwrite_met_directory(SMB2_FILE_NON_DIRECTORY_FILE,
                                               SMB2_FILE_OVERWRITE_IF, false,
                                               CHIMERA_VFS_EISDIR),
          "FILE_NON_DIRECTORY_FILE keeps FILE_IS_A_DIRECTORY");
    CHECK(!chimera_smb_overwrite_met_directory(0, SMB2_FILE_OVERWRITE_IF, true,
                                               CHIMERA_VFS_EISDIR),
          "a named-stream CREATE is not an overwrite of the directory");
    CHECK(!chimera_smb_overwrite_met_directory(0, SMB2_FILE_OVERWRITE_IF, false,
                                               CHIMERA_VFS_EACCES),
          "another refusal keeps its answer");
} /* check_overwrite_met_directory */

int
main(void)
{
    check_reopen_read_only();
    check_overwrite_met_directory();

    /* chimera_smb_open_read_only(desired_access, replaces_content,
     * ro_retried): the open mode CREATE gives the backend, for the file open
     * and the named-stream open. */
    CHECK(chimera_smb_open_read_only(SMB2_FILE_WRITE_ATTRIBUTES, false, false),
          "a metadata-write open is read-only");
    CHECK(!chimera_smb_open_read_only(SMB2_FILE_WRITE_DATA, false, false),
          "a data-write open is read-write");
    CHECK(!chimera_smb_open_read_only(MAX, false, false),
          "a MAXIMUM_ALLOWED open is read-write");
    CHECK(chimera_smb_open_read_only(MAX, false, true),
          "a MAXIMUM_ALLOWED open re-issued read-only is read-only");
    CHECK(!chimera_smb_open_read_only(SMB2_DELETE | SMB2_FILE_WRITE_ATTRIBUTES,
                                      true, false),
          "an open that replaces content is read-write");
    CHECK(chimera_smb_open_read_only(SMB2_DELETE | SMB2_FILE_WRITE_ATTRIBUTES,
                                     true, true),
          "an open that replaces content, re-issued read-only, is read-only");

    for (size_t i = 0; i < sizeof(writes_cases) / sizeof(writes_cases[0]); i++) {
        const struct writes_case *wc = &writes_cases[i];

        CHECK(chimera_smb_access_writes_data(wc->access) == wc->writes,
              "writes_data(%s) != %s", wc->name, wc->writes ? "true" : "false");
    }

    CHECK(chimera_smb_max_allowed_trim(FILE_ALL, false, true) == 0x001f01f9u,
          "a read-only file handle loses WRITE_DATA and APPEND_DATA");
    CHECK(chimera_smb_max_allowed_trim(FILE_ALL, true, true) == FILE_ALL,
          "a read-only directory handle keeps its grant");
    CHECK(chimera_smb_max_allowed_trim(FILE_ALL, false, false) == FILE_ALL,
          "a read-write file handle keeps its grant");

    fprintf(stderr, "smb_access_test: %d passed, %d failed\n", passed, failed);
    return failed ? 1 : 0;
} /* main */
