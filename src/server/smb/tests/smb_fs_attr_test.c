// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* FileFsAttributeInformation (MS-FSCC 2.5.1) FileSystemAttributes derivation
 * (smb_attr.h chimera_smb_fs_attributes): the flag word must follow the
 * serving VFS module's capabilities and the named-streams knob, not a
 * compile-time constant.  The memfs-only ground-truth probe can only see the
 * "everything on" shape; this pins the capability-poor shapes (a proxy
 * backend, a mode-only backend, the knob off) that no quick-tier backend has.
 */

#include <stdint.h>
#include <stdio.h>

#include "server/smb/smb_attr.h"
#include "smb_common/smb2.h"
#include "vfs/sdk/vfs_module.h"

#define CHECK(cond)                                                  \
        do {                                                         \
            if (!(cond)) {                                           \
                fprintf(stderr,                                      \
                        "smb_fs_attr_test: FAILED at %s:%d: %s\n",   \
                        __FILE__, __LINE__, # cond);                 \
                return 1;                                            \
            }                                                        \
        } while (0)

/* Every backend gets these: names are case-sensitive and case-preserving,
 * stored as Unicode, and reparse points (symlinks / device nodes via the NFS
 * reparse tag) ride on symlink_at / mknod_at, which every FS module has. */
#define BASE (SMB2_FS_ATTR_CASE_SENSITIVE_SEARCH |  \
              SMB2_FS_ATTR_CASE_PRESERVED_NAMES |   \
              SMB2_FS_ATTR_UNICODE_ON_DISK |        \
              SMB2_FS_ATTR_SUPPORTS_REPARSE_POINTS)

/* A capability-less module (the nfs proxy shape) advertises only the base
 * set: no sparse, no refcounting, no ACL persistence, no streams. */
static int
test_no_caps(void)
{
    CHECK(chimera_smb_fs_attributes(0, 0) == BASE);
    CHECK(chimera_smb_fs_attributes(0, 1) == BASE);
    return 0;
} /* test_no_caps */

/* Each capability-derived bit is set by exactly its own capability. */
static int
test_each_cap_alone(void)
{
    CHECK(chimera_smb_fs_attributes(CHIMERA_VFS_CAP_SPARSE, 0) ==
          (BASE | SMB2_FS_ATTR_SUPPORTS_SPARSE_FILES));
    CHECK(chimera_smb_fs_attributes(CHIMERA_VFS_CAP_CLONE_RANGE, 0) ==
          (BASE | SMB2_FS_ATTR_SUPPORTS_BLOCK_REFCOUNTING));
    CHECK(chimera_smb_fs_attributes(CHIMERA_VFS_CAP_ACL_NATIVE, 0) ==
          (BASE | SMB2_FS_ATTR_PERSISTENT_ACLS));
    return 0;
} /* test_each_cap_alone */

/* FILE_NAMED_STREAMS needs BOTH the backend capability and the
 * smb_named_streams knob -- the same gate FileStreamInformation and the
 * stream CREATE path apply -- so a client is never told about streams the
 * server would then refuse. */
static int
test_named_streams_gate(void)
{
    CHECK(chimera_smb_fs_attributes(CHIMERA_VFS_CAP_NAMED_STREAMS, 0) == BASE);
    CHECK(chimera_smb_fs_attributes(0, 1) == BASE);
    CHECK(chimera_smb_fs_attributes(CHIMERA_VFS_CAP_NAMED_STREAMS, 1) ==
          (BASE | SMB2_FS_ATTR_NAMED_STREAMS));
    return 0;
} /* test_named_streams_gate */

/* A mode-only passthrough (the linux shape: sparse + reflink, ACLs derived
 * from mode bits) must not claim ACL persistence. */
static int
test_mode_only_backend(void)
{
    uint64_t caps = CHIMERA_VFS_CAP_FS | CHIMERA_VFS_CAP_XATTR |
        CHIMERA_VFS_CAP_SPARSE | CHIMERA_VFS_CAP_CLONE_RANGE |
        CHIMERA_VFS_CAP_DELEGATES_DAC;

    CHECK(chimera_smb_fs_attributes(caps, 1) ==
          (BASE | SMB2_FS_ATTR_SUPPORTS_SPARSE_FILES |
           SMB2_FS_ATTR_SUPPORTS_BLOCK_REFCOUNTING));
    return 0;
} /* test_mode_only_backend */

/* Unrelated capabilities never leak into the word. */
static int
test_unrelated_caps_ignored(void)
{
    uint64_t caps = CHIMERA_VFS_CAP_FS | CHIMERA_VFS_CAP_KV |
        CHIMERA_VFS_CAP_XATTR | CHIMERA_VFS_CAP_LAYOUT |
        CHIMERA_VFS_CAP_MKFS | CHIMERA_VFS_CAP_CHANGE |
        CHIMERA_VFS_CAP_READ_PLUS | CHIMERA_VFS_CAP_WRITE_SAME |
        CHIMERA_VFS_CAP_COPY_RANGE | CHIMERA_VFS_CAP_MOVE_RANGE;

    CHECK(chimera_smb_fs_attributes(caps, 1) == BASE);
    return 0;
} /* test_unrelated_caps_ignored */

int
main(void)
{
    if (test_no_caps()) {
        return 1;
    }
    if (test_each_cap_alone()) {
        return 1;
    }
    if (test_named_streams_gate()) {
        return 1;
    }
    if (test_mode_only_backend()) {
        return 1;
    }
    if (test_unrelated_caps_ignored()) {
        return 1;
    }
    printf("smb_fs_attr_test: all checks passed\n");
    return 0;
} /* main */
