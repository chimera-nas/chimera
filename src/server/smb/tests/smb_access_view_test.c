// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * The SMB server's access check on objects whose access comes from a POSIX
 * mode (chimera_smb_access_check, chimera_smb_acl_view), driven directly
 * without a server.
 *
 * A backend that stores no ACL (linux, io_uring) hands the check the mode
 * alone.  A caller other than the owner is then judged against the mode in
 * Windows terms: an allowed r, w or x grants FILE_GENERIC_READ,
 * FILE_GENERIC_WRITE or FILE_GENERIC_EXECUTE, and a denied w also denies
 * FILE_WRITE_ATTRIBUTES.  QUERY security of an object whose getattr reports no
 * ACL keeps reporting none (chimera_smb_acl_view(attr, 0)), as on io_uring;
 * linux synthesizes an ACL from the mode when a getattr asks for one, so its
 * QUERY security returns the Windows-terms copy.
 *
 * A stored ACL is read the same way when its OWNER@, GROUP@ and EVERYONE@
 * entries have the shape chimera_acl_from_mode() gives them, and is judged as
 * it stands otherwise.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#undef NDEBUG
#include <assert.h>

#include "vfs/vfs.h"
#include "vfs/sdk/vfs_attrs.h"
#include "vfs/sdk/vfs_acl.h"
#include "vfs/sdk/vfs_cred.h"
#include "common/logging.h"

#include "server/smb/smb_internal.h"
#include "server/smb/smb_procs.h"

#define FILE_GENERIC_READ_MASK  0x00120089u
#define FILE_GENERIC_WRITE_MASK 0x00120116u
#define FILE_GENERIC_RW_MASK    (FILE_GENERIC_READ_MASK | FILE_GENERIC_WRITE_MASK)

#define TEST_PASS(name) fprintf(stderr, "  PASS: %s\n", name)

/* A regular file with only a mode, as linux and io_uring report it. */
static void
mode_only(
    struct chimera_vfs_attrs *attr,
    uint32_t                  perm,
    uint32_t                  uid,
    uint32_t                  gid)
{
    memset(attr, 0, sizeof(*attr));
    attr->va_set_mask = CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_UID |
        CHIMERA_VFS_ATTR_GID;
    attr->va_mode = S_IFREG | perm;
    attr->va_uid  = uid;
    attr->va_gid  = gid;
} /* mode_only */

/* Another user's 0644 file: "other" reads it with a generic read, which
 * includes SYNCHRONIZE, and cannot write it. */
static void
test_other_reads_0644(void)
{
    struct chimera_vfs_attrs attr;
    struct chimera_vfs_cred  cred;
    uint32_t                 granted;

    mode_only(&attr, 0644, 0, 0);
    chimera_vfs_cred_init_unix(&cred, 1000, 1000, 0, NULL);

    granted = chimera_smb_access_check(&attr, &cred, CHIMERA_ACE_MASK_ALL);

    /* Exactly FILE_GENERIC_READ: nothing to write with, and none of WRITE_DAC,
     * WRITE_OWNER, DELETE, FILE_EXECUTE or FILE_DELETE_CHILD. */
    assert(granted == FILE_GENERIC_READ_MASK);

    TEST_PASS("other is granted FILE_GENERIC_READ on a mode-only 0644 file");
} /* test_other_reads_0644 */

/* A 0646 file of group 1000: the group may not write, though everyone else
 * may.  The group's denied w also denies FILE_WRITE_ATTRIBUTES, which the
 * everyone entry would otherwise hand it. */
static void
test_group_denied_write_0646(void)
{
    struct chimera_vfs_attrs attr;
    struct chimera_vfs_cred  member, other;
    uint32_t                 granted;

    mode_only(&attr, 0646, 0, 1000);
    chimera_vfs_cred_init_unix(&member, 1000, 1000, 0, NULL);
    chimera_vfs_cred_init_unix(&other, 2000, 2000, 0, NULL);

    granted = chimera_smb_access_check(&attr, &member, CHIMERA_ACE_MASK_ALL);
    assert(granted == FILE_GENERIC_READ_MASK);

    granted = chimera_smb_access_check(&attr, &other, CHIMERA_ACE_MASK_ALL);
    assert(granted == FILE_GENERIC_RW_MASK);

    TEST_PASS("a group the mode keeps from writing gets no FILE_WRITE_ATTRIBUTES");
} /* test_group_denied_write_0646 */

/* A stored ACL whose OWNER@, GROUP@ and EVERYONE@ entries have the shape
* chimera_acl_from_mode() gives them, with no control flags, as a store that
* keeps none hands it back: it is read in Windows terms, by its shape. */
static void
test_stored_mode_shape(void)
{
    uint64_t                  buf[(sizeof(struct chimera_acl) +
                                   8 * sizeof(struct chimera_ace) + 7) / 8];
    struct chimera_acl       *acl = (struct chimera_acl *) buf;
    const struct chimera_acl *view;
    struct chimera_vfs_attrs  attr;
    struct chimera_vfs_cred   member;
    uint32_t                  granted;

    mode_only(&attr, 0664, 0, 1000);
    assert(chimera_acl_from_mode(0664, acl, 8) == 3);
    acl->ctrl_flags   = 0;
    attr.va_acl       = acl;
    attr.va_set_mask |= CHIMERA_VFS_ATTR_ACL;
    chimera_vfs_cred_init_unix(&member, 1000, 1000, 0, NULL);

    granted = chimera_smb_access_check(&attr, &member, CHIMERA_ACE_MASK_ALL);
    assert(granted == FILE_GENERIC_RW_MASK);

    view = chimera_smb_acl_view(&attr, 0);
    assert(view && view != acl && view->num_aces == 3);
    assert(view->aces[1].access_mask == FILE_GENERIC_RW_MASK);
    assert(view->aces[2].access_mask == FILE_GENERIC_READ_MASK);

    TEST_PASS("a stored ACL of the mode's shape is read in Windows terms");
} /* test_stored_mode_shape */

/* A stored entry outside the mode's shape is judged as it stands: an
 * EVERYONE@ grant of FILE_READ_DATA with READ_CONTROL but no
 * READ_ATTRIBUTES gives no SYNCHRONIZE. */
static void
test_stored_other_shape(void)
{
    uint64_t                 buf[(sizeof(struct chimera_acl) +
                                  sizeof(struct chimera_ace) + 7) / 8];
    struct chimera_acl      *acl = (struct chimera_acl *) buf;
    struct chimera_vfs_attrs attr;
    struct chimera_vfs_cred  other;
    uint32_t                 granted;

    mode_only(&attr, 0666, 0, 0);
    memset(acl, 0, sizeof(buf));
    acl->num_aces            = 1;
    acl->aces[0].type        = CHIMERA_ACE_ALLOWED;
    acl->aces[0].access_mask = CHIMERA_ACE_READ_DATA | CHIMERA_ACE_READ_ACL;
    acl->aces[0].who.type    = CHIMERA_PRINCIPAL_SPECIAL;
    acl->aces[0].who.special = CHIMERA_WHO_EVERYONE;
    attr.va_acl              = acl;
    attr.va_set_mask        |= CHIMERA_VFS_ATTR_ACL;
    chimera_vfs_cred_init_unix(&other, 2000, 2000, 0, NULL);

    assert(chimera_smb_acl_view(&attr, 0) == acl);
    granted = chimera_smb_access_check(&attr, &other, CHIMERA_ACE_MASK_ALL);
    assert(granted & CHIMERA_ACE_READ_DATA);
    assert(!(granted & CHIMERA_ACE_SYNCHRONIZE));

    TEST_PASS("a stored entry of another shape is judged as it stands");
} /* test_stored_other_shape */

/* A stored ACL longer than CHIMERA_ACL_MAX_ACES, as no in-tree module returns,
 * is judged as it stands rather than copied into the view's buffer, even when
 * an entry has the mode's shape. */
static void
test_view_over_max_aces(void)
{
    unsigned                 n   = CHIMERA_ACL_MAX_ACES + 1;
    struct chimera_acl      *acl = calloc(1, chimera_acl_size(n));
    struct chimera_vfs_attrs attr;

    assert(acl);
    mode_only(&attr, 0664, 0, 1000);
    assert(chimera_acl_from_mode(0664, acl, n) == 3);
    for (unsigned i = 3; i < n; i++) {
        acl->aces[i]             = acl->aces[2];
        acl->aces[i].access_mask = CHIMERA_ACE_READ_DATA;
    }
    acl->num_aces     = n;
    acl->ctrl_flags   = 0;
    attr.va_acl       = acl;
    attr.va_set_mask |= CHIMERA_VFS_ATTR_ACL;

    assert(chimera_smb_acl_view(&attr, 0) == acl);
    free(acl);

    TEST_PASS("a stored ACL past CHIMERA_ACL_MAX_ACES is judged as it stands");
} /* test_view_over_max_aces */

/* QUERY security of a mode-only object reports no ACL, so the caller keeps
 * emitting the modefromsid entry. */
static void
test_query_view_mode_only(void)
{
    struct chimera_vfs_attrs attr;

    mode_only(&attr, 0644, 0, 0);
    assert(chimera_smb_acl_view(&attr, 0) == NULL);

    TEST_PASS("QUERY security of a mode-only object reports no ACL");
} /* test_query_view_mode_only */

int
main(
    int    argc,
    char **argv)
{
    (void) argc;
    (void) argv;

    chimera_log_init();

    test_other_reads_0644();
    test_group_denied_write_0646();
    test_stored_mode_shape();
    test_stored_other_shape();
    test_query_view_mode_only();
    test_view_over_max_aces();

    fprintf(stderr, "All SMB access view tests passed\n");
    return 0;
} /* main */
