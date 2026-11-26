// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Drives smb_wbclient_identity_handler() directly for the two account keys.
 * wbclient_stub.c, exported from this executable, answers in place of
 * libwbclient, the same interposition smb2_wire_probe_wbstub uses.
 */

#include <stdio.h>
#include <string.h>

#include "server/smb/smb_wbclient.h"
#include "wbclient_stub.h"

static int failures;

#define CHECK(cond, what) \
        do { \
            if (!(cond)) { \
                fprintf(stderr, "FAIL: %s (%s:%d)\n", what, __FILE__, __LINE__); \
                failures++; \
            } \
        } while (0)

static enum chimera_vfs_identity_status
lookup(
    enum chimera_vfs_identity_key       key,
    const char                         *name,
    struct chimera_vfs_identity_result *out)
{
    memset(out, 0, sizeof(*out));
    return smb_wbclient_identity_handler(key, 0, name, out, NULL);
} /* lookup */

static void
expect_account(
    const char                   *what,
    enum chimera_vfs_identity_key key,
    const char                   *in,
    const char                   *sid,
    const char                   *domain,
    const char                   *name,
    uint32_t                      type)
{
    struct chimera_vfs_identity_result out;
    enum chimera_vfs_identity_status   rc = lookup(key, in, &out);

    if (rc != CHIMERA_VFS_IDENTITY_OK ||
        strcmp(out.account.sid, sid) != 0 ||
        strcmp(out.account.domain, domain) != 0 ||
        strcmp(out.account.name, name) != 0 ||
        out.account.sid_type != type) {
        fprintf(stderr,
                "FAIL: %s: rc %d, got {%s, %s, %s, %u}\n", what, (int) rc,
                out.account.sid, out.account.domain, out.account.name,
                out.account.sid_type);
        failures++;
    }
} /* expect_account */

static void
expect_status(
    const char                      *what,
    enum chimera_vfs_identity_key    key,
    const char                      *in,
    enum chimera_vfs_identity_status want)
{
    struct chimera_vfs_identity_result out;
    enum chimera_vfs_identity_status   rc = lookup(key, in, &out);

    if (rc != want) {
        fprintf(stderr, "FAIL: %s: rc %d, want %d\n", what, (int) rc, (int) want);
        failures++;
    }
} /* expect_status */

int
main(void)
{
    struct chimera_vfs_identity_result out;

    expect_account("sid to account: user", CHIMERA_VFS_IDENTITY_SID_TO_ACCOUNT,
                   WBSTUB_USER_SID, WBSTUB_USER_SID, "WBSTUB", "testuser1", 1);
    expect_account("sid to account: group", CHIMERA_VFS_IDENTITY_SID_TO_ACCOUNT,
                   WBSTUB_GROUP_SID, WBSTUB_GROUP_SID, "WBSTUB",
                   "Domain Users", 2);
    expect_status("sid to account: unknown is not found",
                  CHIMERA_VFS_IDENTITY_SID_TO_ACCOUNT, "S-1-5-21-1-2-3-4242",
                  CHIMERA_VFS_IDENTITY_NOT_FOUND);
    expect_status("sid to account: winbind down is unavailable",
                  CHIMERA_VFS_IDENTITY_SID_TO_ACCOUNT, WBSTUB_DOWN_SID,
                  CHIMERA_VFS_IDENTITY_UNAVAILABLE);

    expect_account("name to account: domain\\name",
                   CHIMERA_VFS_IDENTITY_NAME_TO_ACCOUNT, "WBSTUB\\user1002",
                   "S-1-5-21-1-2-3-1002", "WBSTUB", "user1002", 1);
    CHECK(strcmp(wbstub_last_lookup_domain, "WBSTUB") == 0 &&
          strcmp(wbstub_last_lookup_name, "user1002") == 0,
          "the domain is split off before winbind sees it");
    expect_account("name to account: bare name",
                   CHIMERA_VFS_IDENTITY_NAME_TO_ACCOUNT, "user1002",
                   "S-1-5-21-1-2-3-1002", "WBSTUB", "user1002", 1);
    expect_account("name to account: mixed case",
                   CHIMERA_VFS_IDENTITY_NAME_TO_ACCOUNT, "wbstub\\USER1002",
                   "S-1-5-21-1-2-3-1002", "WBSTUB", "user1002", 1);
    expect_account("name to account: group",
                   CHIMERA_VFS_IDENTITY_NAME_TO_ACCOUNT, "Domain Users",
                   WBSTUB_GROUP_SID, "WBSTUB", "Domain Users", 2);
    expect_status("name to account: unknown is not found",
                  CHIMERA_VFS_IDENTITY_NAME_TO_ACCOUNT, "WBSTUB\\nosuch",
                  CHIMERA_VFS_IDENTITY_NOT_FOUND);

    /* Winbind named a SID for the name and then cannot name that SID: the
     * lookup did not finish, which says nothing about the name. */
    expect_status("name to account: a sid winbind cannot name back is "
                  "unavailable, not an unknown name",
                  CHIMERA_VFS_IDENTITY_NAME_TO_ACCOUNT,
                  "WBSTUB\\" WBSTUB_HALF_NAME,
                  CHIMERA_VFS_IDENTITY_UNAVAILABLE);

    CHECK(lookup(CHIMERA_VFS_IDENTITY_BY_SID, WBSTUB_GROUP_SID, &out) ==
          CHIMERA_VFS_IDENTITY_OK && out.is_group == 1 &&
          out.group.gid == WBSTUB_GID,
          "by-sid still yields a group record for a group sid");

    CHECK(wbstub_lookup_calls > 0, "the stub counted lookups");

    if (failures) {
        fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    printf("wbstub_account_test: ok\n");
    return 0;
} /* main */
