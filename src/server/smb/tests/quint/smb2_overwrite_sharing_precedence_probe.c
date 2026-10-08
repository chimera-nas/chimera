/* SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
 *
 * SPDX-License-Identifier: LGPL-2.1-only
 *
 * SMB2 CREATE precedence probe: for a SUPERSEDE/OVERWRITE/OVERWRITE_IF create
 * against an incompatible concurrent open, the share-mode claim is adjudicated
 * BEFORE any ACCESS_DENIED from the overwrite attribute/permission path, so the
 * client sees SHARING_VIOLATION, not ACCESS_DENIED (MS-FSA 2.1.5.1.2; samba
 * smb2.acls.OVERWRITE_READ_ONLY_FILE sharing_tcases).  This is the unprivileged,
 * in-process memfs gate for task smb-create-overwrite-sharing-violation-
 * precedence; the full smbtorture smb2.acls proof is netns/operator-run.
 *
 * The target is stamped FILE_ATTRIBUTE_READONLY so that WITHOUT the fix the
 * overwrite DOS-attribute check returns ACCESS_DENIED before the share claim --
 * exactly the inversion under test.  Two checks:
 *   (1) regression safety: an OVERWRITE of the READONLY file with NO conflicting
 *       open still returns ACCESS_DENIED (the deferred verdict still fires once
 *       the claim is granted -- the fix is a reorder, never a blanket convert);
 *   (2) the fix: with a concurrent handle that denies write, the OVERWRITE
 *       returns SHARING_VIOLATION.
 */

#include "smb2_mbt_common.h"

/* FILE_BASIC_INFORMATION: four 8-byte timestamps (0 = "no change") then a
 * 4-byte FileAttributes field at offset 32 (MS-FSCC 2.4.7), 40 bytes total. */
#define BASIC_INFO_LEN              40
#define BASIC_INFO_ATTR_OFF         32
#define FILE_ATTRIBUTE_READONLY_BIT 0x01u

static int failures = 0;

#define CHECK(cond, ...)                             \
        do {                                         \
            if (cond) {                              \
                printf("ok   - " __VA_ARGS__);       \
                printf("\n");                        \
            } else {                                 \
                printf("FAIL - " __VA_ARGS__);       \
                printf("\n");                        \
                failures++;                          \
            }                                        \
        } while (0)

int
main(
    int   argc,
    char *argv[])
{
    struct smb2_env        env;
    struct smb2_conn      *a, *b;
    struct smb2_create_out setup, hold, o;
    uint8_t                basic[BASIC_INFO_LEN];
    uint32_t               st;

    (void) argc;
    (void) argv;

    smb2_env_start(&env);
    a = smb2_conn_open(&env);
    smb2_handshake(a);
    b = smb2_conn_open(&env);
    smb2_handshake(b);

    /* Create the target and stamp it READONLY via FILE_BASIC_INFORMATION. */
    st = smb2_create(a, "ro_ovr.bin", MBT_FILE_OPEN_IF, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &setup);
    CHECK(st == ST_SUCCESS, "setup: CREATE ro_ovr.bin -> 0x%08x", st);

    memset(basic, 0, sizeof(basic));
    basic[BASIC_INFO_ATTR_OFF] = FILE_ATTRIBUTE_READONLY_BIT;   /* LE32 @ off 32 */
    st                         = smb2_set_info(a, SMB2_INFO_FILE_T, SMB2_FILE_BASIC_INFO_T,
                                               setup.file_id, basic, sizeof(basic));
    CHECK(st == ST_SUCCESS, "setup: SET BasicInformation READONLY -> 0x%08x", st);
    smb2_close(a, setup.file_id);

    /* (1) Regression safety: no conflicting open -> the claim is GRANTED -> the
     * deferred DOS-attribute refusal still fires -> ACCESS_DENIED.  A read-only
     * overwrite must not be silently turned into a share violation. */
    st = smb2_create(a, "ro_ovr.bin", MBT_FILE_OVERWRITE, MBT_FILE_READ_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &o);
    CHECK(st == ST_ACCESS_DENIED,
          "no-conflict OVERWRITE of READONLY -> ACCESS_DENIED (0x%08x)", st);
    if (st == ST_SUCCESS) {
        smb2_close(a, o.file_id);
    }

    /* (2) The fix: hold the file with a share mode that denies write, then an
     * OVERWRITE (which asserts a transient write to replace the content) must
     * lose the share-mode arbitration FIRST -> SHARING_VIOLATION, not the
     * ACCESS_DENIED the pre-claim DOS-attribute check used to return. */
    st = smb2_create(a, "ro_ovr.bin", MBT_FILE_OPEN, MBT_FILE_READ_ACCESS,
                     MBT_FILE_SHARE_READ, NULL, &hold);
    CHECK(st == ST_SUCCESS,
          "hold READONLY file (share=READ, denies write) -> 0x%08x", st);

    st = smb2_create(b, "ro_ovr.bin", MBT_FILE_OVERWRITE, MBT_FILE_READ_ACCESS,
                     MBT_FILE_SHARE_READ, NULL, &o);
    CHECK(st == ST_SHARING_VIOLATION,
          "OVERWRITE vs deny-write holder -> SHARING_VIOLATION, not "
          "ACCESS_DENIED (0x%08x)", st);
    if (st == ST_SUCCESS) {
        smb2_close(b, o.file_id);
    }

    smb2_close(a, hold.file_id);

    smb2_env_stop(&env);

    if (failures) {
        fprintf(stderr, "%d precedence check(s) FAILED\n", failures);
        return 1;
    }
    printf("smb2_overwrite_sharing_precedence_probe: all checks passed\n");
    return 0;
} /* main */
