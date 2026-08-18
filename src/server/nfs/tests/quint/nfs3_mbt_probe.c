// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Regression guard for the NFS3-layer fixes found via model-based testing
 * -- running against an IN-PROCESS
 * chimera server over the inproc transport (see nfs3_mbt_common.h).
 *
 * For each item in DEVIATIONS.md this checks the *current expected*
 * behavior:
 *
 *   - F1/F2 (LINK / RENAME self-alias deadlocks) -- must return the
 *     correct error promptly rather than hanging.  A hang here spins in
 *     mbt_call_wait forever; the alarm() below turns that into a SIGALRM
 *     kill and a failed test.
 *   - F3/F4 (symlink LOOKUP/READDIR; exclusive-create same-verifier
 *     retry) -- were deviations D1-D3, now fixed in the NFS3 layer, so
 *     this asserts the RFC-correct replies.
 *   - F5 (RMDIR/REMOVE type enforcement, was D4) -- asserts the fixed
 *     behavior: RMDIR of a non-directory is NOTDIR, REMOVE of a directory
 *     is ISDIR, and the target survives the rejected call.
 */

#include "nfs3_mbt_common.h"

static int failures = 0;

static void
expect(
    const char *label,
    uint32_t    got,
    uint32_t    want)
{
    if (got != want) {
        printf("  FAIL %s: got status %u, expected %u\n", label, got, want);
        failures++;
    } else {
        printf("  ok  %s: status %u\n", label, got);
    }
} /* expect */

int
main(void)
{
    struct mbt_env    *env = malloc(sizeof(*env));
    struct mbt_result *res;
    struct mbt_fh      root;
    struct mbt_fh      sl;
    struct mbt_fh      d1;
    struct mbt_fh      pd;
    uint8_t            verf_a[NFS3_CREATEVERFSIZE] =
    { 0, 0, 0, 0, 0xab, 0xcd, 0x12, 0x34 };
    uint8_t            verf_b[NFS3_CREATEVERFSIZE] =
    { 0, 0, 0, 0, 0, 0, 0xbe, 0xef };

    /* A deadlock regression would hang a call forever; die loudly. */
    alarm(60);

    mbt_env_start(env);

    res = mbt_mnt(env, "/share");
    if (res->rpc_err || res->status != MNT3_OK) {
        fprintf(stderr, "MNT /share failed: %u\n", res->status);
        return 1;
    }
    root = res->obj_fh;

    printf("F3 symlink LOOKUP/READDIR -> NOTDIR (fixed; was D1/D2):\n");
    res = mbt_symlink(env, &root, "sl", 2, "target", 0777);
    if (res->status != NFS3_OK) {
        fprintf(stderr, "SYMLINK sl failed: %u\n", res->status);
        return 1;
    }
    res = mbt_lookup(env, &root, "sl", 2);
    if (res->status != NFS3_OK || !res->obj_fh.has) {
        fprintf(stderr, "LOOKUP sl failed: %u\n", res->status);
        return 1;
    }
    sl = res->obj_fh;
    expect("LOOKUP via symlink handle",
           mbt_lookup(env, &sl, "x", 1)->status, NFS3ERR_NOTDIR);
    expect("READDIR via symlink handle",
           mbt_readdir(env, &sl)->status, NFS3ERR_NOTDIR);
    expect("READDIRPLUS via symlink handle",
           mbt_readdirplus(env, &sl)->status, NFS3ERR_NOTDIR);

    printf("F4 exclusive-create same-verifier retry is idempotent "
           "(fixed; was D3):\n");
    expect("EXCLUSIVE create fresh",
           mbt_create(env, &root, "ex", 2, EXCLUSIVE, -1, verf_a)->status,
           NFS3_OK);
    expect("EXCLUSIVE retry SAME verifier -> idempotent OK",
           mbt_create(env, &root, "ex", 2, EXCLUSIVE, -1, verf_a)->status,
           NFS3_OK);
    expect("EXCLUSIVE retry DIFFERENT verifier -> EXIST",
           mbt_create(env, &root, "ex", 2, EXCLUSIVE, -1, verf_b)->status,
           NFS3ERR_EXIST);

    printf("F5 RMDIR/REMOVE type enforcement (fixed; was D4):\n");
    res = mbt_create(env, &root, "reg", 3, UNCHECKED, 0644, NULL);
    if (res->status != NFS3_OK) {
        fprintf(stderr, "CREATE reg failed: %u\n", res->status);
        return 1;
    }
    expect("RMDIR of a regular file -> NOTDIR",
           mbt_rmdir(env, &root, "reg", 3)->status, NFS3ERR_NOTDIR);
    expect("  file survives the rejected RMDIR",
           mbt_lookup(env, &root, "reg", 3)->status, NFS3_OK);
    res = mbt_mkdir(env, &root, "adir", 4, 0755);
    if (res->status != NFS3_OK) {
        fprintf(stderr, "MKDIR adir failed: %u\n", res->status);
        return 1;
    }
    expect("REMOVE of a directory -> ISDIR",
           mbt_remove(env, &root, "adir", 4)->status, NFS3ERR_ISDIR);
    expect("  directory survives the rejected REMOVE",
           mbt_lookup(env, &root, "adir", 4)->status, NFS3_OK);

    printf("F1 LINK self-alias deadlock (fixed; must not hang):\n");
    res = mbt_mkdir(env, &root, "d1", 2, 0755);
    if (res->status != NFS3_OK) {
        fprintf(stderr, "MKDIR d1 failed: %u\n", res->status);
        return 1;
    }
    d1 = mbt_lookup(env, &root, "d1", 2)->obj_fh;
    expect("LINK directory onto its own handle",
           mbt_link(env, &d1, &d1, "x", 1)->status, NFS3ERR_ISDIR);

    printf("F2 RENAME parent-alias deadlock (fixed; must not hang):\n");
    res = mbt_mkdir(env, &root, "pd", 2, 0755);
    if (res->status != NFS3_OK) {
        fprintf(stderr, "MKDIR pd failed: %u\n", res->status);
        return 1;
    }
    pd  = mbt_lookup(env, &root, "pd", 2)->obj_fh;
    res = mbt_mknod(env, &pd, "f", 1, NF3FIFO, 0644);
    if (res->status != NFS3_OK) {
        fprintf(stderr, "MKNOD pd/f failed: %u\n", res->status);
        return 1;
    }
    expect("RENAME child onto a name resolving to its parent",
           mbt_rename(env, &pd, "f", 1, &root, "pd", 2)->status,
           NFS3ERR_ISDIR);

    mbt_env_stop(env);
    free(env);

    if (failures) {
        printf("\n%d deviation assertion(s) no longer hold:\n"
               "If chimera was fixed, update the deviation registry + "
               "DEVIATIONS.md; if a deadlock reappeared, that is a "
               "regression.\n", failures);
        return 1;
    }
    printf("\nAll documented deviations reproduce; both deadlock fixes "
           "hold.\n");
    return 0;
} /* main */
