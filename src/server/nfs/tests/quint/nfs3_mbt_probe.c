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
 *   - F6 (a filehandle whose object is gone) -- asserts NFS3ERR_STALE,
 *     which is what RFC 1813 section 3.3 describes and what every error
 *     list that mentions a dead handle names.  It used to be
 *     NFS3ERR_NOENT, which is not in SETATTR's or LINK's error list at
 *     all and which a client takes as final where it would retry a
 *     path resolution on STALE.
 */

#include "nfs3_mbt_common.h"
#include "common/mbt_watchdog.h"

static int failures = 0;

/*
 * Wait for a filehandle to go stale.
 *
 * The contract is that a handle stops resolving once its object is gone, not
 * that it stops on the very next call.  memfs and cairn free the inode inline;
 * diskfs reclaims it in the background, and a client cannot tell the difference
 * because it has already been told the remove succeeded.
 *
 * The gap between checks is the part that matters, and it is not arbitrary.
 * Resolving a handle puts it back in the VFS handle cache, which pins the
 * object until the close sweep releases it -- so a tight poll keeps alive
 * exactly the thing it is waiting to see die, and never terminates.  Checking
 * at an interval well clear of the sweep leaves the handle idle long enough to
 * be reclaimed between attempts.
 *
 * An unlinked object that something holds open is a different case and stays
 * reachable, which is why nothing here holds one open.
 */
#define PROBE_STALE_GAP_US 250000
#define PROBE_STALE_TRIES  20

static uint32_t
await_stale(
    struct mbt_env      *env,
    const struct mbt_fh *fh)
{
    uint32_t st = NFS3_OK;
    int      i;

    for (i = 0; i < PROBE_STALE_TRIES && st == NFS3_OK; i++) {
        st = mbt_getattr(env, fh)->status;
        if (st == NFS3_OK) {
            usleep(PROBE_STALE_GAP_US);
        }
    }
    return st;
} /* await_stale */

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
main(
    int    argc,
    char **argv)
{
    struct mbt_env     *env = malloc(sizeof(*env));
    struct mbt_env_opts opts;
    const char         *backend = "memfs";
    int                 i;
    struct mbt_result  *res;
    struct mbt_fh       root;
    struct mbt_fh       sl;
    struct mbt_fh       d1;
    struct mbt_fh       pd;
    uint8_t             verf_a[NFS3_CREATEVERFSIZE] =
    { 0, 0, 0, 0, 0xab, 0xcd, 0x12, 0x34 };
    uint8_t             verf_b[NFS3_CREATEVERFSIZE] =
    { 0, 0, 0, 0, 0, 0, 0xbe, 0xef };

    /* The fixes this guards are in the NFS3 layer, except F6, which is in
     * each storage backend -- so the backend is selectable and every one that
     * this build has is registered as its own test. */
    for (i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-b") == 0 && i + 1 < argc) {
            backend = argv[++i];
        }
    }

    /* A deadlock regression would hang a call forever; die loudly. */
    mbt_watchdog_arm(60);

    memset(&opts, 0, sizeof(opts));
    opts.module = backend;
    mbt_env_start_opts(env, &opts);

    printf("backend: %s\n", backend);

    res = mbt_mnt(env, "/fs0");
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

    printf("F6 a filehandle whose object is gone -> STALE:\n");
    {
        struct mbt_fh gone;
        struct mbt_fh gone_dir;

        /*
         * The handle is used once while its object is live, so the server has
         * had every chance to cache it, and then the object is removed.  What
         * is left is a well-formed handle that names nothing -- which is the
         * definition of stale, and is distinct from a name that was never
         * there.
         */
        res = mbt_create(env, &root, "f6", 2, GUARDED, 0777, NULL);
        if (res->status != NFS3_OK || !res->obj_fh.has) {
            fprintf(stderr, "CREATE f6 failed: %u\n", res->status);
            return 1;
        }
        gone = res->obj_fh;
        expect("GETATTR while live", mbt_getattr(env, &gone)->status, NFS3_OK);
        expect("REMOVE f6", mbt_remove(env, &root, "f6", 2)->status, NFS3_OK);

        expect("GETATTR on a dead file handle",
               await_stale(env, &gone), NFS3ERR_STALE);
        expect("SETATTR on a dead file handle",
               mbt_setattr(env, &gone, -1, 0, NULL)->status, NFS3ERR_STALE);
        expect("LINK from a dead file handle",
               mbt_link(env, &gone, &root, "f6b", 3)->status, NFS3ERR_STALE);

        /* A dead DIRECTORY handle is stale for the operations that take one,
         * including those that also carry a name: the name never gets looked
         * at, because there is nothing to look in. */
        res = mbt_mkdir(env, &root, "d6", 2, 0777);
        if (res->status != NFS3_OK || !res->obj_fh.has) {
            fprintf(stderr, "MKDIR d6 failed: %u\n", res->status);
            return 1;
        }
        gone_dir = res->obj_fh;
        expect("RMDIR d6", mbt_rmdir(env, &root, "d6", 2)->status, NFS3_OK);

        expect("directory handle goes stale",
               await_stale(env, &gone_dir), NFS3ERR_STALE);
        expect("LOOKUP in a dead directory handle",
               mbt_lookup(env, &gone_dir, "x", 1)->status, NFS3ERR_STALE);
        expect("READDIR of a dead directory handle",
               mbt_readdir(env, &gone_dir)->status, NFS3ERR_STALE);
        expect("CREATE in a dead directory handle",
               mbt_create(env, &gone_dir, "x", 1, GUARDED, 0777, NULL)->status,
               NFS3ERR_STALE);
        expect("REMOVE in a dead directory handle",
               mbt_remove(env, &gone_dir, "x", 1)->status, NFS3ERR_STALE);

        /* And a name that was never there is still NFS3ERR_NOENT: the point of
        * the change is to tell the two apart, not to answer STALE for both. */
        expect("LOOKUP of a name that does not exist",
               mbt_lookup(env, &root, "nope", 4)->status, NFS3ERR_NOENT);
        expect("REMOVE of a name that does not exist",
               mbt_remove(env, &root, "nope", 4)->status, NFS3ERR_NOENT);
    }

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
