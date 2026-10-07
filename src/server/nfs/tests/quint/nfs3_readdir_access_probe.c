// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * READDIR / READDIRPLUS must check that the caller may read the directory,
 * and READDIRPLUS that it may search it before returning an entry's
 * attributes and handle (#1762).  Every READDIR here goes straight onto the
 * wire with no ACCESS call first: a client that consults ACCESS, as the Linux
 * kernel client and chimera's own POSIX client do, never sends the READDIR
 * that shows the hole.
 *
 * Covers the refusals (a non-owner on 0700 and on 0711 -- search without
 * read), read without search (0644 and 0744 for a non-owner, 0644 for the
 * owner: the names, but no attributes or handles), the allowed side (the
 * owner of a 0700 and a 0744 directory, a non-owner on 0755, root on 0700 and
 * 0644), and that NFSv3's stateless READDIR re-evaluates the directory's
 * current mode on every call, both narrowing and widening.
 */

#include "nfs3_mbt_common.h"
#include "common/mbt_watchdog.h"

#define OTHER_UID 1000
#define OWNER_UID 2000

static int failures;

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

/* The entry for `name` in the listing in `res`, or NULL. */
static const struct mbt_entry *
find_entry(
    const struct mbt_result *res,
    const char              *name)
{
    int i;

    for (i = 0; i < res->nentries; i++) {
        if (res->entries[i].name_len == strlen(name) &&
            memcmp(res->entries[i].name, name, res->entries[i].name_len) == 0) {
            return &res->entries[i];
        }
    }
    return NULL;
} /* find_entry */

/* The listing in `res` names `name` (and, for READDIRPLUS, carries its
 * attributes and handle). */
static int
lists(
    const struct mbt_result *res,
    const char              *name,
    int                      plus)
{
    const struct mbt_entry *e = find_entry(res, name);

    return e && (!plus || (e->attrs.has && e->fh.has));
} /* lists */

/* Both READDIR and READDIRPLUS on `dir` answer `want`; on success both list
 * the child "f". */
static void
expect_readdir(
    struct mbt_env      *env,
    const char          *label,
    const struct mbt_fh *dir,
    uint32_t             want)
{
    struct mbt_result *res;
    char               what[160];

    res = mbt_readdir(env, dir);
    snprintf(what, sizeof(what), "READDIR %s", label);
    expect(what, res->status, want);
    if (want == NFS3_OK && res->status == NFS3_OK && !lists(res, "f", 0)) {
        printf("  FAIL %s: listing does not name \"f\"\n", what);
        failures++;
    }

    res = mbt_readdirplus(env, dir);
    snprintf(what, sizeof(what), "READDIRPLUS %s", label);
    expect(what, res->status, want);
    if (want == NFS3_OK && res->status == NFS3_OK && !lists(res, "f", 1)) {
        printf("  FAIL %s: listing does not name \"f\" with its handle\n", what);
        failures++;
    }
} /* expect_readdir */

/* Without search permission on `dir`: READDIR lists the child "f", and
 * READDIRPLUS lists it too but with neither attributes nor handle, as LOOKUP
 * of "f" would be refused. */
static void
expect_names_only(
    struct mbt_env      *env,
    const char          *label,
    const struct mbt_fh *dir)
{
    const struct mbt_entry *e;
    struct mbt_result      *res;
    char                    what[160];

    res = mbt_readdir(env, dir);
    snprintf(what, sizeof(what), "READDIR %s", label);
    expect(what, res->status, NFS3_OK);
    if (res->status == NFS3_OK && !lists(res, "f", 0)) {
        printf("  FAIL %s: listing does not name \"f\"\n", what);
        failures++;
    }

    res = mbt_readdirplus(env, dir);
    snprintf(what, sizeof(what), "READDIRPLUS %s", label);
    expect(what, res->status, NFS3_OK);
    if (res->status != NFS3_OK) {
        return;
    }
    e = find_entry(res, "f");
    if (!e) {
        printf("  FAIL %s: listing does not name \"f\"\n", what);
        failures++;
    } else if (e->attrs.has || e->fh.has) {
        printf("  FAIL %s: \"f\" carries attributes (%d) or a handle (%d) "
               "without search permission\n", what, e->attrs.has, e->fh.has);
        failures++;
    } else {
        printf("  ok  %s: \"f\" listed without attributes or handle\n", what);
    }
} /* expect_names_only */

/* As the current credential: mkdir `name` in `parent` with `mode`, and create
 * the child "f" in it. */
static struct mbt_fh
make_dir(
    struct mbt_env      *env,
    const struct mbt_fh *parent,
    const char          *name,
    int                  mode)
{
    struct mbt_result *res;
    struct mbt_fh      dir;

    /* Create open so the child can be made, then narrow to `mode`. */
    res = mbt_mkdir(env, parent, name, strlen(name), 0777);
    if (res->status != NFS3_OK || !res->obj_fh.has) {
        fprintf(stderr, "MKDIR %s failed: %u\n", name, res->status);
        exit(1);
    }
    dir = res->obj_fh;

    res = mbt_create(env, &dir, "f", 1, UNCHECKED, 0644, NULL);
    if (res->status != NFS3_OK) {
        fprintf(stderr, "CREATE %s/f failed: %u\n", name, res->status);
        exit(1);
    }

    res = mbt_setattr(env, &dir, mode, -1, NULL);
    if (res->status != NFS3_OK) {
        fprintf(stderr, "SETATTR %s mode %o failed: %u\n", name, mode,
                res->status);
        exit(1);
    }

    return dir;
} /* make_dir */

static void
chmod_as_root(
    struct mbt_env      *env,
    const struct mbt_fh *dir,
    int                  mode,
    uint32_t             restore_uid)
{
    struct mbt_result *res;

    mbt_cred_set_uid(env, 0);
    res = mbt_setattr(env, dir, mode, -1, NULL);
    if (res->status != NFS3_OK) {
        fprintf(stderr, "SETATTR mode %o failed: %u\n", mode, res->status);
        exit(1);
    }
    mbt_cred_set_uid(env, restore_uid);
} /* chmod_as_root */

int
main(
    int    argc,
    char **argv)
{
    struct mbt_env     *env = calloc(1, sizeof(*env));
    struct mbt_env_opts opts;
    const char         *backend = "memfs";
    struct mbt_result  *res;
    struct mbt_fh       root, d700, d711, d755, d644, pub, mine, mine644;
    struct mbt_fh       mine744;
    int                 i;

    for (i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-b") == 0 && i + 1 < argc) {
            backend = argv[++i];
        }
    }

    mbt_watchdog_arm(60);

    memset(&opts, 0, sizeof(opts));
    opts.module = backend;
    mbt_env_start_opts(env, &opts);

    printf("backend: %s\n", backend);

    res = mbt_mnt(env, "/fs0");
    if (res->rpc_err || res->status != MNT3_OK) {
        fprintf(stderr, "MNT /fs0 failed: %u\n", res->status);
        return 1;
    }
    root = res->obj_fh;

    /* Root owns these, so OTHER_UID evaluates in the "other" class. */
    mbt_cred_set_uid(env, 0);
    d700 = make_dir(env, &root, "d700", 0700);
    d711 = make_dir(env, &root, "d711", 0711);
    d755 = make_dir(env, &root, "d755", 0755);
    d644 = make_dir(env, &root, "d644", 0644);
    pub  = make_dir(env, &root, "pub", 0777);

    /* OWNER_UID's own directories: the gate must grant the owner, not
     * merely exempt root. */
    mbt_cred_set_uid(env, OWNER_UID);
    mine    = make_dir(env, &pub, "mine", 0700);
    mine644 = make_dir(env, &pub, "mine644", 0644);
    mine744 = make_dir(env, &pub, "mine744", 0744);

    printf("refused: no read permission on the directory\n");
    mbt_cred_set_uid(env, OTHER_UID);
    expect_readdir(env, "non-owner on 0700", &d700, NFS3ERR_ACCES);
    expect_readdir(env, "non-owner on 0711 (search, no read)", &d711,
                   NFS3ERR_ACCES);
    expect_readdir(env, "non-owner on another user's 0700", &mine,
                   NFS3ERR_ACCES);

    printf("allowed: read permission on the directory\n");
    expect_readdir(env, "non-owner on 0755", &d755, NFS3_OK);
    mbt_cred_set_uid(env, OWNER_UID);
    expect_readdir(env, "owner on 0700", &mine, NFS3_OK);
    mbt_cred_set_uid(env, 0);
    expect_readdir(env, "root on 0700", &d700, NFS3_OK);

    /* Read without search: the names, which read permission gives, but not
     * what reaching an entry would need search permission for. */
    printf("read without search: names only\n");
    mbt_cred_set_uid(env, OTHER_UID);
    expect_names_only(env, "non-owner on 0644", &d644);
    expect_names_only(env, "non-owner on another user's 0744", &mine744);
    mbt_cred_set_uid(env, OWNER_UID);
    expect_names_only(env, "owner on 0644 (rw-, no search)", &mine644);

    printf("read and search: full entries\n");
    expect_readdir(env, "owner on 0744", &mine744, NFS3_OK);
    mbt_cred_set_uid(env, 0);
    expect_readdir(env, "root on 0644", &d644, NFS3_OK);

    /* NFS has no directory open to bind rights to: each READDIR is judged
     * against the directory's mode as it is now. */
    printf("stateless: each READDIR sees the current mode\n");
    chmod_as_root(env, &d755, 0700, OTHER_UID);
    expect_readdir(env, "non-owner after chmod 0755 -> 0700", &d755,
                   NFS3ERR_ACCES);
    chmod_as_root(env, &d755, 0755, OTHER_UID);
    expect_readdir(env, "non-owner after chmod 0700 -> 0755", &d755, NFS3_OK);
    chmod_as_root(env, &d755, 0744, OTHER_UID);
    expect_names_only(env, "non-owner after chmod 0755 -> 0744", &d755);
    chmod_as_root(env, &d755, 0755, OTHER_UID);
    expect_readdir(env, "non-owner after chmod 0744 -> 0755", &d755, NFS3_OK);

    mbt_cred_set_uid(env, 0);
    mbt_env_stop(env);
    free(env);

    if (failures) {
        printf("%d failure(s)\n", failures);
        return 1;
    }
    printf("NFSv3 READDIR access: PASS\n");
    return 0;
} /* main */
