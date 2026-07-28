// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * NFSv4 pseudo-fs root mount ("server:/").
 *
 * Mounts the bare pseudo-fs root instead of an export and exercises the
 * pseudo-root compound handlers, most importantly nfs4_root_readdir: listing
 * the root resolves every export's backing path through an asynchronous VFS
 * lookup per entry, a path that once assumed synchronous completion and
 * crashed with a stack-use-after-return when a lookup completed off the
 * request thread.
 *
 * The listing deliberately spans multiple READDIR pages.  Two separate paths
 * are covered, and they do not depend on each other:
 *
 *   - The first_pos resume runs on essentially every entry no matter how the
 *     server pages, because chimera's POSIX readdir consumes one entry per
 *     call and re-issues a fresh READDIR from that entry's cookie.  A
 *     41-entry listing costs 42 READDIR RPCs, each resuming from a cookie.
 *   - The server's mid-walk dbuf overflow -- it fills a page, rolls the
 *     pending entry back and returns short -- needs more exports than one page
 *     holds.  The client's maxcount is fixed at 8192 and each entry costs a
 *     256-byte attr_vals allocation plus its entry4, name and attrmask, which
 *     in practice fits 23 entries per page, so the extra exports below drive
 *     that rollback 18 times over.
 *
 * Every export must appear exactly once across the whole listing, which catches
 * a first_pos resume that repeats or skips an entry.
 *
 * What this test cannot see is the server's eof flag.  chimera's client zeroes
 * it whenever the caller's callback stops the entry walk
 * (src/vfs/nfs/nfs4_readdir.c), which the one-entry-per-call POSIX readdir does
 * on every page, and the loop below ends on an entry-less reply rather than on
 * eof -- so a server that reported eof wrongly in either direction would still
 * pass.  For the same reason a lost page *tail* is invisible: only entry 1 of
 * each page is ever consumed.  Both need a raw-wire NFSv4 harness.
 *
 * Also verifies that the export can be entered by lookup from the root, that
 * files created through the pseudo-root path are readable back through it, and
 * that mounting an export the server does not have reports ENOENT.
 */

#include "posix_test_common.h"

/* Conservative upper bound on the entries the server fits in one READDIR page.
 * Measured at 23 for the current entry size and the client's 8192 maxcount
 * (set in src/vfs/nfs/nfs4_readdir.c); rounded up so a small change in entry
 * size does not quietly invalidate the assert below.  Raising maxcount there
 * means raising this. */
#define PSEUDO_ROOT_MAX_PAGE_ENTRIES 27

/* Enough exports that the pseudo-root listing needs more than one READDIR
 * page -- see the page-size arithmetic above. */
#define PSEUDO_ROOT_EXTRA_EXPORTS    40

/* "share" plus the extras must exceed one page.  Note this only catches
 * lowering PSEUDO_ROOT_EXTRA_EXPORTS: both operands are constants in this file,
 * so it cannot notice the real per-page capacity growing past the export count
 * and the listing quietly collapsing to a single page.  Shrinking the server's
 * 256-byte attr_vals allocation (src/server/nfs/nfs4_root.c) or raising the
 * client's maxcount (src/vfs/nfs/nfs4_readdir.c) both do that; both carry a
 * comment pointing back here, and either means re-deriving
 * PSEUDO_ROOT_MAX_PAGE_ENTRIES by hand. */
_Static_assert(1 + PSEUDO_ROOT_EXTRA_EXPORTS > PSEUDO_ROOT_MAX_PAGE_ENTRIES,
               "pseudo-root export count no longer spans multiple READDIR "
               "pages; raise PSEUDO_ROOT_EXTRA_EXPORTS");

/* Fixed-width export names ("page00") stay distinct only below 100 extras;
 * past that snprintf widens the field and "page100" gains "page10" as a
 * prefix, which chimera_nfs_find_export_path's prefix match would confuse. */
_Static_assert(PSEUDO_ROOT_EXTRA_EXPORTS <= 100,
               "extra export names are no longer fixed width");

int
main(
    int    argc,
    char **argv)
{
    struct posix_test_env env;
    CHIMERA_DIR          *dir;
    struct dirent        *entry;
    struct stat           st;
    int                   rc;
    int                   fd;
    int                   num_entries = 0;
    int                   num_iter    = 0;
    const char           *test_data   = "pseudo-root";
    char                  buf[64];
    ssize_t               len;
    int                   i;

    /* Every export the pseudo-root must list, and how often we saw it. */
    struct {
        char name[32];
        int  seen;
    } expect[1 + PSEUDO_ROOT_EXTRA_EXPORTS];
    const int             num_expect = 1 + PSEUDO_ROOT_EXTRA_EXPORTS;

    posix_test_extra_exports = PSEUDO_ROOT_EXTRA_EXPORTS;

    posix_test_init(&env, argv, argc);

    if (env.nfs_version != 4) {
        fprintf(stderr, "test_pseudo_root requires an NFS4 backend\n");
        posix_test_fail(&env);
    }

    snprintf(expect[0].name, sizeof(expect[0].name), "share");
    expect[0].seen = 0;

    for (i = 0; i < PSEUDO_ROOT_EXTRA_EXPORTS; i++) {
        snprintf(expect[i + 1].name, sizeof(expect[i + 1].name),
                 POSIX_TEST_EXTRA_EXPORT_NAME_FMT, i);
        expect[i + 1].seen = 0;
    }

    /* Mounting an export the server does not have must report ENOENT.  The
     * mount compound's LOOKUP fails with NFS4ERR_NOENT, and the client maps
     * that per-op status rather than collapsing it into EIO. */
    rc = chimera_posix_mount_with_options("/badexport", "nfs",
                                          "127.0.0.1:/nosuchexport", "vers=4");
    if (rc == 0) {
        fprintf(stderr, "Mount of a nonexistent export unexpectedly "
                "succeeded\n");
        posix_test_fail(&env);
    }

    if (errno != ENOENT) {
        fprintf(stderr, "Mount of a nonexistent export reported %s, "
                "expected ENOENT\n", strerror(errno));
        posix_test_fail(&env);
    }

    rc = chimera_posix_mount_with_options("/test", "nfs", "127.0.0.1:/",
                                          "vers=4");
    if (rc != 0) {
        fprintf(stderr, "Failed to mount pseudo-fs root: %s\n",
                strerror(errno));
        posix_test_fail(&env);
    }

    /* READDIR of the pseudo-root must list each export exactly once, across
     * however many pages the listing takes. */
    dir = chimera_posix_opendir("/test");
    if (!dir) {
        fprintf(stderr, "Failed to open pseudo-root directory: %s\n",
                strerror(errno));
        posix_test_fail(&env);
    }

    for (;;) {
        /* chimera_posix_readdir returns NULL for both end-of-directory and
         * failure, setting errno only on failure, so it is cleared before every
         * call: without that the loop would read a mid-listing RPC error as a
         * short but successful listing, and a failure on the final eof-only
         * READDIR would not be noticed at all.  Clearing it once before the
         * loop is not enough -- the fprintf below can set it too. */
        errno = 0;
        entry = chimera_posix_readdir(dir);

        if (!entry) {
            break;
        }

        /* A broken first_pos resume re-serves the same page forever, so bound
         * the listing: a correct one is every export plus "." and "..".
         * Checked ahead of the "."/".." skip so no reply can spin here at all.
         * Without this the failure is a bare ctest timeout, indistinguishable
         * from a slow machine. */
        if (++num_iter > num_expect + 2) {
            fprintf(stderr, "Pseudo-root listing did not terminate after %d "
                    "entries (still returning '%s'); a broken first_pos resume "
                    "re-serves the same page forever\n", num_iter - 1,
                    entry->d_name);
            posix_test_fail(&env);
        }

        if (strcmp(entry->d_name, ".") == 0 ||
            strcmp(entry->d_name, "..") == 0) {
            continue;
        }

        fprintf(stderr, "pseudo-root entry: %s\n", entry->d_name);
        num_entries++;

        for (i = 0; i < num_expect; i++) {
            if (strcmp(entry->d_name, expect[i].name) == 0) {
                expect[i].seen++;
                break;
            }
        }

        if (i == num_expect) {
            fprintf(stderr, "Unexpected pseudo-root entry: %s\n",
                    entry->d_name);
            posix_test_fail(&env);
        }
    }

    if (errno != 0) {
        fprintf(stderr, "READDIR of the pseudo-root failed after %d entries: "
                "%s\n", num_entries, strerror(errno));
        posix_test_fail(&env);
    }

    chimera_posix_closedir(dir);

    for (i = 0; i < num_expect; i++) {
        if (expect[i].seen != 1) {
            fprintf(stderr, "Export '%s' listed %d times in pseudo-root, "
                    "expected exactly once\n", expect[i].name,
                    expect[i].seen);
            posix_test_fail(&env);
        }
    }

    if (num_entries != num_expect) {
        fprintf(stderr, "Pseudo-root listed %d entries, expected %d\n",
                num_entries, num_expect);
        posix_test_fail(&env);
    }

    /* Entering the export from the pseudo-root (nfs4_root_lookup). */
    rc = chimera_posix_stat("/test/share", &st);
    if (rc != 0) {
        fprintf(stderr, "Failed to stat export via pseudo-root: %s\n",
                strerror(errno));
        posix_test_fail(&env);
    }

    if (!S_ISDIR(st.st_mode)) {
        fprintf(stderr, "Export root is not a directory\n");
        posix_test_fail(&env);
    }

    /* I/O through the pseudo-root path crosses into the export's backend. */
    fd = chimera_posix_open("/test/share/pseudo_root_file",
                            O_CREAT | O_RDWR, 0644);
    if (fd < 0) {
        fprintf(stderr, "Failed to create file via pseudo-root: %s\n",
                strerror(errno));
        posix_test_fail(&env);
    }

    len = chimera_posix_write(fd, test_data, strlen(test_data));
    if (len != (ssize_t) strlen(test_data)) {
        fprintf(stderr, "Failed to write via pseudo-root: %s\n",
                strerror(errno));
        posix_test_fail(&env);
    }

    chimera_posix_close(fd);

    fd = chimera_posix_open("/test/share/pseudo_root_file", O_RDONLY, 0);
    if (fd < 0) {
        fprintf(stderr, "Failed to reopen file via pseudo-root: %s\n",
                strerror(errno));
        posix_test_fail(&env);
    }

    len = chimera_posix_read(fd, buf, sizeof(buf));
    if (len != (ssize_t) strlen(test_data) ||
        memcmp(buf, test_data, len) != 0) {
        fprintf(stderr, "Read-back via pseudo-root mismatched\n");
        posix_test_fail(&env);
    }

    chimera_posix_close(fd);

    rc = posix_test_umount();
    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "Pseudo-root mount test passed\n");

    posix_test_success(&env);

    return 0;
} /* main */
