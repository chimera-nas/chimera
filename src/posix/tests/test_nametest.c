// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
// SPDX-FileCopyrightText: 2000-2001 Silicon Graphics, Inc.
//
// SPDX-License-Identifier: GPL-2.0

/*
 * nametest - Namespace stress test
 *
 * Run a fully automatic, random test of the directory routines.
 * Performs random create, delete, and stat operations on files
 * and verifies the results against tracked state.
 *
 * Originally from xfstests, ported to Chimera POSIX userspace API.
 *
 * Note: Unlike the original which reads filenames from a file, this version
 * generates test filenames programmatically for simplicity.
 */

#include "posix_test_common.h"
#include <ctype.h>
#include <limits.h>

#define DOT_COUNT     100   /* print a '.' every X operations */
#define DEFAULT_NAMES 100   /* default number of filenames to use */

struct info {
    ino_t inumber;
    char *name;
    short namelen;
    short exists;
};

static struct info *table;
static int          totalnames;

static int          good_adds, good_rms, good_looks, good_tot; /* ops that succeeded */
static int          bad_adds, bad_rms, bad_looks, bad_tot; /* ops that "failed" (expected) */

static int          verbose       = 0;
static int          mixcase       = 0;
static char         test_dir[512] = "/test/nametest";

static int auto_lookup(
    struct info *ip);
static int auto_create(
    struct info *ip);
static int auto_remove(
    struct info *ip);

static void
usage(void)
{
    fprintf(stderr, "usage: test_nametest -b <backend> [-n numnames] [-i iterations] "
            "[-s seed] [-z] [-v] [-c]\n");
    exit(1);
} /* usage */

/*
 * Get filename, possibly with random case change
 */
static char *
get_name(struct info *ip)
{
    static char namepath[PATH_MAX];
    char       *p;
    size_t      len;

    if (!mixcase) {
        return ip->name;
    }

    /* pick a random character to change case in path */
    strncpy(namepath, ip->name, sizeof(namepath) - 1);
    namepath[sizeof(namepath) - 1] = '\0';
    p                              = strrchr(namepath, '/');
    if (!p) {
        p = namepath;
    }
    len = strlen(p);
    if (len > 0) {
        p += random() % len;
        if (islower((unsigned char) *p)) {
            *p = toupper((unsigned char) *p);
        } else {
            *p = tolower((unsigned char) *p);
        }
    }
    return namepath;
} /* get_name */

static int
auto_lookup(struct info *ip)
{
    struct stat statb;
    int         retval;

    retval = chimera_posix_stat(get_name(ip), &statb);
    if (retval >= 0) {
        good_looks++;
        retval = 0;
        if (ip->exists == 0) {
            fprintf(stderr, "\"%s\"(%lu) lookup, should not exist\n",
                    ip->name, (unsigned long) statb.st_ino);
            retval = 1;
        } else if (ip->inumber != statb.st_ino) {
            fprintf(stderr, "\"%s\"(%lu) lookup, should be inumber %lu\n",
                    ip->name, (unsigned long) statb.st_ino,
                    (unsigned long) ip->inumber);
            retval = 1;
        } else if (verbose) {
            fprintf(stderr, "\"%s\"(%lu) lookup ok\n",
                    ip->name, (unsigned long) statb.st_ino);
        }
    } else if (errno == ENOENT) {
        bad_looks++;
        retval = 0;
        if (ip->exists == 1) {
            fprintf(stderr, "\"%s\"(%lu) lookup, should exist\n",
                    ip->name, (unsigned long) ip->inumber);
            retval = 1;
        } else if (verbose) {
            fprintf(stderr, "\"%s\"(%lu) lookup ENOENT ok\n",
                    ip->name, (unsigned long) ip->inumber);
        }
    } else {
        retval = errno;
        fprintf(stderr, "\"%s\"(%lu) on lookup: %s\n",
                ip->name, (unsigned long) ip->inumber, strerror(errno));
    }
    return retval;
} /* auto_lookup */

static int
auto_create(struct info *ip)
{
    struct stat statb;
    int         retval;
    int         fd;

    fd = chimera_posix_open(get_name(ip), O_RDWR | O_EXCL | O_CREAT, 0666);
    if (fd >= 0) {
        chimera_posix_close(fd);
        good_adds++;
        retval = 0;
        if (chimera_posix_stat(ip->name, &statb) < 0) {
            perror("stat after create");
            return errno;
        }
        if (ip->exists == 1) {
            fprintf(stderr, "\"%s\"(%lu) created, but already existed as inumber %lu\n",
                    ip->name, (unsigned long) statb.st_ino,
                    (unsigned long) ip->inumber);
            retval = 1;
        } else if (verbose) {
            fprintf(stderr, "\"%s\"(%lu) create new ok\n",
                    ip->name, (unsigned long) statb.st_ino);
        }
        ip->exists  = 1;
        ip->inumber = statb.st_ino;
    } else if (errno == EEXIST) {
        bad_adds++;
        retval = 0;
        if (ip->exists == 0) {
            if (chimera_posix_stat(ip->name, &statb) < 0) {
                perror("stat on EEXIST");
                return errno;
            }
            fprintf(stderr, "\"%s\"(%lu) not created, should not exist\n",
                    ip->name, (unsigned long) statb.st_ino);
            retval = 1;
        } else if (verbose) {
            fprintf(stderr, "\"%s\"(%lu) not created ok\n",
                    ip->name, (unsigned long) ip->inumber);
        }
        ip->exists = 1;
    } else {
        retval = errno;
        fprintf(stderr, "\"%s\"(%lu) on create: %s\n",
                ip->name, (unsigned long) ip->inumber, strerror(errno));
    }
    return retval;
} /* auto_create */

static int
auto_remove(struct info *ip)
{
    int retval;

    retval = chimera_posix_unlink(get_name(ip));
    if (retval >= 0) {
        good_rms++;
        retval = 0;
        if (ip->exists == 0) {
            fprintf(stderr, "\"%s\"(%lu) removed, should not have existed\n",
                    ip->name, (unsigned long) ip->inumber);
            retval = 1;
        } else if (verbose) {
            fprintf(stderr, "\"%s\"(%lu) remove ok\n",
                    ip->name, (unsigned long) ip->inumber);
        }
        ip->exists  = 0;
        ip->inumber = 0;
    } else if (errno == ENOENT) {
        bad_rms++;
        retval = 0;
        if (ip->exists == 1) {
            fprintf(stderr, "\"%s\"(%lu) not removed, should have existed\n",
                    ip->name, (unsigned long) ip->inumber);
            retval = 1;
        } else if (verbose) {
            fprintf(stderr, "\"%s\"(%lu) not removed ok\n",
                    ip->name, (unsigned long) ip->inumber);
        }
        ip->exists = 0;
    } else {
        retval = errno;
        fprintf(stderr, "\"%s\"(%lu) on remove: %s\n",
                ip->name, (unsigned long) ip->inumber, strerror(errno));
    }
    return retval;
} /* auto_remove */

int
main(
    int   argc,
    char *argv[])
{
    struct posix_test_env env;
    int                   iterations, zeroout;
    int                   zone = -1, op, pct_remove = 0, pct_create = 0, ch, i, retval;
    struct info          *ip;
    int                   seed, linedots;
    int                   errors = 0;

    linedots   = zeroout = 0;
    seed       = (int) time(NULL) % 1000;
    iterations = 10000;
    totalnames = DEFAULT_NAMES;

    posix_test_init(&env, argv, argc);

    /* Reset optind for our own option parsing */
    optind = 1;

    while ((ch = getopt(argc, argv, "b:n:i:s:zvc")) != EOF) {
        switch (ch) {
            case 'b':
                /* Already handled by posix_test_init */
                break;
            case 'n':
                totalnames = atoi(optarg);
                break;
            case 's':
                seed = atoi(optarg);
                break;
            case 'i':
                iterations = atoi(optarg);
                break;
            case 'z':
                zeroout++;
                break;
            case 'v':
                verbose++;
                break;
            case 'c':
                mixcase++;
                break;
            default:
                usage();
                break;
        } /* switch */
    }

    if (totalnames < 1) {
        totalnames = DEFAULT_NAMES;
    }

    if (posix_test_mount(&env) != 0) {
        fprintf(stderr, "Failed to mount test filesystem\n");
        posix_test_fail(&env);
    }

    /* Create test directory */
    if (chimera_posix_mkdir(test_dir, 0755) != 0 && errno != EEXIST) {
        fprintf(stderr, "Failed to create test directory %s: %s\n",
                test_dir, strerror(errno));
        posix_test_fail(&env);
    }

    /* Allocate info table and generate filenames */
    table = (struct info *) calloc(totalnames, sizeof(struct info));
    if (table == NULL) {
        perror("calloc");
        posix_test_fail(&env);
    }

    /* Generate test filenames */
    for (i = 0; i < totalnames; i++) {
        char namebuf[1024];
        snprintf(namebuf, sizeof(namebuf), "%s/testfile_%05d", test_dir, i);
        table[i].name    = strdup(namebuf);
        table[i].namelen = strlen(namebuf);
        table[i].exists  = 0;
        table[i].inumber = 0;
    }

    fprintf(stderr, "nametest: backend=%s names=%d iterations=%d seed=%d\n",
            env.backend, totalnames, iterations, seed);
    fprintf(stderr, "Seed = %d (use \"-s %d\" to re-execute this test)\n", seed, seed);

    srandom(seed);

    for (i = 0; i < iterations; i++) {
        /*
         * The distribution of transaction types changes over time.
         * At first we have an equal distribution which gives us
         * a steady state directory of 50% total size.
         * Later, we have an unequal distribution which gives us
         * more creates than removes, growing the directory.
         * Later still, we have an unequal distribution which gives
         * us more removes than creates, shrinking the directory.
         */
        if ((i % totalnames) == 0) {
            zone++;
            switch (zone % 3) {
                case 0: pct_remove = 20; pct_create = 60; break;
                case 1: pct_remove = 33; pct_create = 33; break;
                case 2: pct_remove = 60; pct_create = 20; break;
            } /* switch */
        }

        /*
         * Choose an operation based on the current distribution.
         */
        ip = &table[random() % totalnames];
        op = random() % 100;
        if (op > (pct_remove + pct_create)) {
            retval = auto_lookup(ip);
        } else if (op > pct_remove) {
            retval = auto_create(ip);
        } else {
            retval = auto_remove(ip);
        }

        if (retval != 0) {
            errors++;
        }

        /* output '.' every DOT_COUNT ops */
        if ((i % DOT_COUNT) == 0) {
            if (linedots++ == 72) {
                linedots = 0;
                fprintf(stderr, "\n");
            }
            fprintf(stderr, ".");
            fflush(stderr);
        }
    }
    fprintf(stderr, "\n");

    fprintf(stderr, "creates: %6d OK, %6d EEXIST  (%6d total, %2d%% EEXIST)\n",
            good_adds, bad_adds, good_adds + bad_adds,
            (good_adds + bad_adds) ? (bad_adds * 100) / (good_adds + bad_adds) : 0);
    fprintf(stderr, "removes: %6d OK, %6d ENOENT  (%6d total, %2d%% ENOENT)\n",
            good_rms, bad_rms, good_rms + bad_rms,
            (good_rms + bad_rms) ? (bad_rms * 100) / (good_rms + bad_rms) : 0);
    fprintf(stderr, "lookups: %6d OK, %6d ENOENT  (%6d total, %2d%% ENOENT)\n",
            good_looks, bad_looks, good_looks + bad_looks,
            (good_looks + bad_looks) ? (bad_looks * 100) / (good_looks + bad_looks) : 0);

    good_tot = good_looks + good_adds + good_rms;
    bad_tot  = bad_looks + bad_adds + bad_rms;
    fprintf(stderr, "total  : %6d OK, %6d w/error (%6d total, %2d%% w/error)\n",
            good_tot, bad_tot, good_tot + bad_tot,
            (good_tot + bad_tot) ? (bad_tot * 100) / (good_tot + bad_tot) : 0);

    if (errors > 0) {
        fprintf(stderr, "ERRORS: %d unexpected failures\n", errors);
    }

    /*
     * If asked to clear the directory out after the run,
     * remove everything that is left.
     */
    if (zeroout || 1) {  /* Always clean up */
        int cleanup_count = 0;

        for (ip = table, i = 0; i < totalnames; ip++, i++) {
            if (!ip->exists) {
                continue;
            }
            cleanup_count++;
            retval = chimera_posix_unlink(ip->name);
            if (retval < 0) {
                if (errno == ENOENT) {
                    fprintf(stderr, "\"%s\"(%lu) not removed during cleanup, should have existed\n",
                            ip->name, (unsigned long) ip->inumber);
                } else {
                    fprintf(stderr, "\"%s\"(%lu) on cleanup remove: %s\n",
                            ip->name, (unsigned long) ip->inumber, strerror(errno));
                }
            }
        }
        fprintf(stderr, "cleanup: %d removes\n", cleanup_count);
    }

    /* Remove test directory */
    chimera_posix_rmdir(test_dir);

    /* Free allocated memory */
    for (i = 0; i < totalnames; i++) {
        free(table[i].name);
    }
    free(table);

    posix_test_umount();

    if (errors > 0) {
        posix_test_fail(&env);
    }

    fprintf(stderr, "nametest completed successfully\n");
    posix_test_success(&env);
    return 0;
} /* main */
