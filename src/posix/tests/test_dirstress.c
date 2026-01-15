// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
// SPDX-FileCopyrightText: 2000-2001 Silicon Graphics, Inc.
//
// SPDX-License-Identifier: GPL-2.0-only

/*
 * dirstress - directory stress test
 *
 * This is a "crash & burn" test for directory operations.
 * It creates, scrambles, and removes files, directories, and symlinks
 * in a stress test pattern.
 *
 * Originally from xfstests, ported to Chimera POSIX userspace API.
 */

#include <sys/wait.h>
#include "posix_test_common.h"

static int verbose   = 0;
static int pid_val   = 0;
static int checkflag = 0;

#define MKNOD_DEV 0

static int dirstress(
    struct posix_test_env *env,
    const char            *dirname,
    int                    dirnum,
    int                    nfiles,
    int                    keep,
    int                    nprocs_per_dir);
static int create_entries(
    struct posix_test_env *env,
    int                    nfiles);
static int scramble_entries(
    struct posix_test_env *env,
    int                    nfiles);
static int remove_entries(
    struct posix_test_env *env,
    int                    nfiles);

/* Current working directory tracking (since we can't use chdir with Chimera API) */
static char current_dir[4096];

static void
set_cwd(const char *path)
{
    strncpy(current_dir, path, sizeof(current_dir) - 1);
    current_dir[sizeof(current_dir) - 1] = '\0';
} /* set_cwd */

static void
append_cwd(const char *subdir)
{
    size_t len = strlen(current_dir);

    if (len + strlen(subdir) + 2 < sizeof(current_dir)) {
        strcat(current_dir, "/");
        strcat(current_dir, subdir);
    }
} /* append_cwd */

static void
parent_cwd(void)
{
    char *last_slash = strrchr(current_dir, '/');

    if (last_slash && last_slash != current_dir) {
        *last_slash = '\0';
    }
} /* parent_cwd */

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wformat-truncation"
static void
make_path(
    char       *buf,
    size_t      bufsize,
    const char *name)
{
    snprintf(buf, bufsize, "%s/%s", current_dir, name);
} /* make_path */
#pragma GCC diagnostic pop

int
main(
    int   argc,
    char *argv[])
{
    struct posix_test_env env;
    int                   nprocs = 4;
    int                   nfiles = 100;
    int                   c;
    int                   errflg = 0;
    int                   i;
    long                  seed;
    int                   nprocs_per_dir = 1;
    int                   keep           = 0;
    int                   status, istatus;
    int                   childpid;

    /* Initialize test environment first to process -b option */
    posix_test_init(&env, argv, argc);

    /* Reset optind for our own option parsing */
    optind = 1;

    pid_val = getpid();
    seed    = time(NULL);

    while ((c = getopt(argc, argv, "b:p:f:s:n:kvc")) != EOF) {
        switch (c) {
            case 'b':
                /* Already handled by posix_test_init */
                break;
            case 'p':
                nprocs = atoi(optarg);
                break;
            case 'f':
                nfiles = atoi(optarg);
                break;
            case 'n':
                nprocs_per_dir = atoi(optarg);
                break;
            case 's':
                seed = strtol(optarg, NULL, 0);
                break;
            case 'k':
                keep = 1;
                break;
            case 'v':
                verbose++;
                break;
            case 'c':
                checkflag++;
                break;
            case '?':
                errflg++;
                break;
        } /* switch */
    }

    if (errflg) {
        fprintf(stderr, "Usage: test_dirstress -b <backend> [-p nprocs] [-f nfiles] "
                "[-n procs_per_dir] [-v] [-s seed] [-k] [-c]\n");
        posix_test_fail(&env);
    }

    /* Mount the test filesystem */
    if (posix_test_mount(&env) != 0) {
        fprintf(stderr, "Failed to mount test filesystem\n");
        posix_test_fail(&env);
    }

    printf("** [%d] Using seed %ld\n", pid_val, seed);
    printf("** [%d] Backend: %s, nprocs=%d, nfiles=%d\n", pid_val, env.backend, nprocs, nfiles);
    srandom(seed);

    /* For single process mode (simpler), just run directly */
    if (nprocs == 1) {
        int r = dirstress(&env, "/test", 0, nfiles, keep, nprocs_per_dir);

        posix_test_umount();

        if (r != 0) {
            posix_test_fail(&env);
        }
        posix_test_success(&env);
        return 0;
    }

    /* Multi-process mode using fork */
    for (i = 0; i < nprocs; i++) {
        if (verbose) {
            fprintf(stderr, "** [%d] fork\n", pid_val);
        }
        childpid = fork();
        if (childpid < 0) {
            perror("Fork failed");
            posix_test_fail(&env);
        }
        if (childpid == 0) {
            /* child */
            int r;

            pid_val = getpid();
            if (verbose) {
                fprintf(stderr, "** [%d] forked\n", pid_val);
            }
            r = dirstress(&env, "/test", i / nprocs_per_dir, nfiles, keep, nprocs_per_dir);
            if (verbose) {
                fprintf(stderr, "** [%d] exit %d\n", pid_val, r);
            }
            exit(r);
        }
    }

    if (verbose) {
        fprintf(stderr, "** [%d] wait\n", pid_val);
    }
    istatus = 0;

    /* wait & reap children, accumulating error results */
    while (wait(&status) != -1) {
        istatus += WEXITSTATUS(status);
    }

    printf("INFO: Dirstress complete\n");
    if (verbose) {
        fprintf(stderr, "** [%d] parent exit %d\n", pid_val, istatus);
    }

    posix_test_umount();

    if (istatus != 0) {
        posix_test_fail(&env);
    }
    posix_test_success(&env);
    return istatus;
} /* main */

int
dirstress(
    struct posix_test_env *env,
    const char            *dirname,
    int                    dirnum,
    int                    nfiles,
    int                    keep,
    int                    nprocs_per_dir)
{
    int  error;
    char buf[4096];
    char path[4096];
    int  r;

    /* Start from the base directory */
    set_cwd(dirname);

    /* Create stressdir */
    make_path(path, sizeof(path), "stressdir");
    if (verbose) {
        fprintf(stderr, "** [%d] mkdir %s 0777\n", pid_val, path);
    }
    error = chimera_posix_mkdir(path, 0777);
    if (error && (errno != EEXIST)) {
        perror("Create stressdir directory failed");
        return 1;
    }

    append_cwd("stressdir");

    /* Create stress.N directory */
    snprintf(buf, sizeof(buf), "stress.%d", dirnum);
    make_path(path, sizeof(path), buf);
    if (verbose) {
        fprintf(stderr, "** [%d] mkdir %s 0777\n", pid_val, path);
    }
    error = chimera_posix_mkdir(path, 0777);
    if (error && (errno != EEXIST)) {
        perror("Create pid directory failed");
        return 1;
    }

    append_cwd(buf);

    r = 1; /* assume failure */
    if (verbose) {
        fprintf(stderr, "** [%d] create entries\n", pid_val);
    }
    if (create_entries(env, nfiles)) {
        printf("!! [%d] create failed\n", pid_val);
    } else {
        if (verbose) {
            fprintf(stderr, "** [%d] scramble entries\n", pid_val);
        }
        if (scramble_entries(env, nfiles)) {
            printf("!! [%d] scramble failed\n", pid_val);
        } else {
            if (keep) {
                if (verbose) {
                    fprintf(stderr, "** [%d] keep entries\n", pid_val);
                }
                r = 0; /* success */
            } else {
                if (verbose) {
                    fprintf(stderr, "** [%d] remove entries\n", pid_val);
                }
                if (remove_entries(env, nfiles)) {
                    printf("!! [%d] remove failed\n", pid_val);
                } else {
                    r = 0; /* success */
                }
            }
        }
    }

    /* Go back to stressdir */
    parent_cwd();

    if (!keep) {
        snprintf(buf, sizeof(buf), "stress.%d", dirnum);
        make_path(path, sizeof(path), buf);
        if (verbose) {
            fprintf(stderr, "** [%d] rmdir %s\n", pid_val, path);
        }
        if (chimera_posix_rmdir(path)) {
            perror("rmdir");
            if (checkflag) {
                return 1;
            }
        }
    }

    /* Go back to base directory */
    parent_cwd();

    if (!keep) {
        make_path(path, sizeof(path), "stressdir");
        /* Actually we're in dirname, so path should be dirname/stressdir */
        snprintf(path, sizeof(path), "%s/stressdir", dirname);
        if (verbose) {
            fprintf(stderr, "** [%d] rmdir %s\n", pid_val, path);
        }
        if (chimera_posix_rmdir(path)) {
            perror("rmdir stressdir");
            if (checkflag) {
                return 1;
            }
        }
    }

    return r;
} /* dirstress */

int
create_entries(
    struct posix_test_env *env,
    int                    nfiles)
{
    int  i;
    int  fd;
    char buf[4096];
    char path[4096];

    for (i = 0; i < nfiles; i++) {
        snprintf(buf, sizeof(buf), "XXXXXXXXXXXX.%d", i);
        make_path(path, sizeof(path), buf);

        switch (i % 3) {
            case 0:
                /* Create a file */
                if (verbose) {
                    fprintf(stderr, "** [%d] creat %s\n", pid_val, path);
                }
                fd = chimera_posix_open(path, O_CREAT | O_WRONLY | O_TRUNC, 0666);
                if (fd >= 0) {
                    if (verbose) {
                        fprintf(stderr, "** [%d] close %s\n", pid_val, path);
                    }
                    chimera_posix_close(fd);
                } else {
                    fprintf(stderr, "!! [%d] creat %s failed: %s\n", pid_val, path, strerror(errno));
                    if (checkflag) {
                        return 1;
                    }
                }
                break;

            case 1:
                /* Make a directory */
                if (verbose) {
                    fprintf(stderr, "** [%d] mkdir %s 0777\n", pid_val, path);
                }
                if (chimera_posix_mkdir(path, 0777)) {
                    fprintf(stderr, "!! [%d] mkdir %s 0777 failed: %s\n", pid_val, path, strerror(errno));
                    if (checkflag) {
                        return 1;
                    }
                }
                break;

            case 2:
                /* Make a symlink */
                if (verbose) {
                    fprintf(stderr, "** [%d] symlink %s %s\n", pid_val, buf, path);
                }
                if (chimera_posix_symlink(buf, path)) {
                    fprintf(stderr, "!! [%d] symlink %s %s failed: %s\n", pid_val, buf, path, strerror(
                                errno));
                    if (checkflag) {
                        return 1;
                    }
                }
                break;

            default:
                break;
        } /* switch */
    }
    return 0;
} /* create_entries */

int
scramble_entries(
    struct posix_test_env *env,
    int                    nfiles)
{
    int  i;
    char buf[4096];
    char buf1[4096];
    char path[4096];
    char path1[4096];
    long r;
    int  fd;

    for (i = 0; i < nfiles * 2; i++) {
        switch (i % 5) {
            case 0:
                /* rename two random entries */
                r = random() % nfiles;
                snprintf(buf, sizeof(buf), "XXXXXXXXXXXX.%ld", r);
                make_path(path, sizeof(path), buf);
                r = random() % nfiles;
                snprintf(buf1, sizeof(buf1), "XXXXXXXXXXXX.%ld", r);
                make_path(path1, sizeof(path1), buf1);

                if (verbose) {
                    fprintf(stderr, "** [%d] rename %s %s\n", pid_val, path, path1);
                }
                if (chimera_posix_rename(path, path1)) {
                    /* Rename failures are expected when entries don't exist */
                    if (verbose) {
                        perror("rename");
                    }
                    if (checkflag) {
                        return 1;
                    }
                }
                break;

            case 1:
                /* unlink a random entry */
                r = random() % nfiles;
                snprintf(buf, sizeof(buf), "XXXXXXXXXXXX.%ld", r);
                make_path(path, sizeof(path), buf);

                if (verbose) {
                    fprintf(stderr, "** [%d] unlink %s\n", pid_val, path);
                }
                if (chimera_posix_unlink(path)) {
                    /* Unlink failures are expected */
                    if (verbose) {
                        fprintf(stderr, "!! [%d] unlink %s failed: %s\n", pid_val, path, strerror(errno));
                    }
                    if (checkflag) {
                        return 1;
                    }
                }
                break;

            case 2:
                /* rmdir a random entry */
                r = random() % nfiles;
                snprintf(buf, sizeof(buf), "XXXXXXXXXXXX.%ld", r);
                make_path(path, sizeof(path), buf);

                if (verbose) {
                    fprintf(stderr, "** [%d] rmdir %s\n", pid_val, path);
                }
                if (chimera_posix_rmdir(path)) {
                    /* rmdir failures are expected */
                    if (verbose) {
                        fprintf(stderr, "!! [%d] rmdir %s failed: %s\n", pid_val, path, strerror(errno));
                    }
                    if (checkflag) {
                        return 1;
                    }
                }
                break;

            case 3:
                /* create a random entry */
                r = random() % nfiles;
                snprintf(buf, sizeof(buf), "XXXXXXXXXXXX.%ld", r);
                make_path(path, sizeof(path), buf);

                if (verbose) {
                    fprintf(stderr, "** [%d] creat %s 0666\n", pid_val, path);
                }
                fd = chimera_posix_open(path, O_CREAT | O_WRONLY | O_TRUNC, 0666);
                if (fd >= 0) {
                    if (verbose) {
                        fprintf(stderr, "** [%d] close %s\n", pid_val, path);
                    }
                    if (chimera_posix_close(fd)) {
                        fprintf(stderr, "!! [%d] close %s failed: %s\n", pid_val, path, strerror(errno));
                        if (checkflag) {
                            return 1;
                        }
                    }
                } else {
                    /* Create failures may happen if entry exists as directory */
                    if (verbose) {
                        fprintf(stderr, "!! [%d] creat %s 0666 failed: %s\n", pid_val, path, strerror(
                                    errno));
                    }
                    if (checkflag) {
                        return 1;
                    }
                }
                break;

            case 4:
                /* mkdir a random entry */
                r = random() % nfiles;
                snprintf(buf, sizeof(buf), "XXXXXXXXXXXX.%ld", r);
                make_path(path, sizeof(path), buf);

                if (verbose) {
                    fprintf(stderr, "** [%d] mkdir %s\n", pid_val, path);
                }
                if (chimera_posix_mkdir(path, 0777)) {
                    /* mkdir failures are expected */
                    if (verbose) {
                        fprintf(stderr, "!! [%d] mkdir %s failed: %s\n", pid_val, path, strerror(errno));
                    }
                    if (checkflag) {
                        return 1;
                    }
                }
                break;

            default:
                break;
        } /* switch */
    }
    return 0;
} /* scramble_entries */

int
remove_entries(
    struct posix_test_env *env,
    int                    nfiles)
{
    int         i;
    char        buf[1024];
    char        path[2048];
    struct stat statb;
    int         error;

    for (i = 0; i < nfiles; i++) {
        snprintf(buf, sizeof(buf), "XXXXXXXXXXXX.%d", i);
        make_path(path, sizeof(path), buf);

        error = chimera_posix_lstat(path, &statb);
        if (error) {
            /* ignore this one - doesn't exist */
            continue;
        }

        if (S_ISDIR(statb.st_mode)) {
            if (verbose) {
                fprintf(stderr, "** [%d] rmdir %s\n", pid_val, path);
            }
            if (chimera_posix_rmdir(path)) {
                fprintf(stderr, "!! [%d] rmdir %s failed: %s\n", pid_val, path, strerror(errno));
                if (checkflag) {
                    return 1;
                }
            }
        } else {
            if (verbose) {
                fprintf(stderr, "** [%d] unlink %s\n", pid_val, path);
            }
            if (chimera_posix_unlink(path)) {
                fprintf(stderr, "!! [%d] unlink %s failed: %s\n", pid_val, path, strerror(errno));
                if (checkflag) {
                    return 1;
                }
            }
        }
    }
    return 0;
} /* remove_entries */
