// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Record locking test
// Based on cthon/lock/tlock.c from Connectathon
//
// Tests POSIX record locking functionality using fcntl() and lockf()
//

#include "cthon_common.h"
#include <signal.h>
#include <sys/wait.h>

// Maximum file offset for locking tests
static off_t maxeof;

#define PARENT     0
#define CHILD      1

#define PASS       0
#define EQUAL      -1
#define UNEQUAL    -2

#define WARN       1
#define FATAL      2

#define END        (0)

#define DO_UNLINK  1
#define JUST_CLOSE 0

static int  wait_time = 3;

static char arr[1]; // dummy buffer for pipe

static int  parentpipe[2];
static int  childpipe[2];

static char testfile[MAXPATHLEN];
static int  testfd;

static int  testnum;
static int  passnum;
static int  passcnt;
static int  cumpass;
static int  cumwarn;
static int  cumfail;
static int  tstpass;
static int  tstwarn;
static int  tstfail;

static int  parentpid, childpid;
static int  who;

#define OPENFLAGS (O_CREAT | O_RDWR)
#define OPENMODES (0666)

// If lock denied, System V uses EAGAIN, BSD uses EACCES
// We'll accept either
static int  denied_err = EAGAIN;

static void close_testfile(
    int cleanup);
static void testexit(
    int nok);

static void
initialize(const char *basepath)
{
    maxeof  = (off_t) 1 << (sizeof(off_t) * 8 - 2);
    maxeof += maxeof - 1;

    parentpid = getpid();
    snprintf(testfile, sizeof(testfile), "%s/lockfile%d", basepath, parentpid);

    fprintf(stdout, "Creating parent/child synchronization pipes.\n");
    if (pipe(parentpipe) != 0) {
        perror("tlock: parent pipe");
        exit(1);
    }
    if (pipe(childpipe) != 0) {
        perror("tlock: child pipe");
        exit(1);
    }

    fflush(stdout);
} /* initialize */

static void
testreport(int nok)
{
    FILE *outf;
    char *sp;

    cumpass += tstpass;
    cumwarn += tstwarn;
    cumfail += tstfail;
    outf     = (nok ? stderr : stdout);
    sp       = ((who == PARENT) ? "PARENT" : " CHILD");
    fprintf(outf, "\n** %s pass %d results: ", sp, passnum);
    fprintf(outf, "%d/%d pass, %d/%d warn, %d/%d fail (pass/total).\n",
            tstpass, cumpass, tstwarn, cumwarn, tstfail, cumfail);
    tstpass = tstwarn = tstfail = 0;
    fflush(outf);
} /* testreport */

static void
testexit(int nok)
{
    close_testfile(DO_UNLINK);
    if (nok) {
        testreport(1);
    }
    if (who == PARENT) {
        signal(SIGCHLD, SIG_DFL);
        if (nok) {
            signal(SIGINT, SIG_IGN);
            kill(childpid, SIGINT);
        }
        wait((int *) 0);
    } else {
        if (nok) {
            signal(SIGINT, SIG_IGN);
            kill(parentpid, SIGINT);
        }
    }
    exit(nok);
} /* testexit */

static void
parentsig(int sig)
{
    (void) sig;
    testexit(1);
} /* parentsig */

static void
childsig(int sig)
{
    (void) sig;
    testexit(1);
} /* childsig */

static void
header(
    int   test,
    char *string)
{
    printf("\nTest #%d - %s\n", test, string);
    fflush(stdout);
} /* header */

static void
comment(
    const char *fmt,
    ...)
{
    va_list ap;

    va_start(ap, fmt);
    printf("\t%s", ((who == PARENT) ? "Parent: " : "Child:  "));
    vfprintf(stdout, fmt, ap);
    va_end(ap);
    printf("\n");
    fflush(stdout);
} /* comment */

static void
childwait(void)
{
    if (read(parentpipe[0], arr, 1) != 1) {
        perror("tlock: child pipe read");
        testexit(1);
    }
} /* childwait */

static void
childfree(int wait)
{
    if (write(parentpipe[1], arr, 1) != 1) {
        perror("tlock: childfree pipe write");
        testexit(1);
    }
    if (wait) {
        sleep(wait);
    }
} /* childfree */

static void
parentwait(void)
{
    if (read(childpipe[0], arr, 1) != 1) {
        perror("tlock: parentwait pipe read");
        testexit(1);
    }
} /* parentwait */

static void
parentfree(int wait)
{
    if (write(childpipe[1], arr, 1) != 1) {
        perror("tlock: child pipe write");
        testexit(1);
    }
    if (wait) {
        sleep(wait);
    }
} /* parentfree */

static char *
terrstr(int err)
{
    switch (err) {
        case UNEQUAL:  return "unequal";
        case EQUAL:    return "equal";
        case PASS:     return "success";
        case EAGAIN:   return "EAGAIN";
        case EBADF:    return "EBADF";
        case EACCES:   return "EACCES";
        case EFAULT:   return "EFAULT";
        case EINVAL:   return "EINVAL";
        case EOVERFLOW: return "EOVERFLOW";
        case EFBIG:    return "EFBIG";
        case EDEADLK:  return "EDEADLK";
        default: {
            static char tmpstr[16];
            sprintf(tmpstr, "errno=%d", err);
            return tmpstr;
        }
    } /* switch */
} /* terrstr */

static char *
fmtrange(
    off_t offset,
    off_t length)
{
    static char buf[256];

    if (sizeof(offset) == 4) {
        if (length != 0) {
            sprintf(buf, "[%8lx,%8lx] ", (long) offset, (long) length);
        } else {
            sprintf(buf, "[%8lx,  ENDING] ", (long) offset);
        }
    } else {
        if (length != 0) {
            sprintf(buf, "[%16llx,%16llx] ", (long long) offset, (long long) length);
        } else {
            sprintf(buf, "[%16llx,          ENDING] ", (long long) offset);
        }
    }

    return buf;
} /* fmtrange */

static void
report(
    int   num,
    int   sec,
    char *what,
    off_t offset,
    off_t length,
    int   pass,
    int   result,
    int   fail)
{
    printf("\t%s", ((who == PARENT) ? "Parent: " : "Child:  "));
    printf("%d.%-2d - %s %s", num, sec, what, fmtrange(offset, length));
    if (pass == result) {
        printf("PASSED.\n");
        tstpass++;
    } else if (pass == EAGAIN && result == EACCES) {
        printf("WARNING!\n");
        comment("**** Expected %s, returned %s...", terrstr(pass), terrstr(result));
        comment("**** Probably BSD semantics instead of SVID.");
        tstwarn++;
    } else if (pass == EACCES && result == EAGAIN) {
        printf("WARNING!\n");
        comment("**** Expected %s, returned %s...", terrstr(pass), terrstr(result));
        comment("**** Probably SVID semantics instead of BSD.");
        tstwarn++;
    } else if (fail == WARN) {
        printf("WARNING!\n");
        comment("**** Expected %s, returned %s...", terrstr(pass), terrstr(result));
        tstwarn++;
    } else {
        printf("FAILED!\n");
        comment("**** Expected %s, returned %s...", terrstr(pass), terrstr(result));
        tstfail++;
        testexit(1);
    }
    fflush(stdout);
} /* report */

static char *
tfunstr(int fun)
{
    switch (fun) {
        case F_ULOCK: return "F_ULOCK";
        case F_LOCK:  return "F_LOCK ";
        case F_TLOCK: return "F_TLOCK";
        case F_TEST:  return "F_TEST ";
        default:
            fprintf(stderr, "tlock: unknown lockf() F_<%d>.\n", fun);
            testexit(1);
            return NULL;
    } /* switch */
} /* tfunstr */

static void
open_testfile(
    int flags,
    int modes)
{
    testfd = chimera_posix_open(testfile, flags, modes);
    if (testfd < 0) {
        perror("tlock: open");
        testexit(1);
    }
} /* open_testfile */

static void
close_testfile(int cleanup)
{
    if (cleanup == JUST_CLOSE) {
        comment("Closed testfile.");
    }
    chimera_posix_close(testfd);
    if (cleanup == DO_UNLINK) {
        chimera_posix_unlink(testfile);
    }
} /* close_testfile */

static void
test(
    int   num,
    int   sec,
    int   func,
    off_t offset,
    off_t length,
    int   pass,
    int   fail)
{
    int result = PASS;

    if (chimera_posix_lseek(testfd, offset, SEEK_SET) < 0) {
        result = errno;
    }

    if (result == PASS) {
        if (chimera_posix_lockf(testfd, func, length) != 0) {
            result = errno;
        }
    }

    report(num, sec, tfunstr(func), offset, length, pass, result, fail);
} /* test */

// Test 1: Test regions of an unlocked file
static void
test1(void)
{
    if (who == PARENT) {
        parentwait();
        open_testfile(OPENFLAGS, OPENMODES);
        header(1, "Test regions of an unlocked file.");
        test(1, 1, F_TEST, (off_t) 0, (off_t) 1, PASS, FATAL);
        test(1, 2, F_TEST, (off_t) 0, (off_t) END, PASS, FATAL);
        test(1, 3, F_TEST, (off_t) 1, (off_t) 1, PASS, FATAL);
        test(1, 4, F_TEST, (off_t) 1, (off_t) END, PASS, FATAL);
        close_testfile(DO_UNLINK);
        childfree(0);
    } else {
        parentfree(0);
        childwait();
    }
} /* test1 */

// Test 2: Lock the whole file
static void
test2(void)
{
    if (who == PARENT) {
        parentwait();
        header(2, "Try to lock the whole file.");
        open_testfile(OPENFLAGS, OPENMODES);
        test(2, 0, F_TLOCK, (off_t) 0, (off_t) END, PASS, FATAL);
        childfree(0);
        parentwait();
        test(2, 10, F_ULOCK, (off_t) 0, (off_t) END, PASS, FATAL);
        close_testfile(DO_UNLINK);
    } else {
        parentfree(0);
        childwait();
        open_testfile(OPENFLAGS, OPENMODES);
        test(2, 1, F_TEST, (off_t) 0, (off_t) 1, denied_err, FATAL);
        test(2, 2, F_TEST, (off_t) 0, (off_t) END, denied_err, FATAL);
        test(2, 3, F_TEST, (off_t) 1, (off_t) 1, denied_err, FATAL);
        test(2, 4, F_TEST, (off_t) 1, (off_t) END, denied_err, FATAL);
        close_testfile(DO_UNLINK);
        parentfree(0);
    }
} /* test2 */

// Test 3: Lock just the first byte
static void
test3(void)
{
    if (who == PARENT) {
        parentwait();
        header(3, "Try to lock just the 1st byte.");
        open_testfile(OPENFLAGS, OPENMODES);
        test(3, 0, F_TLOCK, (off_t) 0, (off_t) 1, PASS, FATAL);
        childfree(0);
        parentwait();
        test(3, 5, F_ULOCK, (off_t) 0, (off_t) 1, PASS, FATAL);
        close_testfile(DO_UNLINK);
    } else {
        parentfree(0);
        childwait();
        open_testfile(OPENFLAGS, OPENMODES);
        test(3, 1, F_TEST, (off_t) 0, (off_t) 1, denied_err, FATAL);
        test(3, 2, F_TEST, (off_t) 0, (off_t) END, denied_err, FATAL);
        test(3, 3, F_TEST, (off_t) 1, (off_t) 1, PASS, FATAL);
        test(3, 4, F_TEST, (off_t) 1, (off_t) END, PASS, FATAL);
        close_testfile(DO_UNLINK);
        parentfree(0);
    }
} /* test3 */

// Test 4: Lock the 2nd byte
static void
test4(void)
{
    if (who == PARENT) {
        parentwait();
        header(4, "Try to lock the 2nd byte, test around it.");
        open_testfile(OPENFLAGS, OPENMODES);
        test(4, 0, F_TLOCK, (off_t) 1, (off_t) 1, PASS, FATAL);
        childfree(0);
        parentwait();
        test(4, 10, F_ULOCK, (off_t) 1, (off_t) 1, PASS, FATAL);
        close_testfile(DO_UNLINK);
    } else {
        parentfree(0);
        childwait();
        open_testfile(OPENFLAGS, OPENMODES);
        test(4, 1, F_TEST, (off_t) 0, (off_t) 1, PASS, FATAL);
        test(4, 2, F_TEST, (off_t) 0, (off_t) 2, denied_err, FATAL);
        test(4, 3, F_TEST, (off_t) 0, (off_t) END, denied_err, FATAL);
        test(4, 4, F_TEST, (off_t) 1, (off_t) 1, denied_err, FATAL);
        test(4, 5, F_TEST, (off_t) 1, (off_t) 2, denied_err, FATAL);
        test(4, 6, F_TEST, (off_t) 1, (off_t) END, denied_err, FATAL);
        test(4, 7, F_TEST, (off_t) 2, (off_t) 1, PASS, FATAL);
        test(4, 8, F_TEST, (off_t) 2, (off_t) 2, PASS, FATAL);
        test(4, 9, F_TEST, (off_t) 2, (off_t) END, PASS, FATAL);
        close_testfile(DO_UNLINK);
        parentfree(0);
    }
} /* test4 */

// Test 5: Lock split regions
static void
test5(void)
{
    if (who == PARENT) {
        parentwait();
        header(5, "Try to lock 1st and 3rd bytes, test around them.");
        open_testfile(OPENFLAGS, OPENMODES);
        test(5, 0, F_TLOCK, (off_t) 0, (off_t) 1, PASS, FATAL);
        test(5, 1, F_TLOCK, (off_t) 2, (off_t) 1, PASS, FATAL);
        childfree(0);
        parentwait();
        test(5, 14, F_ULOCK, (off_t) 0, (off_t) 1, PASS, FATAL);
        test(5, 15, F_ULOCK, (off_t) 2, (off_t) 1, PASS, FATAL);
        close_testfile(DO_UNLINK);
    } else {
        parentfree(0);
        childwait();
        open_testfile(OPENFLAGS, OPENMODES);
        test(5, 2, F_TEST, (off_t) 0, (off_t) 1, denied_err, FATAL);
        test(5, 3, F_TEST, (off_t) 0, (off_t) 2, denied_err, FATAL);
        test(5, 4, F_TEST, (off_t) 0, (off_t) END, denied_err, FATAL);
        test(5, 5, F_TEST, (off_t) 1, (off_t) 1, PASS, FATAL);
        test(5, 6, F_TEST, (off_t) 1, (off_t) 2, denied_err, FATAL);
        test(5, 7, F_TEST, (off_t) 1, (off_t) END, denied_err, FATAL);
        test(5, 8, F_TEST, (off_t) 2, (off_t) 1, denied_err, FATAL);
        test(5, 9, F_TEST, (off_t) 2, (off_t) 2, denied_err, FATAL);
        test(5, 10, F_TEST, (off_t) 2, (off_t) END, denied_err, FATAL);
        test(5, 11, F_TEST, (off_t) 3, (off_t) 1, PASS, FATAL);
        test(5, 12, F_TEST, (off_t) 3, (off_t) 2, PASS, FATAL);
        test(5, 13, F_TEST, (off_t) 3, (off_t) END, PASS, FATAL);
        close_testfile(DO_UNLINK);
        parentfree(0);
    }
} /* test5 */

static void
runtests(void)
{
    test1();
    test2();
    test3();
    test4();
    test5();
} /* runtests */

int
main(
    int    argc,
    char **argv)
{
    struct posix_test_env   env;
    struct chimera_vfs_cred root_cred;
    struct timespec         tv;
    json_t                 *posix_json_root;
    char                    posix_json_path[300];
    int                     rc;
    int                     opt;

    cthon_Myname = "cthon_lock_tlock";

    passcnt = 1;

    /* Parse all options before fork so both processes share the same settings. */
    optind = 1;
    while ((opt = getopt(argc, argv, "hb:p:t:w:")) != -1) {
        switch (opt) {
            case 'b': break;
            case 'p':
                passcnt = atoi(optarg);
                break;
            case 't':
                testnum = atoi(optarg);
                break;
            case 'w':
                wait_time = atoi(optarg);
                break;
            default: break;
        } /* switch */
    }

    /*
     * Pre-fork setup: create session directory and config file.
     * Chimera client threads must NOT be started before fork() because
     * after fork() only the calling thread survives - the worker threads
     * would be dead in the child and chimera_posix_wait() would block
     * forever.  Each process initialises its own client after fork.
     */
    memset(&env, 0, sizeof(env));
    env.metrics     = prometheus_metrics_create(NULL, NULL, 0);
    env.nfs_version = 0;
    env.server      = NULL;

    /* Pick up the -b backend option */
    {
        int saved_optind = optind;
        int saved_opterr = opterr;
        opterr = 0;
        optind = 1;
        while ((opt = getopt(argc, argv, "+b:")) != -1) {
            if (opt == 'b') {
                env.backend = optarg;
            }
        }
        opterr = saved_opterr;
        optind = saved_optind;
    }

    if (!env.backend) {
        env.backend = "linux";
    }

    chimera_log_init();
    ChimeraLogLevel = CHIMERA_LOG_DEBUG;

    clock_gettime(CLOCK_MONOTONIC, &tv);
    snprintf(env.session_dir, sizeof(env.session_dir),
             "/build/test/posix_session_%d_%lu_%lu",
             getpid(), (unsigned long) tv.tv_sec, (unsigned long) tv.tv_nsec);

    fprintf(stderr, "Creating session directory %s\n", env.session_dir);
    (void) mkdir("/build/test", 0755);
    (void) mkdir(env.session_dir, 0755);

    /* Write posix.json config (no VFS-specific section needed for linux backend). */
    posix_json_root = json_object();
    json_object_set_new(posix_json_root, "config", json_object());
    chimera_test_write_users_json(posix_json_root);
    snprintf(posix_json_path, sizeof(posix_json_path), "%s/posix.json", env.session_dir);
    json_dump_file(posix_json_root, posix_json_path, 0);
    json_decref(posix_json_root);

    chimera_vfs_cred_init_unix(&root_cred, 0, 0, 0, NULL);

    fprintf(stdout, "%s: record locking test\n", cthon_Myname);

    /*
     * initialize() only sets up parentpid, testfile path, and pipes -
     * no chimera calls.  Call it before fork so both processes share
     * the same testfile path and pipe endpoints.
     */
    initialize("/test/nfstestdir");

    /* Fork BEFORE starting chimera client threads. */
    if ((childpid = fork()) == 0) {
        who = CHILD;
        signal(SIGINT, childsig);
    } else {
        who = PARENT;
        signal(SIGINT, parentsig);
        signal(SIGCHLD, SIG_DFL);
    }

    /*
     * Each process (parent and child) starts its own chimera client
     * independently after the fork.
     */
    env.posix = chimera_posix_init_json(posix_json_path, &root_cred, env.metrics);
    if (!env.posix) {
        fprintf(stderr, "%s: Failed to initialize POSIX client\n",
                (who == PARENT) ? "Parent" : "Child");
        exit(EXIT_FAILURE);
    }

    rc = posix_test_mount(&env);
    if (rc != 0) {
        fprintf(stderr, "%s: Failed to mount: %s\n",
                (who == PARENT) ? "Parent" : "Child", strerror(errno));
        exit(EXIT_FAILURE);
    }

    /* Create the shared test directory (chimera call, requires client running). */
    cthon_testdir(NULL);

    /* Run tests for count passes. */
    for (passnum = 1; passnum <= passcnt; passnum++) {
        runtests();
        if (who == CHILD) {
            childwait();
            testreport(0);
        } else {
            testreport(0);
            childfree(0);
        }
    }

    if (who == CHILD) {
        childwait();
    } else {
        signal(SIGCHLD, SIG_DFL);
        childfree(0);
        wait(NULL);
    }

    if (who == PARENT) {
        cthon_complete();
        posix_test_umount();
        posix_test_success(&env);
    }

    return 0;
} /* main */
