// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Tests for chimera_posix_lockf() - POSIX lockf() emulation via
// F_LOCK / F_TLOCK / F_ULOCK / F_TEST.  Covers single-process behaviour
// and cross-process lock conflict detection (requires fork-before-chimera-
// init so each process owns independent worker threads).

#include "posix_test_common.h"
#include <sys/wait.h>

#define TEST_FILE "/test/lockf_test"

static int p2c[2]; /* parent -> child sync pipe */
static int c2p[2]; /* child  -> parent sync pipe */

static void
send_sig(int fd)
{
    char b = 0;

    if (write(fd, &b, 1) == -1) {
        perror("tlock: send_sig write");
        exit(1);
    }
} /* send_sig */

static void
recv_sig(int fd)
{
    char b;

    if (read(fd, &b, 1) != 1) {
        perror("tlock: recv_sig read");
        exit(1);
    }
} /* recv_sig */

/*
 * child_main - cross-process lockf() tests.
 *
 * Returns 0 on success, 1 on failure, 2 if locking is not supported.
 */
static int
child_main(
    struct posix_test_env   *env,
    const char              *posix_json_path,
    struct chimera_vfs_cred *cred)
{
    int fd;
    int rc;

    close(p2c[1]);
    close(c2p[0]);

    env->posix = chimera_posix_init_json(posix_json_path, cred, env->metrics);

    if (!env->posix) {
        fprintf(stderr, "child: chimera init failed\n");
        return 1;
    }

    /* Wait for parent to create + lock the file. optional start NFS server */
    recv_sig(p2c[0]);

    if (posix_test_mount(env) != 0) {
        fprintf(stderr, "child: mount failed: %s\n", strerror(errno));
        return 1;
    }

    fd = chimera_posix_open(TEST_FILE, O_RDWR, 0);

    if (fd < 0) {
        fprintf(stderr, "child: open failed: %s\n", strerror(errno));
        return 1;
    }

    chimera_posix_lseek(fd, 0, SEEK_SET);

    /* --- test: F_TEST detects parent's lock --- */
    rc = chimera_posix_lockf(fd, F_TEST, 0);

    if (rc != -1) {
        if (errno == EOPNOTSUPP) {
            chimera_posix_close(fd);
            posix_test_umount();
            chimera_posix_shutdown();
            return 2; /* skip */
        }

        fprintf(stderr, "child: F_TEST should detect parent lock "
                "(rc=%d errno=%d)\n", rc, errno);
        return 1;
    }

    if (errno != EAGAIN && errno != EACCES) {
        fprintf(stderr, "child: F_TEST expected EAGAIN/EACCES, got %d\n",
                errno);
        return 1;
    }

    fprintf(stderr, "child:  cross-proc F_TEST detects conflict -> EAGAIN: PASS\n");

    /* --- test: F_TLOCK on locked file fails --- */
    rc = chimera_posix_lockf(fd, F_TLOCK, 0);

    if (rc != -1 || (errno != EAGAIN && errno != EACCES)) {
        fprintf(stderr, "child: F_TLOCK should fail (rc=%d errno=%d)\n",
                rc, errno);
        return 1;
    }

    fprintf(stderr, "child:  cross-proc F_TLOCK conflict -> EAGAIN: PASS\n");

    /* Tell parent we're done with conflict tests. */
    send_sig(c2p[1]);

    /* Wait for parent to release its lock. */
    recv_sig(p2c[0]);

    /* --- test: F_TEST succeeds after parent unlocks --- */
    chimera_posix_lseek(fd, 0, SEEK_SET);
    rc = chimera_posix_lockf(fd, F_TEST, 0);

    if (rc != 0) {
        fprintf(stderr, "child: F_TEST after unlock failed (rc=%d errno=%d)\n",
                rc, errno);
        return 1;
    }

    fprintf(stderr, "child:  cross-proc F_TEST after parent unlock -> 0: PASS\n");

    /* --- test: F_TLOCK acquires after parent unlocks --- */
    rc = chimera_posix_lockf(fd, F_TLOCK, 0);

    if (rc != 0) {
        fprintf(stderr, "child: F_TLOCK after unlock failed: %s\n",
                strerror(errno));
        return 1;
    }

    fprintf(stderr, "child:  cross-proc F_TLOCK after parent unlock: PASS\n");

    /* F_ULOCK */
    chimera_posix_lseek(fd, 0, SEEK_SET);
    rc = chimera_posix_lockf(fd, F_ULOCK, 0);

    if (rc != 0) {
        fprintf(stderr, "child: F_ULOCK failed: %s\n", strerror(errno));
        return 1;
    }

    fprintf(stderr, "child:  cross-proc F_ULOCK: PASS\n");

    chimera_posix_close(fd);
    posix_test_umount();
    chimera_posix_shutdown();
    return 0;
} /* child_main */

int
main(
    int    argc,
    char **argv)
{
    struct posix_test_env env;
    char                  posix_json_path[300];
    int                   fd;
    pid_t                 child;
    int                   status;
    int                   rc;
    int                   opt;
    struct timespec       tv;
    json_t               *posix_json_root;
    int                   is_nfs;

    /*
     * Pre-fork setup: create session directory and write posix.json.
     * Chimera client threads must NOT be started before fork() because
     * after fork() only the calling thread survives - worker threads in
     * the child are dead and chimera_posix_wait() blocks forever.
     * Each process initialises its own client after fork().
     */
    memset(&env, 0, sizeof(env));
    env.metrics     = prometheus_metrics_create(NULL, NULL, 0);
    env.nfs_version = 0;
    env.server      = NULL;
    clock_gettime(CLOCK_MONOTONIC, &tv);

    /* Pick up the -b backend option. */
    {
        int saved_opterr = opterr;

        opterr = 0;
        optind = 1;
        while ((opt = getopt(argc, argv, "+b:U:")) != -1) {
            if (opt == 'b') {
                env.backend = optarg;
            } else if (opt == 'U') {
                if (!chimera_test_parse_user(optarg, &env.cred)) {
                    fprintf(stderr, "Unknown user spec '%s'. "
                            "Use: root, johndoe, myuser, or uid:gid\n", optarg);
                    exit(EXIT_FAILURE);
                }
            }
        }
        opterr = saved_opterr;
        optind = 1;
    }

    if (!env.backend) {
        env.backend = "linux";
    }

    // Check if this is an NFS backend (e.g., "nfs3_memfs" or "nfs3rdma_memfs")
    is_nfs = posix_test_parse_nfs_backend(&env);

    chimera_log_init();
    ChimeraLogLevel = CHIMERA_LOG_DEBUG;

    snprintf(env.session_dir, sizeof(env.session_dir),
             "/build/test/posix_session_%d_%lu_%lu",
             getpid(), (unsigned long) tv.tv_sec, (unsigned long) tv.tv_nsec);

    (void) mkdir("/build/test", 0755);
    (void) mkdir(env.session_dir, 0755);

    rc = chown(env.session_dir, env.cred.uid, env.cred.gid);
    if (rc < 0) {
        fprintf(stderr, "Failed to set session_dir uid/gid: %s\n", strerror(errno));
        exit(EXIT_FAILURE);
    }

    posix_json_root = json_object();
    json_object_set_new(posix_json_root, "config", json_object());
    chimera_test_write_users_json(posix_json_root);
    snprintf(posix_json_path, sizeof(posix_json_path),
             "%s/posix.json", env.session_dir);
    json_dump_file(posix_json_root, posix_json_path, 0);
    json_decref(posix_json_root);

    chimera_vfs_cred_init_unix(&env.cred,
                               CHIMERA_TEST_USER_ROOT_UID,
                               CHIMERA_TEST_USER_ROOT_GID,
                               0, NULL);

    if (pipe(p2c) != 0) {
        perror("tlock: parent pipe");
        exit(1);
    }
    if (pipe(c2p) != 0) {
        perror("tlock: child pipe");
        exit(1);
    }

    child = fork();

    if (child == 0) {
        exit(child_main(&env, posix_json_path, &env.cred));
    }

    /* ---- parent ---- */
    close(p2c[0]);
    close(c2p[1]);

    if (is_nfs) {
        posix_test_start_nfs_server(&env);
    }

    env.posix = chimera_posix_init_json(posix_json_path, &env.cred, env.metrics);
    if (!env.posix) {
        fprintf(stderr, "parent: chimera init failed\n");
        posix_test_fail(&env);
    }

    if (posix_test_mount(&env) != 0) {
        fprintf(stderr, "parent: mount failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "parent: Testing chimera_posix_lockf()...\n");

    fd = chimera_posix_open(TEST_FILE, O_CREAT | O_RDWR | O_TRUNC, 0644);

    if (fd < 0) {
        fprintf(stderr, "open failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    /* ---- single-process tests ---- */

    /* F_TEST on unlocked file -> 0 */
    chimera_posix_lseek(fd, 0, SEEK_SET);
    rc = chimera_posix_lockf(fd, F_TEST, 0);

    if (rc != 0) {
        if (errno == EOPNOTSUPP) {
            fprintf(stderr, "parent: locking not supported by backend - SKIPPED\n");
            chimera_posix_close(fd);
            chimera_posix_unlink(TEST_FILE);
            send_sig(p2c[1]);
            waitpid(child, NULL, 0);
            posix_test_umount();
            posix_test_success(&env);
        }

        fprintf(stderr, "parent: F_TEST on unlocked: expected 0 (rc=%d errno=%d)\n",
                rc, errno);
        posix_test_fail(&env);
    }

    fprintf(stderr, "parent: F_TEST on unlocked file -> 0: PASS\n");

    /* F_TLOCK - acquire non-blocking write lock */
    chimera_posix_lseek(fd, 0, SEEK_SET);
    rc = chimera_posix_lockf(fd, F_TLOCK, 0);

    if (rc != 0) {
        fprintf(stderr, "parent: F_TLOCK (no contention) failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "parent: F_TLOCK (no contention): PASS\n");

    /* F_ULOCK */
    chimera_posix_lseek(fd, 0, SEEK_SET);
    rc = chimera_posix_lockf(fd, F_ULOCK, 0);

    if (rc != 0) {
        fprintf(stderr, "parent: F_ULOCK failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "parent: F_ULOCK: PASS\n");

    /* F_LOCK - blocking acquire (no contention, should return immediately) */
    chimera_posix_lseek(fd, 0, SEEK_SET);
    rc = chimera_posix_lockf(fd, F_LOCK, 0);

    if (rc != 0) {
        fprintf(stderr, "parent: F_LOCK (no contention) failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "parent: F_LOCK (no contention): PASS\n");

    /* F_ULOCK the blocking lock */
    chimera_posix_lseek(fd, 0, SEEK_SET);
    chimera_posix_lockf(fd, F_ULOCK, 0);

    /* Partial-range lock: first 10 bytes */
    chimera_posix_lseek(fd, 0, SEEK_SET);
    rc = chimera_posix_lockf(fd, F_TLOCK, 10);

    if (rc != 0) {
        fprintf(stderr, "F_TLOCK partial range failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    /* F_TEST on the locked range (own lock - same process, should return 0) */
    chimera_posix_lseek(fd, 0, SEEK_SET);
    rc = chimera_posix_lockf(fd, F_TEST, 10);

    if (rc != 0) {
        fprintf(stderr, "parent: F_TEST own lock: expected 0 (rc=%d errno=%d)\n",
                rc, errno);
        posix_test_fail(&env);
    }

    fprintf(stderr, "parent: F_TEST own lock -> 0 (no self-conflict): PASS\n");

    /* F_TEST on non-locked range -> 0 */
    chimera_posix_lseek(fd, 100, SEEK_SET);
    rc = chimera_posix_lockf(fd, F_TEST, 10);

    if (rc != 0) {
        fprintf(stderr, "parent: F_TEST unlocked range: expected 0 (rc=%d errno=%d)\n",
                rc, errno);
        posix_test_fail(&env);
    }

    fprintf(stderr, "parent: F_TEST non-locked range -> 0: PASS\n");

    chimera_posix_lseek(fd, 0, SEEK_SET);
    chimera_posix_lockf(fd, F_ULOCK, 10);

    /* Invalid cmd -> EINVAL */
    rc = chimera_posix_lockf(fd, 9999, 0);

    if (rc != -1 || errno != EINVAL) {
        fprintf(stderr, "parent: invalid cmd: expected -1/EINVAL (rc=%d errno=%d)\n",
                rc, errno);
        posix_test_fail(&env);
    }

    fprintf(stderr, "parent: invalid cmd -> EINVAL: PASS\n");

    /* ---- cross-process tests ---- */

    /* Acquire whole-file write lock then signal child. */
    chimera_posix_lseek(fd, 0, SEEK_SET);
    rc = chimera_posix_lockf(fd, F_TLOCK, 0);

    if (rc != 0) {
        fprintf(stderr, "parent: cross-proc F_TLOCK failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "parent: cross-proc parent F_TLOCK: PASS\n");

    send_sig(p2c[1]); /* child can now test conflict */

    /* Wait for child to finish conflict tests. */
    recv_sig(c2p[0]);

    /* Release lock. */
    chimera_posix_lseek(fd, 0, SEEK_SET);
    rc = chimera_posix_lockf(fd, F_ULOCK, 0);

    if (rc != 0) {
        fprintf(stderr, "parent: cross-proc F_ULOCK failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "parent: cross-proc parent F_ULOCK: PASS\n");

    send_sig(p2c[1]); /* tell child lock is released */

    chimera_posix_close(fd);

    /*
     * Wait for child to finish its post-unlock F_TEST/F_TLOCK/F_ULOCK
     * sequence before unlinking. Otherwise the unlink races with the
     * child's NLM LOCK, which opens the file by fh on the server and
     * fails with NLM4_STALE_FH once the file has been removed.
     */
    waitpid(child, &status, 0);

    chimera_posix_unlink(TEST_FILE);

    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        if (WIFEXITED(status) && WEXITSTATUS(status) == 2) {
            fprintf(stderr, "parent: child: locking not supported - SKIPPED\n");
        } else {
            fprintf(stderr, "parent: child process failed (status=%d)\n",
                    WEXITSTATUS(status));
            posix_test_fail(&env);
        }
    }

    fprintf(stderr, "parent: All chimera_posix_lockf() tests PASSED\n");

    posix_test_umount();
    posix_test_success(&env);

    return 0;
} /* main */
