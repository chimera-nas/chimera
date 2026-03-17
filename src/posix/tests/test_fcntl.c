// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Tests for chimera_posix_fcntl() - POSIX record locking via F_SETLK /
// F_SETLKW / F_GETLK.  Covers single-process behaviour and cross-process
// lock conflict detection (requires fork-before-chimera-init so each
// process owns independent worker threads).

#include "posix_test_common.h"
#include <fcntl.h>
#include <sys/wait.h>

#define TEST_FILE "/test/fcntl_lock_test"

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
 * child_main - opened on the already-created TEST_FILE.
 * Waits for the parent to acquire a write lock then tests conflict
 * detection, non-overlapping lock acquisition, and post-unlock re-lock.
 *
 * Returns 0 on success, 1 on failure, 2 if locking is not supported.
 */
static int
child_main(
    struct posix_test_env   *env,
    const char              *posix_json_path,
    struct chimera_vfs_cred *cred)
{
    int          fd;
    struct flock fl;
    int          rc;

    close(p2c[1]);
    close(c2p[0]);

    env->posix = chimera_posix_init_json(posix_json_path, cred, env->metrics);

    if (!env->posix) {
        fprintf(stderr, "child: chimera init failed\n");
        return 1;
    }

    if (posix_test_mount(env) != 0) {
        fprintf(stderr, "child: mount failed: %s\n", strerror(errno));
        return 1;
    }

    /* Wait for parent to create + lock the file. */
    recv_sig(p2c[0]);

    fd = chimera_posix_open(TEST_FILE, O_RDWR, 0);

    if (fd < 0) {
        fprintf(stderr, "child: open failed: %s\n", strerror(errno));
        return 1;
    }

    /* --- test: F_GETLK detects parent's write lock --- */
    fl.l_type   = F_WRLCK;
    fl.l_whence = SEEK_SET;
    fl.l_start  = 0;
    fl.l_len    = 10;
    rc          = chimera_posix_fcntl(fd, F_GETLK, &fl);

    if (rc != 0) {
        if (errno == EOPNOTSUPP) {
            chimera_posix_close(fd);
            posix_test_umount();
            chimera_posix_shutdown();
            return 2; /* skip */
        }

        fprintf(stderr, "child: F_GETLK failed: %s\n", strerror(errno));
        return 1;
    }

    if (fl.l_type == F_UNLCK) {
        fprintf(stderr, "child: F_GETLK should detect parent lock (got F_UNLCK)\n");
        return 1;
    }

    fprintf(stderr, "  cross-proc F_GETLK detects conflict: PASS\n");

    /* --- test: F_SETLK on locked range fails with EAGAIN/EACCES --- */
    fl.l_type   = F_WRLCK;
    fl.l_whence = SEEK_SET;
    fl.l_start  = 0;
    fl.l_len    = 10;
    rc          = chimera_posix_fcntl(fd, F_SETLK, &fl);

    if (rc != -1 || (errno != EAGAIN && errno != EACCES)) {
        fprintf(stderr, "child: F_SETLK on locked range should fail "
                "(rc=%d errno=%d)\n", rc, errno);
        return 1;
    }

    fprintf(stderr, "  cross-proc F_SETLK conflict -> EAGAIN/EACCES: PASS\n");

    /* --- test: F_SETLK on non-overlapping range succeeds --- */
    fl.l_type   = F_WRLCK;
    fl.l_whence = SEEK_SET;
    fl.l_start  = 100;
    fl.l_len    = 10;
    rc          = chimera_posix_fcntl(fd, F_SETLK, &fl);

    if (rc != 0) {
        fprintf(stderr, "child: F_SETLK non-overlapping range failed: %s\n",
                strerror(errno));
        return 1;
    }

    fprintf(stderr, "  cross-proc F_SETLK non-overlapping range: PASS\n");

    fl.l_type = F_UNLCK;
    chimera_posix_fcntl(fd, F_SETLK, &fl);

    /* Tell parent we are done with conflict tests. */
    send_sig(c2p[1]);

    /* Wait for parent to release its lock. */
    recv_sig(p2c[0]);

    /* --- test: F_SETLKW acquires after parent unlocks --- */
    fl.l_type   = F_WRLCK;
    fl.l_whence = SEEK_SET;
    fl.l_start  = 0;
    fl.l_len    = 10;
    rc          = chimera_posix_fcntl(fd, F_SETLKW, &fl);

    if (rc != 0) {
        fprintf(stderr, "child: F_SETLKW after parent unlock failed: %s\n",
                strerror(errno));
        return 1;
    }

    fprintf(stderr, "  cross-proc F_SETLKW after parent unlock: PASS\n");

    fl.l_type = F_UNLCK;
    chimera_posix_fcntl(fd, F_SETLK, &fl);

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
    struct posix_test_env   env;
    struct chimera_vfs_cred root_cred;
    char                    posix_json_path[300];
    int                     fd;
    struct flock            fl;
    pid_t                   child;
    int                     status;
    int                     rc;
    int                     opt;
    struct timespec         tv;
    json_t                 *posix_json_root, *posix_json_config;

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

    /* Pick up the -b backend option. */
    {
        int saved_opterr = opterr;

        opterr = 0;
        optind = 1;
        while ((opt = getopt(argc, argv, "+b:")) != -1) {
            if (opt == 'b') {
                env.backend = optarg;
            }
        }
        opterr = saved_opterr;
        optind = 1;
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

    (void) mkdir("/build/test", 0755);
    (void) mkdir(env.session_dir, 0755);

    posix_json_root   = json_object();
    posix_json_config = json_object();
    json_object_set_new(posix_json_root, "config", posix_json_config);
    chimera_test_write_users_json(posix_json_root);
    snprintf(posix_json_path, sizeof(posix_json_path),
             "%s/posix.json", env.session_dir);
    json_dump_file(posix_json_root, posix_json_path, 0);
    json_decref(posix_json_root);

    chimera_vfs_cred_init_unix(&root_cred, 0, 0, 0, NULL);

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
        exit(child_main(&env, posix_json_path, &root_cred));
    }

    /* ---- parent ---- */
    close(p2c[0]);
    close(c2p[1]);

    env.posix = chimera_posix_init_json(posix_json_path, &root_cred, env.metrics);

    if (!env.posix) {
        fprintf(stderr, "parent: chimera init failed\n");
        posix_test_fail(&env);
    }

    if (posix_test_mount(&env) != 0) {
        fprintf(stderr, "parent: mount failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "Testing chimera_posix_fcntl()...\n");

    fd = chimera_posix_open(TEST_FILE, O_CREAT | O_RDWR | O_TRUNC, 0644);

    if (fd < 0) {
        fprintf(stderr, "open failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    /* ---- single-process tests ---- */

    /* F_GETLK on unlocked region -> F_UNLCK */
    fl.l_type   = F_WRLCK;
    fl.l_whence = SEEK_SET;
    fl.l_start  = 0;
    fl.l_len    = 10;
    rc          = chimera_posix_fcntl(fd, F_GETLK, &fl);

    if (rc != 0 || fl.l_type != F_UNLCK) {
        fprintf(stderr, "F_GETLK unlocked: expected F_UNLCK "
                "(rc=%d type=%d errno=%d)\n", rc, fl.l_type, errno);
        posix_test_fail(&env);
    }

    fprintf(stderr, "  F_GETLK on unlocked region -> F_UNLCK: PASS\n");

    /* F_SETLK WRLCK */
    fl.l_type = F_WRLCK;
    rc        = chimera_posix_fcntl(fd, F_SETLK, &fl);

    if (rc != 0) {
        if (errno == EOPNOTSUPP) {
            fprintf(stderr, "  locking not supported by backend - SKIPPED\n");
            chimera_posix_close(fd);
            chimera_posix_unlink(TEST_FILE);
            send_sig(p2c[1]);   /* unblock child */
            waitpid(child, NULL, 0);
            posix_test_umount();
            posix_test_success(&env);
        }

        fprintf(stderr, "F_SETLK WRLCK failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "  F_SETLK write lock: PASS\n");

    /* F_SETLK UNLCK */
    fl.l_type = F_UNLCK;
    rc        = chimera_posix_fcntl(fd, F_SETLK, &fl);

    if (rc != 0) {
        fprintf(stderr, "F_SETLK UNLCK failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "  F_SETLK unlock: PASS\n");

    /* F_SETLK RDLCK */
    fl.l_type = F_RDLCK;
    rc        = chimera_posix_fcntl(fd, F_SETLK, &fl);

    if (rc != 0) {
        fprintf(stderr, "F_SETLK RDLCK failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "  F_SETLK read lock: PASS\n");

    /* F_SETLKW UNLCK */
    fl.l_type = F_UNLCK;
    rc        = chimera_posix_fcntl(fd, F_SETLKW, &fl);

    if (rc != 0) {
        fprintf(stderr, "F_SETLKW UNLCK failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "  F_SETLKW unlock: PASS\n");

    /* F_SETLK SEEK_CUR */
    chimera_posix_lseek(fd, 50, SEEK_SET);
    fl.l_type   = F_WRLCK;
    fl.l_whence = SEEK_CUR;
    fl.l_start  = 0;
    fl.l_len    = 10;
    rc          = chimera_posix_fcntl(fd, F_SETLK, &fl);

    if (rc != 0) {
        fprintf(stderr, "F_SETLK SEEK_CUR failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fl.l_type = F_UNLCK;
    chimera_posix_fcntl(fd, F_SETLK, &fl);
    fprintf(stderr, "  F_SETLK SEEK_CUR: PASS\n");

    /* Invalid cmd -> EINVAL */
    rc = chimera_posix_fcntl(fd, 9999, NULL);

    if (rc != -1 || errno != EINVAL) {
        fprintf(stderr, "invalid cmd: expected -1/EINVAL (rc=%d errno=%d)\n",
                rc, errno);
        posix_test_fail(&env);
    }

    fprintf(stderr, "  invalid cmd -> EINVAL: PASS\n");

    /* Invalid fd -> EBADF */
    fl.l_type   = F_RDLCK;
    fl.l_whence = SEEK_SET;
    fl.l_start  = 0;
    fl.l_len    = 1;
    rc          = chimera_posix_fcntl(9999, F_SETLK, &fl);

    if (rc != -1 || errno != EBADF) {
        fprintf(stderr, "invalid fd: expected -1/EBADF (rc=%d errno=%d)\n",
                rc, errno);
        posix_test_fail(&env);
    }

    fprintf(stderr, "  invalid fd -> EBADF: PASS\n");

    /* ---- cross-process tests ---- */

    /* Acquire write lock then signal child to start. */
    fl.l_type   = F_WRLCK;
    fl.l_whence = SEEK_SET;
    fl.l_start  = 0;
    fl.l_len    = 10;
    rc          = chimera_posix_fcntl(fd, F_SETLK, &fl);

    if (rc != 0) {
        fprintf(stderr, "cross-proc F_SETLK WRLCK failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "  cross-proc parent acquires write lock: PASS\n");

    send_sig(p2c[1]); /* child can now open + test */

    /* Wait for child to finish conflict tests. */
    recv_sig(c2p[0]);

    /* Release the lock. */
    fl.l_type = F_UNLCK;
    chimera_posix_fcntl(fd, F_SETLK, &fl);
    fprintf(stderr, "  cross-proc parent releases lock: PASS\n");

    send_sig(p2c[1]); /* child can now acquire */

    chimera_posix_close(fd);
    chimera_posix_unlink(TEST_FILE);

    waitpid(child, &status, 0);

    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        if (WIFEXITED(status) && WEXITSTATUS(status) == 2) {
            fprintf(stderr, "  child: locking not supported - SKIPPED\n");
        } else {
            fprintf(stderr, "child process failed (status=%d)\n",
                    WEXITSTATUS(status));
            posix_test_fail(&env);
        }
    }

    fprintf(stderr, "All chimera_posix_fcntl() tests PASSED\n");

    posix_test_umount();
    posix_test_success(&env);

    return 0;
} /* main */
