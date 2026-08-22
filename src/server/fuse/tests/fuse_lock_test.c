// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * POSIX byte-range locks over FUSE, driven with plain fcntl(2) against two
 * mounts of the same share (argv[1], argv[2]).  Two mounts are two FUSE
 * connections and therefore two independent lock-owner namespaces, so one
 * process can exercise real conflicts, F_GETLK reporting, lock splitting,
 * and unlock-on-close without coordinating processes; a forked child covers
 * F_SETLKW blocking and its EINTR path.
 */

#define _GNU_SOURCE 1

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <signal.h>
#include <sys/wait.h>
#include <sys/time.h>

static int failures;

#define CHECK(cond, ...) \
        do { \
            if (!(cond)) { \
                printf("FAIL: " __VA_ARGS__); \
                printf(" (errno %d %s)\n", errno, strerror(errno)); \
                failures++; \
            } else { \
                printf("ok:   " __VA_ARGS__); printf("\n"); \
            } \
        } while (0)

static int
try_lock(
    int   fd,
    int   type,
    off_t start,
    off_t len)
{
    struct flock fl = {
        .l_type   = type,
        .l_whence = SEEK_SET,
        .l_start  = start,
        .l_len    = len,
    };

    return fcntl(fd, F_SETLK, &fl);
} /* try_lock */

static int
probe_lock(
    int           fd,
    int           type,
    off_t         start,
    off_t         len,
    struct flock *out)
{
    memset(out, 0, sizeof(*out));
    out->l_type   = type;
    out->l_whence = SEEK_SET;
    out->l_start  = start;
    out->l_len    = len;

    return fcntl(fd, F_GETLK, out);
} /* probe_lock */

static void
alarm_handler(int sig)
{
    /* Just interrupt the blocked fcntl. */
} /* alarm_handler */

int
main(
    int   argc,
    char *argv[])
{
    char             path_a[512], path_b[512];
    struct flock     fl;
    struct sigaction sa;
    struct timeval   t0, t1;
    int              fda, fdb, fda2, rc, status;
    int              pipefd[2];
    long             elapsed_ms;
    char             c;
    pid_t            child;

    if (argc < 3) {
        fprintf(stderr, "usage: fuse_lock_test <mount_a> <mount_b>\n");
        return 1;
    }

    /* A hang in a lock test is a real outcome; keep the trail visible. */
    setvbuf(stdout, NULL, _IONBF, 0);

    snprintf(path_a, sizeof(path_a), "%s/lockfile", argv[1]);
    snprintf(path_b, sizeof(path_b), "%s/lockfile", argv[2]);

    fda = open(path_a, O_CREAT | O_RDWR, 0644);
    CHECK(fda >= 0 && ftruncate(fda, 4096) == 0, "create lock file via A");

    fdb = open(path_b, O_RDWR);
    CHECK(fdb >= 0, "open same file via B");

    /* --- exclusive conflict across owners --- */

    CHECK(try_lock(fda, F_WRLCK, 10, 10) == 0, "A write-locks [10,19]");

    rc = try_lock(fdb, F_WRLCK, 10, 10);
    CHECK(rc < 0 && (errno == EAGAIN || errno == EACCES),
          "B's conflicting write lock fails EAGAIN");

    rc = try_lock(fdb, F_RDLCK, 15, 1);
    CHECK(rc < 0 && (errno == EAGAIN || errno == EACCES),
          "B's read lock inside A's write lock fails");

    /* --- F_GETLK reporting --- */

    rc = probe_lock(fdb, F_WRLCK, 10, 10, &fl);
    CHECK(rc == 0 && fl.l_type == F_WRLCK && fl.l_start == 10 && fl.l_len == 10,
          "B's F_GETLK reports A's lock (type %d start %lld len %lld)",
          fl.l_type, (long long) fl.l_start, (long long) fl.l_len);

    rc = probe_lock(fdb, F_WRLCK, 30, 10, &fl);
    CHECK(rc == 0 && fl.l_type == F_UNLCK, "F_GETLK on a free range says so");

    /* --- shared locks coexist; writer excluded --- */

    CHECK(try_lock(fda, F_RDLCK, 30, 20) == 0, "A read-locks [30,49]");
    CHECK(try_lock(fdb, F_RDLCK, 40, 20) == 0, "B read-locks [40,59] alongside");

    rc = try_lock(fda, F_WRLCK, 45, 1);
    CHECK(rc < 0 && (errno == EAGAIN || errno == EACCES),
          "A cannot write-lock inside B's read lock");

    /* --- POSIX split: unlock the middle of A's [10,19] --- */

    CHECK(try_lock(fda, F_UNLCK, 13, 4) == 0, "A unlocks [13,16]");

    rc = probe_lock(fdb, F_WRLCK, 13, 4, &fl);
    CHECK(rc == 0 && fl.l_type == F_UNLCK, "middle of the split is free");

    rc = probe_lock(fdb, F_WRLCK, 10, 3, &fl);
    CHECK(rc == 0 && fl.l_type == F_WRLCK, "left piece [10,12] survives");

    rc = probe_lock(fdb, F_WRLCK, 17, 3, &fl);
    CHECK(rc == 0 && fl.l_type == F_WRLCK, "right piece [17,19] survives");

    CHECK(try_lock(fdb, F_WRLCK, 13, 4) == 0, "B locks the released middle");
    CHECK(try_lock(fdb, F_UNLCK, 13, 4) == 0, "B releases it again");

    /* --- replacement: downgrade the middle of a write lock --- */

    CHECK(try_lock(fda, F_WRLCK, 100, 100) == 0, "A write-locks [100,199]");
    CHECK(try_lock(fda, F_RDLCK, 150, 10) == 0, "A downgrades [150,159] to read");

    rc = probe_lock(fdb, F_WRLCK, 150, 10, &fl);
    CHECK(rc == 0 && fl.l_type == F_RDLCK, "downgraded middle reports read");

    CHECK(try_lock(fdb, F_RDLCK, 150, 10) == 0,
          "B shares the downgraded middle");

    rc = probe_lock(fdb, F_WRLCK, 100, 10, &fl);
    CHECK(rc == 0 && fl.l_type == F_WRLCK, "outer piece still exclusive");

    CHECK(try_lock(fdb, F_UNLCK, 150, 10) == 0, "B releases its share");

    /* --- unlock-on-close: a second fd of the same file via A --- */

    fda2 = open(path_a, O_RDWR);
    CHECK(fda2 >= 0 && try_lock(fda2, F_WRLCK, 200, 10) == 0,
          "A (2nd fd) write-locks [200,209]");

    rc = try_lock(fdb, F_WRLCK, 200, 10);
    CHECK(rc < 0, "B blocked from [200,209] while A holds it");

    /* POSIX: ANY close by the owning process drops its locks on the file;
     * closing the second fd must release everything A holds here. */
    close(fda2);
    close(fda);

    CHECK(try_lock(fdb, F_WRLCK, 200, 10) == 0,
          "B locks [200,209] after A's close released it");
    CHECK(try_lock(fdb, F_WRLCK, 10, 3) == 0,
          "B locks A's old left piece after close");

    close(fdb);

    /* --- F_SETLKW blocks until release --- */

    snprintf(path_a, sizeof(path_a), "%s/blockfile", argv[1]);
    snprintf(path_b, sizeof(path_b), "%s/blockfile", argv[2]);

    CHECK(pipe(pipefd) == 0, "pipe for child handshake");

    child = fork();

    if (child == 0) {
        int cfd = open(path_b, O_CREAT | O_RDWR, 0644);

        if (cfd < 0 || try_lock(cfd, F_WRLCK, 0, 0) != 0) {
            _exit(2);
        }

        if (write(pipefd[1], "L", 1) != 1) {
            _exit(2);
        }

        usleep(300000);

        /* Exit releases the lock (child's close drops its locks). */
        _exit(0);
    }

    CHECK(read(pipefd[0], &c, 1) == 1, "child holds the whole-file lock");

    fda = open(path_a, O_RDWR);
    CHECK(fda >= 0, "parent opens block file via A");

    gettimeofday(&t0, NULL);

    fl = (struct flock) {
        .l_type = F_WRLCK, .l_whence = SEEK_SET, .l_start = 0, .l_len = 0
    };
    rc = fcntl(fda, F_SETLKW, &fl);

    gettimeofday(&t1, NULL);
    elapsed_ms = (t1.tv_sec - t0.tv_sec) * 1000 +
        (t1.tv_usec - t0.tv_usec) / 1000;

    CHECK(rc == 0, "F_SETLKW acquired after the child released");
    CHECK(elapsed_ms >= 200, "F_SETLKW actually blocked (%ld ms)", elapsed_ms);

    waitpid(child, &status, 0);
    CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0, "lock child clean");

    CHECK(try_lock(fda, F_UNLCK, 0, 0) == 0, "parent releases");

    /* --- a signal interrupts a blocked F_SETLKW with EINTR --- */

    child = fork();

    if (child == 0) {
        int cfd = open(path_b, O_RDWR);

        if (cfd < 0 || try_lock(cfd, F_WRLCK, 0, 0) != 0) {
            _exit(2);
        }

        if (write(pipefd[1], "L", 1) != 1) {
            _exit(2);
        }

        /* Hold until the parent is done timing its EINTR. */
        usleep(3000000);
        _exit(0);
    }

    CHECK(read(pipefd[0], &c, 1) == 1, "child holds the lock again");

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = alarm_handler; /* no SA_RESTART: the wait must break */
    sigaction(SIGALRM, &sa, NULL);

    alarm(1);

    gettimeofday(&t0, NULL);

    fl = (struct flock) {
        .l_type = F_WRLCK, .l_whence = SEEK_SET, .l_start = 0, .l_len = 0
    };
    rc = fcntl(fda, F_SETLKW, &fl);

    gettimeofday(&t1, NULL);
    elapsed_ms = (t1.tv_sec - t0.tv_sec) * 1000 +
        (t1.tv_usec - t0.tv_usec) / 1000;

    alarm(0);

    CHECK(rc < 0 && errno == EINTR, "blocked F_SETLKW interrupted with EINTR");
    CHECK(elapsed_ms >= 800 && elapsed_ms < 2500,
          "EINTR arrived on the alarm, not the child's exit (%ld ms)",
          elapsed_ms);

    kill(child, SIGKILL);
    waitpid(child, &status, 0);

    close(fda);
    close(pipefd[0]);
    close(pipefd[1]);

    printf("\n%s (%d failures)\n", failures ? "FAILED" : "PASSED", failures);

    return failures != 0;
} /* main */
