// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <pthread.h>
#include <stdatomic.h>
#include "posix_test_common.h"

#define NUM_THREADS       16
#define WRITES_PER_THREAD 64
#define BLOCK_SIZE        256
#define TOTAL_WRITES      (NUM_THREADS * WRITES_PER_THREAD)
#define EXPECTED_SIZE     ((off_t) TOTAL_WRITES * BLOCK_SIZE)

struct worker_args {
    int         fd;
    int         thread_id;
    atomic_int *error_count;
    atomic_int *success_count;
};

static void *
write_worker(void *arg)
{
    struct worker_args *args = arg;
    char                buf[BLOCK_SIZE];
    ssize_t             written;
    int                 i;

    // Fill buffer with thread-specific pattern
    memset(buf, args->thread_id, BLOCK_SIZE);

    for (i = 0; i < WRITES_PER_THREAD; i++) {
        written = chimera_posix_write(args->fd, buf, BLOCK_SIZE);

        if (written != BLOCK_SIZE) {
            fprintf(stderr, "Thread %d: write %d failed: got %zd, expected %d: %s\n",
                    args->thread_id, i, written, BLOCK_SIZE, strerror(errno));
            atomic_fetch_add(args->error_count, 1);
            return NULL;
        }

        atomic_fetch_add(args->success_count, 1);
    }

    return NULL;
} /* write_worker */

static void *
read_worker(void *arg)
{
    struct worker_args *args = arg;
    char                buf[BLOCK_SIZE];
    ssize_t             bytes_read;
    int                 i;

    for (i = 0; i < WRITES_PER_THREAD; i++) {
        bytes_read = chimera_posix_read(args->fd, buf, BLOCK_SIZE);

        if (bytes_read != BLOCK_SIZE) {
            fprintf(stderr, "Thread %d: read %d failed: got %zd, expected %d: %s\n",
                    args->thread_id, i, bytes_read, BLOCK_SIZE,
                    bytes_read < 0 ? strerror(errno) : "unexpected EOF");
            atomic_fetch_add(args->error_count, 1);
            return NULL;
        }

        atomic_fetch_add(args->success_count, 1);
    }

    return NULL;
} /* read_worker */

int
main(
    int    argc,
    char **argv)
{
    struct posix_test_env env;
    int                   fd;
    int                   rc;
    struct stat           st;
    pthread_t             threads[NUM_THREADS];
    struct worker_args    args[NUM_THREADS];
    atomic_int            error_count;
    atomic_int            success_count;
    int                   i;

    posix_test_init(&env, argv, argc);

    rc = posix_test_mount(&env);

    if (rc != 0) {
        fprintf(stderr, "Failed to mount test module: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    // Create test file
    fd = chimera_posix_open("/test/io_serialize_test", O_CREAT | O_RDWR | O_TRUNC, 0644);

    if (fd < 0) {
        fprintf(stderr, "Failed to create test file: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "Testing concurrent writes with %d threads, %d writes each...\n",
            NUM_THREADS, WRITES_PER_THREAD);

    // Initialize counters
    atomic_init(&error_count, 0);
    atomic_init(&success_count, 0);

    // Set up worker args and spawn threads
    for (i = 0; i < NUM_THREADS; i++) {
        args[i].fd            = fd;
        args[i].thread_id     = i;
        args[i].error_count   = &error_count;
        args[i].success_count = &success_count;

        rc = pthread_create(&threads[i], NULL, write_worker, &args[i]);
        if (rc != 0) {
            fprintf(stderr, "Failed to create thread %d: %s\n", i, strerror(rc));
            chimera_posix_close(fd);
            posix_test_fail(&env);
        }
    }

    // Wait for all threads to complete
    for (i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }

    fprintf(stderr, "Write phase complete: %d successful writes, %d errors\n",
            atomic_load(&success_count), atomic_load(&error_count));

    if (atomic_load(&error_count) > 0) {
        fprintf(stderr, "Write phase had errors\n");
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    if (atomic_load(&success_count) != TOTAL_WRITES) {
        fprintf(stderr, "Expected %d successful writes, got %d\n",
                TOTAL_WRITES, atomic_load(&success_count));
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    // Verify file size using fstat on the open fd
    rc = chimera_posix_fstat(fd, &st);

    if (rc != 0) {
        fprintf(stderr, "Failed to fstat file: %s\n", strerror(errno));
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    fprintf(stderr, "File size after writes: %lu (expected %lu)\n",
            (unsigned long) st.st_size, (unsigned long) EXPECTED_SIZE);

    if (st.st_size != EXPECTED_SIZE) {
        fprintf(stderr, "File size mismatch: expected %lu, got %lu\n",
                (unsigned long) EXPECTED_SIZE, (unsigned long) st.st_size);
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    // Rewind to beginning for read test
    if (chimera_posix_lseek(fd, 0, SEEK_SET) != 0) {
        fprintf(stderr, "Failed to rewind file: %s\n", strerror(errno));
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    fprintf(stderr, "Testing concurrent reads with %d threads, %d reads each...\n",
            NUM_THREADS, WRITES_PER_THREAD);

    // Reset counters
    atomic_store(&error_count, 0);
    atomic_store(&success_count, 0);

    // Spawn read threads
    for (i = 0; i < NUM_THREADS; i++) {
        args[i].fd            = fd;
        args[i].thread_id     = i;
        args[i].error_count   = &error_count;
        args[i].success_count = &success_count;

        rc = pthread_create(&threads[i], NULL, read_worker, &args[i]);
        if (rc != 0) {
            fprintf(stderr, "Failed to create read thread %d: %s\n", i, strerror(rc));
            chimera_posix_close(fd);
            posix_test_fail(&env);
        }
    }

    // Wait for all read threads to complete
    for (i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }

    fprintf(stderr, "Read phase complete: %d successful reads, %d errors\n",
            atomic_load(&success_count), atomic_load(&error_count));

    if (atomic_load(&error_count) > 0) {
        fprintf(stderr, "Read phase had errors\n");
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    if (atomic_load(&success_count) != TOTAL_WRITES) {
        fprintf(stderr, "Expected %d successful reads, got %d\n",
                TOTAL_WRITES, atomic_load(&success_count));
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    fprintf(stderr, "IO serialization test passed!\n");

    chimera_posix_close(fd);

    rc = posix_test_umount();

    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
} /* main */
