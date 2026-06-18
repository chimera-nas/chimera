// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Sparse-file support: lseek(SEEK_DATA/SEEK_HOLE) and fallocate(2) hole punching
 * (FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE).
 *
 * Works at 1 MiB granularity so every offset is block-aligned on all backends.
 * Part 1 builds a sparse file by writing two data regions with an un-written
 * gap between them and checks SEEK_DATA/SEEK_HOLE navigate it (and that the gap
 * reads back as zeros).  Part 2 writes a fully-allocated file, punches a hole in
 * the middle, and checks the size is unchanged, the hole reads as zeros, the
 * surrounding data is intact, and SEEK sees the punched hole.
 */

#define _GNU_SOURCE 1
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <linux/falloc.h>

#include "../../pjd_common.h"

#define BLK         (1024 * 1024UL)
#define CHUNK       (128 * 1024UL) /* write granularity (stays under the NFS wsize) */

/* Fill [off, off+BLK) with byte 0xab, writing in wsize-safe chunks.  Returns 0
* on success or -1.  (A single 1 MiB write exceeds the NFS transport limit.) */
static int
fill_block(
    int   fd,
    off_t off)
{
    char  *c = malloc(CHUNK);
    size_t done;

    memset(c, 0xab, CHUNK);

    for (done = 0; done < BLK; done += CHUNK) {
        if (chimera_posix_pwrite(fd, c, CHUNK, off + (off_t) done) != (ssize_t) CHUNK) {
            free(c);
            return -1;
        }
    }
    free(c);
    return 0;
} /* fill_block */

/* True if the first `n` bytes of `buf` are all zero. */
static int
all_zero(
    const char *buf,
    size_t      n)
{
    for (size_t i = 0; i < n; i++) {
        if (buf[i]) {
            return 0;
        }
    }
    return 1;
} /* all_zero */

/* pread `n` bytes at `off` (in wsize-safe chunks) and report whether they are
 * all zero. */
static int
region_is_zero(
    int    fd,
    off_t  off,
    size_t n)
{
    char  *c = malloc(CHUNK);
    size_t done;

    for (done = 0; done < n; done += CHUNK) {
        size_t  want = (n - done < CHUNK) ? (n - done) : CHUNK;
        ssize_t r    = chimera_posix_pread(fd, c, want, off + (off_t) done);

        if (r != (ssize_t) want || !all_zero(c, want)) {
            free(c);
            return 0;
        }
    }
    free(c);
    return 1;
} /* region_is_zero */

int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);

    char *n0 = pjd_namegen();

    /* ---- Part 1: a sparsely-written file (data, gap, data). ---- */
    EXPECT(0, pjd_create(n0, 0644));
    int   fd = pjd_open(n0, O_RDWR, 0644);
    PJD_CHECK(fd >= 0, "open sparse file rw");

    EXPECT_EQ(0, fill_block(fd, 0));
    EXPECT_EQ(0, fill_block(fd, 2 * BLK));
    EXPECT_EQ((long) (3 * BLK), pjd_stat_size(n0));

    /* SEEK_DATA/SEEK_HOLE navigate the data/hole/data layout. */
    EXPECT_EQ(0, chimera_posix_lseek(fd, 0, SEEK_DATA));
    EXPECT_EQ((long) BLK, chimera_posix_lseek(fd, 0, SEEK_HOLE));
    EXPECT_EQ((long) (2 * BLK), chimera_posix_lseek(fd, BLK, SEEK_DATA));
    EXPECT_EQ((long) (3 * BLK), chimera_posix_lseek(fd, 2 * BLK, SEEK_HOLE));

    /* SEEK_DATA at EOF has no further data -> ENXIO. */
    errno = 0;
    PJD_CHECK(chimera_posix_lseek(fd, 3 * BLK, SEEK_DATA) < 0 && errno == ENXIO,
              "SEEK_DATA at EOF returns ENXIO");

    /* The un-written gap reads back as zeros. */
    PJD_CHECK(region_is_zero(fd, BLK, BLK), "sparse gap reads as zeros");

    chimera_posix_close(fd);
    EXPECT(0, pjd_unlink(n0));

    /* ---- Part 2: punch a hole into a fully-allocated file. ---- */
    EXPECT(0, pjd_create(n0, 0644));
    fd = pjd_open(n0, O_RDWR, 0644);
    PJD_CHECK(fd >= 0, "open dense file rw");

    for (unsigned i = 0; i < 3; i++) {
        EXPECT_EQ(0, fill_block(fd, (off_t) i * BLK));
    }
    EXPECT_EQ((long) (3 * BLK), pjd_stat_size(n0));

    /* Punch [BLK, 2*BLK); size must not change. */
    EXPECT_EQ(0, chimera_posix_fallocate_mode(fd,
                                              FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                                              BLK, BLK));
    EXPECT_EQ((long) (3 * BLK), pjd_stat_size(n0));

    /* The punched range reads as zeros; the surrounding data is intact. */
    PJD_CHECK(region_is_zero(fd, BLK, BLK), "punched hole reads as zeros");
    {
        char first = 0;
        EXPECT_EQ(1, chimera_posix_pread(fd, &first, 1, 0));
        PJD_CHECK(first == (char) 0xab, "data before hole intact");
        EXPECT_EQ(1, chimera_posix_pread(fd, &first, 1, 2 * BLK));
        PJD_CHECK(first == (char) 0xab, "data after hole intact");
    }

    /* SEEK sees the punched hole. */
    EXPECT_EQ((long) BLK, chimera_posix_lseek(fd, 0, SEEK_HOLE));
    EXPECT_EQ((long) (2 * BLK), chimera_posix_lseek(fd, BLK, SEEK_DATA));

    chimera_posix_close(fd);
    EXPECT(0, pjd_unlink(n0));

    pjd_end();
    return 0;
} /* main */
