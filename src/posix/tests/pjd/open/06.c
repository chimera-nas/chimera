// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Ported from pjdfstest tests/open/06.t:
 * open returns EACCES when the requested access (read and/or write) is not
 * granted by the file's mode for the calling credential.  (The FIFO section,
 * which involves O_NONBLOCK/ENXIO open semantics, is omitted; the regular-file
 * and directory permission matrices are covered.) */

#include "../../pjd_common.h"

struct row {
    int mode;
    int uid;
    int gid;
    int rd;   /* expected errno for O_RDONLY (0 = success) */
    int wr;   /* O_WRONLY */
    int rw;   /* O_RDWR   */
};

/* File owned 65534:65534; each row exercises one permission class.
 * *INDENT-OFF* */
static const struct row file_rows[] = {
    { 0600, 65534, 65534, 0, 0, 0 },
    { 0060, 65533, 65534, 0, 0, 0 },
    { 0006, 65533, 65533, 0, 0, 0 },
    { 0477, 65534, 65534, 0, EACCES, EACCES },
    { 0747, 65533, 65534, 0, EACCES, EACCES },
    { 0774, 65533, 65533, 0, EACCES, EACCES },
    { 0277, 65534, 65534, EACCES, 0, EACCES },
    { 0727, 65533, 65534, EACCES, 0, EACCES },
    { 0772, 65533, 65533, EACCES, 0, EACCES },
    { 0177, 65534, 65534, EACCES, EACCES, EACCES },
    { 0717, 65533, 65534, EACCES, EACCES, EACCES },
    { 0771, 65533, 65533, EACCES, EACCES, EACCES },
    { 0077, 65534, 65534, EACCES, EACCES, EACCES },
    { 0707, 65533, 65534, EACCES, EACCES, EACCES },
    { 0770, 65533, 65533, EACCES, EACCES, EACCES },
};

/* Directory open (O_RDONLY) needs read permission on the directory. */
static const struct row dir_rows[] = {
    { 0600, 65534, 65534, 0, 0, 0 },
    { 0060, 65533, 65534, 0, 0, 0 },
    { 0006, 65533, 65533, 0, 0, 0 },
    { 0477, 65534, 65534, 0, 0, 0 },
    { 0747, 65533, 65534, 0, 0, 0 },
    { 0774, 65533, 65533, 0, 0, 0 },
    { 0277, 65534, 65534, EACCES, 0, 0 },
    { 0727, 65533, 65534, EACCES, 0, 0 },
    { 0772, 65533, 65533, EACCES, 0, 0 },
    { 0177, 65534, 65534, EACCES, 0, 0 },
    { 0717, 65533, 65534, EACCES, 0, 0 },
    { 0771, 65533, 65533, EACCES, 0, 0 },
    { 0077, 65534, 65534, EACCES, 0, 0 },
    { 0707, 65533, 65534, EACCES, 0, 0 },
    { 0770, 65533, 65533, EACCES, 0, 0 },
};
/* *INDENT-ON* */

int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);

    char *n0 = pjd_namegen();
    char *n1 = pjd_namegen();

    EXPECT(0, pjd_mkdir(n0, 0755));
    EXPECT(0, pjd_chown(n0, 65534, 65534));
    pjd_cd(n0);

    /* Regular file. */
    pjd_set_user(65534, 65534);
    EXPECT(0, pjd_create(n1, 0644));
    pjd_set_root();

    for (unsigned i = 0; i < sizeof(file_rows) / sizeof(file_rows[0]); i++) {
        const struct row *r = &file_rows[i];
        pjd_set_user(65534, 65534);
        EXPECT(0, pjd_chmod(n1, r->mode));
        pjd_set_root();
        pjd_set_user(r->uid, r->gid);
        EXPECT_EQ(r->rd, pjd_open_e(n1, O_RDONLY, 0));
        EXPECT_EQ(r->wr, pjd_open_e(n1, O_WRONLY, 0));
        EXPECT_EQ(r->rw, pjd_open_e(n1, O_RDWR, 0));
        pjd_set_root();
    }
    pjd_set_user(65534, 65534);
    EXPECT(0, pjd_unlink(n1));
    pjd_set_root();

    /* Directory. */
    pjd_set_user(65534, 65534);
    EXPECT(0, pjd_mkdir(n1, 0755));
    pjd_set_root();

    for (unsigned i = 0; i < sizeof(dir_rows) / sizeof(dir_rows[0]); i++) {
        const struct row *r = &dir_rows[i];
        pjd_set_user(65534, 65534);
        EXPECT(0, pjd_chmod(n1, r->mode));
        pjd_set_root();
        pjd_set_user(r->uid, r->gid);
        EXPECT_EQ(r->rd, pjd_open_e(n1, O_RDONLY, 0));
        pjd_set_root();
    }
    pjd_set_user(65534, 65534);
    EXPECT(0, pjd_rmdir(n1));
    pjd_set_root();

    pjd_cd("..");
    EXPECT(0, pjd_rmdir(n0));

    return pjd_end();
} /* main */
