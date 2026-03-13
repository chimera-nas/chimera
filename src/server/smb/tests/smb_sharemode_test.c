// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * SMB Share Mode Test Program
 *
 * Tests the share mode conflict detection algorithm:
 * - Table init/destroy lifecycle
 * - Compatible and conflicting access modes
 * - Bidirectional conflict checks (existing vs new, new vs existing)
 * - File identity by parent file handle + filename
 * - Release and reacquire behavior
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#undef NDEBUG
#include <assert.h>

#include "server/smb/smb_sharemode.h"
#include "server/smb/smb_session.h"
#include "common/logging.h"

#define TEST_PASS(name) do { fprintf(stderr, "  PASS: %s\n", name); passed++; } while (0)
#define TEST_FAIL(name) do { fprintf(stderr, "  FAIL: %s\n", name); failed++; } while (0)

static int passed = 0;
static int failed = 0;

/* Helper: allocate a mock open_file with parent_fh and name set */
static struct chimera_smb_open_file *
make_open_file(
    const uint8_t *parent_fh,
    uint32_t       parent_fh_len,
    const char    *name,
    uint32_t       name_len)
{
    struct chimera_smb_open_file *of = calloc(1, sizeof(*of));

    assert(of);
    of->parent_fh_len = parent_fh_len;
    of->name_len      = name_len;

    if (parent_fh_len > 0) {
        memcpy(of->parent_fh, parent_fh, parent_fh_len);
    }

    if (name_len > 0) {
        memcpy(of->name, name, name_len);
    }

    return of;
} /* make_open_file */

/* ------------------------------------------------------------------ */
/* Test 1: init + immediate destroy                                    */
/* ------------------------------------------------------------------ */
static void
test_init_destroy(void)
{
    struct chimera_smb_sharemode_table table;

    fprintf(stderr, "\ntest_init_destroy\n");

    chimera_smb_sharemode_init(&table);
    chimera_smb_sharemode_destroy(&table);

    TEST_PASS("init + destroy (no crash)");
} /* test_init_destroy */

/* ------------------------------------------------------------------ */
/* Test 2: single acquire then release                                 */
/* ------------------------------------------------------------------ */
static void
test_single_acquire_release(void)
{
    struct chimera_smb_sharemode_table table;
    struct chimera_smb_open_file      *of;
    const uint8_t                      fh[] = { 0xAA, 0xBB };
    int                                rc;

    fprintf(stderr, "\ntest_single_acquire_release\n");

    chimera_smb_sharemode_init(&table);

    of = make_open_file(fh, sizeof(fh), "file.txt", 8);

    rc = chimera_smb_sharemode_acquire(&table, fh, sizeof(fh),
                                       "file.txt", 8,
                                       SMB2_FILE_READ_DATA,
                                       SMB2_FILE_SHARE_READ,
                                       of);

    if (rc == 0) {
        TEST_PASS("acquire single entry");
    } else {
        TEST_FAIL("acquire single entry");
    }

    chimera_smb_sharemode_release(&table, of);
    TEST_PASS("release single entry");

    chimera_smb_sharemode_destroy(&table);
    free(of);
} /* test_single_acquire_release */

/* ------------------------------------------------------------------ */
/* Test 3: two readers with SHARE_READ are compatible                  */
/* ------------------------------------------------------------------ */
static void
test_read_read_compatible(void)
{
    struct chimera_smb_sharemode_table table;
    struct chimera_smb_open_file      *of1, *of2;
    const uint8_t                      fh[] = { 0x01 };
    int                                rc;

    fprintf(stderr, "\ntest_read_read_compatible\n");

    chimera_smb_sharemode_init(&table);

    of1 = make_open_file(fh, sizeof(fh), "data.bin", 8);
    of2 = make_open_file(fh, sizeof(fh), "data.bin", 8);

    rc = chimera_smb_sharemode_acquire(&table, fh, sizeof(fh),
                                       "data.bin", 8,
                                       SMB2_FILE_READ_DATA,
                                       SMB2_FILE_SHARE_READ,
                                       of1);
    assert(rc == 0);

    rc = chimera_smb_sharemode_acquire(&table, fh, sizeof(fh),
                                       "data.bin", 8,
                                       SMB2_FILE_READ_DATA,
                                       SMB2_FILE_SHARE_READ,
                                       of2);

    if (rc == 0) {
        TEST_PASS("two READ_DATA + SHARE_READ compatible");
    } else {
        TEST_FAIL("two READ_DATA + SHARE_READ compatible");
    }

    chimera_smb_sharemode_release(&table, of1);
    chimera_smb_sharemode_release(&table, of2);
    chimera_smb_sharemode_destroy(&table);
    free(of1);
    free(of2);
} /* test_read_read_compatible */

/* ------------------------------------------------------------------ */
/* Test 4: write without SHARE_WRITE conflicts                         */
/* ------------------------------------------------------------------ */
static void
test_write_no_share_write(void)
{
    struct chimera_smb_sharemode_table table;
    struct chimera_smb_open_file      *of1, *of2;
    const uint8_t                      fh[] = { 0x02 };
    int                                rc;

    fprintf(stderr, "\ntest_write_no_share_write\n");

    chimera_smb_sharemode_init(&table);

    of1 = make_open_file(fh, sizeof(fh), "doc.txt", 7);
    of2 = make_open_file(fh, sizeof(fh), "doc.txt", 7);

    /* First: WRITE_DATA + SHARE_READ only */
    rc = chimera_smb_sharemode_acquire(&table, fh, sizeof(fh),
                                       "doc.txt", 7,
                                       SMB2_FILE_WRITE_DATA,
                                       SMB2_FILE_SHARE_READ,
                                       of1);
    assert(rc == 0);

    /* Second: also WRITE_DATA + SHARE_READ → conflict (existing write, new doesn't share write) */
    rc = chimera_smb_sharemode_acquire(&table, fh, sizeof(fh),
                                       "doc.txt", 7,
                                       SMB2_FILE_WRITE_DATA,
                                       SMB2_FILE_SHARE_READ,
                                       of2);

    if (rc != 0) {
        TEST_PASS("WRITE_DATA without SHARE_WRITE conflicts");
    } else {
        TEST_FAIL("WRITE_DATA without SHARE_WRITE should conflict");
        chimera_smb_sharemode_release(&table, of2);
    }

    chimera_smb_sharemode_release(&table, of1);
    chimera_smb_sharemode_destroy(&table);
    free(of1);
    free(of2);
} /* test_write_no_share_write */

/* ------------------------------------------------------------------ */
/* Test 5: existing READ, new doesn't share READ → conflict            */
/* ------------------------------------------------------------------ */
static void
test_read_no_share_read(void)
{
    struct chimera_smb_sharemode_table table;
    struct chimera_smb_open_file      *of1, *of2;
    const uint8_t                      fh[] = { 0x03 };
    int                                rc;

    fprintf(stderr, "\ntest_read_no_share_read\n");

    chimera_smb_sharemode_init(&table);

    of1 = make_open_file(fh, sizeof(fh), "log.txt", 7);
    of2 = make_open_file(fh, sizeof(fh), "log.txt", 7);

    /* First: READ_DATA + SHARE_WRITE (shares write only) */
    rc = chimera_smb_sharemode_acquire(&table, fh, sizeof(fh),
                                       "log.txt", 7,
                                       SMB2_FILE_READ_DATA,
                                       SMB2_FILE_SHARE_WRITE,
                                       of1);
    assert(rc == 0);

    /* Second: WRITE_DATA + 0 (no sharing) → conflict (existing has READ, new shares nothing) */
    rc = chimera_smb_sharemode_acquire(&table, fh, sizeof(fh),
                                       "log.txt", 7,
                                       SMB2_FILE_WRITE_DATA,
                                       0,
                                       of2);

    if (rc != 0) {
        TEST_PASS("existing READ + new no SHARE_READ conflicts");
    } else {
        TEST_FAIL("existing READ + new no SHARE_READ should conflict");
        chimera_smb_sharemode_release(&table, of2);
    }

    chimera_smb_sharemode_release(&table, of1);
    chimera_smb_sharemode_destroy(&table);
    free(of1);
    free(of2);
} /* test_read_no_share_read */

/* ------------------------------------------------------------------ */
/* Test 6: DELETE access without SHARE_DELETE conflicts                 */
/* ------------------------------------------------------------------ */
static void
test_delete_conflict(void)
{
    struct chimera_smb_sharemode_table table;
    struct chimera_smb_open_file      *of1, *of2;
    const uint8_t                      fh[] = { 0x04 };
    int                                rc;

    fprintf(stderr, "\ntest_delete_conflict\n");

    chimera_smb_sharemode_init(&table);

    of1 = make_open_file(fh, sizeof(fh), "temp.dat", 8);
    of2 = make_open_file(fh, sizeof(fh), "temp.dat", 8);

    /* First: DELETE + SHARE_READ|SHARE_WRITE (no SHARE_DELETE) */
    rc = chimera_smb_sharemode_acquire(&table, fh, sizeof(fh),
                                       "temp.dat", 8,
                                       SMB2_DELETE,
                                       SMB2_FILE_SHARE_READ | SMB2_FILE_SHARE_WRITE,
                                       of1);
    assert(rc == 0);

    /* Second: READ_DATA + SHARE_READ (no SHARE_DELETE) → conflict (existing DELETE, new doesn't share delete) */
    rc = chimera_smb_sharemode_acquire(&table, fh, sizeof(fh),
                                       "temp.dat", 8,
                                       SMB2_FILE_READ_DATA,
                                       SMB2_FILE_SHARE_READ,
                                       of2);

    if (rc != 0) {
        TEST_PASS("DELETE without SHARE_DELETE conflicts");
    } else {
        TEST_FAIL("DELETE without SHARE_DELETE should conflict");
        chimera_smb_sharemode_release(&table, of2);
    }

    chimera_smb_sharemode_release(&table, of1);
    chimera_smb_sharemode_destroy(&table);
    free(of1);
    free(of2);
} /* test_delete_conflict */

/* ------------------------------------------------------------------ */
/* Test 7: full sharing allows all access combinations                 */
/* ------------------------------------------------------------------ */
static void
test_full_sharing(void)
{
    struct chimera_smb_sharemode_table table;
    struct chimera_smb_open_file      *of1, *of2;
    const uint8_t                      fh[] = { 0x05 };
    uint32_t                           full_access;
    uint32_t                           full_share;
    int                                rc;

    fprintf(stderr, "\ntest_full_sharing\n");

    chimera_smb_sharemode_init(&table);

    full_access = SMB2_FILE_READ_DATA | SMB2_FILE_WRITE_DATA;
    full_share  = SMB2_FILE_SHARE_READ | SMB2_FILE_SHARE_WRITE | SMB2_FILE_SHARE_DELETE;

    of1 = make_open_file(fh, sizeof(fh), "shared.db", 9);
    of2 = make_open_file(fh, sizeof(fh), "shared.db", 9);

    rc = chimera_smb_sharemode_acquire(&table, fh, sizeof(fh),
                                       "shared.db", 9,
                                       full_access, full_share, of1);
    assert(rc == 0);

    rc = chimera_smb_sharemode_acquire(&table, fh, sizeof(fh),
                                       "shared.db", 9,
                                       full_access, full_share, of2);

    if (rc == 0) {
        TEST_PASS("full sharing is compatible");
    } else {
        TEST_FAIL("full sharing should be compatible");
    }

    chimera_smb_sharemode_release(&table, of1);
    chimera_smb_sharemode_release(&table, of2);
    chimera_smb_sharemode_destroy(&table);
    free(of1);
    free(of2);
} /* test_full_sharing */

/* ------------------------------------------------------------------ */
/* Test 8: different filenames never conflict                          */
/* ------------------------------------------------------------------ */
static void
test_different_files(void)
{
    struct chimera_smb_sharemode_table table;
    struct chimera_smb_open_file      *of1, *of2;
    const uint8_t                      fh[] = { 0x06 };
    int                                rc;

    fprintf(stderr, "\ntest_different_files\n");

    chimera_smb_sharemode_init(&table);

    of1 = make_open_file(fh, sizeof(fh), "alpha.txt", 9);
    of2 = make_open_file(fh, sizeof(fh), "beta.txt", 8);

    /* Exclusive access with no sharing on both */
    rc = chimera_smb_sharemode_acquire(&table, fh, sizeof(fh),
                                       "alpha.txt", 9,
                                       SMB2_FILE_WRITE_DATA,
                                       0, of1);
    assert(rc == 0);

    rc = chimera_smb_sharemode_acquire(&table, fh, sizeof(fh),
                                       "beta.txt", 8,
                                       SMB2_FILE_WRITE_DATA,
                                       0, of2);

    if (rc == 0) {
        TEST_PASS("different filenames do not conflict");
    } else {
        TEST_FAIL("different filenames should not conflict");
    }

    chimera_smb_sharemode_release(&table, of1);
    chimera_smb_sharemode_release(&table, of2);
    chimera_smb_sharemode_destroy(&table);
    free(of1);
    free(of2);
} /* test_different_files */

/* ------------------------------------------------------------------ */
/* Test 9: release first, then reacquire previously conflicting open   */
/* ------------------------------------------------------------------ */
static void
test_release_then_reacquire(void)
{
    struct chimera_smb_sharemode_table table;
    struct chimera_smb_open_file      *of1, *of2;
    const uint8_t                      fh[] = { 0x07 };
    int                                rc;

    fprintf(stderr, "\ntest_release_then_reacquire\n");

    chimera_smb_sharemode_init(&table);

    of1 = make_open_file(fh, sizeof(fh), "excl.dat", 8);
    of2 = make_open_file(fh, sizeof(fh), "excl.dat", 8);

    /* First: exclusive write */
    rc = chimera_smb_sharemode_acquire(&table, fh, sizeof(fh),
                                       "excl.dat", 8,
                                       SMB2_FILE_WRITE_DATA,
                                       0, of1);
    assert(rc == 0);

    /* Second: should fail */
    rc = chimera_smb_sharemode_acquire(&table, fh, sizeof(fh),
                                       "excl.dat", 8,
                                       SMB2_FILE_WRITE_DATA,
                                       0, of2);

    if (rc != 0) {
        TEST_PASS("second exclusive acquire fails");
    } else {
        TEST_FAIL("second exclusive acquire should fail");
        chimera_smb_sharemode_release(&table, of2);
    }

    /* Release first, retry second */
    chimera_smb_sharemode_release(&table, of1);

    rc = chimera_smb_sharemode_acquire(&table, fh, sizeof(fh),
                                       "excl.dat", 8,
                                       SMB2_FILE_WRITE_DATA,
                                       0, of2);

    if (rc == 0) {
        TEST_PASS("reacquire after release succeeds");
    } else {
        TEST_FAIL("reacquire after release should succeed");
    }

    chimera_smb_sharemode_release(&table, of2);
    chimera_smb_sharemode_destroy(&table);
    free(of1);
    free(of2);
} /* test_release_then_reacquire */

/* ------------------------------------------------------------------ */
/* Test 10: three compatible opens all succeed                         */
/* ------------------------------------------------------------------ */
static void
test_multiple_compatible(void)
{
    struct chimera_smb_sharemode_table table;
    struct chimera_smb_open_file      *of1, *of2, *of3;
    const uint8_t                      fh[] = { 0x08 };
    uint32_t                           share;
    int                                rc;

    fprintf(stderr, "\ntest_multiple_compatible\n");

    chimera_smb_sharemode_init(&table);

    share = SMB2_FILE_SHARE_READ | SMB2_FILE_SHARE_WRITE;

    of1 = make_open_file(fh, sizeof(fh), "multi.log", 9);
    of2 = make_open_file(fh, sizeof(fh), "multi.log", 9);
    of3 = make_open_file(fh, sizeof(fh), "multi.log", 9);

    rc = chimera_smb_sharemode_acquire(&table, fh, sizeof(fh),
                                       "multi.log", 9,
                                       SMB2_FILE_READ_DATA, share, of1);
    assert(rc == 0);

    rc = chimera_smb_sharemode_acquire(&table, fh, sizeof(fh),
                                       "multi.log", 9,
                                       SMB2_FILE_WRITE_DATA, share, of2);
    assert(rc == 0);

    rc = chimera_smb_sharemode_acquire(&table, fh, sizeof(fh),
                                       "multi.log", 9,
                                       SMB2_FILE_READ_DATA, share, of3);

    if (rc == 0) {
        TEST_PASS("three compatible opens all succeed");
    } else {
        TEST_FAIL("three compatible opens should all succeed");
    }

    chimera_smb_sharemode_release(&table, of1);
    chimera_smb_sharemode_release(&table, of2);
    chimera_smb_sharemode_release(&table, of3);
    chimera_smb_sharemode_destroy(&table);
    free(of1);
    free(of2);
    free(of3);
} /* test_multiple_compatible */

/* ------------------------------------------------------------------ */
/* Test 11: attribute-only access does not trigger conflict             */
/* ------------------------------------------------------------------ */
static void
test_attribute_only(void)
{
    struct chimera_smb_sharemode_table table;
    struct chimera_smb_open_file      *of1, *of2;
    const uint8_t                      fh[] = { 0x09 };
    int                                rc;

    fprintf(stderr, "\ntest_attribute_only\n");

    chimera_smb_sharemode_init(&table);

    of1 = make_open_file(fh, sizeof(fh), "attrs.cfg", 9);
    of2 = make_open_file(fh, sizeof(fh), "attrs.cfg", 9);

    /* First: READ_DATA + SHARE_READ */
    rc = chimera_smb_sharemode_acquire(&table, fh, sizeof(fh),
                                       "attrs.cfg", 9,
                                       SMB2_FILE_READ_DATA,
                                       SMB2_FILE_SHARE_READ,
                                       of1);
    assert(rc == 0);

    /* Second: READ_ATTRIBUTES only + no sharing
     * READ_ATTRIBUTES (0x80) is not in the READ_DATA/EXECUTE/WRITE_DATA/APPEND_DATA/DELETE set,
     * so it should not trigger any conflict checks.
     */
    rc = chimera_smb_sharemode_acquire(&table, fh, sizeof(fh),
                                       "attrs.cfg", 9,
                                       SMB2_FILE_READ_ATTRIBUTES,
                                       0,
                                       of2);

    if (rc == 0) {
        TEST_PASS("READ_ATTRIBUTES does not conflict");
    } else {
        TEST_FAIL("READ_ATTRIBUTES should not conflict");
    }

    chimera_smb_sharemode_release(&table, of1);
    chimera_smb_sharemode_release(&table, of2);
    chimera_smb_sharemode_destroy(&table);
    free(of1);
    free(of2);
} /* test_attribute_only */

/* ------------------------------------------------------------------ */
/* Test 12: same filename but different parent FH → no conflict        */
/* ------------------------------------------------------------------ */
static void
test_different_parent_fh(void)
{
    struct chimera_smb_sharemode_table table;
    struct chimera_smb_open_file      *of1, *of2;
    const uint8_t                      fh1[] = { 0x10, 0x20 };
    const uint8_t                      fh2[] = { 0x30, 0x40 };
    int                                rc;

    fprintf(stderr, "\ntest_different_parent_fh\n");

    chimera_smb_sharemode_init(&table);

    of1 = make_open_file(fh1, sizeof(fh1), "same.txt", 8);
    of2 = make_open_file(fh2, sizeof(fh2), "same.txt", 8);

    /* Exclusive write on both — different parent dirs */
    rc = chimera_smb_sharemode_acquire(&table, fh1, sizeof(fh1),
                                       "same.txt", 8,
                                       SMB2_FILE_WRITE_DATA,
                                       0, of1);
    assert(rc == 0);

    rc = chimera_smb_sharemode_acquire(&table, fh2, sizeof(fh2),
                                       "same.txt", 8,
                                       SMB2_FILE_WRITE_DATA,
                                       0, of2);

    if (rc == 0) {
        TEST_PASS("different parent FH = different file, no conflict");
    } else {
        TEST_FAIL("different parent FH should not conflict");
    }

    chimera_smb_sharemode_release(&table, of1);
    chimera_smb_sharemode_release(&table, of2);
    chimera_smb_sharemode_destroy(&table);
    free(of1);
    free(of2);
} /* test_different_parent_fh */

/* ------------------------------------------------------------------ */
/* main                                                                */
/* ------------------------------------------------------------------ */
int
main(void)
{
    chimera_log_init();

    fprintf(stderr, "Running SMB share mode tests...\n");

    test_init_destroy();
    test_single_acquire_release();
    test_read_read_compatible();
    test_write_no_share_write();
    test_read_no_share_read();
    test_delete_conflict();
    test_full_sharing();
    test_different_files();
    test_release_then_reacquire();
    test_multiple_compatible();
    test_attribute_only();
    test_different_parent_fh();

    fprintf(stderr, "\n========================================\n");
    fprintf(stderr, "Results: %d passed, %d failed\n", passed, failed);
    fprintf(stderr, "========================================\n");

    if (failed > 0) {
        return 1;
    }

    return 0;
} /* main */
