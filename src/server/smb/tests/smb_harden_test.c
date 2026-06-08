// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Hardening unit tests for the SMB2 request parse path.
 *
 * These exercise the bounds-checked cursor primitives, the safe-seek helper,
 * and the CREATE-context bounds arithmetic with malformed/adversarial input.
 * The whole point is that none of these inputs may abort() or read out of
 * bounds -- a malformed request must be rejected cleanly.  Run under the Debug
 * (AddressSanitizer) build so an over-read is a hard failure, not a silent pass.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#include "common/evpl_iovec_cursor.h"
#include "server/smb/smb_internal.h"
#include "server/smb/smb_procs.h"

static int passed = 0;
static int failed = 0;

#define TEST_PASS(name)   do { fprintf(stderr, "  PASS: %s\n", name); passed++; } while (0)
#define TEST_FAIL(name)   do { fprintf(stderr, "  FAIL: %s\n", name); failed++; } while (0)
#define CHECK(cond, name) do { if (cond) { TEST_PASS(name); } else { TEST_FAIL(name); } } while (0)

static void
put_le32(
    uint8_t *buf,
    uint32_t off,
    uint32_t v)
{
    buf[off]     = v & 0xff;
    buf[off + 1] = (v >> 8) & 0xff;
    buf[off + 2] = (v >> 16) & 0xff;
    buf[off + 3] = (v >> 24) & 0xff;
} /* put_le32 */

static void
put_le16(
    uint8_t *buf,
    uint32_t off,
    uint16_t v)
{
    buf[off]     = v & 0xff;
    buf[off + 1] = (v >> 8) & 0xff;
} /* put_le16 */

/* Build a cursor over a single contiguous buffer (ref unused by the readers). */
static void
cursor_over(
    struct evpl_iovec_cursor *cursor,
    struct evpl_iovec        *iov,
    void                     *buf,
    int                       len)
{
    iov->data   = buf;
    iov->length = len;
    iov->pad    = 0;
    iov->ref    = NULL;
    evpl_iovec_cursor_init(cursor, iov, 1);
} /* cursor_over */

/* ------------------------------------------------------------------ *
*  Checked cursor primitives                                         *
* ------------------------------------------------------------------ */

static void
test_try_readers_within_data(void)
{
    uint8_t                  buf[16];
    struct evpl_iovec        iov;
    struct evpl_iovec_cursor cur;
    uint32_t                 v32;
    uint16_t                 v16;
    int                      rc;

    for (int i = 0; i < 16; i++) {
        buf[i] = (uint8_t) i;
    }

    cursor_over(&cur, &iov, buf, sizeof(buf));

    rc  = evpl_iovec_cursor_try_get_uint16(&cur, &v16);
    rc |= evpl_iovec_cursor_try_get_uint16(&cur, &v16); /* realign pad */
    rc |= evpl_iovec_cursor_try_get_uint32(&cur, &v32);
    CHECK(rc == 0, "checked readers succeed within data");
} /* test_try_readers_within_data */

static void
test_try_copy_past_end_rejected(void)
{
    uint8_t                  buf[8] = { 0 };
    uint8_t                  out[32];
    struct evpl_iovec        iov;
    struct evpl_iovec_cursor cur;

    cursor_over(&cur, &iov, buf, sizeof(buf));

    /* Ask for 32 bytes from an 8-byte buffer: must return -1, never abort. */
    CHECK(evpl_iovec_cursor_try_copy(&cur, out, 32) == -1,
          "try_copy past end of data returns -1 (no abort)");
} /* test_try_copy_past_end_rejected */

static void
test_limit_fences_reads(void)
{
    uint8_t                  buf[64] = { 0 };
    uint8_t                  out[16];
    struct evpl_iovec        iov;
    struct evpl_iovec_cursor cur;

    cursor_over(&cur, &iov, buf, sizeof(buf));

    /* Plenty of data (64 bytes) but fence the window to 8. */
    evpl_iovec_cursor_set_limit(&cur, 8);

    CHECK(evpl_iovec_cursor_remaining(&cur) == 8, "remaining reflects the limit");
    CHECK(evpl_iovec_cursor_try_copy(&cur, out, 8) == 0,
          "read up to the limit succeeds");
    CHECK(evpl_iovec_cursor_remaining(&cur) == 0, "remaining hits zero at limit");
    CHECK(evpl_iovec_cursor_try_get_uint8(&cur, out) == -1,
          "read past the limit (but within data) is rejected");
} /* test_limit_fences_reads */

static void
test_unbounded_default(void)
{
    uint8_t                  buf[8] = { 0 };
    uint8_t                  out[8];
    struct evpl_iovec        iov;
    struct evpl_iovec_cursor cur;

    cursor_over(&cur, &iov, buf, sizeof(buf));

    /* Without set_limit the limit is INT_MAX, so remaining is bounded only by
     * data; a full-buffer read must still succeed. */
    CHECK(evpl_iovec_cursor_remaining(&cur) > (int) sizeof(buf),
          "default limit is effectively unbounded");
    CHECK(evpl_iovec_cursor_try_copy(&cur, out, 8) == 0,
          "default-limit read bounded only by data");
} /* test_unbounded_default */

/* ------------------------------------------------------------------ *
*  smb_cursor_seek_to                                                *
* ------------------------------------------------------------------ */

static void
test_seek_to(void)
{
    uint8_t                  buf[64] = { 0 };
    struct evpl_iovec        iov;
    struct evpl_iovec_cursor cur;
    uint32_t                 v32;

    cursor_over(&cur, &iov, buf, sizeof(buf));
    evpl_iovec_cursor_set_limit(&cur, 64);

    /* Consume 8 bytes so consumed == 8. */
    evpl_iovec_cursor_try_get_uint32(&cur, &v32);
    evpl_iovec_cursor_try_get_uint32(&cur, &v32);

    CHECK(smb_cursor_seek_to(&cur, 4) == -1,
          "seek backward (offset < consumed) rejected");
    CHECK(smb_cursor_seek_to(&cur, 32) == 0,
          "seek forward within window accepted");
    CHECK(smb_cursor_seek_to(&cur, 4096) == -1,
          "seek past the window rejected (no abort)");
} /* test_seek_to */

/*
 * Regression: a compound element whose parser fences a sub-buffer (SET_INFO /
 * IOCTL call set_limit on the shared cursor) must not leave that smaller limit
 * in place for the compound loop's skip to the next element.  This reproduces
 * the cursor interaction that closed connections on the Linux kernel client's
 * CREATE+SET_INFO+CLOSE compounds: with next_command=128, a SET_INFO sub-window
 * of buffer_length=16 clobbers the element limit, and the skip to offset 128
 * would fail unless the element boundary is restored first.
 */
static void
test_compound_subwindow_then_skip(void)
{
    uint8_t                  buf[256] = { 0 };
    struct evpl_iovec        iov;
    struct evpl_iovec_cursor cur;
    uint8_t                  sink[16];

    cursor_over(&cur, &iov, buf, sizeof(buf));

    /* Element window = NextCommand (128). */
    evpl_iovec_cursor_set_limit(&cur, 128);

    /* Parse a fixed body (consume 96), then a SET_INFO-style sub-buffer at
     * offset 96 fenced to 16 bytes, fully consumed. */
    CHECK(evpl_iovec_cursor_try_skip(&cur, 96) == 0, "consume fixed body");
    evpl_iovec_cursor_set_limit(&cur, 16);           /* clobbers element limit */
    CHECK(evpl_iovec_cursor_try_copy(&cur, sink, 16) == 0, "read sub-buffer");

    /* Without restoring the element limit the skip to NextCommand=128 is
     * rejected (remaining() is now 0).  Restore it, then the skip must succeed. */
    CHECK(smb_cursor_seek_to(&cur, 128) == -1,
          "skip is (correctly) blocked while the sub-window limit is in place");
    evpl_iovec_cursor_set_limit(&cur, 128 - evpl_iovec_cursor_consumed(&cur));
    CHECK(smb_cursor_seek_to(&cur, 128) == 0,
          "skip to next compound element succeeds after restoring element limit");
} /* test_compound_subwindow_then_skip */

/* ------------------------------------------------------------------ *
*  CREATE-context bounds arithmetic                                  *
* ------------------------------------------------------------------ */

/* A DataLength near UINT32_MAX must not wrap the bounds check (data_off +
 * data_len) and let an out-of-bounds data pointer through. */
static void
test_create_ctx_datalen_uint32_overflow(void)
{
    struct chimera_smb_request req;
    uint8_t                    buf[64] = { 0 };

    memset(&req, 0, sizeof(req));

    put_le32(buf, 0, 0);            /* Next = 0 (last)           */
    put_le16(buf, 4, 16);           /* NameOffset                */
    put_le16(buf, 6, 4);            /* NameLength                */
    put_le16(buf, 10, 24);          /* DataOffset                */
    put_le32(buf, 12, 0xFFFFFFFF);  /* DataLength: wraps if added */
    memcpy(buf + 16, "DH2Q", 4);

    CHECK(chimera_smb_parse_create_contexts(buf, sizeof(buf), &req) < 0 &&
          req.status == SMB2_STATUS_INVALID_PARAMETER,
          "CREATE ctx: UINT32-overflow DataLength rejected (no OOB)");
} /* test_create_ctx_datalen_uint32_overflow */

/* A Next field near UINT32_MAX must not wrap (pos + next) and loop forever or
 * skip the bounds check. */
static void
test_create_ctx_next_uint32_overflow(void)
{
    struct chimera_smb_request req;
    uint8_t                    buf[64] = { 0 };

    memset(&req, 0, sizeof(req));

    put_le32(buf, 0, 0xFFFFFFF8);  /* Next: 8-aligned but wraps pos+next */
    put_le16(buf, 4, 16);
    put_le16(buf, 6, 4);
    put_le16(buf, 10, 0);
    put_le32(buf, 12, 0);
    memcpy(buf + 16, "MxAc", 4);

    CHECK(chimera_smb_parse_create_contexts(buf, sizeof(buf), &req) < 0 &&
          req.status == SMB2_STATUS_INVALID_PARAMETER,
          "CREATE ctx: UINT32-overflow Next rejected (no wrap)");
} /* test_create_ctx_next_uint32_overflow */

/* NameOffset/NameLength that run past the entry must be rejected. */
static void
test_create_ctx_name_overflow(void)
{
    struct chimera_smb_request req;
    uint8_t                    buf[32] = { 0 };

    memset(&req, 0, sizeof(req));

    put_le32(buf, 0, 0);
    put_le16(buf, 4, 16);           /* NameOffset 16 */
    put_le16(buf, 6, 0xFFFF);       /* NameLength huge */
    put_le16(buf, 10, 0);
    put_le32(buf, 12, 0);

    CHECK(chimera_smb_parse_create_contexts(buf, sizeof(buf), &req) < 0 &&
          req.status == SMB2_STATUS_INVALID_PARAMETER,
          "CREATE ctx: oversized NameLength rejected");
} /* test_create_ctx_name_overflow */

int
main(
    int   argc,
    char *argv[])
{
    (void) argc;
    (void) argv;

    chimera_log_init();

    fprintf(stderr, "=== SMB parse hardening: checked cursor primitives ===\n");
    test_try_readers_within_data();
    test_try_copy_past_end_rejected();
    test_limit_fences_reads();
    test_unbounded_default();

    fprintf(stderr, "=== SMB parse hardening: smb_cursor_seek_to ===\n");
    test_seek_to();
    test_compound_subwindow_then_skip();

    fprintf(stderr, "=== SMB parse hardening: CREATE-context bounds ===\n");
    test_create_ctx_datalen_uint32_overflow();
    test_create_ctx_next_uint32_overflow();
    test_create_ctx_name_overflow();

    fprintf(stderr, "\nTotal: %d passed, %d failed\n", passed, failed);
    return failed == 0 ? 0 : 1;
} /* main */
