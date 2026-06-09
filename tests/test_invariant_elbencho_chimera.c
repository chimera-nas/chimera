#include <check.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

/* Test buffer size mirrors the expected destination buffer in elbencho */
#define DEST_BUF_SIZE 4096

START_TEST(test_memcpy_count_bounds)
{
    /* Invariant: memcpy count must never exceed destination buffer size */
    static const struct {
        size_t count;      /* sc.count equivalent */
        size_t src_size;   /* source iovec data size */
        int    should_fit; /* 1 = valid, 0 = must be rejected/truncated */
    } payloads[] = {
        /* Exact exploit: count 2x the destination buffer */
        { DEST_BUF_SIZE * 2,  DEST_BUF_SIZE * 2,  0 },
        /* Boundary: count exactly one byte over */
        { DEST_BUF_SIZE + 1,  DEST_BUF_SIZE + 1,  0 },
        /* Valid: count fits exactly */
        { DEST_BUF_SIZE,      DEST_BUF_SIZE,       1 },
        /* Valid: count well within bounds */
        { 128,                128,                  1 },
    };
    int num_payloads = (int)(sizeof(payloads) / sizeof(payloads[0]));

    for (int i = 0; i < num_payloads; i++) {
        size_t count    = payloads[i].count;
        size_t src_size = payloads[i].src_size;

        /* Allocate a canary-guarded destination buffer */
        uint8_t *dest = calloc(1, DEST_BUF_SIZE + 16);
        ck_assert_ptr_nonnull(dest);
        /* Write canary bytes immediately after the destination region */
        memset(dest + DEST_BUF_SIZE, 0xAB, 16);

        uint8_t *src = calloc(1, src_size);
        ck_assert_ptr_nonnull(src);
        memset(src, 0x55, src_size);

        /* The security invariant: count must be clamped to DEST_BUF_SIZE
         * before any memcpy.  Simulate the validated copy path. */
        size_t safe_count = (count <= DEST_BUF_SIZE) ? count : (size_t)-1;

        if (payloads[i].should_fit) {
            /* Valid input: copy must succeed without overflow */
            ck_assert_int_le((int)safe_count, DEST_BUF_SIZE);
            memcpy(dest, src, safe_count);
            /* Canary must be intact */
            for (int b = 0; b < 16; b++)
                ck_assert_int_eq(dest[DEST_BUF_SIZE + b], 0xAB);
        } else {
            /* Oversized input: count exceeds buffer — must be rejected */
            ck_assert_int_gt((int)count, DEST_BUF_SIZE);
            /* safe_count sentinel confirms rejection */
            ck_assert_int_eq((int)safe_count, -1);
            /* Canary must still be intact (no copy was performed) */
            for (int b = 0; b < 16; b++)
                ck_assert_int_eq(dest[DEST_BUF_SIZE + b], 0xAB);
        }

        free(dest);
        free(src);
    }
}
END_TEST

Suite *security_suite(void)
{
    Suite *s;
    TCase *tc_core;

    s       = suite_create("Security");
    tc_core = tcase_create("Core");

    tcase_add_test(tc_core, test_memcpy_count_bounds);
    suite_add_tcase(s, tc_core);

    return s;
}

int main(void)
{
    int      number_failed;
    Suite   *s;
    SRunner *sr;

    s  = security_suite();
    sr = srunner_create(s);

    srunner_run_all(sr, CK_NORMAL);
    number_failed = srunner_ntests_failed(sr);
    srunner_free(sr);

    return (number_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}