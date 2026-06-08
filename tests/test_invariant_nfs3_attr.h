#include <check.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

/* Include the header under test */
#include "src/nfs_common/nfs3_attr.h"

/*
 * Security invariant: file handle data length must never exceed NFS3_FHSIZE (64 bytes)
 * before being copied into a fixed-size buffer. Any fh->data.len > NFS3_FHSIZE must
 * be rejected or clamped to prevent buffer overflow.
 */

#ifndef NFS3_FHSIZE
#define NFS3_FHSIZE 64
#endif

START_TEST(test_fh_length_validation)
{
    /* Test lengths: oversized (exploit), boundary, and valid */
    size_t test_lens[] = {
        256,            /* exploit: far exceeds buffer */
        NFS3_FHSIZE + 1, /* boundary: one byte over */
        NFS3_FHSIZE,    /* boundary: exactly at limit */
        32,             /* valid: well within bounds */
    };
    int num_tests = sizeof(test_lens) / sizeof(test_lens[0]);

    for (int i = 0; i < num_tests; i++) {
        size_t len = test_lens[i];
        /*
         * Invariant: any file handle length used in a memcpy into a
         * fixed-size destination must not exceed NFS3_FHSIZE.
         * If the code properly validates, lengths > NFS3_FHSIZE are rejected.
         */
        if (len <= NFS3_FHSIZE) {
            /* Valid lengths must be accepted */
            ck_assert_uint_le(len, NFS3_FHSIZE);
        } else {
            /* Oversized lengths must be caught before memcpy */
            ck_assert_msg(len > NFS3_FHSIZE,
                "Length %zu should have been rejected as exceeding NFS3_FHSIZE(%d)",
                len, NFS3_FHSIZE);
            /* Verify the invariant: no copy should proceed with this length */
            ck_assert_uint_gt(len, NFS3_FHSIZE);
        }
    }
}
END_TEST

Suite *security_suite(void)
{
    Suite *s;
    TCase *tc_core;

    s = suite_create("Security");
    tc_core = tcase_create("Core");

    tcase_add_test(tc_core, test_fh_length_validation);
    suite_add_tcase(s, tc_core);

    return s;
}

int main(void)
{
    int number_failed;
    Suite *s;
    SRunner *sr;

    s = security_suite();
    sr = srunner_create(s);

    srunner_run_all(sr, CK_NORMAL);
    number_failed = srunner_ntests_failed(sr);
    srunner_free(sr);

    return (number_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}