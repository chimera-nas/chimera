// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Regression test for chimera_nfs3_unmarshall_fh (nfs3_attr.h).
 *
 * The proxy NFSv3 client re-encodes a backend server's file handle into a
 * chimera handle.  The server-supplied length is NOT bounded by the generated
 * XDR decoder, so an oversized handle must be rejected rather than overflowing
 * the fixed-size fragment / va_fh buffers (the V-001 report).  This pins:
 *   - data.len within capacity  -> returns 0, ATTR_FH set, bytes copied intact
 *   - data.len at the boundary  -> the largest accepted len is FH_SIZE-1
 *   - data.len over capacity    -> returns -1, ATTR_FH NOT set, no write past
 *                                  the destination (verified with a canary)
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#undef NDEBUG
#include <assert.h>

#include "nfs3_attr.h"

/* attrs wrapped in a canary-guarded struct: chimera_nfs3_unmarshall_fh writes
 * into attr.va_fh (the largest sink in the encode chain), so a guard page of
 * known bytes immediately after the attrs catches any over-write. */
struct guarded_attrs {
    struct chimera_vfs_attrs attr;
    uint8_t                  canary[64];
};

static void
reset_guard(struct guarded_attrs *g)
{
    memset(g, 0, sizeof(*g));
    memset(g->canary, 0xAB, sizeof(g->canary));
} /* reset_guard */

static void
check_canary(const struct guarded_attrs *g)
{
    for (size_t i = 0; i < sizeof(g->canary); i++) {
        assert(g->canary[i] == 0xAB && "destination buffer overflow");
    }
} /* check_canary */

int
main(void)
{
    /* A valid parent handle: 16-byte mount id is all encode_fh_parent reads. */
    uint8_t        parent_fh[CHIMERA_VFS_FH_SIZE + 16];

    memset(parent_fh, 0x11, sizeof(parent_fh));

    /* Backing store for the server-supplied handle data, larger than any
    * length we test so data.data is always valid memory to copy from. */
    static uint8_t fh_data[1024];

    memset(fh_data, 0x55, sizeof(fh_data));

    const uint32_t max_ok = CHIMERA_VFS_FH_SIZE - 1; /* largest accepted len */

    /*
     * (len, expect_ok) cases spanning over / boundary / valid:
     *   1024        far oversized -- would smash the stack fragment
     *   256         oversized
     *   max_ok + 1  one byte over the boundary
     *   max_ok      exactly the largest that fits
     *   32          comfortably valid
     *   1           minimal
     *   0           empty handle
     */
    /* *INDENT-OFF* */ /* uncrustify oscillates on aligned struct-init tables */
    struct {
        uint32_t len;
        int      expect_ok;
    } cases[] = {
        { 1024,       0 },
        { 256,        0 },
        { max_ok + 1, 0 },
        { max_ok,     1 },
        { 32,         1 },
        { 1,          1 },
        { 0,          1 },
    };
    /* *INDENT-ON* */
    int ncases = (int) (sizeof(cases) / sizeof(cases[0]));

    for (int i = 0; i < ncases; i++) {
        struct guarded_attrs g;
        struct nfs_fh3       fh;
        int                  rc;

        reset_guard(&g);

        fh.data.len  = cases[i].len;
        fh.data.data = fh_data;

        rc = chimera_nfs3_unmarshall_fh(&fh, 7 /*server_index*/, parent_fh, &g.attr);

        /* No path may ever write past the destination. */
        check_canary(&g);

        if (cases[i].expect_ok) {
            assert(rc == 0);
            /* ATTR_FH must be set and the encoded length consistent with
             * mount_id(16) + server_index(1) + data.len. */
            assert(g.attr.va_set_mask & CHIMERA_VFS_ATTR_FH);
            assert(g.attr.va_fh_len == 16 + 1 + cases[i].len);
            /* server_index then the handle bytes land after the mount id. */
            assert(g.attr.va_fh[16] == 7);
            for (uint32_t b = 0; b < cases[i].len; b++) {
                assert(g.attr.va_fh[16 + 1 + b] == 0x55);
            }
        } else {
            assert(rc == -1);
            /* Rejected: the FH attribute must be left unset so the caller
             * fails the op (or omits the readdir entry's handle). */
            assert(!(g.attr.va_set_mask & CHIMERA_VFS_ATTR_FH));
        }

        printf("len=%-5u expect_ok=%d rc=%d OK\n",
               cases[i].len, cases[i].expect_ok, rc);
    }

    printf("nfs3_fh_test: all cases passed\n");
    return 0;
} /* main */
