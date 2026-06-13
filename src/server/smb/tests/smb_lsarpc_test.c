// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Verifies that the ndrzcc-generated LSARPC marshalling reproduces, byte for
 * byte, the responses the hand-rolled handler used to emit for the migrated
 * operations (LsarOpenPolicy2 / LsarClose): a fixed opaque policy handle
 * (4-byte type 0 + 16 bytes of 0xaa) followed by STATUS_SUCCESS.
 *
 * This is the regression guard for the "same functionality, verified" goal of
 * the LSARPC migration; it links the generated marshaller directly, no server.
 */

#include "lsarpc_ndr.h"
#include <stdio.h>
#include <string.h>

/* The exact 24 bytes the legacy handler produced for OpenPolicy2 and Close. */
static const uint8_t golden[24] = {
    0x00, 0x00, 0x00, 0x00, /* handle_type */
    0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa,     /* uuid[16]    */
    0x00, 0x00, 0x00, 0x00, /* status      */
};

static int
check(
    const char *name,
    int         opnum)
{
    uint8_t                   buf[128];
    struct ndr_writer         w;
    const struct ndr_op_desc *op;
    int                       n;

    op = ndr_find_op(lsarpc_op_table, lsarpc_op_count, opnum);
    if (!op) {
        printf("FAIL %s: opnum %d not in generated table\n", name, opnum);
        return 1;
    }

    ndr_writer_init(&w, buf, sizeof(buf));

    if (opnum == 44) {
        struct lsa_OpenPolicy2_out o;
        memset(&o, 0, sizeof(o));
        o.handle.handle_type = 0;
        memset(o.handle.uuid, 0xaa, sizeof(o.handle.uuid));
        o.status = 0;
        n        = op->push_out(&w, &o);
    } else {
        struct lsa_Close_out o;
        memset(&o, 0, sizeof(o));
        o.handle.handle_type = 0;
        memset(o.handle.uuid, 0xaa, sizeof(o.handle.uuid));
        o.status = 0;
        n        = op->push_out(&w, &o);
    }

    if (n != (int) sizeof(golden) || memcmp(buf, golden, sizeof(golden)) != 0) {
        printf("FAIL %s (opnum %d): n=%d\n", name, opnum, n);
        for (int i = 0; i < n; i++) {
            printf(" %02x", buf[i]);
        }
        printf("\n");
        return 1;
    }

    printf("PASS %s (opnum %d): %d bytes, byte-identical to legacy\n", name, opnum, n);
    return 0;
} /* check */

int
main(void)
{
    int rc = 0;

    rc |= check("OpenPolicy2", 44);
    rc |= check("Close", 0);
    return rc;
} /* main */
