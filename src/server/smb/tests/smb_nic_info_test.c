// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * FSCTL_QUERY_NETWORK_INTERFACE_INFO capability word (MS-SMB2 2.2.32.5).
 *
 * The Capability field of each NETWORK_INTERFACE_INFO record is what a Windows
 * client uses to decide whether to open additional channels to that address.
 * RSS_CAPABLE (0x1) and RDMA_CAPABLE (0x2) are independent bits that must
 * follow the per-interface rss / rdma configuration exactly: a TCP-only Windows
 * client pairs on the RSS bit, an RDMA client on the RDMA bit.  This drives
 * chimera_smb_ioctl_reply() over a flat buffer and checks every record of a
 * four-interface reply, one per rss/rdma combination.  The first three are
 * IPv4 and the last is IPv6; the Capability word does not depend on the
 * family, but the record's wire family does.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#include "common/test_host.h"
#include "common/evpl_iovec_cursor.h"
#include "server/smb/smb_internal.h"
#include "server/smb/smb_procs.h"

static int passed = 0;
static int failed = 0;

#define CHECK(cond, name) do { if (cond) { fprintf(stderr, "  PASS: %s\n", name); passed++; } \
                               else { fprintf(stderr, "  FAIL: %s\n", name); failed++; } } while (0)

/* Reply layout written by chimera_smb_ioctl_reply(): the 48-byte IOCTL reply
 * body (this harness emits no SMB2 header), then one 152-byte record per
 * interface: Next(4) IfIndex(4) Capability(4) Reserved(4) LinkSpeed(8)
 * SockAddr(128), the SockAddr opening with its 16-bit family. */
#define IOCTL_REPLY_BODY   48
#define IOCTL_OUTCOUNT_OFF 36
#define NIC_RECORD_SIZE    152
#define NIC_IFINDEX_OFF    4
#define NIC_CAPABILITY_OFF 8
#define NIC_LINKSPEED_OFF  16
#define NIC_FAMILY_OFF     24
#define NIC_COUNT          4

/* SOCKADDR_STORAGE family values on the wire are the Windows AF_* numbers
 * (MS-SMB2 2.2.32.5.1), not the host's: Linux AF_INET6 is 10. */
#define WIRE_AF_INET       0x02u
#define WIRE_AF_INET6      0x17u

#define RSS_CAPABLE        0x1u
#define RDMA_CAPABLE       0x2u

#define LINK_SPEED_10G     10000000000ULL

static uint16_t
get_le16(const uint8_t *p)
{
    return (uint16_t) (p[0] | (p[1] << 8));
} /* get_le16 */

static uint32_t
get_le32(const uint8_t *p)
{
    return (uint32_t) p[0] | ((uint32_t) p[1] << 8) |
           ((uint32_t) p[2] << 16) | ((uint32_t) p[3] << 24);
} /* get_le32 */

static uint64_t
get_le64(const uint8_t *p)
{
    return (uint64_t) get_le32(p) | ((uint64_t) get_le32(p + 4) << 32);
} /* get_le64 */

struct nic_case {
    const char *name;
    int         family;
    uint8_t     rss;
    uint8_t     rdma;
    uint32_t    expect;
};

int
main(
    int    argc,
    char **argv)
{
    static const struct nic_case      cases[NIC_COUNT] = {
        {
            .name   = "neither rss nor rdma reports 0",
            .family = AF_INET,
            .rss    = 0,
            .rdma   = 0,
            .expect = 0,
        },
        {
            .name   = "rss only reports RSS_CAPABLE",
            .family = AF_INET,
            .rss    = 1,
            .rdma   = 0,
            .expect = RSS_CAPABLE,
        },
        {
            .name   = "rdma only reports RDMA_CAPABLE",
            .family = AF_INET,
            .rss    = 0,
            .rdma   = 1,
            .expect = RDMA_CAPABLE,
        },
        {
            .name   = "rss and rdma report both bits (IPv6)",
            .family = AF_INET6,
            .rss    = 1,
            .rdma   = 1,
            .expect = RSS_CAPABLE | RDMA_CAPABLE,
        },
    };
    struct chimera_server_smb_shared *shared;
    struct chimera_server_smb_thread  thread;
    struct chimera_smb_compound       compound;
    struct chimera_smb_request        request;
    struct chimera_smb_nic_info      *nic;
    struct evpl_iovec                 iov;
    struct evpl_iovec_cursor          cursor;
    uint8_t                           buf[IOCTL_REPLY_BODY + NIC_COUNT * NIC_RECORD_SIZE + 64];
    const uint8_t                    *rec;
    int                               i;

    (void) argc;
    (void) argv;

    chimera_log_init();

    shared = calloc(1, sizeof(*shared));
    memset(&thread, 0, sizeof(thread));
    memset(&compound, 0, sizeof(compound));
    memset(&request, 0, sizeof(request));
    memset(buf, 0xA5, sizeof(buf));

    thread.shared          = shared;
    compound.thread        = &thread;
    request.compound       = &compound;
    request.ioctl.ctl_code = SMB2_FSCTL_QUERY_NETWORK_INTERFACE_INFO;

    shared->config.num_nic_info = NIC_COUNT;
    for (i = 0; i < NIC_COUNT; i++) {
        nic                 = &shared->config.nic_info[i];
        nic->addr.ss_family = cases[i].family;
        nic->speed          = LINK_SPEED_10G; /* already scaled to bps, as smb.c stores it */
        nic->rss            = cases[i].rss;
        nic->rdma           = cases[i].rdma;
    }

    iov.data   = buf;
    iov.length = sizeof(buf);
    iov.pad    = 0;
    iov.ref    = NULL;
    evpl_iovec_cursor_init(&cursor, &iov, 1);

    chimera_smb_ioctl_reply(&cursor, &request);

    CHECK(evpl_iovec_cursor_consumed(&cursor) == IOCTL_REPLY_BODY + NIC_COUNT * NIC_RECORD_SIZE,
          "reply is the 48-byte body plus one 152-byte record per interface");
    CHECK(get_le32(buf + IOCTL_OUTCOUNT_OFF) == NIC_COUNT * NIC_RECORD_SIZE,
          "OutputCount covers every record");

    for (i = 0; i < NIC_COUNT; i++) {
        rec = buf + IOCTL_REPLY_BODY + i * NIC_RECORD_SIZE;
        CHECK(get_le32(rec + NIC_CAPABILITY_OFF) == cases[i].expect, cases[i].name);
        CHECK(get_le32(rec) == (i == NIC_COUNT - 1 ? 0u : (uint32_t) NIC_RECORD_SIZE),
              "Next links to the following record, 0 on the last");
        CHECK(get_le64(rec + NIC_LINKSPEED_OFF) == LINK_SPEED_10G,
              "LinkSpeed is untouched by the capability bits");
        /* A server must not report IfIndex 0 (MS-SMB2 3.3.5.15.11). */
        CHECK(get_le32(rec + NIC_IFINDEX_OFF) == (uint32_t) (i + 1),
              "IfIndex is the 1-based interface position");
        CHECK(get_le16(rec + NIC_FAMILY_OFF) ==
              (cases[i].family == AF_INET ? WIRE_AF_INET : WIRE_AF_INET6),
              "SockAddr family is the Windows AF_* value");
    }

    free(shared);

    fprintf(stderr, "%d passed, %d failed\n", passed, failed);

    return failed ? 1 : 0;
} /* main */
