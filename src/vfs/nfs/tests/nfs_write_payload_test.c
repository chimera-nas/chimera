// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include "nfs_write_payload.h"
#include "nfs3_xdr.h"
#include "nfs4_xdr.h"

static void
send_attempt(
    struct evpl       *evpl,
    struct evpl_iovec *original,
    int                version,
    uint32_t           length)
{
    struct evpl_iovec           header, output[260];
    struct evpl_rpc2_rdma_chunk chunk   = { 0 };
    struct evpl_iovec          *payload = chimera_nfs_write_payload_clone(original, 2);
    int                         niov = 260, encoded;

    assert(payload);
    assert(evpl_iovec_alloc(evpl, 4096, 0, 1, 0, &header) == 1);
    if (version == 3) {
        struct WRITE3args args = { 0 };
        args.count     = args.data.length = length;
        args.data.iov  = payload;
        args.data.niov = 2;
        args.stable    = FILE_SYNC;
        encoded        = marshall_WRITE3args(&args, &header, output, &niov, &chunk, 0);
    } else {
        struct nfs_argop4    operation = { .argop = OP_WRITE };
        struct COMPOUND4args args      = { .minorversion = 1, .num_argarray = 1, .argarray = &operation };
        operation.opwrite.data.length = length;
        operation.opwrite.data.iov    = payload;
        operation.opwrite.data.niov   = 2;
        operation.opwrite.stable      = FILE_SYNC4;
        encoded                       = marshall_COMPOUND4args(&args, &header, output, &niov, &chunk, 0);
    }
    assert(encoded >= 0 && niov > 0);
    assert((payload[0].data == NULL) == (length > 0));
    assert((payload[1].data == NULL) == (length > 8));
    /* An RPC consumes moved clones; parked/zero-length/trailing pieces remain
     * ours. Neither cleanup path may release the retained frontend input. */
    chimera_nfs_write_payload_discard(evpl, payload, 2);
    evpl_iovecs_release_internal(evpl, output, niov);
    evpl_iovec_release(evpl, &header);
    assert(!memcmp(original[0].data, "payload0", 8));
    assert(!memcmp(original[1].data, "payload1", 8));
} /* send_attempt */

int
main(void)
{
    struct evpl_iovec original[2];

    evpl_init(NULL);
    struct evpl      *evpl = evpl_create(NULL);

    for (int global = 0; global < 2; global++) {
        for (int i = 0; i < 2; i++) {
            if (global) {
                evpl_iovec_alloc_global(evpl, &original[i]);
                evpl_iovec_set_length(&original[i], 8);
            } else {
                assert(evpl_iovec_alloc(evpl, 8, 0, 1, 0, &original[i]) == 1);
            }
            memcpy(original[i].data, i ? "payload1" : "payload0", 8);
        }
        /* Simulate slot parking before marshalling, followed by actual XDR
         * sends and a retry of the same original input. */
        struct evpl_iovec *parked = chimera_nfs_write_payload_clone(original, 2);
        assert(parked);
        chimera_nfs_write_payload_discard(evpl, parked, 2);
        for (int version = 3; version <= 4; version++) {
            send_attempt(evpl, original, version, 16);
            send_attempt(evpl, original, version, 16);
            send_attempt(evpl, original, version, 8);
            send_attempt(evpl, original, version, 0);
        }
        evpl_iovecs_release(evpl, original, 2);
    }
    evpl_destroy(evpl);
    puts("ok: NFS3/NFS4 XDR sends retain WRITE input across moves, parking and retry");
    return 0;
} /* main */
