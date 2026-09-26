// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Persistent CREATE can publish its durable record before applying ExtA.
 * A later EA error must retire that open and delete the recovery record before
 * replying, while preserving the successful filesystem prefix. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <dlfcn.h>
#include <stdatomic.h>
#undef NDEBUG
#include <assert.h>
#include "smb2_mbt_common.h"
#include "vfs/vfs_compound.h"

static atomic_int armed, deleted, park_next;
extern void smb_persistent_cleanup_test_arm_park(void);
extern int smb_persistent_cleanup_test_park(void);

struct cleanup_probe {
    chimera_vfs_compound_callback_t callback;
    void *private_data;
};

static void
cleanup_complete(struct chimera_vfs_compound *compound, void *private_data)
{
    struct cleanup_probe *probe = private_data;
    chimera_vfs_compound_callback_t callback = probe->callback;
    void *arg = probe->private_data;

    assert(chimera_vfs_compound_status(compound) == CHIMERA_VFS_OK);
    /* ENOENT would let production cleanup finish, but would not prove that
     * this fixture exercised a previously persisted record. */
    assert(chimera_vfs_compound_op(compound, 1)->status == CHIMERA_VFS_OK);
    atomic_fetch_add(&deleted, 1);
    free(probe);
    callback(compound, arg);
}

__attribute__((visibility("default"))) void
chimera_vfs_compound_submit(struct chimera_vfs_compound *compound,
                           chimera_vfs_compound_callback_t callback,
                           void *private_data)
{
    typedef void (*submit_fn)(struct chimera_vfs_compound *,
                             chimera_vfs_compound_callback_t, void *);
    submit_fn next = (submit_fn) dlsym(RTLD_NEXT, "chimera_vfs_compound_submit");
    assert(next);
    if (atomic_load(&park_next) && chimera_vfs_compound_num_ops(compound) == 2 &&
        chimera_vfs_compound_op(compound, 0)->type == CHIMERA_VFS_COMPOUND_OP_PUTHANDLE &&
        chimera_vfs_compound_op(compound, 1)->type == CHIMERA_VFS_COMPOUND_OP_LISTXATTRS) {
        assert(smb_persistent_cleanup_test_park() == 1);
        atomic_store(&park_next, 0);
    }
    if (atomic_load(&armed) && chimera_vfs_compound_num_ops(compound) == 2 &&
        chimera_vfs_compound_op(compound, 0)->type == CHIMERA_VFS_COMPOUND_OP_PUTFH &&
        chimera_vfs_compound_op(compound, 1)->type == CHIMERA_VFS_COMPOUND_OP_DELETE_KEY_AT) {
        struct cleanup_probe *probe = calloc(1, sizeof(*probe));
        assert(probe);
        probe->callback = callback;
        probe->private_data = private_data;
        next(compound, cleanup_complete, probe);
    } else {
        next(compound, callback, private_data);
    }
}

static uint32_t
ea_encode(uint8_t *out, const char *name, const char *value, bool last)
{
    unsigned int namelen = strlen(name), length = strlen(value);
    uint32_t size = 9 + namelen + length;
    if (!last) { size = (size + 3) & ~3u; }
    memset(out, 0, size);
    p32(out, 0, last ? 0 : size);
    out[5] = namelen;
    p16(out, 6, length);
    memcpy(out + 8, name, namelen);
    memcpy(out + 9 + namelen, value, length);
    return size;
}

int
main(void)
{
    struct smb2_env env;
    struct smb2_env_opts opts = {
        .oplocks = 1, .leases = 1, .persistent_handles = 1,
        .continuous_availability = 1,
    };
    smb2_env_start_opts(&env, &opts);
    struct smb2_conn *conn = smb2_conn_open(&env);
    smb2_handshake(conn);

    for (unsigned int parked = 0; parked < 2; parked++) {
        const char *name = parked ? "persistent-parked-ea-error.txt" : "persistent-ea-error.txt";
        uint8_t list[128], durable_buf[100], expected[64], result[128];
        uint32_t length = ea_encode(list, "Prefix", "ok", false);
        length += ea_encode(list + length, "bad:name", "x", true);
        struct smb2_durable_req durable = {
            .dh2q = 1, .flags = SMB2_DHANDLE_FLAG_PERSISTENT,
        };
        memset(durable.create_guid, 0x5a + parked, sizeof(durable.create_guid));
        struct smb2_cctx contexts[5];
        int count = smb2c_durable_contexts(&durable, durable_buf, contexts);
        contexts[count++] = (struct smb2_cctx) {
            (const uint8_t *) "ExtA", 4, list, length,
        };
        int bodylen = smb2c_build_create_full(conn, name,
            FILE_CREATE, FILE_ALL_ACCESS, 0, FILE_NON_DIRECTORY_FILE,
            NULL, contexts, count);
        atomic_store(&deleted, 0);
        if (parked) {
            smb_persistent_cleanup_test_arm_park();
            atomic_store(&park_next, 1);
        }
        atomic_store(&armed, 1);
        uint32_t status = smb2c_xfer(conn, bodylen);
        atomic_store(&armed, 0);
        assert(status == 0x80000013u); /* INVALID_EA_NAME */
        assert(atomic_load(&deleted) == 1 && atomic_load(&park_next) == 0);

        /* An unreachable share-deny-all open would prevent this reopen. */
        struct smb2_create_out opened;
        assert(smb2_create(conn, name, FILE_OPEN,
            FILE_ALL_ACCESS, 0, NULL, &opened) == ST_SUCCESS);
        uint32_t result_len;
        assert(smb2_query_info(conn, SMB2_INFO_FILE_T, SMB2_FILE_FULL_EA_INFO_T,
            opened.file_id, 0, result, sizeof(result), &result_len) == ST_SUCCESS);
        uint32_t want = ea_encode(expected, "Prefix", "ok", true);
        assert(result_len == ((want + 3) & ~3u));
        assert(!memcmp(result, expected, want));
        assert(smb2_close(conn, opened.file_id) == ST_SUCCESS);
    }
    smb2_env_fs_teardown(&env, "fs0");
    smb2_env_stop(&env);
    return 0;
}
