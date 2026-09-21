/* SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
 *
 * SPDX-License-Identifier: LGPL-2.1-only
 *
 * Reproduce a rename completion after TREE_DISCONNECT without racing sleeps:
 * hold RENAME_AT at the backend, await the disconnect reply, then dispatch it
 * on the original server thread. The request's open survives the disconnect,
 * but its tree no longer owns a share. A peer open must still follow the rename.
 */

#include <stdatomic.h>
#include "smb2_mbt_common.h"
#include "vfs/vfs.h"

extern struct chimera_vfs_module   vfs_memfs;

static void                        (*memfs_dispatch)(
    struct chimera_vfs_request *,
    void *);
static struct chimera_vfs_request *held_rename;
static void                       *held_private;
static struct evpl_doorbell        resume_rename;
static atomic_int                  rename_held;
static int                         failures;

#define CHECK(cond, ...) do {                         \
            if (!(cond)) {                                   \
                fprintf(stderr, "FAIL: " __VA_ARGS__);        \
                fprintf(stderr, "\n");                       \
                failures++;                                  \
            }                                                \
} while (0)

static void
resume_dispatch(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell)
{
    evpl_remove_doorbell(evpl, doorbell);
    memfs_dispatch(held_rename, held_private);
} /* resume_dispatch */

static void
hold_rename_dispatch(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    if (request->opcode == CHIMERA_VFS_OP_RENAME_AT &&
        !atomic_load(&rename_held)) {
        held_rename  = request;
        held_private = private_data;
        evpl_add_doorbell(request->thread->evpl, &resume_rename, resume_dispatch);
        atomic_store(&rename_held, 1);
        return;
    }
    memfs_dispatch(request, private_data);
} /* hold_rename_dispatch */

static void
post_rename(
    struct smb2_conn *c,
    const uint8_t     file_id[16],
    const char       *name)
{
    int      b    = smb2c_begin(c, SMB2_SET_INFO, 0);
    uint8_t *body = c->sbuf + b;
    uint8_t *info = body + 32;
    int      nlen = utf16le(name, info + 20);

    p16(body, 0, 33);
    body[2] = SMB2_INFO_FILE_T;
    body[3] = SMB2_FILE_RENAME_INFO_T;
    p32(body, 4, 20 + nlen);
    p16(body, 8, SMB2_HDR_SIZE + 32);
    memcpy(body + 16, file_id, 16);
    p32(info, 16, nlen);
    smb2c_send(c, 32 + 20 + nlen);
} /* post_rename */

int
main(void)
{
    struct smb2_env        env;
    struct smb2_conn      *renamer, *peer;
    struct smb2_create_out original, sibling, reopened;
    uint64_t               deadline, rename_mid;
    uint32_t               st, count = 0, length = 0;
    const char             payload[] = "rename survives disconnect";
    uint8_t                data[sizeof(payload)];

    memfs_dispatch     = vfs_memfs.dispatch;
    vfs_memfs.dispatch = hold_rename_dispatch;
    smb2_env_start(&env);
    renamer = smb2_conn_open(&env);
    peer    = smb2_conn_open(&env);
    smb2_handshake(renamer);
    smb2_handshake(peer);

    st = smb2_create(renamer, "before", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &original);
    CHECK(st == ST_SUCCESS, "create source: 0x%08x", st);
    if (st != ST_SUCCESS) {
        goto out;
    }
    st = smb2_write(renamer, original.file_id, 0, payload, sizeof(payload), &count);
    CHECK(st == ST_SUCCESS && count == sizeof(payload), "write source: 0x%08x", st);
    st = smb2_create(peer, "before", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &sibling);
    CHECK(st == ST_SUCCESS, "open peer handle: 0x%08x", st);
    if (st != ST_SUCCESS) {
        goto out;
    }

    post_rename(renamer, original.file_id, "after");
    rename_mid = renamer->last_msg_id;
    deadline   = smb2c_now_ms() + SMB2C_HANG_MS;
    while (!atomic_load(&rename_held)) {
        smb2_pump(&env);
        if (renamer->reply_ready || smb2c_now_ms() >= deadline) {
            fprintf(stderr, "rename did not reach the backend hold\n");
            exit(1);
        }
    }

    st = smb2_tree_disconnect(renamer);
    CHECK(st == ST_SUCCESS, "TREE_DISCONNECT while rename is held: 0x%08x", st);
    renamer->reply_ready = 0;
    evpl_ring_doorbell(&resume_rename);
    st = smb2c_wait(renamer);
    CHECK(st == ST_SUCCESS && renamer->reply_mid == rename_mid,
          "pending rename completes after disconnect: 0x%08x", st);

    /* The sibling still has its ACCESS claim and must have its cached path
     * updated, even though the operating handle was closed by disconnect. */
    st = smb2_rename(peer, sibling.file_id, "final", 0);
    CHECK(st == ST_SUCCESS, "rename through the surviving peer: 0x%08x", st);
    st = smb2_create(peer, "final", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                     0, NULL, &reopened);
    CHECK(st == ST_SHARING_VIOLATION,
          "surviving peer still enforces sharing after rename: 0x%08x", st);
    if (st == ST_SUCCESS) {
        smb2_close(peer, reopened.file_id);
    }
    smb2_close(peer, sibling.file_id);
    st = smb2_create(peer, "final", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &reopened);
    CHECK(st == ST_SUCCESS, "open final name: 0x%08x", st);
    if (st == ST_SUCCESS) {
        st = smb2_read(peer, reopened.file_id, 0, sizeof(data), data, &length);
        CHECK(st == ST_SUCCESS && length == sizeof(payload) &&
              memcmp(data, payload, sizeof(payload)) == 0,
              "renamed file retains its data: 0x%08x", st);
        smb2_close(peer, reopened.file_id);
    }
    st = smb2_create(peer, "before", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                     MBT_FILE_SHARE_RWD, NULL, &reopened);
    CHECK(st == ST_OBJECT_NAME_NOT_FOUND, "old name is gone: 0x%08x", st);
    if (st == ST_SUCCESS) {
        smb2_close(peer, reopened.file_id);
    }

 out:
    smb2_env_stop(&env);
    vfs_memfs.dispatch = memfs_dispatch;
    if (!failures) {
        printf("rename across tree disconnect passed\n");
    }
    return failures ? 1 : 0;
} /* main */
