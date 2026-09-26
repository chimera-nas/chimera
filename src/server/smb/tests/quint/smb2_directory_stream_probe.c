// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <dlfcn.h>
#include <stdatomic.h>
#undef NDEBUG
#include <assert.h>
#include "smb2_mbt_common.h"
#include "vfs/vfs_compound.h"

/* Directory metadata is shared with its ADS, but the ADS is a data fork, not
 * an SMB directory. Count actual submissions to catch a legacy redispatch. */
static atomic_int armed, submissions;
static unsigned int expected_groups;

__attribute__((visibility("default"))) void
chimera_vfs_compound_submit(struct chimera_vfs_compound *compound,
                            chimera_vfs_compound_callback_t callback, void *private_data)
{
    typedef void (*submit_fn)(struct chimera_vfs_compound *, chimera_vfs_compound_callback_t, void *);
    submit_fn next = (submit_fn) dlsym(RTLD_NEXT, "chimera_vfs_compound_submit");
    assert(next);
    if (atomic_load(&armed)) {
        atomic_fetch_add(&submissions, 1);
        assert(chimera_vfs_compound_num_groups(compound) == expected_groups);
    }
    next(compound, callback, private_data);
}

static void
related_header(uint8_t *header, uint16_t opcode, uint64_t mid, uint32_t next)
{
    memset(header, 0, SMB2_HDR_SIZE);
    memcpy(header, "\xfeSMB", 4);
    p16(header, 4, SMB2_HDR_SIZE); p16(header, 6, 1);
    p16(header, 12, opcode); p16(header, 14, 32);
    p32(header, 16, SMB2_FLAGS_RELATED_OPERATIONS); p32(header, 20, next);
    p64(header, 24, mid); p32(header, 36, UINT32_MAX); p64(header, 40, UINT64_MAX);
}

static void
chain(struct smb2_conn *conn, const char *name, uint32_t disposition,
      uint32_t options, uint32_t action, uint64_t initial_size, bool write,
      const struct smb2_cctx *contexts, unsigned int count)
{
    fprintf(stderr, "# directory stream %s disposition %u write %u\n", name, disposition, write);
    int length = smb2c_build_create_full(conn, name, disposition, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, options, NULL, contexts, count);
    uint8_t *wire = conn->sbuf + 4;
    uint64_t mid = g64(wire, 24);
    unsigned int offset = (SMB2_HDR_SIZE + length + 7) & ~7u;
    memset(wire + SMB2_HDR_SIZE + length, 0, offset - SMB2_HDR_SIZE - length);
    p32(wire, 20, offset);
    unsigned int slot = 1;
    if (write) {
        uint8_t *header = wire + offset;
        related_header(header, SMB2_WRITE, mid + slot++, 120);
        uint8_t *body = header + SMB2_HDR_SIZE;
        memset(body, 0, 56); p16(body, 0, 49); p16(body, 2, 112); p32(body, 4, 4);
        memset(body + 16, 0xff, 16); memcpy(body + 48, "fork", 4);
        offset += 120;
        header = wire + offset;
        related_header(header, SMB2_READ, mid + slot++, 112);
        body = header + SMB2_HDR_SIZE;
        memset(body, 0, 48); p16(body, 0, 49); p32(body, 4, 4); memset(body + 16, 0xff, 16);
        offset += 112;
    }
    uint8_t *header = wire + offset;
    related_header(header, SMB2_QUERY_INFO, mid + slot++, 104);
    uint8_t *body = header + SMB2_HDR_SIZE;
    memset(body, 0, 40); p16(body, 0, 41); body[2] = SMB2_INFO_FILE_T;
    body[3] = SMB2_FILE_STANDARD_INFO_T; p32(body, 4, 4096); memset(body + 24, 0xff, 16);
    offset += 104;
    header = wire + offset;
    related_header(header, SMB2_CLOSE, mid + slot++, 0);
    body = header + SMB2_HDR_SIZE;
    memset(body, 0, 24); p16(body, 0, 24); p16(body, 2, 1); memset(body + 8, 0xff, 16);
    offset += 88;
    expected_groups = slot; atomic_store(&submissions, 0); atomic_store(&armed, 1);
    smb2c_send(conn, offset - SMB2_HDR_SIZE); conn->msg_id = mid + slot; smb2c_wait(conn);
    atomic_store(&armed, 0);
    assert(atomic_load(&submissions) == 1);
    struct smb2_create_out created;
    smb2c_parse_create(conn, &created);
    assert(created.status == ST_SUCCESS && created.action == action);
    assert(created.end_of_file == initial_size);
    const uint8_t *reply = conn->rbuf + 4;
    assert(!(g32(reply + SMB2_HDR_SIZE, 56) & SMB2_FILE_ATTRIBUTE_DIRECTORY));
    uint64_t size = write && initial_size < 4 ? 4 : initial_size;
    for (unsigned int i = 0; i < slot; i++) {
        const uint8_t *out = reply + SMB2_HDR_SIZE;
        uint16_t opcode = g16(reply, 12);
        if (g32(reply, 8) != ST_SUCCESS) {
            fprintf(stderr, "# slot %u opcode %u status %08x\n", i, opcode, g32(reply, 8));
        }
        assert(g32(reply, 8) == ST_SUCCESS && g64(reply, 24) == mid + i);
        if (opcode == SMB2_READ) {
            assert(g32(out, 4) == 4 && !memcmp(reply + out[2], "fork", 4));
        } else if (opcode == SMB2_QUERY_INFO) {
            const uint8_t *attrs = reply + g16(out, 2);
            assert(g32(out, 4) == 24 && g64(attrs, 8) == size && attrs[21] == 0);
            assert(g64(attrs, 0) >= size);
        } else if (opcode == SMB2_CLOSE) {
            assert(g16(out, 0) == 60 && (g16(out, 2) & 1));
            assert(g64(out, 48) == size && g64(out, 40) >= size);
            assert(!(g32(out, 56) & SMB2_FILE_ATTRIBUTE_DIRECTORY));
        }
        uint32_t next = g32(reply, 20);
        if (i + 1 < slot) { assert(next); reply += next; }
        else { assert(!next); }
    }
    assert(smb2_close(conn, created.file_id) == ST_FILE_CLOSED);
}

static void
assert_bytes(struct smb2_conn *conn, const uint8_t id[16], const char *want)
{
    uint8_t data[32]; uint32_t bytes;
    assert(smb2_read(conn, id, 0, sizeof(data), data, &bytes) == ST_SUCCESS);
    assert(bytes == strlen(want) && !memcmp(data, want, bytes));
}

static uint32_t
ea_encode(uint8_t *out, const char *name, const char *value, bool last)
{
    uint32_t namelen = strlen(name), vallen = strlen(value), size = 9 + namelen + vallen;
    if (!last) { size = (size + 3) & ~3u; }
    memset(out, 0, size); p32(out, 0, last ? 0 : size); out[5] = namelen; p16(out, 6, vallen);
    memcpy(out + 8, name, namelen); memcpy(out + 9 + namelen, value, vallen);
    return size;
}

int
main(void)
{
    struct smb2_env env;
    struct smb2_env_opts opts = { .oplocks = 1, .leases = 1, .named_streams = 1,
        .persistent_handles = 1 };
    smb2_env_start_opts(&env, &opts);
    struct smb2_conn *conn = smb2_conn_open(&env); smb2_handshake(conn);
    struct smb2_create_out base, child, sibling, seed, rejected;
    uint32_t bytes;
    assert(smb2_create_opts(conn, "directory", FILE_CREATE, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, FILE_DIRECTORY_FILE, NULL, &base) == ST_SUCCESS);
    assert(smb2_create(conn, "directory\\child", FILE_CREATE, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, NULL, &child) == ST_SUCCESS);
    assert(smb2_write(conn, child.file_id, 0, "child", 5, &bytes) == ST_SUCCESS);
    assert(smb2_create(conn, "directory:sibling", FILE_CREATE, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, NULL, &sibling) == ST_SUCCESS);
    assert(smb2_write(conn, sibling.file_id, 0, "sibling", 7, &bytes) == ST_SUCCESS);

    chain(conn, "directory:new:$DATA", FILE_CREATE, FILE_NON_DIRECTORY_FILE, 2, 0, true, NULL, 0);
    chain(conn, "directory:new:$DATA", FILE_OPEN, FILE_NON_DIRECTORY_FILE, 1, 4, false, NULL, 0);
    chain(conn, "directory:new:$DATA", FILE_OPEN_IF, 0, 1, 4, true, NULL, 0);
    chain(conn, "directory:open-if", FILE_OPEN_IF, FILE_NON_DIRECTORY_FILE, 2, 0, true, NULL, 0);
    const uint32_t dispositions[] = { FILE_OVERWRITE, FILE_OVERWRITE_IF, FILE_SUPERSEDE };
    for (unsigned int i = 0; i < 3; i++) {
        chain(conn, "directory:new:$DATA", dispositions[i], FILE_NON_DIRECTORY_FILE,
            dispositions[i] == FILE_SUPERSEDE ? 0 : 3, 0, true, NULL, 0);
        if (dispositions[i] != FILE_OVERWRITE) {
            char name[64]; snprintf(name, sizeof(name), "directory:created-%u", i);
            chain(conn, name, dispositions[i], FILE_NON_DIRECTORY_FILE, 2, 0, false, NULL, 0);
        }
        assert_bytes(conn, sibling.file_id, "sibling");
        assert_bytes(conn, child.file_id, "child");
    }
    expected_groups = 1; atomic_store(&submissions, 0); atomic_store(&armed, 1);
    assert(smb2_create(conn, "directory:missing", FILE_OVERWRITE, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, NULL, &rejected) == ST_OBJECT_NAME_NOT_FOUND);
    atomic_store(&armed, 0); assert(atomic_load(&submissions) == 1);
    assert(smb2_create(conn, "directory:missing", FILE_OPEN, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, NULL, &rejected) == ST_OBJECT_NAME_NOT_FOUND);

    /* A stream SHARE denial still happens before overwrite. */
    assert(smb2_create(conn, "directory:new", FILE_OPEN, FILE_ALL_ACCESS,
        FILE_SHARE_READ, NULL, &seed) == ST_SUCCESS);
    assert(smb2_create(conn, "directory:new", FILE_OVERWRITE, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, NULL, &rejected) == ST_SHARING_VIOLATION);
    assert_bytes(conn, seed.file_id, "fork");
    assert(smb2_close(conn, seed.file_id) == ST_SUCCESS);

    /* EAs remain shared base metadata; a later invalid entry preserves its
     * successful prefix and the created stream, but releases both claims. */
    uint8_t eas[128], result[128], expected[64];
    uint32_t length = ea_encode(eas, "DirEA", "one", false);
    length += ea_encode(eas + length, "direa", "two", true);
    struct smb2_cctx ctx = { (const uint8_t *) "ExtA", 4, eas, length };
    chain(conn, "directory:ea", FILE_CREATE, FILE_NON_DIRECTORY_FILE, 2, 0, false, &ctx, 1);
    uint32_t result_len;
    assert(smb2_query_info(conn, SMB2_INFO_FILE_T, SMB2_FILE_FULL_EA_INFO_T,
        base.file_id, 0, result, sizeof(result), &result_len) == ST_SUCCESS);
    uint32_t want = ea_encode(expected, "DirEA", "two", true);
    assert(result_len == ((want + 3) & ~3u) && !memcmp(result, expected, want));
    length = ea_encode(eas, "DirEA", "ok", false);
    length += ea_encode(eas + length, "bad:name", "x", true); ctx.data_len = length;
    int body = smb2c_build_create_full(conn, "directory:failed-ea", FILE_CREATE,
        FILE_ALL_ACCESS, 0, FILE_NON_DIRECTORY_FILE, NULL, &ctx, 1);
    assert(smb2c_xfer(conn, body) == 0x80000013u);
    assert(smb2_create(conn, "directory:failed-ea", FILE_OPEN, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, NULL, &seed) == ST_SUCCESS);
    assert(smb2_close(conn, seed.file_id) == ST_SUCCESS);
    assert(smb2_query_info(conn, SMB2_INFO_FILE_T, SMB2_FILE_FULL_EA_INFO_T,
        base.file_id, 0, result, sizeof(result), &result_len) == ST_SUCCESS);
    want = ea_encode(expected, "DirEA", "ok", true);
    assert(result_len == ((want + 3) & ~3u) && !memcmp(result, expected, want));

    /* Reparse-option and CREATE-time DOC still use their legacy boundaries.
     * They must agree with native stream type/size and preserve the directory. */
    assert(smb2_create_opts(conn, "directory:new", FILE_OPEN, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, FILE_NON_DIRECTORY_FILE | 0x00200000u, NULL, &seed) == ST_SUCCESS);
    assert(seed.end_of_file == 4);
    assert(!(g32(conn->rbuf + 4 + SMB2_HDR_SIZE, 56) & SMB2_FILE_ATTRIBUTE_DIRECTORY));
    assert_bytes(conn, seed.file_id, "fork");
    assert(smb2_close(conn, seed.file_id) == ST_SUCCESS);
    assert(smb2_create_opts(conn, "directory:doc", FILE_CREATE, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, FILE_NON_DIRECTORY_FILE | FILE_DELETE_ON_CLOSE, NULL, &seed) == ST_SUCCESS);
    assert(smb2_write(conn, seed.file_id, 0, "doc", 3, &bytes) == ST_SUCCESS);
    assert(smb2_close(conn, seed.file_id) == ST_SUCCESS);
    assert(smb2_create(conn, "directory:doc", FILE_OPEN, FILE_READ_ACCESS,
        FILE_SHARE_RWD, NULL, &rejected) == ST_OBJECT_NAME_NOT_FOUND);

    /* A surviving durable stream already carries its STREAM identity. The
     * reconnect getattr callback must not reclassify it from the base mode. */
    struct smb2_conn *holder = smb2_conn_open(&env); smb2_handshake(holder);
    struct smb2_oplock_req caching = { .level = SMB2_OPLOCK_LEVEL_BATCH };
    struct smb2_durable_req durable = { .dhnq = 1 };
    assert(smb2_create_dur(holder, "directory:durable", FILE_CREATE, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, &caching, &durable, &seed) == ST_SUCCESS && seed.has_dhnq);
    assert(smb2_write(holder, seed.file_id, 0, "durable", 7, &bytes) == ST_SUCCESS);
    smb2_conn_disconnect(holder);
    struct smb2_conn *reconnected = smb2_conn_reopen(&env, holder);
    memset(&durable, 0, sizeof(durable)); durable.dhnc = 1;
    memcpy(durable.file_id, seed.file_id, 16);
    assert(smb2_create_dur(reconnected, "", FILE_OPEN, FILE_ALL_ACCESS,
        FILE_SHARE_RWD, NULL, &durable, &seed) == ST_SUCCESS);
    assert(seed.end_of_file == 7);
    assert(!(g32(reconnected->rbuf + 4 + SMB2_HDR_SIZE, 56) & SMB2_FILE_ATTRIBUTE_DIRECTORY));
    assert_bytes(reconnected, seed.file_id, "durable");
    assert(smb2_close(reconnected, seed.file_id) == ST_SUCCESS);
    smb2_conn_disconnect(reconnected);

    /* Index/default forks and DIRECTORY_FILE do not become ordinary ADS. */
    assert(smb2_create_opts(conn, "directory:new", FILE_OPEN, FILE_READ_ACCESS,
        FILE_SHARE_RWD, FILE_DIRECTORY_FILE, NULL, &rejected) == 0xc0000103u); /* NOT_A_DIRECTORY */
    assert(smb2_create_opts(conn, "directory::$DATA", FILE_OPEN, FILE_READ_ACCESS,
        FILE_SHARE_RWD, 0, NULL, &rejected) == 0xc00000bau); /* FILE_IS_A_DIRECTORY */
    assert(smb2_query_info(conn, SMB2_INFO_FILE_T, SMB2_FILE_STANDARD_INFO_T,
        base.file_id, 0, result, sizeof(result), &result_len) == ST_SUCCESS);
    assert(result_len == 24 && result[21] && g64(result, 0) == 0 && g64(result, 8) == 0);
    assert_bytes(conn, sibling.file_id, "sibling"); assert_bytes(conn, child.file_id, "child");
    assert(smb2_close(conn, sibling.file_id) == ST_SUCCESS);
    assert(smb2_close(conn, child.file_id) == ST_SUCCESS);
    assert(smb2_close(conn, base.file_id) == ST_SUCCESS);
    smb2_env_fs_teardown(&env, "fs0"); smb2_env_stop(&env);
    return 0;
}
