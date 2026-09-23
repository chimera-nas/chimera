// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"
#include "nfs4_open_state.h"

#define CHECK(expr) do { \
            if (!(expr)) { \
                fprintf(stderr, "line %d: %s\n", __LINE__, #expr); \
                return 1; \
            } \
} while (0)

/* Exercise server reply order independently of thread scheduling: the server
 * processes CLOSE then OPEN, but the fresh OPEN reply reaches us first. */
int
main(void)
{
    struct chimera_nfs_client_server server  = { 0 };
    uint8_t                          fh[18]  = { 0 };
    struct stateid4                  old_sid = { .seqid = 1, .other = { 1 } };
    struct stateid4                  new_sid = { .seqid = 1, .other = { 2 } };
    struct stateid4                  closing_sid;
    struct chimera_nfs4_open_file   *old_file, *new_file, *alias;

    evpl_mutex_init(&server.open_state_lock, NULL);
    CHECK(chimera_nfs4_open_file_get(&server, fh, sizeof(fh), &old_sid, &old_file) == 0);
    old_file->layout.last_write_offset = 8192;
    CHECK(chimera_nfs4_open_file_put(&server, fh, sizeof(fh), &closing_sid) == 1);
    CHECK(memcmp(closing_sid.other, old_sid.other, sizeof(old_sid.other)) == 0);

    /* An OPEN processed before CLOSE upgraded the doomed state: retry it. */
    old_sid.seqid = 2;
    CHECK(chimera_nfs4_open_file_get(&server, fh, sizeof(fh), &old_sid, &alias) == -1);

    /* A fresh OPEN processed after CLOSE must survive the delayed reply. */
    CHECK(chimera_nfs4_open_file_get(&server, fh, sizeof(fh), &new_sid, &new_file) == 0);
    CHECK(new_file != old_file);
    CHECK(new_file->layout.last_write_offset == 0);
    CHECK(old_file->layout.last_write_offset == 8192);
    new_sid.seqid = 2;
    CHECK(chimera_nfs4_open_file_get(&server, fh, sizeof(fh), &new_sid, &alias) == 0);
    CHECK(alias == new_file);
    CHECK(chimera_nfs4_open_file_get(&server, fh, sizeof(fh), &old_sid, &alias) == -1);
    chimera_nfs4_open_file_close_done(&server, old_file);

    /* Retiring the old entry must leave both new handles tracked. */
    CHECK(server.open_files == new_file);
    CHECK(chimera_nfs4_open_file_put(&server, fh, sizeof(fh), &closing_sid) == 0);
    CHECK(chimera_nfs4_open_file_put(&server, fh, sizeof(fh), &closing_sid) == 1);
    CHECK(memcmp(closing_sid.other, new_sid.other, sizeof(new_sid.other)) == 0);
    chimera_nfs4_open_file_close_done(&server, new_file);
    CHECK(server.open_files == NULL);
    evpl_mutex_destroy(&server.open_state_lock);
    return 0;
} /* main */
