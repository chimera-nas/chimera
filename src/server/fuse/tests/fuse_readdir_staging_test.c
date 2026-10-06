// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only

/* Exercise the production stream/reset/publication callbacks with a real node
 * table. The grant sink records the externally visible effects; all unrelated
 * FUSE handlers are discarded by function-section linking. */
#include <stdio.h>
#include "../fuse_proc_dir.c"

#define CHECK(c) do { if (!(c)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #c); abort(); } } while (0)

static unsigned grants;

int
chimera_fuse_grant_ensure(
    struct chimera_fuse_thread *thread,
    struct chimera_fuse_mount  *mount,
    uint64_t                    nodeid,
    const uint8_t              *fh,
    uint32_t                    fh_len,
    uint64_t                    fh_hash)
{
    (void) thread;
    (void) mount;
    (void) nodeid;
    (void) fh;
    (void) fh_len;
    (void) fh_hash;
    grants++;
    /* HELD at publication does not prove it survived the backend fetch:
    * another request may have broken and rearmed it in the meantime. */
    return CHIMERA_FUSE_COVER_HELD;
} /* chimera_fuse_grant_ensure */

int
main(void)
{
    struct chimera_fuse_mount           mount = {
        .node_table     = chimera_fuse_node_table_create(),
        .coherence_sync = 1,                               .entry_timeout_ms = 60000, .attr_timeout_ms = 60000,
    };
    struct chimera_fuse_channel         channel = { .mount = &mount };
    struct chimera_fuse_readdir_pending pending[2];
    struct chimera_fuse_request         req   = { .channel = &channel, .entry_cover = CHIMERA_FUSE_COVER_HELD };
    struct chimera_vfs_attrs            attrs = {
        .va_set_mask = CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MODE,
        .va_mode     = S_IFREG | 0644,                             .va_fh_len = 1, .va_fh = { 7 },
    };
    uint8_t                             new_fh = 8;
    uint64_t                            existing = chimera_fuse_node_insert(mount.node_table, attrs.va_fh, 1), found;

    req.buf.data          = calloc(1, CHIMERA_FUSE_REPLY_OFF + 4096);
    req.u.readdir.plus    = 1;
    req.u.readdir.size    = 4096;
    req.u.readdir.pending = pending;

    for (unsigned attempt = 0; attempt < 2; attempt++) {
        attrs.va_fh[0] = 7;
        CHECK(chimera_fuse_readdir_entry(NULL, 0, 7, 1, "existing", 8, &attrs, &req) == 0);
        attrs.va_fh[0] = new_fh;
        CHECK(chimera_fuse_readdir_entry(NULL, 0, 8, 2, "new", 3, &attrs, &req) == 0);
        CHECK(grants == 0);
        CHECK(chimera_fuse_node_lookup(mount.node_table, &new_fh, 1, &found) == -1);
        CHECK(((struct fuse_direntplus *) chimera_fuse_reply_space(&req))->entry_out.nodeid == 0);
        /* Reject this fully executed attempt at finish. No shared counts or
         * grants exist to roll back, even for an already interned node. */
        chimera_fuse_readdir_reset(NULL, 0, &req);
        CHECK(req.u.readdir.used == 0 && req.u.readdir.num_pending == 0);
        CHECK(chimera_fuse_node_forget(mount.node_table, existing, 1) == 1);
        uint8_t existing_fh = 7;
        existing = chimera_fuse_node_insert(mount.node_table, &existing_fh, 1);
    }
    attrs.va_fh[0] = 7;
    CHECK(chimera_fuse_readdir_entry(NULL, 0, 7, 1, "existing", 8, &attrs, &req) == 0);
    attrs.va_fh[0] = new_fh;
    CHECK(chimera_fuse_readdir_entry(NULL, 0, 8, 2, "new", 3, &attrs, &req) == 0);
    chimera_fuse_readdirplus_publish(&req);
    CHECK(grants == 2);
    CHECK(chimera_fuse_node_lookup(mount.node_table, &new_fh, 1, &found) == 0);
    CHECK(chimera_fuse_node_forget(mount.node_table, found, 1) == 1);
    CHECK(chimera_fuse_node_forget(mount.node_table, existing, 1) == 0);
    CHECK(chimera_fuse_node_forget(mount.node_table, existing, 1) == 1);
    struct fuse_direntplus *plus = (struct fuse_direntplus *) chimera_fuse_reply_space(&req);
    CHECK(plus->entry_out.nodeid == existing && plus->entry_out.entry_valid == 0);
    CHECK(plus->entry_out.entry_valid_nsec == 0);
    CHECK(plus->entry_out.attr_valid == 0 && plus->entry_out.attr_valid_nsec == 0);
    free(req.buf.data);
    chimera_fuse_node_table_destroy(mount.node_table);
    puts("ok: READDIRPLUS retries keep nodes and grants private until accepted publication");
    return 0;
} /* main */
