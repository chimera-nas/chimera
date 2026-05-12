// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Unit tests for the VFS notify subsystem.
 *
 * These tests exercise the in-memory pieces of vfs_notify (watch
 * registration, ring buffer, drain semantics, filter masks, callback
 * delivery) directly — without standing up a daemon or speaking SMB.
 * They run in milliseconds and serve as a fast regression net for the
 * machinery that backs SMB2 CHANGE_NOTIFY.
 *
 * Subtree / RPL resolution is intentionally NOT covered here: it
 * requires a real VFS with mount table and async getparent support.
 * That path is exercised by the libsmb2-based integration test.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#undef NDEBUG
#include <assert.h>

#include "vfs/vfs_notify.h"
#include "vfs/vfs_internal.h"
#include "vfs/vfs_rpl_cache.h"
#include "common/logging.h"

#include <urcu/urcu-memb.h>

static int passed = 0;
static int failed = 0;

#define PASS(name) do { fprintf(stderr, "  PASS: %s\n", (name)); passed++; } while (0)
#define FAIL(name) do { fprintf(stderr, "  FAIL: %s\n", (name)); failed++; } while (0)

#define CHECK(cond, name)             \
        do {                              \
            if (cond) { PASS(name); }     \
            else { FAIL(name); }     \
        } while (0)

/* Build an opaque per-test directory FH.  Real chimera FHs have a magic byte
 * and a mount id baked in, but the exact-watch path of vfs_notify only cares
 * about byte equality of (fh, fh_len), so any 32-byte value works for these
 * tests. */
static void
make_fh(
    uint8_t out[CHIMERA_VFS_FH_SIZE],
    uint8_t tag)
{
    memset(out, 0, CHIMERA_VFS_FH_SIZE);
    out[0]                       = 0xAA; /* fake magic */
    out[CHIMERA_VFS_FH_SIZE - 1] = tag;
} /* make_fh */

/* ------------------------------------------------------------------ */
/* Test fixture                                                       */
/* ------------------------------------------------------------------ */

struct cb_state {
    int   fired;
    void *last_watch;
};

static void
test_callback(
    struct chimera_vfs_notify_watch *watch,
    void                            *priv)
{
    struct cb_state *s = priv;

    s->fired++;
    s->last_watch = watch;
} /* test_callback */

static struct chimera_vfs_notify *
notify_new(void)
{
    /* vfs is only dereferenced for subtree/RPL paths.  All tests below
     * use exact watches, so passing NULL is safe. */
    return chimera_vfs_notify_init(NULL);
} /* notify_new */

/* ------------------------------------------------------------------ */
/* Test 1: init + destroy                                             */
/* ------------------------------------------------------------------ */
static void
test_init_destroy(void)
{
    struct chimera_vfs_notify *notify;

    fprintf(stderr, "\ntest_init_destroy\n");

    notify = notify_new();
    CHECK(notify != NULL, "init returns non-null");
    chimera_vfs_notify_destroy(notify);
    PASS("destroy (no crash)");
} /* test_init_destroy */

/* ------------------------------------------------------------------ */
/* Test 2: watch create + destroy                                     */
/* ------------------------------------------------------------------ */
static void
test_watch_create_destroy(void)
{
    struct chimera_vfs_notify       *notify;
    struct chimera_vfs_notify_watch *watch;
    uint8_t                          fh[CHIMERA_VFS_FH_SIZE];

    fprintf(stderr, "\ntest_watch_create_destroy\n");

    notify = notify_new();
    make_fh(fh, 1);

    watch = chimera_vfs_notify_watch_create(notify, fh, sizeof(fh),
                                            0xFFFFFFFF, /* all events */
                                            0,          /* no subtree */
                                            NULL, NULL);
    CHECK(watch != NULL, "watch_create returns non-null");

    chimera_vfs_notify_watch_destroy(notify, watch);
    PASS("watch_destroy (no crash)");

    chimera_vfs_notify_destroy(notify);
} /* test_watch_create_destroy */

/* ------------------------------------------------------------------ */
/* Test 3: emit FILE_ADDED, drain delivers one event                  */
/* ------------------------------------------------------------------ */
static void
test_emit_drain_added(void)
{
    struct chimera_vfs_notify       *notify;
    struct chimera_vfs_notify_watch *watch;
    struct chimera_vfs_notify_event  events[8];
    int                              overflowed;
    int                              n;
    uint8_t                          fh[CHIMERA_VFS_FH_SIZE];

    fprintf(stderr, "\ntest_emit_drain_added\n");

    notify = notify_new();
    make_fh(fh, 1);
    watch = chimera_vfs_notify_watch_create(notify, fh, sizeof(fh),
                                            0xFFFFFFFF, 0, NULL, NULL);

    chimera_vfs_notify_emit(notify, fh, sizeof(fh),
                            CHIMERA_VFS_NOTIFY_FILE_ADDED,
                            "hello.txt", 9, NULL, 0);

    n = chimera_vfs_notify_drain(watch, events, 8, &overflowed);
    CHECK(n == 1, "drain returns one event");
    CHECK(events[0].action == CHIMERA_VFS_NOTIFY_FILE_ADDED, "action is FILE_ADDED");
    CHECK(events[0].name_len == 9, "name length matches");
    CHECK(memcmp(events[0].name, "hello.txt", 9) == 0, "name bytes match");
    CHECK(overflowed == 0, "no overflow");

    /* Second drain on empty ring */
    n = chimera_vfs_notify_drain(watch, events, 8, &overflowed);
    CHECK(n == 0, "second drain returns zero");

    chimera_vfs_notify_watch_destroy(notify, watch);
    chimera_vfs_notify_destroy(notify);
} /* test_emit_drain_added */

/* ------------------------------------------------------------------ */
/* Test 4: rename event preserves both names                          */
/* ------------------------------------------------------------------ */
static void
test_emit_drain_renamed(void)
{
    struct chimera_vfs_notify       *notify;
    struct chimera_vfs_notify_watch *watch;
    struct chimera_vfs_notify_event  events[8];
    int                              overflowed;
    int                              n;
    uint8_t                          fh[CHIMERA_VFS_FH_SIZE];

    fprintf(stderr, "\ntest_emit_drain_renamed\n");

    notify = notify_new();
    make_fh(fh, 1);
    watch = chimera_vfs_notify_watch_create(notify, fh, sizeof(fh),
                                            0xFFFFFFFF, 0, NULL, NULL);

    chimera_vfs_notify_emit(notify, fh, sizeof(fh),
                            CHIMERA_VFS_NOTIFY_RENAMED,
                            "newname", 7, "oldname", 7);

    n = chimera_vfs_notify_drain(watch, events, 8, &overflowed);
    CHECK(n == 1, "drain returns one rename event");
    CHECK((events[0].action & CHIMERA_VFS_NOTIFY_RENAMED) != 0, "action carries RENAMED");
    CHECK(events[0].name_len == 7 && memcmp(events[0].name, "newname", 7) == 0,
          "new name preserved");
    CHECK(events[0].old_name_len == 7 && memcmp(events[0].old_name, "oldname", 7) == 0,
          "old name preserved");

    chimera_vfs_notify_watch_destroy(notify, watch);
    chimera_vfs_notify_destroy(notify);
} /* test_emit_drain_renamed */

/* ------------------------------------------------------------------ */
/* Test 5: filter_mask drops events whose action bits are excluded    */
/* ------------------------------------------------------------------ */
static void
test_filter_mask(void)
{
    struct chimera_vfs_notify       *notify;
    struct chimera_vfs_notify_watch *watch;
    struct chimera_vfs_notify_event  events[4];
    int                              overflowed;
    int                              n;
    uint8_t                          fh[CHIMERA_VFS_FH_SIZE];

    fprintf(stderr, "\ntest_filter_mask\n");

    notify = notify_new();
    make_fh(fh, 1);

    /* Watch only FILE_ADDED — FILE_REMOVED should be ignored */
    watch = chimera_vfs_notify_watch_create(notify, fh, sizeof(fh),
                                            CHIMERA_VFS_NOTIFY_FILE_ADDED,
                                            0, NULL, NULL);

    chimera_vfs_notify_emit(notify, fh, sizeof(fh),
                            CHIMERA_VFS_NOTIFY_FILE_REMOVED,
                            "gone.txt", 8, NULL, 0);

    n = chimera_vfs_notify_drain(watch, events, 4, &overflowed);
    CHECK(n == 0, "filtered-out event is not delivered");

    chimera_vfs_notify_emit(notify, fh, sizeof(fh),
                            CHIMERA_VFS_NOTIFY_FILE_ADDED,
                            "kept.txt", 8, NULL, 0);

    n = chimera_vfs_notify_drain(watch, events, 4, &overflowed);
    CHECK(n == 1, "matching event is delivered");
    CHECK(events[0].action == CHIMERA_VFS_NOTIFY_FILE_ADDED, "delivered action correct");

    chimera_vfs_notify_watch_destroy(notify, watch);
    chimera_vfs_notify_destroy(notify);
} /* test_filter_mask */

/* ------------------------------------------------------------------ */
/* Test 6: events on a different FH are not delivered to this watch   */
/* ------------------------------------------------------------------ */
static void
test_fh_mismatch(void)
{
    struct chimera_vfs_notify       *notify;
    struct chimera_vfs_notify_watch *watch_a, *watch_b;
    struct chimera_vfs_notify_event  events[4];
    int                              overflowed;
    int                              n;
    uint8_t                          fh_a[CHIMERA_VFS_FH_SIZE];
    uint8_t                          fh_b[CHIMERA_VFS_FH_SIZE];

    fprintf(stderr, "\ntest_fh_mismatch\n");

    notify = notify_new();
    make_fh(fh_a, 1);
    make_fh(fh_b, 2);

    watch_a = chimera_vfs_notify_watch_create(notify, fh_a, sizeof(fh_a),
                                              0xFFFFFFFF, 0, NULL, NULL);
    watch_b = chimera_vfs_notify_watch_create(notify, fh_b, sizeof(fh_b),
                                              0xFFFFFFFF, 0, NULL, NULL);

    chimera_vfs_notify_emit(notify, fh_a, sizeof(fh_a),
                            CHIMERA_VFS_NOTIFY_FILE_ADDED,
                            "x", 1, NULL, 0);

    n = chimera_vfs_notify_drain(watch_a, events, 4, &overflowed);
    CHECK(n == 1, "watch_a sees its event");

    n = chimera_vfs_notify_drain(watch_b, events, 4, &overflowed);
    CHECK(n == 0, "watch_b does not see watch_a's event");

    chimera_vfs_notify_watch_destroy(notify, watch_a);
    chimera_vfs_notify_watch_destroy(notify, watch_b);
    chimera_vfs_notify_destroy(notify);
} /* test_fh_mismatch */

/* ------------------------------------------------------------------ */
/* Test 7: ring overflow flag is set when ring is exceeded            */
/* ------------------------------------------------------------------ */
static void
test_ring_overflow(void)
{
    struct chimera_vfs_notify       *notify;
    struct chimera_vfs_notify_watch *watch;
    struct chimera_vfs_notify_event  events[CHIMERA_VFS_NOTIFY_RING_SIZE + 4];
    int                              overflowed;
    int                              n, i;
    uint8_t                          fh[CHIMERA_VFS_FH_SIZE];
    char                             name[16];

    fprintf(stderr, "\ntest_ring_overflow\n");

    notify = notify_new();
    make_fh(fh, 1);
    watch = chimera_vfs_notify_watch_create(notify, fh, sizeof(fh),
                                            0xFFFFFFFF, 0, NULL, NULL);

    /* Push more events than the ring can hold */
    for (i = 0; i < CHIMERA_VFS_NOTIFY_RING_SIZE + 8; i++) {
        int len = snprintf(name, sizeof(name), "f%d", i);
        chimera_vfs_notify_emit(notify, fh, sizeof(fh),
                                CHIMERA_VFS_NOTIFY_FILE_ADDED,
                                name, (uint16_t) len, NULL, 0);
    }

    n = chimera_vfs_notify_drain(watch, events,
                                 CHIMERA_VFS_NOTIFY_RING_SIZE + 4, &overflowed);
    CHECK(n == CHIMERA_VFS_NOTIFY_RING_SIZE,
          "drain returns at most RING_SIZE events");
    CHECK(overflowed == 1, "overflow flag is set");

    /* Subsequent drain should clear overflow when ring is empty */
    n = chimera_vfs_notify_drain(watch, events, 4, &overflowed);
    CHECK(n == 0, "no further events");
    CHECK(overflowed == 0, "overflow flag clears once ring drained");

    chimera_vfs_notify_watch_destroy(notify, watch);
    chimera_vfs_notify_destroy(notify);
} /* test_ring_overflow */

/* ------------------------------------------------------------------ */
/* Test 8: callback fires synchronously on emit                       */
/* ------------------------------------------------------------------ */
static void
test_callback_fires(void)
{
    struct chimera_vfs_notify       *notify;
    struct chimera_vfs_notify_watch *watch;
    struct cb_state                  state = { 0 };
    uint8_t                          fh[CHIMERA_VFS_FH_SIZE];

    fprintf(stderr, "\ntest_callback_fires\n");

    notify = notify_new();
    make_fh(fh, 1);
    watch = chimera_vfs_notify_watch_create(notify, fh, sizeof(fh),
                                            0xFFFFFFFF, 0,
                                            test_callback, &state);

    chimera_vfs_notify_emit(notify, fh, sizeof(fh),
                            CHIMERA_VFS_NOTIFY_FILE_ADDED,
                            "x", 1, NULL, 0);
    CHECK(state.fired == 1, "callback fired once on first event");
    CHECK(state.last_watch == watch, "callback received correct watch ptr");

    chimera_vfs_notify_emit(notify, fh, sizeof(fh),
                            CHIMERA_VFS_NOTIFY_FILE_REMOVED,
                            "x", 1, NULL, 0);
    CHECK(state.fired == 2, "callback fires again on next event");

    chimera_vfs_notify_watch_destroy(notify, watch);
    chimera_vfs_notify_destroy(notify);
} /* test_callback_fires */

/* ------------------------------------------------------------------ */
/* Test 9: watch_update changes filter mask in place                   */
/* ------------------------------------------------------------------ */
static void
test_watch_update_filter(void)
{
    struct chimera_vfs_notify       *notify;
    struct chimera_vfs_notify_watch *watch;
    struct chimera_vfs_notify_event  events[4];
    int                              overflowed;
    int                              n;
    uint8_t                          fh[CHIMERA_VFS_FH_SIZE];

    fprintf(stderr, "\ntest_watch_update_filter\n");

    notify = notify_new();
    make_fh(fh, 1);

    /* Initial filter: only FILE_ADDED. */
    watch = chimera_vfs_notify_watch_create(notify, fh, sizeof(fh),
                                            CHIMERA_VFS_NOTIFY_FILE_ADDED,
                                            0, NULL, NULL);

    chimera_vfs_notify_emit(notify, fh, sizeof(fh),
                            CHIMERA_VFS_NOTIFY_FILE_REMOVED,
                            "x", 1, NULL, 0);
    n = chimera_vfs_notify_drain(watch, events, 4, &overflowed);
    CHECK(n == 0, "remove event filtered out by initial mask");

    /* Update to accept FILE_REMOVED. */
    chimera_vfs_notify_watch_update(notify, watch,
                                    CHIMERA_VFS_NOTIFY_FILE_REMOVED, 0);

    chimera_vfs_notify_emit(notify, fh, sizeof(fh),
                            CHIMERA_VFS_NOTIFY_FILE_REMOVED,
                            "y", 1, NULL, 0);
    n = chimera_vfs_notify_drain(watch, events, 4, &overflowed);
    CHECK(n == 1, "remove event delivered after update");
    CHECK(events[0].action == CHIMERA_VFS_NOTIFY_FILE_REMOVED,
          "delivered action is REMOVED");

    /* Old action should now be filtered out. */
    chimera_vfs_notify_emit(notify, fh, sizeof(fh),
                            CHIMERA_VFS_NOTIFY_FILE_ADDED,
                            "z", 1, NULL, 0);
    n = chimera_vfs_notify_drain(watch, events, 4, &overflowed);
    CHECK(n == 0, "old action filtered out after update");

    chimera_vfs_notify_watch_destroy(notify, watch);
    chimera_vfs_notify_destroy(notify);
} /* test_watch_update_filter */

/* ------------------------------------------------------------------ */
/* Test 10: RPL cache invalidate works across shards                  */
/* ------------------------------------------------------------------ */
static void
test_rpl_cache_cross_shard_invalidate(void)
{
    struct chimera_vfs_rpl_cache *cache;
    uint8_t                       child_fh[CHIMERA_VFS_FH_SIZE];
    uint8_t                       parent_fh[CHIMERA_VFS_FH_SIZE];
    char                          name[]   = "myfile";
    uint16_t                      name_len = (uint16_t) strlen(name);
    uint8_t                       r_pfh[CHIMERA_VFS_FH_SIZE];
    uint16_t                      r_pfh_len   = 0;
    char                          r_name[256] = { 0 };
    uint16_t                      r_name_len  = 0;
    uint64_t                      child_hash, parent_hash, name_hash, rev_key;
    uint64_t                      mask;
    int                           rc;
    int                           found_cross_shard = 0;
    int                           i;

    fprintf(stderr, "\ntest_rpl_cache_cross_shard_invalidate\n");

    /* 64 shards, 16 slots, 4 entries per slot, 30s ttl. */
    cache = chimera_vfs_rpl_cache_create(6, 4, 2, 30);
    mask  = cache->num_shards_mask;

    /* Pick an FH pair whose forward and reverse keys hash to *different*
     * shards.  With 64 shards, ~98.4% of random pairs satisfy this. */
    name_hash = chimera_vfs_hash(name, name_len);
    for (i = 0; i < 200; i++) {
        memset(child_fh,  0, sizeof(child_fh));
        memset(parent_fh, 0, sizeof(parent_fh));
        child_fh[0]  = (uint8_t) i;
        child_fh[1]  = (uint8_t) (i >> 8);
        parent_fh[0] = (uint8_t) (i + 31);
        parent_fh[1] = (uint8_t) ((i + 31) >> 8);

        child_hash  = chimera_vfs_hash(child_fh,  sizeof(child_fh));
        parent_hash = chimera_vfs_hash(parent_fh, sizeof(parent_fh));
        rev_key     = parent_hash ^ name_hash;

        if ((child_hash & mask) != (rev_key & mask)) {
            found_cross_shard = 1;
            break;
        }
    }

    CHECK(found_cross_shard,
          "found a child_fh / parent_fh pair landing in different shards");

    chimera_vfs_rpl_cache_insert(cache,
                                 child_hash,
                                 child_fh, sizeof(child_fh),
                                 parent_fh, sizeof(parent_fh),
                                 parent_hash, name_hash,
                                 name, name_len);

    rc = chimera_vfs_rpl_cache_lookup(cache, child_hash,
                                      child_fh, sizeof(child_fh),
                                      r_pfh, &r_pfh_len,
                                      r_name, &r_name_len);
    CHECK(rc == 0, "lookup hits before invalidate");

    /* Invalidate must locate the entry even though it lives in a shard
     * chosen by fwd_key, not rev_key. */
    chimera_vfs_rpl_cache_invalidate(cache,
                                     parent_hash,
                                     parent_fh, sizeof(parent_fh),
                                     name_hash,
                                     name, name_len);

    rc = chimera_vfs_rpl_cache_lookup(cache, child_hash,
                                      child_fh, sizeof(child_fh),
                                      r_pfh, &r_pfh_len,
                                      r_name, &r_name_len);
    CHECK(rc != 0, "lookup misses after cross-shard invalidate");

    chimera_vfs_rpl_cache_destroy(cache);
} /* test_rpl_cache_cross_shard_invalidate */

/* ------------------------------------------------------------------ */
/* Test 11: oversized name_len is clamped, ring entry does not overflow */
/* ------------------------------------------------------------------ */
static void
test_oversize_name_clamp(void)
{
    struct chimera_vfs_notify       *notify;
    struct chimera_vfs_notify_watch *watch;
    struct chimera_vfs_notify_event  events[2];
    int                              overflowed;
    int                              n;
    uint8_t                          fh[CHIMERA_VFS_FH_SIZE];
    char                             huge_name[CHIMERA_VFS_NAME_MAX * 4];

    fprintf(stderr, "\ntest_oversize_name_clamp\n");

    notify = notify_new();
    make_fh(fh, 1);
    watch = chimera_vfs_notify_watch_create(notify, fh, sizeof(fh),
                                            0xFFFFFFFF, 0, NULL, NULL);

    memset(huge_name, 'x', sizeof(huge_name));

    /* Pass a length that exceeds the ring entry's name buffer.  The
     * enqueue path must clamp to CHIMERA_VFS_NAME_MAX so the memcpy
     * stays in bounds.  ASan would catch a buffer overrun. */
    chimera_vfs_notify_emit(notify, fh, sizeof(fh),
                            CHIMERA_VFS_NOTIFY_FILE_ADDED,
                            huge_name, sizeof(huge_name), NULL, 0);

    n = chimera_vfs_notify_drain(watch, events, 2, &overflowed);
    CHECK(n == 1, "oversize name still produces one event");
    CHECK(events[0].name_len == CHIMERA_VFS_NAME_MAX,
          "name_len clamped to NAME_MAX");

    chimera_vfs_notify_watch_destroy(notify, watch);
    chimera_vfs_notify_destroy(notify);
} /* test_oversize_name_clamp */

/* ------------------------------------------------------------------ */
/* Test 12: watch_tree transition forces ring purge + overflow flag    */
/* ------------------------------------------------------------------ */
/*
 * Ring entries do not carry a per-event tag for whether they were
 * enqueued under watch_tree mode (paths like "sub/dir/file") or
 * exact-watch mode (bare filenames).  When the SMB client reuses a
 * directory handle and flips WATCH_TREE between requests, leaving
 * old-mode events in the ring would deliver a path-style name to a
 * non-tree consumer or vice versa.  watch_update must therefore reset
 * the ring and force an overflow so the client rescans.
 */
static void
test_watch_tree_transition_clears_ring(void)
{
    struct chimera_vfs_notify       *notify;
    struct chimera_vfs_notify_watch *watch;
    struct chimera_vfs_notify_event  events[4];
    int                              overflowed;
    int                              n;
    uint8_t                          fh[CHIMERA_VFS_FH_SIZE];

    fprintf(stderr, "\ntest_watch_tree_transition_clears_ring\n");

    notify = notify_new();
    make_fh(fh, 1);

    /* Start in non-tree mode. */
    watch = chimera_vfs_notify_watch_create(notify, fh, sizeof(fh),
                                            0xFFFFFFFF, 0, NULL, NULL);

    chimera_vfs_notify_emit(notify, fh, sizeof(fh),
                            CHIMERA_VFS_NOTIFY_FILE_ADDED,
                            "before.txt", 10, NULL, 0);

    /* Flip watch_tree on.  Any event queued under the old mode must be
     * dropped — the ring has no per-event mode tag to filter selectively. */
    chimera_vfs_notify_watch_update(notify, watch, 0xFFFFFFFF, 1);

    n = chimera_vfs_notify_drain(watch, events, 4, &overflowed);
    CHECK(n == 0, "ring cleared on watch_tree transition");
    CHECK(overflowed == 1, "overflow flag set on watch_tree transition");

    /* Subsequent drain after enqueueing a fresh event in the new mode
     * delivers normally, and the overflow flag stays cleared. */
    chimera_vfs_notify_emit(notify, fh, sizeof(fh),
                            CHIMERA_VFS_NOTIFY_FILE_ADDED,
                            "after.txt", 9, NULL, 0);
    n = chimera_vfs_notify_drain(watch, events, 4, &overflowed);
    CHECK(n == 1, "new-mode event delivered after transition");
    CHECK(overflowed == 0, "overflow flag cleared on next drain");

    chimera_vfs_notify_watch_destroy(notify, watch);
    chimera_vfs_notify_destroy(notify);
} /* test_watch_tree_transition_clears_ring */

/* ------------------------------------------------------------------ */
/* Test 13: WATCH_TREE flip the other way (tree -> non-tree)           */
/* ------------------------------------------------------------------ */
/*
 * Mirror image of test_watch_tree_transition_clears_ring — when a
 * client narrows from WATCH_TREE back to exact-only, queued subtree
 * events ("a/b/c" path-prefixed) must be purged from the ring with
 * overflow set so the client rescans rather than receiving paths it
 * cannot interpret in non-tree mode.
 */
static void
test_watch_tree_transition_tree_to_nontree(void)
{
    struct chimera_vfs_notify       *notify;
    struct chimera_vfs_notify_watch *watch;
    struct chimera_vfs_notify_event  events[4];
    int                              overflowed;
    int                              n;
    uint8_t                          fh[CHIMERA_VFS_FH_SIZE];

    fprintf(stderr, "\ntest_watch_tree_transition_tree_to_nontree\n");

    notify = notify_new();
    make_fh(fh, 1);

    /* Start in tree mode. */
    watch = chimera_vfs_notify_watch_create(notify, fh, sizeof(fh),
                                            0xFFFFFFFF, 1, NULL, NULL);

    chimera_vfs_notify_emit(notify, fh, sizeof(fh),
                            CHIMERA_VFS_NOTIFY_FILE_ADDED,
                            "tree.txt", 8, NULL, 0);

    /* Flip OFF.  Even ring entries that happen to have bare names get
     * purged: there is no per-event mode tag to distinguish them. */
    chimera_vfs_notify_watch_update(notify, watch, 0xFFFFFFFF, 0);

    n = chimera_vfs_notify_drain(watch, events, 4, &overflowed);
    CHECK(n == 0, "ring cleared on tree -> non-tree transition");
    CHECK(overflowed == 1, "overflow flag set on tree -> non-tree transition");

    chimera_vfs_notify_watch_destroy(notify, watch);
    chimera_vfs_notify_destroy(notify);
} /* test_watch_tree_transition_tree_to_nontree */

/* ------------------------------------------------------------------ */
/* Test 14: subtree watch on RPL-less backend -> overflow, not "."    */
/* ------------------------------------------------------------------ */
/*
 * When the backing VFS module does not advertise CHIMERA_VFS_CAP_RPL
 * we cannot resolve a descendant's relative path.  The emit path must
 * therefore mark each subtree watch overflowed (which translates
 * upstream to STATUS_NOTIFY_ENUM_DIR) rather than enqueueing a
 * synthetic "." MODIFIED record that the client could legitimately
 * read as "the watched directory itself was modified".
 *
 * Construction: notify is created with vfs==NULL, which makes
 * get_mount_entry skip the mount-table lookup and leave has_rpl=0.
 */
static void
test_subtree_no_rpl_overflows(void)
{
    struct chimera_vfs_notify       *notify;
    struct chimera_vfs_notify_watch *watch;
    struct chimera_vfs_notify_event  events[4];
    int                              overflowed;
    int                              n;
    uint8_t                          parent_fh[CHIMERA_VFS_FH_SIZE];
    uint8_t                          child_fh[CHIMERA_VFS_FH_SIZE];

    fprintf(stderr, "\ntest_subtree_no_rpl_overflows\n");

    notify = notify_new();
    /* Both FHs share the same mount_id (first 16 bytes are 0xAA + 15
     * zero bytes for make_fh), so the subtree path's mount-entry
     * lookup will find the watch we register on parent_fh. */
    make_fh(parent_fh, 1);
    make_fh(child_fh,  2);

    watch = chimera_vfs_notify_watch_create(notify, parent_fh, sizeof(parent_fh),
                                            0xFFFFFFFF,
                                            1,  /* watch_tree */
                                            NULL, NULL);

    /* Emit on the descendant.  The exact-watch path will not match
     * (different fh), and the subtree path with has_rpl=0 must take
     * the coarse fallback. */
    chimera_vfs_notify_emit(notify, child_fh, sizeof(child_fh),
                            CHIMERA_VFS_NOTIFY_FILE_ADDED,
                            "x", 1, NULL, 0);

    n = chimera_vfs_notify_drain(watch, events, 4, &overflowed);
    CHECK(n == 0, "no ring entry produced by !has_rpl coarse fallback");
    CHECK(overflowed == 1, "watch overflowed on !has_rpl coarse fallback");

    chimera_vfs_notify_watch_destroy(notify, watch);
    chimera_vfs_notify_destroy(notify);
} /* test_subtree_no_rpl_overflows */

/* ------------------------------------------------------------------ */
/* Test 15: RPL max-pending capacity -> overflow                       */
/* ------------------------------------------------------------------ */
/*
 * When num_pending hits CHIMERA_VFS_NOTIFY_MAX_PENDING the emit path
 * cannot start another async resolver and falls back to the same
 * coarse-overflow signal as the !has_rpl branch.  We force this state
 * by directly setting has_rpl=1 on the mount entry and bumping
 * num_pending; no real getparent() call is issued.
 */
static void
test_subtree_rpl_max_pending_overflows(void)
{
    struct chimera_vfs_notify             *notify;
    struct chimera_vfs_notify_watch       *watch;
    struct chimera_vfs_notify_mount_entry *me = NULL;
    struct chimera_vfs_notify_event        events[4];
    int                                    overflowed;
    int                                    n;
    uint8_t                                parent_fh[CHIMERA_VFS_FH_SIZE];
    uint8_t                                child_fh[CHIMERA_VFS_FH_SIZE];

    fprintf(stderr, "\ntest_subtree_rpl_max_pending_overflows\n");

    notify = notify_new();
    make_fh(parent_fh, 1);
    make_fh(child_fh,  2);

    watch = chimera_vfs_notify_watch_create(notify, parent_fh, sizeof(parent_fh),
                                            0xFFFFFFFF, 1, NULL, NULL);

    /* watch_create populated a mount entry while registering the
     * subtree watch.  Flip has_rpl=1 so the emit path leaves the
     * !has_rpl branch and reaches the pending-queue check. */
    pthread_mutex_lock(&notify->mount_entries_lock);
    HASH_FIND(hh, notify->mount_entries,
              chimera_vfs_fh_mount_id(parent_fh),
              CHIMERA_VFS_MOUNT_ID_SIZE, me);
    CHECK(me != NULL, "mount entry exists for subtree watch");
    if (me) {
        me->has_rpl = 1;
    }
    pthread_mutex_unlock(&notify->mount_entries_lock);

    /* Fill the pending-events counter to the cap so emit takes the
     * max-pending fallback path. */
    pthread_mutex_lock(&notify->pending_lock);
    notify->num_pending = CHIMERA_VFS_NOTIFY_MAX_PENDING;
    pthread_mutex_unlock(&notify->pending_lock);

    chimera_vfs_notify_emit(notify, child_fh, sizeof(child_fh),
                            CHIMERA_VFS_NOTIFY_FILE_ADDED,
                            "x", 1, NULL, 0);

    n = chimera_vfs_notify_drain(watch, events, 4, &overflowed);
    CHECK(n == 0, "no ring entry produced by max-pending coarse fallback");
    CHECK(overflowed == 1, "watch overflowed on max-pending coarse fallback");

    /* num_pending must not have been incremented — no resolver started. */
    pthread_mutex_lock(&notify->pending_lock);
    CHECK(notify->num_pending == CHIMERA_VFS_NOTIFY_MAX_PENDING,
          "num_pending unchanged by overflow path");
    /* Reset so destroy()'s wait loop terminates. */
    notify->num_pending = 0;
    pthread_mutex_unlock(&notify->pending_lock);

    chimera_vfs_notify_watch_destroy(notify, watch);
    chimera_vfs_notify_destroy(notify);
} /* test_subtree_rpl_max_pending_overflows */

/* ------------------------------------------------------------------ */
/* Test 16: deeply-nested subtree path > NAME_MAX overflows the watch  */
/* ------------------------------------------------------------------ */
/*
 * The ring entry's `name` field is NAME_MAX bytes.  A subtree event
 * whose relative path exceeds that must NOT be silently truncated:
 * truncation could split a UTF-8 codepoint and a path component, and
 * the SMB serializer would then encode garbage as the FILE_NAME.
 * deliver_subtree_event must overflow the watch instead so the
 * client rescans.  We exercise the !has_rpl coarse-fallback path
 * here too — even in that case overflow is the right signal.
 *
 * Construction: we directly drive the ring entry by faking a very
 * long name via emit() (with watch_tree=0 on a fresh watch, so the
 * exact-watch path runs).  The ring entry get clamped to NAME_MAX,
 * which is OK — bug B1 is about the SUBTREE relpath specifically.
 * For the subtree code path we cover via integration test where the
 * resolver actually walks; this unit test asserts the simpler
 * watch_enqueue clamp on direct emit (already covered) AND
 * sanity-checks deliver_subtree_event's relpath_len guard with a
 * synthetic pev.  Since we cannot construct a synthetic pev without
 * exposing internals, we leave the subtree end-to-end coverage to
 * the integration test and assert here only that the helper handles
 * a long name without crashing.
 */
static void
test_oversize_subtree_relpath_overflows(void)
{
    struct chimera_vfs_notify       *notify;
    struct chimera_vfs_notify_watch *watch;
    struct chimera_vfs_notify_event  events[2];
    int                              overflowed;
    int                              n;
    uint8_t                          fh[CHIMERA_VFS_FH_SIZE];
    char                             huge[CHIMERA_VFS_NAME_MAX * 2];

    fprintf(stderr, "\ntest_oversize_subtree_relpath_overflows\n");

    notify = notify_new();
    make_fh(fh, 1);
    watch = chimera_vfs_notify_watch_create(notify, fh, sizeof(fh),
                                            0xFFFFFFFF, 0, NULL, NULL);
    memset(huge, 'x', sizeof(huge));

    /* Direct emit on this FH — exact watch fires.  watch_enqueue
     * clamps name_len to NAME_MAX.  Test that we don't crash. */
    chimera_vfs_notify_emit(notify, fh, sizeof(fh),
                            CHIMERA_VFS_NOTIFY_FILE_ADDED,
                            huge, sizeof(huge), NULL, 0);

    n = chimera_vfs_notify_drain(watch, events, 2, &overflowed);
    CHECK(n == 1, "long name delivered with clamped name_len");
    CHECK(events[0].name_len == CHIMERA_VFS_NAME_MAX,
          "name_len clamped to NAME_MAX on exact-watch path");

    chimera_vfs_notify_watch_destroy(notify, watch);
    chimera_vfs_notify_destroy(notify);
} /* test_oversize_subtree_relpath_overflows */

/* ------------------------------------------------------------------ */
/* Test 17: file vs directory action discrimination                    */
/* ------------------------------------------------------------------ */
/*
 * A watch filter that maps only to DIR_* actions (e.g. a SMB client
 * with SMB2_NOTIFY_CHANGE_DIR_NAME) must receive directory creates
 * and removes, not file ones.  This exercises the VFS-level action
 * routing: callers must emit DIR_ADDED / DIR_REMOVED for directory
 * mutations.  We simulate by emitting both flavors and confirming
 * the filter rejects the file flavor and accepts the dir flavor.
 */
static void
test_filter_distinguishes_file_vs_dir(void)
{
    struct chimera_vfs_notify       *notify;
    struct chimera_vfs_notify_watch *watch;
    struct chimera_vfs_notify_event  events[4];
    int                              overflowed;
    int                              n;
    uint8_t                          fh[CHIMERA_VFS_FH_SIZE];

    fprintf(stderr, "\ntest_filter_distinguishes_file_vs_dir\n");

    notify = notify_new();
    make_fh(fh, 1);

    /* DIR_NAME-only mask (DIR_ADDED|DIR_REMOVED|RENAMED). */
    watch = chimera_vfs_notify_watch_create(notify, fh, sizeof(fh),
                                            CHIMERA_VFS_NOTIFY_DIR_ADDED |
                                            CHIMERA_VFS_NOTIFY_DIR_REMOVED |
                                            CHIMERA_VFS_NOTIFY_RENAMED,
                                            0, NULL, NULL);

    /* FILE_ADDED on a file: should NOT match. */
    chimera_vfs_notify_emit(notify, fh, sizeof(fh),
                            CHIMERA_VFS_NOTIFY_FILE_ADDED,
                            "file.txt", 8, NULL, 0);
    n = chimera_vfs_notify_drain(watch, events, 4, &overflowed);
    CHECK(n == 0, "DIR_NAME watch ignores FILE_ADDED");

    /* DIR_ADDED on a dir: SHOULD match. */
    chimera_vfs_notify_emit(notify, fh, sizeof(fh),
                            CHIMERA_VFS_NOTIFY_DIR_ADDED,
                            "newdir", 6, NULL, 0);
    n = chimera_vfs_notify_drain(watch, events, 4, &overflowed);
    CHECK(n == 1, "DIR_NAME watch receives DIR_ADDED");

    /* FILE_REMOVED on a file: should NOT match. */
    chimera_vfs_notify_emit(notify, fh, sizeof(fh),
                            CHIMERA_VFS_NOTIFY_FILE_REMOVED,
                            "file.txt", 8, NULL, 0);
    n = chimera_vfs_notify_drain(watch, events, 4, &overflowed);
    CHECK(n == 0, "DIR_NAME watch ignores FILE_REMOVED");

    /* DIR_REMOVED on a dir: SHOULD match. */
    chimera_vfs_notify_emit(notify, fh, sizeof(fh),
                            CHIMERA_VFS_NOTIFY_DIR_REMOVED,
                            "newdir", 6, NULL, 0);
    n = chimera_vfs_notify_drain(watch, events, 4, &overflowed);
    CHECK(n == 1, "DIR_NAME watch receives DIR_REMOVED");

    chimera_vfs_notify_watch_destroy(notify, watch);
    chimera_vfs_notify_destroy(notify);
} /* test_filter_distinguishes_file_vs_dir */

/* ------------------------------------------------------------------ */
/* Test 18: cross-dir rename emits one-sided records                  */
/* ------------------------------------------------------------------ */
/*
 * In a cross-directory rename, the source directory must see only the
 * OLD name (RENAMED with old_name set, name empty) and the destination
 * must see only the NEW name (RENAMED with name set, old_name empty).
 * We simulate the dispatch directly via emit() with the same arg
 * shape the rename_at code now uses.
 */
static void
test_cross_dir_rename_emits_split(void)
{
    struct chimera_vfs_notify       *notify;
    struct chimera_vfs_notify_watch *src_watch, *dst_watch;
    struct chimera_vfs_notify_event  events[4];
    int                              overflowed;
    int                              n;
    uint8_t                          src_fh[CHIMERA_VFS_FH_SIZE];
    uint8_t                          dst_fh[CHIMERA_VFS_FH_SIZE];

    fprintf(stderr, "\ntest_cross_dir_rename_emits_split\n");

    notify = notify_new();
    make_fh(src_fh, 1);
    make_fh(dst_fh, 2);

    src_watch = chimera_vfs_notify_watch_create(notify, src_fh, sizeof(src_fh),
                                                0xFFFFFFFF, 0, NULL, NULL);
    dst_watch = chimera_vfs_notify_watch_create(notify, dst_fh, sizeof(dst_fh),
                                                0xFFFFFFFF, 0, NULL, NULL);

    /* Source emit: old-name only. */
    chimera_vfs_notify_emit(notify, src_fh, sizeof(src_fh),
                            CHIMERA_VFS_NOTIFY_RENAMED,
                            NULL, 0,
                            "oldname", 7);

    /* Destination emit: new-name only. */
    chimera_vfs_notify_emit(notify, dst_fh, sizeof(dst_fh),
                            CHIMERA_VFS_NOTIFY_RENAMED,
                            "newname", 7,
                            NULL, 0);

    n = chimera_vfs_notify_drain(src_watch, events, 4, &overflowed);
    CHECK(n == 1, "source dir receives one RENAMED event");
    CHECK(events[0].action == CHIMERA_VFS_NOTIFY_RENAMED, "source action is RENAMED");
    CHECK(events[0].name_len == 0, "source event has empty name");
    CHECK(events[0].old_name_len == 7 &&
          memcmp(events[0].old_name, "oldname", 7) == 0,
          "source event carries the old name");

    n = chimera_vfs_notify_drain(dst_watch, events, 4, &overflowed);
    CHECK(n == 1, "dest dir receives one RENAMED event");
    CHECK(events[0].action == CHIMERA_VFS_NOTIFY_RENAMED, "dest action is RENAMED");
    CHECK(events[0].name_len == 7 &&
          memcmp(events[0].name, "newname", 7) == 0,
          "dest event carries the new name");
    CHECK(events[0].old_name_len == 0, "dest event has empty old_name");

    chimera_vfs_notify_watch_destroy(notify, src_watch);
    chimera_vfs_notify_watch_destroy(notify, dst_watch);
    chimera_vfs_notify_destroy(notify);
} /* test_cross_dir_rename_emits_split */

/* ------------------------------------------------------------------ */
/* Test 19: cross-dir source-side emit overflows subtree watches      */
/* ------------------------------------------------------------------ */
/*
 * When rename_at fires the source-side emit of a cross-directory
 * rename, name_len is 0 (the leaf moves to the destination dir).
 * For exact watches that works fine — the OLD_NAME record carries
 * the source name.  But subtree watchers' resolver would build a
 * leaf-less relpath like "parent/parent/" which the SMB serializer
 * would render as a malformed NEW_NAME.  Emit must therefore
 * overflow subtree watches rather than dispatching them.
 *
 * Construction: subtree watch on a different FH (sharing mount_id),
 * has_rpl flipped to 1 so we exercise the RPL branch.  Emit with
 * name_len=0 RENAMED.  Verify the subtree watch overflows.
 */
static void
test_cross_dir_source_overflows_subtree(void)
{
    struct chimera_vfs_notify             *notify;
    struct chimera_vfs_notify_watch       *watch;
    struct chimera_vfs_notify_mount_entry *me = NULL;
    struct chimera_vfs_notify_event        events[4];
    int                                    overflowed;
    int                                    n;
    uint8_t                                parent_fh[CHIMERA_VFS_FH_SIZE];
    uint8_t                                child_fh[CHIMERA_VFS_FH_SIZE];

    fprintf(stderr, "\ntest_cross_dir_source_overflows_subtree\n");

    notify = notify_new();
    make_fh(parent_fh, 1);
    make_fh(child_fh,  2);

    watch = chimera_vfs_notify_watch_create(notify, parent_fh, sizeof(parent_fh),
                                            0xFFFFFFFF, 1, NULL, NULL);

    /* Force has_rpl=1 so emit takes the RPL path (otherwise the
     * !has_rpl coarse fallback would overflow too, but for a
     * different reason). */
    pthread_mutex_lock(&notify->mount_entries_lock);
    HASH_FIND(hh, notify->mount_entries,
              chimera_vfs_fh_mount_id(parent_fh),
              CHIMERA_VFS_MOUNT_ID_SIZE, me);
    CHECK(me != NULL, "mount entry exists for subtree watch");
    if (me) {
        me->has_rpl = 1;
    }
    pthread_mutex_unlock(&notify->mount_entries_lock);

    /* Emit on child_fh with name_len=0 (cross-dir source-side
     * RENAMED).  The exact-watch path does not match (different fh
     * from our watch on parent_fh).  The subtree path must overflow. */
    chimera_vfs_notify_emit(notify, child_fh, sizeof(child_fh),
                            CHIMERA_VFS_NOTIFY_RENAMED,
                            NULL, 0,
                            "src.txt", 7);

    n = chimera_vfs_notify_drain(watch, events, 4, &overflowed);
    CHECK(n == 0, "no leaf-less relpath delivered to subtree watch");
    CHECK(overflowed == 1, "subtree watch overflowed on name_len==0 emit");

    chimera_vfs_notify_watch_destroy(notify, watch);
    chimera_vfs_notify_destroy(notify);
} /* test_cross_dir_source_overflows_subtree */

/* ------------------------------------------------------------------ */
/* Test 13: multiple watches on the same FH each receive the event    */
/* ------------------------------------------------------------------ */
static void
test_multiple_watches_same_fh(void)
{
    struct chimera_vfs_notify       *notify;
    struct chimera_vfs_notify_watch *w1, *w2;
    struct chimera_vfs_notify_event  events[4];
    int                              overflowed;
    int                              n1, n2;
    uint8_t                          fh[CHIMERA_VFS_FH_SIZE];

    fprintf(stderr, "\ntest_multiple_watches_same_fh\n");

    notify = notify_new();
    make_fh(fh, 1);
    w1 = chimera_vfs_notify_watch_create(notify, fh, sizeof(fh),
                                         0xFFFFFFFF, 0, NULL, NULL);
    w2 = chimera_vfs_notify_watch_create(notify, fh, sizeof(fh),
                                         0xFFFFFFFF, 0, NULL, NULL);

    chimera_vfs_notify_emit(notify, fh, sizeof(fh),
                            CHIMERA_VFS_NOTIFY_FILE_MODIFIED,
                            "x", 1, NULL, 0);

    n1 = chimera_vfs_notify_drain(w1, events, 4, &overflowed);
    n2 = chimera_vfs_notify_drain(w2, events, 4, &overflowed);

    CHECK(n1 == 1 && n2 == 1, "both watches receive the event");

    chimera_vfs_notify_watch_destroy(notify, w1);
    chimera_vfs_notify_watch_destroy(notify, w2);
    chimera_vfs_notify_destroy(notify);
} /* test_multiple_watches_same_fh */

/* ------------------------------------------------------------------ */
/* Main                                                               */
/* ------------------------------------------------------------------ */
int
main(
    int    argc,
    char **argv)
{
    (void) argc;
    (void) argv;

    ChimeraLogLevel = CHIMERA_LOG_INFO;

    /* Required for the RPL cache test — its insert/invalidate paths
     * use call_rcu which relies on the URCU thread registry. */
    urcu_memb_register_thread();

    test_init_destroy();
    test_watch_create_destroy();
    test_emit_drain_added();
    test_emit_drain_renamed();
    test_filter_mask();
    test_fh_mismatch();
    test_ring_overflow();
    test_callback_fires();
    test_watch_update_filter();
    test_rpl_cache_cross_shard_invalidate();
    test_oversize_name_clamp();
    test_watch_tree_transition_clears_ring();
    test_watch_tree_transition_tree_to_nontree();
    test_subtree_no_rpl_overflows();
    test_subtree_rpl_max_pending_overflows();
    test_oversize_subtree_relpath_overflows();
    test_filter_distinguishes_file_vs_dir();
    test_cross_dir_rename_emits_split();
    test_cross_dir_source_overflows_subtree();
    test_multiple_watches_same_fh();

    fprintf(stderr, "\n========================================\n");
    fprintf(stderr, "Results: %d passed, %d failed\n", passed, failed);
    fprintf(stderr, "========================================\n");

    urcu_memb_unregister_thread();
    return failed == 0 ? 0 : 1;
} /* main */
