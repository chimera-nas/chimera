// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "vfs_daos.h"

#include "dfs_mock.h"

static void
setup(
    struct vfs_daos_state *s,
    size_t                 entries,
    size_t                 handles)
{
    memset(s, 0, sizeof(*s));
    s->config.registry_max_entries = entries;
    s->config.open_max_handles     = handles;
    s->incarnation                 = 19;
} /* setup */

static struct vfs_daos_entry *
register_object(
    struct vfs_daos_state *s,
    const char            *name,
    dfs_obj_t            **obj,
    uint8_t                fh[33])
{
    struct vfs_daos_entry *e;

    CHECK(vfs_daos_lookup_rel(s, s->root, name, O_RDONLY, obj, NULL, NULL) == 0);
    CHECK(vfs_daos_registry_add(s, *obj, &e) == 0);
    struct vfs_daos_fh     key = { e->oid.hi, e->oid.lo, e->type };
    uint32_t               length;
    CHECK(vfs_daos_fh_encode(s->mount_id, &key, fh, &length) == 0);
    CHECK(length == 33);
    return e;
} /* register_object */

static void
finish(struct vfs_daos_state *s)
{
    if (s->root) {
        CHECK(vfs_daos_release(s, s->root) == 0);
        s->root = NULL;
    }
    vfs_daos_unreserve(s);
    CHECK(vfs_daos_registry_clear(s) == 0);
    CHECK(s->registry_count == 0 && s->handle_count == 0 && !s->owned);
} /* finish */

static void
test_pins_without_eviction(void)
{
    struct vfs_daos_state  s;
    struct vfs_daos_entry *root, *e, *same;
    dfs_obj_t             *obj, *second, *wrapper, *resolved;
    uint8_t                root_fh[33], fh[33], second_fh[33];

    setup(&s, 2, 5);
    root = register_object(&s, "0", &s.root, root_fh);
    CHECK(root->type == 2);
    e    = register_object(&s, "1", &obj, fh);
    same = register_object(&s, "1", &second, second_fh);
    CHECK(e == same && s.registry_count == 2 && !memcmp(fh, second_fh, 33));
    CHECK(vfs_daos_dup(&s, second, O_RDWR, &wrapper) == 0);
    CHECK(vfs_daos_release(&s, second) == 0);
    CHECK(vfs_daos_release(&s, obj) == 0);
    CHECK(e->pins == 1); /* The wrapper survives caller/core-cache idleness. */
    CHECK(vfs_daos_lookup_rel(&s, s.root, "2", O_RDONLY, &obj, NULL, NULL) == 0);
    CHECK(vfs_daos_registry_add(&s, obj, &same) == EMFILE);
    CHECK(vfs_daos_release(&s, obj) == 0);
    CHECK(s.registry_admission_failures == 1);
    unsigned int before = lookups;
    CHECK(vfs_daos_resolve(&s, fh, 33, &resolved) == 0);
    CHECK(lookups == before && resolved == e->owned->obj);
    CHECK(vfs_daos_release(&s, resolved) == 0);
    CHECK(vfs_daos_release(&s, wrapper) == 0);
    CHECK(e->pins == 0 && s.handle_count == 2);
    CHECK(vfs_daos_lookup_rel(&s, s.root, "2", O_RDONLY, &obj, NULL, NULL) == 0);
    CHECK(vfs_daos_registry_add(&s, obj, &same) == EMFILE);
    CHECK(vfs_daos_release(&s, obj) == 0);
    CHECK(s.registry_evictions == 0);
    before = lookups;
    CHECK(vfs_daos_resolve(&s, fh, 33, &resolved) == 0);
    CHECK(lookups == before);
    CHECK(vfs_daos_release(&s, resolved) == 0);
    CHECK(vfs_daos_resolve(&s, root_fh, 33, &resolved) == 0);
    CHECK(vfs_daos_release(&s, resolved) == 0);
    finish(&s);
} /* test_pins_without_eviction */

static void
test_validation_and_repair(void)
{
    struct vfs_daos_state  s;
    dfs_obj_t             *obj, *wrapper, *resolved;
    uint8_t                fh[33];
    struct vfs_daos_entry *e;

    setup(&s, 4, 8);
    e = register_object(&s, "3", &obj, fh);
    CHECK(e->type == 3); /* The entry keeps the symlink type. */
    CHECK(vfs_daos_dup(&s, obj, O_RDONLY, &wrapper) == 0);
    unsigned int           before = stats;
    CHECK(vfs_daos_resolve(&s, fh, 32, &resolved) == ESTALE);
    CHECK(stats == before && s.fh_decode_failures == 1);
    stat_error = EIO;
    CHECK(vfs_daos_resolve(&s, fh, 33, &resolved) == EIO);
    CHECK(!e->invalid && !resolved);
    stat_error = 0;
    vfs_daos_repair(&s, e->oid, obj, obj, "renamed", 0);
    CHECK(obj->repairs == 1 && wrapper->repairs == 1);
    CHECK(vfs_daos_release(&s, obj) == 0);
    CHECK(vfs_daos_resolve(&s, fh, 33, &resolved) == 0);
    CHECK(vfs_daos_release(&s, resolved) == 0);
    repair_error = EIO;
    vfs_daos_repair(&s, e->oid, wrapper, wrapper, "again", 0);
    CHECK(e->invalid && e->repair_failed && !s.unavailable);
    CHECK(vfs_daos_resolve(&s, fh, 33, &resolved) == EIO);
    CHECK(vfs_daos_validate(&s, wrapper) == EIO);
    repair_error = 0;
    CHECK(vfs_daos_release(&s, wrapper) == 0);
    finish(&s);

    setup(&s, 4, 8);
    e = register_object(&s, "1", &obj, fh);
    CHECK(vfs_daos_dup(&s, obj, O_RDONLY, &wrapper) == 0);
    vfs_daos_invalidate(&s, e->oid);
    CHECK(e->removed && vfs_daos_resolve(&s, fh, 33, &resolved) == ESTALE);
    CHECK(vfs_daos_validate(&s, wrapper) == 0);
    stat_error = ENOENT;
    CHECK(vfs_daos_validate(&s, wrapper) == 0);
    stat_error = 0;
    CHECK(vfs_daos_release(&s, wrapper) == 0);
    CHECK(vfs_daos_release(&s, obj) == 0);
    finish(&s);

    setup(&s, 4, 8);
    e = register_object(&s, "1", &obj, fh);
    s.incarnation++;
    CHECK(vfs_daos_resolve(&s, fh, 33, &resolved) == ESTALE);
    s.incarnation--;
    obj->mode = S_IFDIR;
    CHECK(vfs_daos_resolve(&s, fh, 33, &resolved) == ESTALE && e->invalid);
    CHECK(vfs_daos_release(&s, obj) == 0);
    finish(&s);

    setup(&s, 4, 8);
    e           = register_object(&s, "1", &obj, fh);
    obj->oid.lo = 99;
    struct vfs_daos_entry *changed = NULL;
    CHECK(vfs_daos_registry_add(&s, obj, &changed) == ESTALE);
    CHECK(!changed && e->invalid && s.registry_count == 1);
    CHECK(vfs_daos_release(&s, obj) == 0);
    finish(&s);
} /* test_validation_and_repair */

static void
test_admission_and_drain(void)
{
    struct vfs_daos_state  s;
    dfs_obj_t             *obj, *copy;
    uint8_t                fh[33];
    struct vfs_daos_entry *e;

    setup(&s, 2, 1);
    register_object(&s, "0", &s.root, fh);
    unsigned int           before = allocations;
    CHECK(vfs_daos_reserve(&s, 1, false) == EMFILE);
    CHECK(vfs_daos_dup(&s, s.root, O_RDONLY, &obj) == EMFILE && !obj);
    CHECK(vfs_daos_open(&s, s.root, "1", S_IFREG, O_CREAT, 0, 0, NULL, &obj) == EMFILE);
    CHECK(allocations == before && creates == 0);
    CHECK(s.handle_count == 1 && s.handle_admission_failures == 3);
    finish(&s);

    setup(&s, 2, 4);
    register_object(&s, "0", &s.root, fh);
    CHECK(vfs_daos_reserve(&s, 3, true) == 0);
    CHECK(s.handle_count + s.handle_reserved == 4);
    CHECK(vfs_daos_open(&s, s.root, "1", S_IFREG, O_CREAT, 0, 0, NULL, &obj) == 0);
    CHECK(vfs_daos_registry_add(&s, obj, &e) == 0);
    CHECK(vfs_daos_dup(&s, obj, O_RDWR, &copy) == 0);
    vfs_daos_unreserve(&s);
    CHECK(vfs_daos_release(&s, obj) == 0);
    CHECK(vfs_daos_registry_clear(&s) == EBUSY);
    CHECK(vfs_daos_release(&s, copy) == 0);
    finish(&s);

    setup(&s, 2, 2);
    allocation_error = EIO;
    CHECK(vfs_daos_lookup_root(&s, &obj) == EIO && !obj);
    CHECK(s.handle_count == 0);
    allocation_error = 0;
    finish(&s);
} /* test_admission_and_drain */

static void
test_oid_and_parent_collisions(void)
{
    struct vfs_daos_state  s;
    struct vfs_daos_entry *e;
    dfs_obj_t             *obj, *canonical, *parent, *temp;
    uint8_t                root_fh[33];

    setup(&s, 8, 16);
    register_object(&s, "0", &s.root, root_fh);
    CHECK(vfs_daos_lookup_rel(&s, s.root, "1", O_RDONLY, &obj, NULL, NULL) == 0);
    CHECK(vfs_daos_admit(&s, &obj, s.root, false) == 0);
    canonical = obj;
    CHECK(vfs_daos_release(&s, obj) == 0);
    CHECK(vfs_daos_lookup_rel(&s, s.root, "1", O_RDONLY, &temp, NULL, NULL) == 0);
    unsigned int before = releases;
    CHECK(vfs_daos_admit(&s, &temp, s.root, false) == 0 && temp == canonical);
    CHECK(releases == before + 1 && s.registry_count == 2);
    CHECK(vfs_daos_release(&s, temp) == 0);
    CHECK(vfs_daos_lookup_rel(&s, s.root, "2", O_RDONLY, &parent, NULL, NULL) == 0);
    CHECK(vfs_daos_admit(&s, &parent, s.root, false) == 0);
    CHECK(vfs_daos_lookup_rel(&s, parent, "1", O_RDONLY, &temp, NULL, NULL) == 0);
    CHECK(vfs_daos_admit(&s, &temp, parent, false) == EIO);
    CHECK(vfs_daos_release(&s, temp) == 0);
    CHECK(vfs_daos_lookup_rel(&s, s.root, "1", O_RDONLY, &temp, NULL, NULL) == 0);
    CHECK(vfs_daos_admit(&s, &temp, s.root, true) == EIO);
    temp->oid.hi++;
    CHECK(vfs_daos_registry_add(&s, temp, &e) == EIO); /* Same lo, different hi. */
    CHECK(vfs_daos_release(&s, temp) == 0);
    vfs_daos_invalidate(&s, canonical->oid);
    CHECK(vfs_daos_lookup_rel(&s, s.root, "1", O_RDONLY, &temp, NULL, NULL) == 0);
    CHECK(vfs_daos_admit(&s, &temp, s.root, false) == EIO);
    CHECK(vfs_daos_release(&s, temp) == 0);
    CHECK(vfs_daos_release(&s, parent) == 0);
    finish(&s);
} /* test_oid_and_parent_collisions */

static void
test_uncertain_release(void)
{
    struct vfs_daos_state  s;
    dfs_obj_t             *obj, *copy;
    uint8_t                fh[33];
    struct vfs_daos_entry *e;

    setup(&s, 1, 2);
    e = register_object(&s, "1", &obj, fh);
    CHECK(vfs_daos_dup(&s, obj, O_RDWR, &copy) == 0);
    CHECK(vfs_daos_release(&s, obj) == 0);
    unsigned int           before = releases;
    release_error = EIO;
    CHECK(vfs_daos_release(&s, copy) == EIO);
    CHECK(vfs_daos_release(&s, copy) == EIO);
    CHECK(releases == before + 1 && s.uncertain_release_count == 1);
    CHECK(e->pins == 1 && s.handle_count == 2 && s.unavailable);
    CHECK(vfs_daos_registry_clear(&s) == EBUSY);
    /* Test-only disposal of fake quarantine, never retry the DFS release. */
    while (s.owned) {
        struct vfs_daos_owned *h = s.owned;
        s.owned = h->next;
        free(h->obj);
        free(h);
    }
    free(s.registry);
    release_error = 0;
} /* test_uncertain_release */

int
main(void)
{
    test_pins_without_eviction();
    test_validation_and_repair();
    test_admission_and_drain();
    test_oid_and_parent_collisions();
    CHECK(allocations == releases);
    test_uncertain_release();
    puts("Registry ownership, pins, retention, validation, repair, admission and quarantine passed");
    return 0;
} /* main */
