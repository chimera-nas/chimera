// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include "vfs_daos.h"

static bool
oid_equal(
    daos_obj_id_t a,
    daos_obj_id_t b)
{
    return a.hi == b.hi && a.lo == b.lo;
} /* oid_equal */

static uint8_t
object_type(mode_t mode)
{
    return S_ISREG(mode) ? 1 : S_ISDIR(mode) ? 2 : S_ISLNK(mode) ? 3 : 0;
} /* object_type */

static struct vfs_daos_owned *
owned_find(
    struct vfs_daos_state *s,
    dfs_obj_t             *obj)
{
    for (struct vfs_daos_owned *h = s->owned; h; h = h->next) {
        if (h->obj == obj) {
            return h;
        }
    }
    return NULL;
} /* owned_find */

static struct vfs_daos_entry *
entry_find(
    struct vfs_daos_state *s,
    daos_obj_id_t          oid)
{
    for (struct vfs_daos_entry *e = s->registry; e; e = e->next) {
        if (oid_equal(e->oid, oid)) {
            return e;
        }
    }
    return NULL;
} /* entry_find */

int
vfs_daos_hold(
    struct vfs_daos_state *s,
    dfs_obj_t             *obj)
{
    struct vfs_daos_owned *h = owned_find(s, obj);

    if (!h || h->uncertain || h->refs == SIZE_MAX) {
        return ESTALE;
    }
    h->refs++;
    return 0;
} /* vfs_daos_hold */

int
vfs_daos_release(
    struct vfs_daos_state *s,
    dfs_obj_t             *obj)
{
    struct vfs_daos_owned **p = &s->owned;

    if (!obj) {
        return 0;
    }
    while (*p && (*p)->obj != obj) {
        p = &(*p)->next;
    }
    if (!*p || (*p)->uncertain) {
        return EIO;
    }
    struct vfs_daos_owned *h = *p;
    if (h->entry) {
        h->entry->touched = ++s->registry_clock;
    }
    if (--h->refs) {
        return 0;
    }
    int                    rc = dfs_release(obj);
    if (rc) {
        /* Never retry: DFS may have consumed an object even on failure. Keep
        * the slot, entry pin and connection ownership until process exit. */
        h->uncertain = true;
        s->uncertain_release_count++;
        s->unavailable = true;
        return rc;
    }
    if (h->entry && h->entry->owned != h) {
        h->entry->pins--;
    }
    *p = h->next;
    s->handle_count--;
    free(h);
    return 0;
} /* vfs_daos_release */

static int
handle_room(
    struct vfs_daos_state *s,
    size_t                 count)
{
    if (count > s->config.open_max_handles ||
        s->handle_count + s->handle_reserved > s->config.open_max_handles - count) {
        s->handle_admission_failures++;
        return EMFILE;
    }
    return s->unavailable ? EIO : 0;
} /* handle_room */

static int
entry_room(struct vfs_daos_state *s)
{
    if (s->registry_count >= s->config.registry_max_entries) {
        s->registry_admission_failures++;
        return EMFILE;
    }
    return 0;
} /* entry_room */

static struct vfs_daos_owned *
handle_alloc(
    struct vfs_daos_state *s,
    int                   *rc)
{
    struct vfs_daos_owned *h;

    *rc = s->unavailable ? EIO : 0;
    if (*rc) {
        return NULL;
    }
    if (s->reserved_handles) {
        h                   = s->reserved_handles;
        s->reserved_handles = h->next;
        s->handle_reserved--;
    } else {
        *rc = handle_room(s, 1);
        if (*rc) {
            return NULL;
        }
        h = calloc(1, sizeof(*h));
        if (!h) {
            *rc = ENOMEM;
            return NULL;
        }
    }
    h->next = NULL;
    return h;
} /* handle_alloc */

static int
handle_finish(
    struct vfs_daos_state *s,
    struct vfs_daos_owned *h,
    int                    rc,
    dfs_obj_t            **out)
{
    *out = NULL;
    if (rc) {
        free(h);
        return rc;
    }
    h->refs  = 1;
    h->next  = s->owned;
    s->owned = h;
    s->handle_count++;
    s->handle_admissions++;
    *out = h->obj;
    return 0;
} /* handle_finish */

int
vfs_daos_dup(
    struct vfs_daos_state *s,
    dfs_obj_t             *obj,
    int                    flags,
    dfs_obj_t            **out)
{
    int                    rc;
    struct vfs_daos_owned *source = owned_find(s, obj);
    struct vfs_daos_owned *h      = handle_alloc(s, &rc);

    *out = NULL;
    if (!h) {
        return rc;
    }
    rc = dfs_dup(s->dfs, obj, flags, &h->obj);
    if (!rc && source && source->entry) {
        h->entry = source->entry;
        h->entry->pins++;
    }
    return handle_finish(s, h, rc, out);
} /* vfs_daos_dup */

int
vfs_daos_lookup_rel(
    struct vfs_daos_state *s,
    dfs_obj_t             *parent,
    const char            *name,
    int                    flags,
    dfs_obj_t            **out,
    mode_t                *mode,
    struct stat           *st)
{
    int                    rc;
    struct vfs_daos_owned *h = handle_alloc(s, &rc);

    *out = NULL;
    if (!h) {
        return rc;
    }
    rc = dfs_lookup_rel(s->dfs, parent, name, flags, &h->obj, mode, st);
    return handle_finish(s, h, rc, out);
} /* vfs_daos_lookup_rel */

int
vfs_daos_lookup_root(
    struct vfs_daos_state *s,
    dfs_obj_t            **out)
{
    int                    rc;
    struct vfs_daos_owned *h = handle_alloc(s, &rc);

    *out = NULL;
    if (!h) {
        return rc;
    }
    int                    flags = s->config.read_only ? O_RDONLY : O_RDWR;
    rc = dfs_lookup(s->dfs, "/", flags, &h->obj, NULL, NULL);
    return handle_finish(s, h, rc, out);
} /* vfs_daos_lookup_root */

int
vfs_daos_open(
    struct vfs_daos_state *s,
    dfs_obj_t             *parent,
    const char            *name,
    mode_t                 mode,
    int                    flags,
    daos_oclass_id_t       oclass,
    daos_size_t            chunk,
    const char            *value,
    dfs_obj_t            **out)
{
    int                    rc;
    struct vfs_daos_owned *h = handle_alloc(s, &rc);

    *out = NULL;
    if (!h) {
        return rc;
    }
    rc = dfs_open(s->dfs, parent, name, mode, flags, oclass, chunk, value, &h->obj);
    return handle_finish(s, h, rc, out);
} /* vfs_daos_open */

int
vfs_daos_reserve(
    struct vfs_daos_state *s,
    size_t                 handles,
    bool                   entry)
{
    int rc;

    if (s->reserved_handles || s->reserved_entry ||
        s->reserved_wrapper) {
        return EBUSY;
    }
    rc = handle_room(s, handles);

    if (rc) {
        return rc;
    }
    while (handles--) {
        struct vfs_daos_owned *h = calloc(1, sizeof(*h));
        if (!h) {
            return ENOMEM;
        }
        h->next             = s->reserved_handles;
        s->reserved_handles = h;
        s->handle_reserved++;
    }
    if (!entry) {
        return 0;
    }
    rc = entry_room(s);
    if (rc) {
        return rc;
    }
    s->reserved_entry   = calloc(1, sizeof(*s->reserved_entry));
    s->reserved_wrapper = calloc(1, sizeof(*s->reserved_wrapper));
    return s->reserved_entry && s->reserved_wrapper ? 0 : ENOMEM;
} /* vfs_daos_reserve */

int
vfs_daos_reserve_root(struct vfs_daos_state *s)
{
    int rc = vfs_daos_reserve(s, 1, false);

    if (!rc) {
        rc = entry_room(s);
    }
    if (!rc) {
        s->reserved_entry = calloc(1, sizeof(*s->reserved_entry));
        rc                = s->reserved_entry ? 0 : ENOMEM;
    }
    return rc;
} /* vfs_daos_reserve_root */

void
vfs_daos_unreserve(struct vfs_daos_state *s)
{
    while (s->reserved_handles) {
        struct vfs_daos_owned *h = s->reserved_handles;
        s->reserved_handles = h->next;
        free(h);
    }
    s->handle_reserved = 0;
    free(s->reserved_entry);
    free(s->reserved_wrapper);
    s->reserved_entry   = NULL;
    s->reserved_wrapper = NULL;
} /* vfs_daos_unreserve */

int
vfs_daos_registry_add(
    struct vfs_daos_state  *s,
    dfs_obj_t              *obj,
    struct vfs_daos_entry **out)
{
    daos_obj_id_t          oid;
    struct stat            st;
    struct vfs_daos_owned *h = owned_find(s, obj);
    struct vfs_daos_entry *e;
    int                    rc;

    *out = NULL;
    if (s->unavailable || !h || h->uncertain) {
        return EIO;
    }
    rc = dfs_obj2id(obj, &oid);
    if (!rc) {
        rc = dfs_ostat(s->dfs, obj, &st);
    }
    if (rc) {
        return rc == ENOENT ? ESTALE : rc;
    }
    if (!object_type(st.st_mode)) {
        return ESTALE;
    }
    if (h->entry && (!oid_equal(h->entry->oid, oid) ||
                     h->entry->type != object_type(st.st_mode))) {
        h->entry->invalid = true;
        return ESTALE;
    }
    e = entry_find(s, oid);
    if (e) {
        if (e->type != object_type(st.st_mode)) {
            e->invalid = true;
        }
        if (e->invalid || e->removed || e->incarnation != s->incarnation) {
            return EIO;
        }
        if (!h->entry) {
            h->entry = e;
            e->pins++;
        }
    } else {
        for (struct vfs_daos_entry *p = s->registry; p; p = p->next) {
            uint64_t existing = p->oid.hi ^ p->oid.lo;
            if (!p->removed && existing == (oid.hi ^ oid.lo) &&
                (p->oid.hi != oid.hi || p->oid.lo != oid.lo)) {
                return EIO;
            }
        }
        rc = entry_room(s);
        if (rc) {
            return rc;
        }
        e                 = s->reserved_entry;
        s->reserved_entry = NULL;
        if (!e) {
            e = calloc(1, sizeof(*e));
        }
        if (!e) {
            return ENOMEM;
        }
        e->oid         = oid;
        e->type        = object_type(st.st_mode);
        e->incarnation = s->incarnation;
        e->owned       = h;
        h->refs++; /* The registry now owns one canonical reference. */
        h->entry    = e;
        e->next     = s->registry;
        s->registry = e;
        s->registry_count++;
        s->registry_admissions++;
    }
    e->touched = ++s->registry_clock;
    *out       = e;
    return 0;
} /* vfs_daos_registry_add */

int
vfs_daos_validate(
    struct vfs_daos_state *s,
    dfs_obj_t             *obj)
{
    struct vfs_daos_owned *h = owned_find(s, obj);
    struct stat            st;
    daos_obj_id_t          oid;
    int                    rc;

    if (h && h->entry && h->entry->repair_failed) {
        return EIO;
    }
    if (!h || h->uncertain || !h->entry || (h->entry->invalid && !h->entry->removed) ||
        h->entry->incarnation != s->incarnation) {
        return ESTALE;
    }
    if (h->entry->removed) {
        /* An existing pin does not need a directory entry to remain open. */
        return 0;
    }
    rc = dfs_obj2id(obj, &oid);
    if (!rc) {
        rc = dfs_ostat(s->dfs, obj, &st);
    }
    if (rc == ENOENT || (!rc && (!oid_equal(oid, h->entry->oid) ||
                                 object_type(st.st_mode) != h->entry->type))) {
        h->entry->invalid = true;
        return ESTALE;
    }
    return rc;
} /* vfs_daos_validate */

int
vfs_daos_resolve(
    struct vfs_daos_state *s,
    const void            *fh,
    size_t                 length,
    dfs_obj_t            **obj)
{
    struct vfs_daos_fh     key;
    struct vfs_daos_entry *e;
    daos_obj_id_t          oid;
    int                    rc = vfs_daos_fh_decode(s->mount_id, fh, length, &key);

    *obj = NULL;
    if (rc) {
        s->fh_decode_failures++;
        return rc;
    }
    if (s->unavailable) {
        return EIO;
    }
    oid.hi = key.hi;
    oid.lo = key.lo;
    e      = entry_find(s, oid);
    if (e && e->repair_failed) {
        return EIO;
    }
    if (!e || e->invalid || e->removed || e->type != key.type || e->incarnation != s->incarnation) {
        s->registry_misses++;
        return ESTALE;
    }
    s->registry_hits++;
    e->owned->refs++;
    rc = vfs_daos_validate(s, e->owned->obj);
    if (rc) {
        vfs_daos_release(s, e->owned->obj);
        return rc;
    }
    e->touched = ++s->registry_clock;
    *obj       = e->owned->obj;
    return 0;
} /* vfs_daos_resolve */

void
vfs_daos_invalidate(
    struct vfs_daos_state *s,
    daos_obj_id_t          oid)
{
    struct vfs_daos_entry *e = entry_find(s, oid);

    if (e) {
        e->invalid = true;
        e->removed = true;
    }
    for (struct vfs_daos_object *o = s->objects; o; o = o->next) {
        if (oid_equal(o->oid, oid)) {
            o->removed = true;
        }
    }
} /* vfs_daos_invalidate */

void
vfs_daos_repair(
    struct vfs_daos_state *s,
    daos_obj_id_t          oid,
    dfs_obj_t             *moved,
    dfs_obj_t             *parent,
    const char            *name,
    int                    error)
{
    struct vfs_daos_entry *e = entry_find(s, oid);

    if (!e) {
        return;
    }
    if (s->unavailable) {
        error = EIO;
    }
    if (!error) {
        daos_obj_id_t moved_oid;
        struct stat   st;
        error = dfs_obj2id(moved, &moved_oid);
        if (!error) {
            error = dfs_ostat(s->dfs, moved, &st);
        }
        if (!error && (!oid_equal(moved_oid, oid) || object_type(st.st_mode) != e->type)) {
            error = ESTALE;
        }
    }
    daos_obj_id_t parent_oid;
    if (!error) {
        error = dfs_obj2id(parent, &parent_oid);
    }
    for (struct vfs_daos_owned *h = s->owned; h; h = h->next) {
        if (h->entry == e && !h->uncertain) {
            int rc = error ? error : dfs_update_parent(h->obj, moved, name);
            if (rc) {
                e->invalid       = true;
                e->repair_failed = true;
            }
        }
    }
    if (!e->repair_failed) {
        e->parent_oid   = parent_oid;
        e->parent_valid = true;
    }

} /* vfs_daos_repair */

int
vfs_daos_registry_clear(struct vfs_daos_state *s)
{
    while (s->registry) {
        struct vfs_daos_entry *e = s->registry;
        if (e->pins || e->owned->refs != 1 || e->owned->uncertain) {
            return EBUSY;
        }
        e->owned->entry = NULL;
        int                    rc = vfs_daos_release(s, e->owned->obj);
        if (rc) {
            return rc;
        }
        s->registry = e->next;
        s->registry_count--;
        free(e);
    }
    return 0;
} /* vfs_daos_registry_clear */

int
vfs_daos_admit(
    struct vfs_daos_state *s,
    dfs_obj_t            **obj,
    dfs_obj_t             *parent,
    bool                   created)
{
    daos_obj_id_t          oid, parent_oid;
    struct vfs_daos_entry *e;
    int                    rc = dfs_obj2id(*obj, &oid);

    if (!rc) {
        rc = dfs_obj2id(parent, &parent_oid);
    }
    if (rc) {
        return rc;
    }
    e = entry_find(s, oid);
    if (e && (created || e->removed || e->invalid || !e->parent_valid ||
              !oid_equal(e->parent_oid, parent_oid))) {
        return EIO;
    }
    if (e) {
        struct stat st;
        rc = dfs_ostat(s->dfs, *obj, &st);
        if (rc || object_type(st.st_mode) != e->type) {
            return rc ? rc : EIO;
        }
        rc   = vfs_daos_release(s, *obj);
        *obj = NULL;
        if (!rc) {
            rc = vfs_daos_hold(s, e->owned->obj);
            if (!rc) {
                *obj = e->owned->obj;
            }
        }
        return rc;
    }
    rc = vfs_daos_registry_add(s, *obj, &e);
    if (!rc) {
        e->parent_oid   = parent_oid;
        e->parent_valid = true;
    }
    return rc;
} /* vfs_daos_admit */

int
vfs_daos_parent(
    struct vfs_daos_state *s,
    dfs_obj_t             *obj,
    dfs_obj_t            **parent)
{
    daos_obj_id_t          oid;

    *parent = NULL;
    int                    rc = dfs_obj2id(obj, &oid);
    if (rc) {
        return rc;
    }
    struct vfs_daos_entry *e = entry_find(s, oid);
    if (!e || !e->parent_valid || e->repair_failed) {
        return EIO;
    }
    e = entry_find(s, e->parent_oid);
    if (!e || e->removed || e->invalid) {
        return EIO;
    }
    rc = vfs_daos_hold(s, e->owned->obj);
    if (!rc) {
        *parent = e->owned->obj;
    }
    return rc;
} /* vfs_daos_parent */
