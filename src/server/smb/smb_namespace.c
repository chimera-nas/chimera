// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only

#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include "smb_namespace.h"
#include "common/macros.h"

struct namespace_object {
    uint32_t                          fh_len;
    uint8_t                           fh[CHIMERA_VFS_FH_SIZE];
    struct chimera_smb_namespace_path initial, current;
    bool                              seeded;
};

struct chimera_smb_namespace_edit {
    struct chimera_smb_namespace_edit     *next;
    struct chimera_smb_namespace_registry *registry;
    struct namespace_object               *objects;
    struct chimera_smb_namespace_change   *changes;
    uint32_t                               object_capacity, change_capacity, object_count, change_count;
    bool                                   global, acquired, published;
};

struct chimera_smb_namespace_participant {
    struct chimera_smb_namespace_participant *next;
    struct chimera_smb_namespace_registry    *registry;
    uint32_t                                  fh_len;
    uint8_t                                   fh[CHIMERA_VFS_FH_SIZE];
    struct chimera_smb_namespace_snapshot     snapshot;
    struct chimera_smb_namespace_snapshot     initial;
    bool                                      pending, bound, invalid_path;
    void                                     *context;
    chimera_smb_namespace_context_t           release;
    chimera_smb_namespace_publish_t           publish;
};

static bool
identity(
    const uint8_t *a,
    uint32_t       alen,
    const uint8_t *b,
    uint32_t       blen)
{
    return alen == blen && !memcmp(a, b, alen);
} /* identity */

static bool
path_valid(const struct chimera_smb_namespace_path *path)
{
    return path && path->view_fh_len && path->view_fh_len <= CHIMERA_VFS_FH_SIZE &&
           path->parent_fh_len <= CHIMERA_VFS_FH_SIZE &&
           path->name_len <= SMB_FILENAME_MAX && path->full_path_len < SMB_PATH_MAX;
} /* path_valid */

static bool
same_link(
    const struct chimera_smb_namespace_path *a,
    const struct chimera_smb_namespace_path *b)
{
    return identity(a->parent_fh, a->parent_fh_len, b->parent_fh, b->parent_fh_len) &&
           a->name_len == b->name_len && !memcmp(a->name, b->name, a->name_len);
} /* same_link */
static bool
same_view(
    const struct chimera_smb_namespace_path *a,
    const struct chimera_smb_namespace_path *b)
{
    return identity(a->view_fh, a->view_fh_len, b->view_fh, b->view_fh_len);
} /* same_view */

static bool
covers(
    const struct chimera_smb_namespace_edit *edit,
    const uint8_t                           *fh,
    uint32_t                                 len)
{
    if (edit->global) {
        return true;
    }
    for (uint32_t i = 0; i < edit->object_count; i++) {
        if (identity(edit->objects[i].fh, edit->objects[i].fh_len, fh, len)) {
            return true;
        }
    }
    return false;
} /* covers */

/* Caller holds registry.lock. */
static bool
busy(
    struct chimera_smb_namespace_registry   *registry,
    const uint8_t                           *fh,
    uint32_t                                 len,
    const struct chimera_smb_namespace_edit *except)
{
    for (struct chimera_smb_namespace_edit *e = registry->active; e; e = e->next) {
        if (e != except && covers(e, fh, len)) {
            return true;
        }
    }
    return false;
} /* busy */

void
chimera_smb_namespace_init(struct chimera_smb_namespace_registry *registry)
{
    memset(registry, 0, sizeof(*registry));
    pthread_mutex_init(&registry->lock, NULL);
} /* chimera_smb_namespace_init */

void
chimera_smb_namespace_destroy(struct chimera_smb_namespace_registry *registry)
{
    assert(!registry->active && !registry->participants && !registry->doc_fences && !registry->view_fences &&
        !registry->constructors && !registry->replacements);
    pthread_mutex_destroy(&registry->lock);
} /* chimera_smb_namespace_destroy */

bool
chimera_smb_namespace_open_begin(struct chimera_smb_namespace_open_token *token,
    struct chimera_smb_namespace_registry *registry, const void *owner)
{
    assert(owner);
    pthread_mutex_lock(&registry->lock);
    if (token->registry) {
        bool same = token->registry == registry && token->owner == owner;
        pthread_mutex_unlock(&registry->lock);
        return same;
    }
    for (struct chimera_smb_namespace_replace_token *r = registry->replacements; r; r = r->next) {
        if (r->owner != owner) {
            pthread_mutex_unlock(&registry->lock);
            return false;
        }
    }
    token->registry = registry;
    token->owner = owner;
    token->next = registry->constructors;
    registry->constructors = token;
    pthread_mutex_unlock(&registry->lock);
    return true;
}

void
chimera_smb_namespace_open_end(struct chimera_smb_namespace_open_token *token)
{
    if (!token || !token->registry) return;
    struct chimera_smb_namespace_registry *registry = token->registry;
    pthread_mutex_lock(&registry->lock);
    struct chimera_smb_namespace_open_token **p = &registry->constructors;
    while (*p && *p != token) p = &(*p)->next;
    assert(*p == token);
    *p = token->next;
    memset(token, 0, sizeof(*token));
    pthread_mutex_unlock(&registry->lock);
}

bool
chimera_smb_namespace_replace_begin(struct chimera_smb_namespace_replace_token *token,
    struct chimera_smb_namespace_registry *registry, const void *owner)
{
    assert(owner);
    pthread_mutex_lock(&registry->lock);
    if (token->registry) {
        bool same = token->registry == registry && token->owner == owner;
        pthread_mutex_unlock(&registry->lock);
        return same;
    }
    for (struct chimera_smb_namespace_replace_token *r = registry->replacements; r; r = r->next) {
        if (r->owner != owner) goto busy;
    }
    for (struct chimera_smb_namespace_open_token *o = registry->constructors; o; o = o->next) {
        if (o->owner != owner) goto busy;
    }
    token->registry = registry;
    token->owner = owner;
    token->next = registry->replacements;
    registry->replacements = token;
    pthread_mutex_unlock(&registry->lock);
    return true;
busy:
    pthread_mutex_unlock(&registry->lock);
    return false;
}

void
chimera_smb_namespace_replace_end(struct chimera_smb_namespace_replace_token *token)
{
    if (!token || !token->registry) return;
    struct chimera_smb_namespace_registry *registry = token->registry;
    pthread_mutex_lock(&registry->lock);
    struct chimera_smb_namespace_replace_token **p = &registry->replacements;
    while (*p && *p != token) p = &(*p)->next;
    assert(*p == token);
    *p = token->next;
    memset(token, 0, sizeof(*token));
    pthread_mutex_unlock(&registry->lock);
}

bool
chimera_smb_namespace_identity_present_locked(struct chimera_smb_namespace_registry *registry,
    const uint8_t *fh, uint32_t fh_len)
{
    for (struct chimera_smb_namespace_participant *p = registry->participants; p; p = p->next) {
        if (identity(p->fh, p->fh_len, fh, fh_len)) return true;
    }
    return false;
}

bool
chimera_smb_namespace_directory_peer_allowed(const uint8_t *source_fh, uint32_t source_fh_len,
    const struct chimera_smb_namespace_path *source_path,
    const uint8_t *peer_fh, uint32_t peer_fh_len,
    const struct chimera_smb_namespace_path *peer_path)
{
    if (!peer_fh_len || !path_valid(peer_path) || !same_view(source_path, peer_path)) return false;
    if (identity(source_fh, source_fh_len, peer_fh, peer_fh_len) ||
        identity(source_path->view_fh, source_path->view_fh_len, peer_fh, peer_fh_len)) return true;
    return identity(peer_path->parent_fh, peer_path->parent_fh_len,
                    source_path->view_fh, source_path->view_fh_len) &&
        peer_path->name_len && peer_path->name_len == peer_path->full_path_len &&
        !(peer_path->name_len == 1 && peer_path->name[0] == '.') &&
        !(peer_path->name_len == 2 && !memcmp(peer_path->name, "..", 2)) &&
        !memcmp(peer_path->name, peer_path->full_path, peer_path->name_len) &&
        !memchr(peer_path->name, '/', peer_path->name_len) &&
        !memchr(peer_path->name, '\\', peer_path->name_len);
}

bool
chimera_smb_namespace_directory_isolated_locked(struct chimera_smb_namespace_registry *registry,
    const uint8_t *source_fh, uint32_t source_fh_len,
    const struct chimera_smb_namespace_path *path)
{
    for (struct chimera_smb_namespace_participant *p = registry->participants; p; p = p->next) {
        /* An unresolved constructor cannot establish which subtree it will
         * publish into. Closing rows still carry paths until actual detach. */
        if ((p->pending && !p->bound) || p->invalid_path ||
            !chimera_smb_namespace_directory_peer_allowed(source_fh, source_fh_len,
                path, p->fh, p->fh_len, &p->snapshot.path)) return false;
    }
    return true;
}

bool
chimera_smb_namespace_rebind_locked(struct chimera_smb_namespace_participant *p,
    const uint8_t *old_fh, uint32_t old_fh_len, const uint8_t *new_fh, uint32_t new_fh_len)
{
    if (!p || !p->registry || p->pending || !new_fh || !new_fh_len ||
        new_fh_len > CHIMERA_VFS_FH_SIZE || !identity(p->fh, p->fh_len, old_fh, old_fh_len)) return false;
    memcpy(p->fh, new_fh, new_fh_len);
    p->fh_len = new_fh_len;
    return true;
}

struct chimera_smb_namespace_participant *
chimera_smb_namespace_participant_reserve(
    void                           *context,
    chimera_smb_namespace_context_t retain,
    chimera_smb_namespace_context_t release,
    chimera_smb_namespace_publish_t publish)
{
    struct chimera_smb_namespace_participant *p = calloc(1, sizeof(*p));

    if (!p) {
        return NULL;
    }
    p->context = context;
    p->release = release;
    p->publish = publish;
    if (retain) {
        retain(context);
    }
    return p;
} /* chimera_smb_namespace_participant_reserve */

bool
chimera_smb_namespace_participant_bind(
    struct chimera_smb_namespace_participant    *p,
    const uint8_t                               *fh,
    uint32_t                                     fh_len,
    const struct chimera_smb_namespace_snapshot *snapshot)
{
    if (!p || p->registry || !fh || !fh_len || fh_len > CHIMERA_VFS_FH_SIZE ||
        !snapshot || !path_valid(&snapshot->path)) {
        return false;
    }
    p->fh_len = fh_len;
    memcpy(p->fh, fh, fh_len);
    p->snapshot = *snapshot;
    return true;
} /* chimera_smb_namespace_participant_bind */

struct chimera_smb_namespace_participant *
chimera_smb_namespace_participant_alloc(
    const uint8_t                               *fh,
    uint32_t                                     fh_len,
    const struct chimera_smb_namespace_snapshot *snapshot,
    void                                        *context,
    chimera_smb_namespace_context_t              retain,
    chimera_smb_namespace_context_t              release,
    chimera_smb_namespace_publish_t              publish)
{
    struct chimera_smb_namespace_participant *p = chimera_smb_namespace_participant_reserve(
        context, retain, release, publish);

    if (!p) {
        return NULL;
    }
    if (!chimera_smb_namespace_participant_bind(p, fh, fh_len, snapshot)) {
        chimera_smb_namespace_participant_free(p);
        return NULL;
    }
    return p;
} /* chimera_smb_namespace_participant_alloc */

void
chimera_smb_namespace_participant_free(struct chimera_smb_namespace_participant *p)
{
    if (!p) {
        return;
    }
    assert(!p->registry);
    if (p->release) {
        p->release(p->context);
    }
    free(p);
} /* chimera_smb_namespace_participant_free */

bool
chimera_smb_namespace_attach(
    struct chimera_smb_namespace_registry    *registry,
    struct chimera_smb_namespace_participant *p,
    const struct chimera_smb_namespace_edit  *own_guard)
{
    pthread_mutex_lock(&registry->lock);
    if (p->registry) {
        bool ok = p->registry == registry && p->pending && p->bound && !p->invalid_path;
        if (ok) {
            /* The constructor's private open changes only on its own accepted
             * publication, never from another request's rename callback. */
            if (p->publish) {
                p->publish(p->context, &p->snapshot);
            }
            p->pending = false;
        }
        pthread_mutex_unlock(&registry->lock);
        return ok;
    }
    if (!p->fh_len) {
        pthread_mutex_unlock(&registry->lock);
        return false;
    }
    if ((own_guard && (own_guard->registry != registry || !own_guard->acquired || own_guard->change_count ||
                       !covers(own_guard, p->fh, p->fh_len))) ||
        busy(registry, p->fh, p->fh_len, own_guard)) {
        pthread_mutex_unlock(&registry->lock);
        return false;
    }
    p->registry            = registry;
    p->next                = registry->participants;
    registry->participants = p;
    pthread_mutex_unlock(&registry->lock);
    return true;
} /* chimera_smb_namespace_attach */

bool
chimera_smb_namespace_promote_stream_pair(
    struct chimera_smb_namespace_registry *registry,
    struct chimera_smb_namespace_participant *base,
    struct chimera_smb_namespace_participant *stream,
    const uint8_t *fh, uint32_t fh_len,
    const struct chimera_smb_namespace_edit *own_guard)
{
    if (!registry || !base || !stream || base == stream || !fh || !fh_len ||
        fh_len > CHIMERA_VFS_FH_SIZE) { return false; }
    pthread_mutex_lock(&registry->lock);
    bool valid = base->registry == registry && base->pending && base->bound &&
        !base->invalid_path && !stream->registry && stream->fh_len &&
        path_valid(&base->snapshot.path) && path_valid(&stream->snapshot.path);
    if (valid && ((own_guard && (own_guard->registry != registry ||
            !own_guard->acquired || own_guard->change_count || !covers(own_guard, fh, fh_len))) ||
            busy(registry, fh, fh_len, own_guard))) { valid = false; }
    if (valid) {
        /* A rename already admitted before stream-holder publication can
         * still finish now. It must observe either the pending base alone or
         * both fully attached identities, never a half-published pair. */
        stream->snapshot.path = base->snapshot.path;
        stream->fh_len = fh_len;
        memcpy(stream->fh, fh, fh_len);
        if (base->publish) { base->publish(base->context, &base->snapshot); }
        base->pending = false;
        stream->registry = registry;
        stream->next = registry->participants;
        registry->participants = stream;
    }
    pthread_mutex_unlock(&registry->lock);
    return valid;
}

struct chimera_smb_namespace_participant *
chimera_smb_namespace_register(
    struct chimera_smb_namespace_registry       *registry,
    const uint8_t                               *fh,
    uint32_t                                     fh_len,
    const struct chimera_smb_namespace_snapshot *snapshot,
    void                                        *context,
    chimera_smb_namespace_context_t              retain,
    chimera_smb_namespace_context_t              release,
    chimera_smb_namespace_publish_t              publish)
{
    struct chimera_smb_namespace_participant *p = chimera_smb_namespace_participant_alloc(
        fh, fh_len, snapshot, context, retain, release, publish);

    if (!p) {
        return NULL;
    }
    if (!chimera_smb_namespace_attach(registry, p, NULL)) {
        chimera_smb_namespace_participant_free(p);
        return NULL;
    }
    return p;
} /* chimera_smb_namespace_register */

void
chimera_smb_namespace_lock(struct chimera_smb_namespace_registry *registry)
{
    pthread_mutex_lock(&registry->lock);
} /* chimera_smb_namespace_lock */

void
chimera_smb_namespace_unlock(struct chimera_smb_namespace_registry *registry)
{
    pthread_mutex_unlock(&registry->lock);
} /* chimera_smb_namespace_unlock */

void
chimera_smb_namespace_foreach_locked(
    struct chimera_smb_namespace_registry *registry,
    const uint8_t                         *fh,
    uint32_t                               fh_len,
    chimera_smb_namespace_visit_t          visit,
    void                                  *private_data)
{
    if (!fh || !fh_len || fh_len > CHIMERA_VFS_FH_SIZE) {
        return;
    }
    for (struct chimera_smb_namespace_participant *p = registry->participants; p; p = p->next) {
        if (!p->pending && identity(p->fh, p->fh_len, fh, fh_len)) {
            visit(p->context, &p->snapshot, private_data);
        }
    }
} /* chimera_smb_namespace_foreach_locked */

void
chimera_smb_namespace_begin_close(struct chimera_smb_namespace_participant *p)
{
    pthread_mutex_lock(&p->registry->lock);
    p->snapshot.closing = true;
    pthread_mutex_unlock(&p->registry->lock);
} /* chimera_smb_namespace_begin_close */

void
chimera_smb_namespace_snapshot(
    struct chimera_smb_namespace_participant *p,
    struct chimera_smb_namespace_snapshot    *snapshot)
{
    pthread_mutex_lock(&p->registry->lock);
    *snapshot = p->snapshot;
    pthread_mutex_unlock(&p->registry->lock);
} /* chimera_smb_namespace_snapshot */

bool
chimera_smb_namespace_detach(
    struct chimera_smb_namespace_participant *p,
    const struct chimera_smb_namespace_edit  *own_guard)
{
    struct chimera_smb_namespace_registry *registry = p->registry;

    pthread_mutex_lock(&registry->lock);
    if (own_guard && (own_guard->registry != registry || !own_guard->acquired ||
                      !covers(own_guard, p->fh, p->fh_len))) {
        pthread_mutex_unlock(&registry->lock);
        return false;
    }
    if (!p->pending && busy(registry, p->fh, p->fh_len, own_guard)) {
        pthread_mutex_unlock(&registry->lock);
        return false;
    }
    struct chimera_smb_namespace_participant **link = &registry->participants;
    while (*link && *link != p) {
        link = &(*link)->next;
    }
    assert(*link == p);
    *link = p->next;
    pthread_mutex_unlock(&registry->lock);
    p->registry = NULL;
    chimera_smb_namespace_participant_free(p);
    return true;
} /* chimera_smb_namespace_detach */

struct chimera_smb_namespace_edit *
chimera_smb_namespace_edit_alloc(
    struct chimera_smb_namespace_registry *registry,
    uint32_t                               objects,
    uint32_t                               changes,
    bool                                   global)
{
    struct chimera_smb_namespace_edit *edit = calloc(1, sizeof(*edit));

    if (!edit) {
        return NULL;
    }
    edit->objects = objects ? calloc(objects, sizeof(*edit->objects)) : NULL;
    edit->changes = changes ? calloc(changes, sizeof(*edit->changes)) : NULL;
    if ((objects && !edit->objects) || (changes && !edit->changes)) {
        free(edit->objects); free(edit->changes); free(edit); return NULL;
    }
    edit->registry        = registry;
    edit->object_capacity = objects;
    edit->change_capacity = changes;
    edit->global          = global;
    return edit;
} /* chimera_smb_namespace_edit_alloc */

int
chimera_smb_namespace_edit_add(
    struct chimera_smb_namespace_edit *edit,
    const uint8_t                     *fh,
    uint32_t                           fh_len)
{
    if (edit->acquired || !fh || !fh_len || fh_len > CHIMERA_VFS_FH_SIZE) {
        return -1;
    }
    if (edit->object_count == edit->object_capacity) {
        return -1;
    }
    uint32_t index = edit->object_count++;
    edit->objects[index].fh_len = fh_len;
    memcpy(edit->objects[index].fh, fh, fh_len);
    return index;
} /* chimera_smb_namespace_edit_add */

bool
chimera_smb_namespace_edit_try_acquire(struct chimera_smb_namespace_edit *edit)
{
    struct chimera_smb_namespace_registry *registry = edit->registry;

    pthread_mutex_lock(&registry->lock);
    if (edit->acquired) {
        pthread_mutex_unlock(&registry->lock); return true;
    }
    if (edit->global && registry->active) {
        pthread_mutex_unlock(&registry->lock); return false;
    }
    for (uint32_t i = 0; i < edit->object_count; i++) {
        if (busy(registry, edit->objects[i].fh, edit->objects[i].fh_len, NULL)) {
            pthread_mutex_unlock(&registry->lock); return false;
        }
    }
    /* An empty local edit must still respect a global legacy fence. */
    for (struct chimera_smb_namespace_edit *e = registry->active; e; e = e->next) {
        if (e->global) {
            pthread_mutex_unlock(&registry->lock); return false;
        }
    }
    edit->next       = registry->active;
    registry->active = edit;
    edit->acquired   = true;
    pthread_mutex_unlock(&registry->lock);
    return true;
} /* chimera_smb_namespace_edit_try_acquire */

bool
chimera_smb_namespace_edit_seed(
    struct chimera_smb_namespace_edit       *edit,
    uint32_t                                 object,
    const struct chimera_smb_namespace_path *path)
{
    if (!edit->acquired || edit->published || object >= edit->object_count || !path_valid(path) ||
        edit->objects[object].seeded) {
        return false;
    }
    for (uint32_t i = 0; i < edit->object_count; i++) {
        if (i != object && edit->objects[i].seeded &&
            identity(edit->objects[i].fh, edit->objects[i].fh_len,
                     edit->objects[object].fh, edit->objects[object].fh_len) &&
            same_link(&edit->objects[i].initial, path) && same_view(&edit->objects[i].initial, path)) {
            return false;
        }
    }
    edit->objects[object].initial = edit->objects[object].current = *path;
    edit->objects[object].seeded  = true;
    return true;
} /* chimera_smb_namespace_edit_seed */

const struct chimera_smb_namespace_path *
chimera_smb_namespace_edit_path(
    const struct chimera_smb_namespace_edit *edit,
    const uint8_t                           *fh,
    uint32_t                                 fh_len,
    const struct chimera_smb_namespace_path *initial_link)
{
    if (!edit || !edit->acquired || !path_valid(initial_link) || !fh || !fh_len || fh_len > CHIMERA_VFS_FH_SIZE) {
        return NULL;
    }
    for (uint32_t i = 0; i < edit->object_count; i++) {
        const struct namespace_object *o = &edit->objects[i];
        if (o->seeded && identity(o->fh, o->fh_len, fh, fh_len) &&
            same_link(&o->initial, initial_link) && same_view(&o->initial, initial_link)) {
            return &o->current;
        }
    }
    return NULL;
} /* chimera_smb_namespace_edit_path */

int
chimera_smb_namespace_edit_prepare(
    struct chimera_smb_namespace_edit       *edit,
    uint32_t                                 object,
    const struct chimera_smb_namespace_path *next,
    const uint8_t                            parent_lease_key[16])
{
    if (!edit->acquired || edit->published || object >= edit->object_count ||
        !edit->objects[object].seeded || !path_valid(next) || edit->change_count == edit->change_capacity) {
        return -1;
    }
    /* One filesystem namespace step at a time, as with the compound executor. */
    if (edit->change_count && !edit->changes[edit->change_count - 1].completed) {
        return -1;
    }
    const struct namespace_object *object_path = &edit->objects[object];
    if (!same_view(&object_path->current, next)) {
        return -1;
    }
    /* A differing share root needs an independently prepared path translation.
     * Reject before mutation until that facility exists. Project earlier private
     * events so an A->B->C chain still checks peers whose public path is A. */
    pthread_mutex_lock(&edit->registry->lock);
    for (struct chimera_smb_namespace_participant *p = edit->registry->participants; p; p = p->next) {
        if (!identity(p->fh, p->fh_len, object_path->fh, object_path->fh_len)) {
            continue;
        }
        const struct chimera_smb_namespace_path *projected = &p->snapshot.path;
        for (uint32_t i = 0; i < edit->change_count; i++) {
            const struct chimera_smb_namespace_change *prior        = &edit->changes[i];
            const struct namespace_object             *prior_object = &edit->objects[prior->object];
            if (prior->success && identity(p->fh, p->fh_len, prior_object->fh, prior_object->fh_len) &&
                same_link(projected, &prior->before) && same_view(projected, &prior->before)) {
                projected = &prior->after;
            }
        }
        if (same_link(projected, &object_path->current) && !same_view(projected, next)) {
            pthread_mutex_unlock(&edit->registry->lock);
            return -1;
        }
    }
    pthread_mutex_unlock(&edit->registry->lock);
    uint32_t                             index  = edit->change_count++;
    struct chimera_smb_namespace_change *change = &edit->changes[index];
    memset(change, 0, sizeof(*change));
    change->object = object;
    change->before = edit->objects[object].current;
    change->after  = *next;
    if (parent_lease_key) {
        memcpy(change->parent_lease_key, parent_lease_key, 16);
    }
    return index;
} /* chimera_smb_namespace_edit_prepare */

void
chimera_smb_namespace_edit_complete(
    struct chimera_smb_namespace_edit *edit,
    uint32_t                           index,
    bool                               success)
{
    assert(edit->acquired && !edit->published && index < edit->change_count);
    struct chimera_smb_namespace_change *change = &edit->changes[index];
    assert(!change->completed);
    change->completed = true;
    change->success   = success;
    if (success) {
        edit->objects[change->object].current = change->after;
    }
} /* chimera_smb_namespace_edit_complete */

void
chimera_smb_namespace_edit_reset(struct chimera_smb_namespace_edit *edit)
{
    assert(edit->acquired && !edit->published);
    for (uint32_t i = 0; i < edit->object_count; i++) {
        edit->objects[i].current = edit->objects[i].initial;
    }
    edit->change_count = 0;
} /* chimera_smb_namespace_edit_reset */

void
chimera_smb_namespace_edit_publish(struct chimera_smb_namespace_edit *edit)
{
    assert(edit->acquired && !edit->published);
    struct chimera_smb_namespace_registry *registry = edit->registry;
    pthread_mutex_lock(&registry->lock);
    for (uint32_t i = 0; i < edit->change_count; i++) {
        const struct chimera_smb_namespace_change *c = &edit->changes[i];
        assert(c->completed);
        if (!c->success) {
            continue;
        }
        const struct namespace_object             *o = &edit->objects[c->object];
        for (struct chimera_smb_namespace_participant *p = registry->participants; p; p = p->next) {
            if (identity(p->fh, p->fh_len, o->fh, o->fh_len) &&
                same_link(&p->snapshot.path, &c->before)) {
                assert(same_view(&p->snapshot.path, &c->before));
                p->snapshot.path = c->after;
                if (p->publish) {
                    p->publish(p->context, &p->snapshot);
                }
            }
        }
    }
    edit->published = true;
    pthread_mutex_unlock(&registry->lock);
} /* chimera_smb_namespace_edit_publish */

const struct chimera_smb_namespace_change *
chimera_smb_namespace_edit_change(
    const struct chimera_smb_namespace_edit *edit,
    uint32_t                                 index)
{
    return index < edit->change_count ? &edit->changes[index] : NULL;
} /* chimera_smb_namespace_edit_change */

void
chimera_smb_namespace_edit_free(struct chimera_smb_namespace_edit *edit)
{
    if (!edit) {
        return;
    }
    if (edit->acquired) {
        pthread_mutex_lock(&edit->registry->lock);
        struct chimera_smb_namespace_edit **link = &edit->registry->active;
        while (*link && *link != edit) {
            link = &(*link)->next;
        }
        assert(*link == edit);
        *link = edit->next;
        pthread_mutex_unlock(&edit->registry->lock);
    }
    free(edit->objects); free(edit->changes); free(edit);
} /* chimera_smb_namespace_edit_free */

static bool
smb_doc_fence_conflict(
    struct chimera_smb_namespace_registry *registry,
    const uint8_t                         *fh,
    uint32_t                               fh_len,
    const void                            *owner)
{
    for (struct chimera_smb_doc_fence *f = registry->doc_fences; f; f = f->next) {
        if ((!owner || f->owner != owner) && identity(f->fh, f->fh_len, fh, fh_len)) {
            return true;
        }
    }
    return false;
} /* smb_doc_fence_conflict */

bool
chimera_smb_doc_fence_acquire(
    struct chimera_smb_doc_fence          *fence,
    struct chimera_smb_namespace_registry *registry,
    const uint8_t                         *fh,
    uint32_t                               fh_len,
    const void                            *owner)
{
    if (!fh || !fh_len || fh_len > CHIMERA_VFS_FH_SIZE || !owner) {
        return false;
    }
    pthread_mutex_lock(&registry->lock);
    if (fence->acquired) {
        bool same = fence->registry == registry && fence->owner == owner &&
            identity(fence->fh, fence->fh_len, fh, fh_len);
        pthread_mutex_unlock(&registry->lock);
        return same;
    }
    if (smb_doc_fence_conflict(registry, fh, fh_len, owner)) {
        pthread_mutex_unlock(&registry->lock);
        return false;
    }
    fence->registry = registry;
    fence->owner    = owner;
    fence->fh_len   = fh_len;
    memcpy(fence->fh, fh, fh_len);
    fence->acquired      = true;
    fence->next          = registry->doc_fences;
    registry->doc_fences = fence;
    pthread_mutex_unlock(&registry->lock);
    return true;
} /* chimera_smb_doc_fence_acquire */

SYMBOL_EXPORT void
chimera_smb_doc_fence_release(struct chimera_smb_doc_fence *fence)
{
    if (!fence->acquired) {
        return;
    }
    struct chimera_smb_namespace_registry *registry = fence->registry;
    pthread_mutex_lock(&registry->lock);
    struct chimera_smb_doc_fence         **link = &registry->doc_fences;
    while (*link != fence) {
        assert(*link); link = &(*link)->next;
    }
    *link           = fence->next;
    fence->next     = NULL;
    fence->acquired = false;
    pthread_mutex_unlock(&registry->lock);
} /* chimera_smb_doc_fence_release */

bool
chimera_smb_doc_mutation_begin(
    struct chimera_smb_namespace_registry *registry,
    const uint8_t                         *fh,
    uint32_t                               fh_len,
    const void                            *owner)
{
    pthread_mutex_lock(&registry->lock);
    if (smb_doc_fence_conflict(registry, fh, fh_len, owner)) {
        pthread_mutex_unlock(&registry->lock);
        return false;
    }
    return true;
} /* chimera_smb_doc_mutation_begin */

void
chimera_smb_doc_mutation_end(struct chimera_smb_namespace_registry *registry)
{
    pthread_mutex_unlock(&registry->lock);
} /* chimera_smb_doc_mutation_end */

/* Caller owns registry.lock; update the authoritative view after an accepted
 * same-parent rename. The frontend has already updated its protocol fields. */
void
chimera_smb_namespace_set_path_locked(
    struct chimera_smb_namespace_participant *participant,
    const struct chimera_smb_namespace_path  *path)
{
    if (participant) {
        participant->snapshot.path = *path;
    }
} /* chimera_smb_namespace_set_path_locked */

bool
chimera_smb_namespace_attached(const struct chimera_smb_namespace_participant *p)
{
    return p && p->registry;
} /* chimera_smb_namespace_attached */

bool
chimera_smb_namespace_pending_begin(
    struct chimera_smb_namespace_registry       *registry,
    struct chimera_smb_namespace_participant    *p,
    const struct chimera_smb_namespace_snapshot *initial)
{
    if (!p || !initial || !path_valid(&initial->path)) {
        return false;
    }
    pthread_mutex_lock(&registry->lock);
    const struct chimera_smb_namespace_path *candidate = p->registry ? &p->snapshot.path : &initial->path;
    for (struct chimera_smb_namespace_view_fence *f = registry->view_fences; f; f = f->next) {
        if ((same_link(candidate, &f->before) || same_link(&initial->path, &f->before)) &&
            !same_view(&initial->path, &f->before)) {
            pthread_mutex_unlock(&registry->lock); return false;
        }
    }
    if (p->registry && (p->registry != registry || !p->pending)) {
        pthread_mutex_unlock(&registry->lock); return false;
    }
    if (!p->registry || !same_link(&p->initial.path, &initial->path) ||
        !same_view(&p->initial.path, &initial->path) ||
        p->initial.path.full_path_len != initial->path.full_path_len ||
        memcmp(p->initial.path.full_path, initial->path.full_path, initial->path.full_path_len)) {
        p->initial      = p->snapshot = *initial;
        p->fh_len       = 0;
        p->invalid_path = false;
    }
    p->bound   = false;
    p->pending = true;
    if (!p->registry) {
        p->registry            = registry;
        p->next                = registry->participants;
        registry->participants = p;
    }
    pthread_mutex_unlock(&registry->lock);
    return true;
} /* chimera_smb_namespace_pending_begin */

bool
chimera_smb_namespace_pending_bind(
    struct chimera_smb_namespace_participant *p,
    const uint8_t                            *fh,
    uint32_t                                  fh_len)
{
    if (!p || !p->registry || !fh || !fh_len || fh_len > CHIMERA_VFS_FH_SIZE) {
        return false;
    }
    pthread_mutex_lock(&p->registry->lock);
    if (!p->pending) {
        pthread_mutex_unlock(&p->registry->lock); return false;
    }
    if (p->fh_len && !identity(p->fh, p->fh_len, fh, fh_len)) {
        /* The original spelling was rebound to a different inode. An accepted
         * rename of the former link is not input for this new open. */
        p->snapshot     = p->initial;
        p->invalid_path = false;
    }
    memcpy(p->fh, fh, fh_len);
    p->fh_len = fh_len;
    p->bound  = true;
    bool valid = !p->invalid_path;
    pthread_mutex_unlock(&p->registry->lock);
    return valid;
} /* chimera_smb_namespace_pending_bind */

bool
chimera_smb_namespace_pending_snapshot(
    struct chimera_smb_namespace_participant *p,
    struct chimera_smb_namespace_snapshot    *snapshot)
{
    if (!p || !p->registry) {
        return false;
    }
    pthread_mutex_lock(&p->registry->lock);
    bool found = p->pending && p->bound && !p->invalid_path;
    if (found) {
        *snapshot = p->snapshot;
    }
    pthread_mutex_unlock(&p->registry->lock);
    return found;
} /* chimera_smb_namespace_pending_snapshot */

static bool
pending_rename_matches(
    struct chimera_smb_namespace_participant *p,
    const uint8_t                            *fh,
    uint32_t                                  fh_len,
    const struct chimera_smb_namespace_path  *before)
{
    return p->pending && (!p->fh_len || identity(p->fh, p->fh_len, fh, fh_len)) &&
           same_link(&p->snapshot.path, before);
} /* pending_rename_matches */

static bool
pending_path_fits(
    const struct chimera_smb_namespace_path *path,
    uint32_t                                 name_len)
{
    return path->full_path_len >= path->name_len &&
           !memcmp(path->full_path + path->full_path_len - path->name_len, path->name, path->name_len) &&
           path->full_path_len - path->name_len + name_len < SMB_PATH_MAX;
} /* pending_path_fits */

bool
chimera_smb_namespace_pending_rename_check_locked(
    struct chimera_smb_namespace_registry   *registry,
    const uint8_t                           *fh,
    uint32_t                                 fh_len,
    const struct chimera_smb_namespace_path *before,
    uint32_t                                 name_len)
{
    for (struct chimera_smb_namespace_participant *p = registry->participants; p; p = p->next) {
        if (pending_rename_matches(p, fh, fh_len, before) && !pending_path_fits(&p->snapshot.path, name_len)) {
            return false;
        }
    }
    return true;
} /* chimera_smb_namespace_pending_rename_check_locked */

void
chimera_smb_namespace_pending_rename_locked(
    struct chimera_smb_namespace_registry   *registry,
    const uint8_t                           *fh,
    uint32_t                                 fh_len,
    const struct chimera_smb_namespace_path *before,
    const char                              *name,
    uint32_t                                 name_len)
{
    assert(name_len <= SMB_FILENAME_MAX);
    for (struct chimera_smb_namespace_participant *p = registry->participants; p; p = p->next) {
        if (!pending_rename_matches(p, fh, fh_len, before)) {
            continue;
        }
        struct chimera_smb_namespace_path *path = &p->snapshot.path;
        if (!pending_path_fits(path, name_len)) {
            /* A new invisible token may arrive after validation. It cannot
             * acquire ACCESS through the held fence; its bind fails before
             * exposing an open whose translated path exceeds the protocol cap. */
            p->invalid_path = true;
        } else {
            uint32_t prefix = path->full_path_len - path->name_len;
            memcpy(path->full_path + prefix, name, name_len);
            path->full_path_len                  = prefix + name_len;
            path->full_path[path->full_path_len] = 0;
        }
        path->name_len = name_len;
        memcpy(path->name, name, name_len);
        path->name[name_len] = 0;
        if (!p->fh_len) {
            memcpy(p->fh, fh, fh_len); p->fh_len = fh_len;
        }
    }
} /* chimera_smb_namespace_pending_rename_locked */

/* Legacy moves can carry a destination path in the initiating share's view.
 * Matching remains link-specific and never calls into the private producer. */
void
chimera_smb_namespace_pending_move_locked(
    struct chimera_smb_namespace_registry   *registry,
    const uint8_t                           *fh,
    uint32_t                                 fh_len,
    const struct chimera_smb_namespace_path *before,
    const struct chimera_smb_namespace_path *after)
{
    bool same_parent = identity(before->parent_fh, before->parent_fh_len,
                                after->parent_fh, after->parent_fh_len);

    for (struct chimera_smb_namespace_participant *p = registry->participants; p; p = p->next) {
        if (!pending_rename_matches(p, fh, fh_len, before)) {
            continue;
        }
        struct chimera_smb_namespace_path *path = &p->snapshot.path;
        if (!same_parent && same_view(path, after)) {
            path->full_path_len = after->full_path_len;
            memcpy(path->full_path, after->full_path, after->full_path_len + 1);
        } else if (pending_path_fits(path, after->name_len)) {
            /* For differing views, preserve the peer's own prefix. A move
             * between parents still needs a share-relative translation for
             * name queries; parent/name identity below is nevertheless exact. */
            uint32_t prefix = path->full_path_len - path->name_len;
            memcpy(path->full_path + prefix, after->name, after->name_len);
            path->full_path_len                  = prefix + after->name_len;
            path->full_path[path->full_path_len] = 0;
        } else {
            p->invalid_path = true;
        }
        path->parent_fh_len = after->parent_fh_len;
        memcpy(path->parent_fh, after->parent_fh, after->parent_fh_len);
        path->name_len = after->name_len;
        memcpy(path->name, after->name, after->name_len + 1);
        if (!p->fh_len) {
            memcpy(p->fh, fh, fh_len); p->fh_len = fh_len;
        }
    }
} /* chimera_smb_namespace_pending_move_locked */

bool
chimera_smb_namespace_view_fence_acquire(
    struct chimera_smb_namespace_view_fence *fence,
    struct chimera_smb_namespace_registry   *registry,
    const uint8_t                           *fh,
    uint32_t                                 fh_len,
    const struct chimera_smb_namespace_path *before)
{
    assert(!fence->registry || fence->registry == registry);
    pthread_mutex_lock(&registry->lock);
    for (struct chimera_smb_namespace_participant *p = registry->participants; p; p = p->next) {
        if (pending_rename_matches(p, fh, fh_len, before) && !same_view(&p->snapshot.path, before)) {
            pthread_mutex_unlock(&registry->lock); return false;
        }
    }
    fence->before = *before;
    fence->fh_len = fh_len;
    memcpy(fence->fh, fh, fh_len);
    if (!fence->registry) {
        fence->registry       = registry;
        fence->next           = registry->view_fences;
        registry->view_fences = fence;
    }
    pthread_mutex_unlock(&registry->lock);
    return true;
} /* chimera_smb_namespace_view_fence_acquire */

void
chimera_smb_namespace_view_fence_release(struct chimera_smb_namespace_view_fence *fence)
{
    struct chimera_smb_namespace_registry    *registry = fence->registry;

    if (!registry) {
        return;
    }
    pthread_mutex_lock(&registry->lock);
    struct chimera_smb_namespace_view_fence **p = &registry->view_fences;
    while (*p && *p != fence) {
        p = &(*p)->next;
    }
    assert(*p == fence);
    *p              = fence->next;
    fence->registry = NULL;
    fence->next     = NULL;
    pthread_mutex_unlock(&registry->lock);
} /* chimera_smb_namespace_view_fence_release */
