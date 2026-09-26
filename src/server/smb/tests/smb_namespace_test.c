// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only

/* Namespace lifetime and path-overlay contracts. Real protocol integration
 * and command grouping are exercised separately by the SMB wire probes. */
#undef NDEBUG
#include <assert.h>
#include <string.h>
#include "server/smb/smb_namespace.h"

struct peer {
    unsigned int                          refs, publishes;
    struct chimera_smb_namespace_snapshot last;
};

static void retain(void *context) { ((struct peer *) context)->refs++; }
static void
release(void *context)
{
    struct peer *peer = context;

    assert(peer->refs);
    peer->refs--;
} /* release */
static void
publish(
    void                                        *context,
    const struct chimera_smb_namespace_snapshot *snapshot)
{
    struct peer *peer = context;

    assert(peer->refs);
    peer->publishes++;
    peer->last = *snapshot;
} /* publish */
static void
count(
    void                                        *context,
    const struct chimera_smb_namespace_snapshot *snapshot,
    void                                        *arg)
{
    struct peer  *peer   = context;
    unsigned int *counts = arg;

    assert(peer->refs);
    counts[0]++;
    counts[1] += snapshot->closing;
    counts[2] += snapshot->delete_on_close;
} /* count */
static struct chimera_smb_namespace_path
path(const char *name)
{
    struct chimera_smb_namespace_path value = { .view_fh_len = 1, .view_fh = { 8 }, .parent_fh_len = 1, .parent_fh = { 9
                                                } };

    value.name_len = value.full_path_len = strlen(name);
    memcpy(value.name, name, value.name_len + 1);
    memcpy(value.full_path, name, value.full_path_len + 1);
    return value;
} /* path */
static struct chimera_smb_namespace_edit *
edit(
    struct chimera_smb_namespace_registry *registry,
    const uint8_t                         *fh,
    unsigned int                           changes)
{
    struct chimera_smb_namespace_edit *value = chimera_smb_namespace_edit_alloc(registry, 1, changes, false);

    assert(value && chimera_smb_namespace_edit_add(value, fh, 1) == 0);
    return value;
} /* edit */

static void
directory_isolation(void)
{
    struct chimera_smb_namespace_registry registry;
    const uint8_t source[] = { 1 }, sibling[] = { 2 };
    struct chimera_smb_namespace_snapshot snapshot = { .path = path("directory") };
    snapshot.path.parent_fh[0] = snapshot.path.view_fh[0];
    struct peer source_peer = { 0 }, root_peer = { 0 }, other_peer = { 0 };
    chimera_smb_namespace_init(&registry);
    struct chimera_smb_namespace_participant *directory = chimera_smb_namespace_register(
        &registry, source, 1, &snapshot, &source_peer, retain, release, publish);
    assert(directory);
    struct chimera_smb_namespace_participant *root = chimera_smb_namespace_register(
        &registry, snapshot.path.view_fh, 1, &snapshot, &root_peer, retain, release, publish);
    assert(root);
    chimera_smb_namespace_lock(&registry);
    assert(chimera_smb_namespace_directory_isolated_locked(&registry, source, 1, &snapshot.path));
    chimera_smb_namespace_unlock(&registry);
    struct chimera_smb_namespace_snapshot root_sibling = { .path = path("sibling") };
    root_sibling.path.parent_fh[0] = root_sibling.path.view_fh[0];
    struct chimera_smb_namespace_participant *other = chimera_smb_namespace_register(
        &registry, sibling, 1, &root_sibling, &other_peer, retain, release, publish);
    assert(other);
    chimera_smb_namespace_begin_close(other);
    chimera_smb_namespace_lock(&registry);
    assert(chimera_smb_namespace_directory_isolated_locked(&registry, source, 1, &snapshot.path));
    struct chimera_smb_namespace_path descendant = path("child");
    descendant.parent_fh[0] = source[0];
    strcpy(descendant.full_path, "directory\\child");
    descendant.full_path_len = strlen(descendant.full_path);
    chimera_smb_namespace_set_path_locked(other, &descendant);
    assert(!chimera_smb_namespace_directory_isolated_locked(&registry, source, 1, &snapshot.path));
    chimera_smb_namespace_unlock(&registry);
    assert(chimera_smb_namespace_detach(other, NULL));

    /* Pending constructors reject until their identity/path resolves. A bound
     * root sibling is allowed, but a bound descendant still excludes a move. */
    other = chimera_smb_namespace_participant_reserve(&other_peer, retain, release, publish);
    assert(other && chimera_smb_namespace_pending_begin(&registry, other, &snapshot));
    chimera_smb_namespace_lock(&registry);
    assert(!chimera_smb_namespace_directory_isolated_locked(&registry, source, 1, &snapshot.path));
    chimera_smb_namespace_unlock(&registry);
    assert(chimera_smb_namespace_pending_bind(other, sibling, 1));
    chimera_smb_namespace_lock(&registry);
    assert(chimera_smb_namespace_directory_isolated_locked(&registry, source, 1, &snapshot.path));
    chimera_smb_namespace_set_path_locked(other, &descendant);
    assert(!chimera_smb_namespace_directory_isolated_locked(&registry, source, 1, &snapshot.path));
    chimera_smb_namespace_unlock(&registry);
    assert(chimera_smb_namespace_pending_bind(other, source, 1));
    chimera_smb_namespace_lock(&registry);
    assert(chimera_smb_namespace_directory_isolated_locked(&registry, source, 1, &snapshot.path));
    chimera_smb_namespace_unlock(&registry);
    assert(chimera_smb_namespace_detach(other, NULL));

    /* The same inode from another share root still needs path translation. */
    struct chimera_smb_namespace_snapshot foreign = snapshot;
    foreign.path.view_fh[0]++;
    other = chimera_smb_namespace_register(&registry, source, 1, &foreign,
        &other_peer, retain, release, publish);
    assert(other);
    chimera_smb_namespace_lock(&registry);
    assert(!chimera_smb_namespace_directory_isolated_locked(&registry, source, 1, &snapshot.path));
    chimera_smb_namespace_unlock(&registry);
    assert(chimera_smb_namespace_detach(other, NULL));
    assert(chimera_smb_namespace_detach(root, NULL));
    assert(chimera_smb_namespace_detach(directory, NULL));
    assert(!source_peer.refs && !root_peer.refs && !other_peer.refs);
    chimera_smb_namespace_destroy(&registry);
}

int
main(void)
{
    directory_isolation();
    struct chimera_smb_namespace_registry     registry;
    const uint8_t                             a[] = { 1 }, b[] = { 2 };
    struct chimera_smb_namespace_path         first = path("first"), second = path("second"), third = path("third");
    struct chimera_smb_namespace_snapshot     initial = { .path = first, .delete_on_close = true };
    struct peer                               peer1 = { 0 }, peer2 = { 0 }, refused = { 0 };

    chimera_smb_namespace_init(&registry);
    /* Prebackend constructors are readers; a replacement cannot jump ahead
     * of a delayed OPEN, and admission after writer acquisition is excluded.
     * A wire batch can promote its own constructor and compose writers. */
    struct chimera_smb_namespace_open_token constructor_a = { 0 }, constructor_b = { 0 };
    struct chimera_smb_namespace_replace_token writer_a = { 0 }, writer_a2 = { 0 }, writer_b = { 0 };
    assert(chimera_smb_namespace_open_begin(&constructor_a, &registry, &peer1));
    assert(chimera_smb_namespace_open_begin(&constructor_a, &registry, &peer1));
    assert(!chimera_smb_namespace_replace_begin(&writer_b, &registry, &peer2));
    assert(!writer_b.registry);
    assert(chimera_smb_namespace_replace_begin(&writer_a, &registry, &peer1));
    assert(chimera_smb_namespace_replace_begin(&writer_a2, &registry, &peer1));
    assert(!chimera_smb_namespace_open_begin(&constructor_b, &registry, &peer2));
    assert(!constructor_b.registry);
    chimera_smb_namespace_replace_end(&writer_a);
    assert(!chimera_smb_namespace_open_begin(&constructor_b, &registry, &peer2));
    chimera_smb_namespace_replace_end(&writer_a2);
    assert(chimera_smb_namespace_open_begin(&constructor_b, &registry, &peer2));
    assert(!chimera_smb_namespace_replace_begin(&writer_a, &registry, &peer1));
    chimera_smb_namespace_open_end(&constructor_b);
    chimera_smb_namespace_open_end(&constructor_a);
    assert(chimera_smb_namespace_replace_begin(&writer_b, &registry, &peer2));
    assert(!chimera_smb_namespace_replace_begin(&writer_a, &registry, &peer1));
    chimera_smb_namespace_replace_end(&writer_b);
    chimera_smb_namespace_open_end(&constructor_a); /* cleanup is idempotent */
    initial.doc_cred.uid = 1001;
    struct chimera_smb_namespace_participant *p1 = chimera_smb_namespace_register(&registry,
                                                                                  a, 1, &initial, &peer1, retain,
                                                                                  release, publish);
    initial.doc_cred.uid = 1002;
    struct chimera_smb_namespace_participant *p2 = chimera_smb_namespace_register(&registry,
                                                                                  a, 1, &initial, &peer2, retain,
                                                                                  release, publish);
    assert(p1 && p2 && peer1.refs == 1 && peer2.refs == 1);

    /* Provisional CREATE may rebind its private slot on retry; neither identity
     * is discoverable until accepted attachment, which allocates nothing. */
    struct chimera_smb_namespace_participant *private_slot =
        chimera_smb_namespace_participant_reserve(&refused, retain, release, publish);
    assert(private_slot && refused.refs == 1);
    assert(!chimera_smb_namespace_attach(&registry, private_slot, NULL));
    assert(chimera_smb_namespace_participant_bind(private_slot, a, 1, &initial));
    assert(chimera_smb_namespace_participant_bind(private_slot, b, 1, &initial));
    unsigned int                              private_counts[3] = { 0 };
    chimera_smb_namespace_lock(&registry);
    chimera_smb_namespace_foreach_locked(&registry, b, 1, count, private_counts);
    chimera_smb_namespace_unlock(&registry);
    assert(!private_counts[0]);
    assert(chimera_smb_namespace_attach(&registry, private_slot, NULL));
    assert(!chimera_smb_namespace_participant_bind(private_slot, a, 1, &initial));
    chimera_smb_namespace_lock(&registry);
    chimera_smb_namespace_foreach_locked(&registry, b, 1, count, private_counts);
    chimera_smb_namespace_unlock(&registry);
    assert(private_counts[0] == 1);
    assert(chimera_smb_namespace_detach(private_slot, NULL) && !refused.refs);

    /* Failure on one identity does not retain a different identity in the set. */
    struct chimera_smb_namespace_edit *held = edit(&registry, a, 0);
    assert(chimera_smb_namespace_edit_try_acquire(held));
    struct chimera_smb_namespace_edit *both = chimera_smb_namespace_edit_alloc(&registry, 2, 0, false);
    assert(both && chimera_smb_namespace_edit_add(both, b, 1) == 0);
    assert(chimera_smb_namespace_edit_add(both, a, 1) == 1);
    assert(!chimera_smb_namespace_edit_try_acquire(both));
    struct chimera_smb_namespace_edit *other = edit(&registry, b, 0);
    assert(chimera_smb_namespace_edit_try_acquire(other));
    chimera_smb_namespace_edit_free(other);
    chimera_smb_namespace_edit_free(held);
    assert(chimera_smb_namespace_edit_try_acquire(both));
    chimera_smb_namespace_edit_free(both);

    struct chimera_smb_namespace_edit *rename = edit(&registry, a, 2);
    assert(chimera_smb_namespace_edit_try_acquire(rename));
    assert(chimera_smb_namespace_edit_seed(rename, 0, &first));
    uint8_t                            key[16] = { 7 };
    assert(chimera_smb_namespace_edit_prepare(rename, 0, &second, key) == 0);
    assert(!strcmp(chimera_smb_namespace_edit_path(rename, a, 1, &first)->name, "first"));
    chimera_smb_namespace_edit_complete(rename, 0, true);
    assert(!strcmp(chimera_smb_namespace_edit_path(rename, a, 1, &first)->name, "second"));
    assert(!peer1.publishes && !peer2.publishes);

    /* Cache/ACCESS rights may already have drained. The closing peer is still
     * discoverable and anchored until DOC can consume the accepted path. */
    chimera_smb_namespace_begin_close(p2);
    assert(!chimera_smb_namespace_detach(p2, NULL));
    unsigned int                          counts[3] = { 0 };
    chimera_smb_namespace_lock(&registry);
    chimera_smb_namespace_foreach_locked(&registry, a, 1, count, counts);
    chimera_smb_namespace_unlock(&registry);
    assert(counts[0] == 2 && counts[1] == 1 && counts[2] == 2);
    assert(!chimera_smb_namespace_register(&registry, a, 1, &initial, &refused, retain, release, publish));
    assert(refused.refs == 0);
    struct chimera_smb_namespace_snapshot observed;
    chimera_smb_namespace_snapshot(p2, &observed);
    assert(observed.closing && !strcmp(observed.path.name, "first"));

    /* Rejection resets only private names/events, preserving close/lifetime. */
    chimera_smb_namespace_edit_reset(rename);
    assert(!chimera_smb_namespace_edit_change(rename, 0));
    assert(!strcmp(chimera_smb_namespace_edit_path(rename, a, 1, &first)->name, "first"));
    assert(chimera_smb_namespace_edit_prepare(rename, 0, &second, key) == 0);
    chimera_smb_namespace_edit_complete(rename, 0, false);
    assert(!strcmp(chimera_smb_namespace_edit_path(rename, a, 1, &first)->name, "first"));
    assert(chimera_smb_namespace_edit_prepare(rename, 0, &third, key) == 1);
    chimera_smb_namespace_edit_complete(rename, 1, true);
    assert(chimera_smb_namespace_edit_prepare(rename, 0, &second, key) == -1);
    const struct chimera_smb_namespace_change *event = chimera_smb_namespace_edit_change(rename, 1);
    assert(event && !strcmp(event->before.name, "first") && !strcmp(event->after.name, "third"));
    assert(event->parent_lease_key[0] == 7);
    chimera_smb_namespace_edit_publish(rename);
    assert(peer1.publishes == 1 && peer2.publishes == 1);
    assert(!strcmp(peer1.last.path.name, "third") && !strcmp(peer2.last.path.name, "third"));
    assert(peer2.last.closing && peer2.last.delete_on_close && peer2.last.doc_cred.uid == 1002);
    assert(peer1.last.doc_cred.uid == 1001);
    chimera_smb_namespace_edit_free(rename);
    assert(chimera_smb_namespace_detach(p1, NULL));
    assert(chimera_smb_namespace_detach(p2, NULL));
    assert(peer1.refs == 0 && peer2.refs == 0);

    /* Global legacy admission covers identities that become known only during
     * lookup. Preallocated publication into the caller's own guard cannot fail. */
    struct chimera_smb_namespace_edit *global = chimera_smb_namespace_edit_alloc(&registry, 0, 0, true);
    assert(global && chimera_smb_namespace_edit_try_acquire(global));
    other = edit(&registry, b, 0);
    assert(!chimera_smb_namespace_edit_try_acquire(other));
    p1 = chimera_smb_namespace_participant_alloc(a, 1, &initial, &peer1, retain, release, publish);
    assert(p1 && !chimera_smb_namespace_attach(&registry, p1, NULL));
    assert(chimera_smb_namespace_attach(&registry, p1, global));
    assert(!chimera_smb_namespace_detach(p1, NULL));
    assert(chimera_smb_namespace_detach(p1, global));
    chimera_smb_namespace_edit_free(global);
    assert(chimera_smb_namespace_edit_try_acquire(other));
    chimera_smb_namespace_edit_free(other);
    assert(!peer1.refs);
    /* Inode identity is only the guard key. Different hardlinks must retain
     * distinct paths, while repeated rename of one link follows its overlay. */
    struct chimera_smb_namespace_path alias = path("alias");
    peer1.publishes = peer2.publishes = 0;
    initial.path    = first;
    p1              = chimera_smb_namespace_register(&registry, a, 1, &initial, &peer1, retain, release, publish);
    initial.path    = alias;
    p2              = chimera_smb_namespace_register(&registry, a, 1, &initial, &peer2, retain, release, publish);
    assert(p1 && p2);
    rename = chimera_smb_namespace_edit_alloc(&registry, 2, 2, false);
    assert(rename && chimera_smb_namespace_edit_add(rename, a, 1) == 0);
    assert(chimera_smb_namespace_edit_add(rename, a, 1) == 1);
    assert(chimera_smb_namespace_edit_try_acquire(rename));
    assert(chimera_smb_namespace_edit_seed(rename, 0, &first));
    assert(chimera_smb_namespace_edit_seed(rename, 1, &alias));
    assert(chimera_smb_namespace_edit_prepare(rename, 0, &second, key) == 0);
    chimera_smb_namespace_edit_complete(rename, 0, true);
    assert(chimera_smb_namespace_edit_prepare(rename, 0, &third, key) == 1);
    chimera_smb_namespace_edit_complete(rename, 1, true);
    assert(!strcmp(chimera_smb_namespace_edit_path(rename, a, 1, &first)->name, "third"));
    assert(!strcmp(chimera_smb_namespace_edit_path(rename, a, 1, &alias)->name, "alias"));
    event = chimera_smb_namespace_edit_change(rename, 1);
    assert(event && !strcmp(event->before.name, "second"));
    chimera_smb_namespace_edit_publish(rename);
    assert(peer1.publishes == 2 && !peer2.publishes);
    chimera_smb_namespace_snapshot(p2, &observed);
    assert(!strcmp(observed.path.name, "alias"));
    chimera_smb_namespace_edit_free(rename);
    assert(chimera_smb_namespace_detach(p1, NULL) && chimera_smb_namespace_detach(p2, NULL));

    /* Identical namespace link viewed through different share roots needs a
    * prepared translation, not copying the initiating share's full_path. */
    initial.path = first;
    p1           = chimera_smb_namespace_register(&registry, a, 1, &initial, &peer1, retain, release, publish
                                                  );
    initial.path.view_fh[0] = 77;
    memcpy(initial.path.full_path, "nested/first", 13);
    initial.path.full_path_len = 12;
    p2                         = chimera_smb_namespace_register(&registry, a, 1, &initial, &peer2, retain, release,
                                                                publish);
    assert(p1 && p2);
    rename = edit(&registry, a, 1);
    assert(chimera_smb_namespace_edit_try_acquire(rename));
    assert(chimera_smb_namespace_edit_seed(rename, 0, &first));
    assert(chimera_smb_namespace_edit_prepare(rename, 0, &second, key) == -1);
    assert(!chimera_smb_namespace_edit_change(rename, 0));
    chimera_smb_namespace_edit_free(rename);
    chimera_smb_namespace_snapshot(p2, &observed);
    assert(!strcmp(observed.path.full_path, "nested/first"));
    assert(chimera_smb_namespace_detach(p1, NULL) && chimera_smb_namespace_detach(p2, NULL));
    assert(!peer1.refs && !peer2.refs);
    /* DOC-only close admission is nonblocking, composes within a wire batch,
     * and rejects intent changes without retaining a mutex on conflict. */
    struct chimera_smb_doc_fence close_a = { 0 }, close_a2 = { 0 }, close_b = { 0 };
    assert(chimera_smb_doc_fence_acquire(&close_a, &registry, a, 1, &peer1));
    assert(chimera_smb_doc_fence_acquire(&close_a, &registry, a, 1, &peer1));
    assert(chimera_smb_doc_fence_acquire(&close_a2, &registry, a, 1, &peer1));
    assert(!chimera_smb_doc_fence_acquire(&close_b, &registry, a, 1, &peer2));
    assert(!chimera_smb_doc_mutation_begin(&registry, a, 1, NULL));
    assert(chimera_smb_doc_mutation_begin(&registry, a, 1, &peer1));
    chimera_smb_doc_mutation_end(&registry);
    assert(chimera_smb_doc_fence_acquire(&close_b, &registry, b, 1, &peer2));
    chimera_smb_doc_fence_release(&close_a);
    assert(!chimera_smb_doc_mutation_begin(&registry, a, 1, NULL));
    chimera_smb_doc_fence_release(&close_a2);
    assert(chimera_smb_doc_mutation_begin(&registry, a, 1, NULL));
    chimera_smb_doc_mutation_end(&registry);
    chimera_smb_doc_fence_release(&close_b);

    /* An unpublished producer is invisible to DOC/peer scans, but accepted
     * rename updates its stable token rather than its private open/context. */
    struct peer                               producer = { 0 };
    initial.path = first;
    struct chimera_smb_namespace_participant *pending =
        chimera_smb_namespace_participant_reserve(&producer, retain, release, publish);
    assert(pending && producer.refs == 1);
    assert(chimera_smb_namespace_pending_begin(&registry, pending, &initial));
    unsigned int                              invisible[3] = { 0 };
    chimera_smb_namespace_lock(&registry);
    chimera_smb_namespace_foreach_locked(&registry, a, 1, count, invisible);
    assert(!invisible[0]);
    assert(chimera_smb_namespace_pending_rename_check_locked(&registry, a, 1, &first, second.name_len));
    chimera_smb_namespace_pending_rename_locked(&registry, a, 1, &first, second.name, second.name_len);
    chimera_smb_namespace_pending_rename_locked(&registry, a, 1, &second, third.name, third.name_len);
    chimera_smb_namespace_unlock(&registry);
    assert(!producer.publishes && !producer.last.path.name_len);
    assert(chimera_smb_namespace_pending_bind(pending, a, 1));
    assert(chimera_smb_namespace_pending_snapshot(pending, &observed));
    assert(!strcmp(observed.path.name, "third"));
    /* Rejection/reset repeats original request input but retains an accepted
     * external update if the next attempt obtains the same object. */
    assert(chimera_smb_namespace_pending_begin(&registry, pending, &initial));
    assert(chimera_smb_namespace_pending_bind(pending, a, 1));
    assert(chimera_smb_namespace_pending_snapshot(pending, &observed));
    assert(!strcmp(observed.path.name, "third"));
    assert(chimera_smb_namespace_attach(&registry, pending, NULL));
    assert(producer.publishes == 1 && !strcmp(producer.last.path.name, "third"));
    assert(chimera_smb_namespace_detach(pending, NULL) && !producer.refs);

    /* The original spelling can be rebound to another inode before OPEN's
     * result arrives. That result must not inherit the old inode's rename. */
    pending = chimera_smb_namespace_participant_reserve(&producer, retain, release, publish);
    assert(chimera_smb_namespace_pending_begin(&registry, pending, &initial));
    chimera_smb_namespace_lock(&registry);
    chimera_smb_namespace_pending_rename_locked(&registry, a, 1, &first, second.name, second.name_len);
    chimera_smb_namespace_pending_rename_locked(&registry, a, 1, &second, third.name, third.name_len);
    chimera_smb_namespace_unlock(&registry);
    assert(chimera_smb_namespace_pending_bind(pending, b, 1));
    assert(chimera_smb_namespace_pending_snapshot(pending, &observed));
    assert(!strcmp(observed.path.name, "first"));
    /* Discard can detach an invisible token even while a namespace guard is
     * active; it never waits while retaining the producer's claim journal. */
    rename = edit(&registry, b, 1);
    assert(chimera_smb_namespace_edit_try_acquire(rename));
    assert(chimera_smb_namespace_detach(pending, NULL) && !producer.refs);
    chimera_smb_namespace_edit_free(rename);

    /* A legacy cross-directory move can translate a pending open in the
     * same share view without touching a same-inode hardlink alias. */
    struct peer                               alias_producer = { 0 };
    pending = chimera_smb_namespace_participant_reserve(&producer, retain, release, publish);
    struct chimera_smb_namespace_participant *pending_alias =
        chimera_smb_namespace_participant_reserve(&alias_producer, retain, release, publish);
    initial.path = first;
    assert(chimera_smb_namespace_pending_begin(&registry, pending, &initial));
    initial.path = alias;
    assert(chimera_smb_namespace_pending_begin(&registry, pending_alias, &initial));
    struct chimera_smb_namespace_path         moved = third;
    moved.parent_fh[0] = 91;
    memcpy(moved.full_path, "sub/third", 10);
    moved.full_path_len = 9;
    chimera_smb_namespace_lock(&registry);
    chimera_smb_namespace_pending_move_locked(&registry, a, 1, &first, &moved);
    chimera_smb_namespace_unlock(&registry);
    assert(chimera_smb_namespace_pending_bind(pending, a, 1));
    assert(chimera_smb_namespace_pending_snapshot(pending, &observed));
    assert(observed.path.parent_fh[0] == 91 && !strcmp(observed.path.full_path, "sub/third"));
    assert(chimera_smb_namespace_pending_bind(pending_alias, a, 1));
    assert(chimera_smb_namespace_pending_snapshot(pending_alias, &observed));
    assert(!strcmp(observed.path.name, "alias"));
    assert(chimera_smb_namespace_detach(pending, NULL));
    assert(chimera_smb_namespace_detach(pending_alias, NULL));
    assert(!producer.refs && !alias_producer.refs);

    /* Cross-parent path validation excludes a late producer in another
     * share view, without publishing a private open or retaining it on denial. */
    struct chimera_smb_namespace_view_fence view_fence = { 0 };
    assert(chimera_smb_namespace_view_fence_acquire(&view_fence, &registry, a, 1, &first));
    pending                 = chimera_smb_namespace_participant_reserve(&producer, retain, release, publish);
    initial.path            = first;
    initial.path.view_fh[0] = 77;
    assert(!chimera_smb_namespace_pending_begin(&registry, pending, &initial));
    assert(!chimera_smb_namespace_attached(pending));
    initial.path = first;
    assert(chimera_smb_namespace_pending_begin(&registry, pending, &initial));
    assert(chimera_smb_namespace_detach(pending, NULL));
    chimera_smb_namespace_view_fence_release(&view_fence);
    pending                 = chimera_smb_namespace_participant_reserve(&producer, retain, release, publish);
    initial.path.view_fh[0] = 77;
    assert(chimera_smb_namespace_pending_begin(&registry, pending, &initial));
    assert(!chimera_smb_namespace_view_fence_acquire(&view_fence, &registry, a, 1, &first));
    assert(!view_fence.registry);
    assert(chimera_smb_namespace_detach(pending, NULL) && !producer.refs);

    /* Stream publication consumes the latest accepted base rename and
     * attaches both identities atomically; a guard conflict promotes neither. */
    struct peer base_peer = { 0 }, stream_peer = { 0 };
    struct chimera_smb_namespace_snapshot base_snapshot = { .path = first };
    struct chimera_smb_namespace_snapshot stream_snapshot = {
        .path = first, .delete_on_close = true, .doc_cred = { .uid = 777 }
    };
    struct chimera_smb_namespace_participant *base_token =
        chimera_smb_namespace_participant_reserve(&base_peer, retain, release, publish);
    struct chimera_smb_namespace_participant *stream_token =
        chimera_smb_namespace_participant_alloc(b, 1, &stream_snapshot,
            &stream_peer, retain, release, publish);
    assert(base_token && stream_token);
    assert(chimera_smb_namespace_pending_begin(&registry, base_token, &base_snapshot));
    assert(chimera_smb_namespace_pending_bind(base_token, a, 1));
    chimera_smb_namespace_lock(&registry);
    chimera_smb_namespace_pending_move_locked(&registry, a, 1, &first, &second);
    chimera_smb_namespace_unlock(&registry);
    struct chimera_smb_namespace_edit *stream_guard = edit(&registry, b, 1);
    assert(chimera_smb_namespace_edit_try_acquire(stream_guard));
    assert(!chimera_smb_namespace_promote_stream_pair(&registry, base_token,
        stream_token, b, 1, NULL));
    assert(chimera_smb_namespace_pending_snapshot(base_token, &observed));
    assert(base_peer.publishes == 0 && !chimera_smb_namespace_attached(stream_token));
    chimera_smb_namespace_edit_free(stream_guard);
    assert(chimera_smb_namespace_promote_stream_pair(&registry, base_token,
        stream_token, b, 1, NULL));
    assert(base_peer.publishes == 1 && !strcmp(base_peer.last.path.name, "second"));
    assert(!chimera_smb_namespace_pending_snapshot(base_token, &observed));
    chimera_smb_namespace_snapshot(stream_token, &observed);
    assert(!strcmp(observed.path.name, "second") && observed.delete_on_close &&
           observed.doc_cred.uid == 777);
    unsigned int paired_counts[3] = { 0 };
    chimera_smb_namespace_lock(&registry);
    chimera_smb_namespace_foreach_locked(&registry, a, 1, count, paired_counts);
    chimera_smb_namespace_foreach_locked(&registry, b, 1, count, paired_counts);
    chimera_smb_namespace_unlock(&registry);
    assert(paired_counts[0] == 2 && paired_counts[2] == 1);
    assert(chimera_smb_namespace_detach(base_token, NULL));
    assert(chimera_smb_namespace_detach(stream_token, NULL));
    assert(!base_peer.refs && !stream_peer.refs);

    /* Accepted identity migration keeps the exact link and DOC metadata;
     * invisible bound producers count as identity users before publication. */
    pending = chimera_smb_namespace_participant_reserve(&producer, retain, release, publish);
    initial.path = first;
    assert(chimera_smb_namespace_pending_begin(&registry, pending, &initial));
    assert(chimera_smb_namespace_pending_bind(pending, a, 1));
    chimera_smb_namespace_lock(&registry);
    assert(chimera_smb_namespace_identity_present_locked(&registry, a, 1));
    assert(!chimera_smb_namespace_rebind_locked(pending, a, 1, b, 1));
    chimera_smb_namespace_unlock(&registry);
    assert(chimera_smb_namespace_attach(&registry, pending, NULL));
    chimera_smb_namespace_lock(&registry);
    assert(!chimera_smb_namespace_rebind_locked(pending, b, 1, a, 1));
    assert(chimera_smb_namespace_rebind_locked(pending, a, 1, b, 1));
    assert(!chimera_smb_namespace_identity_present_locked(&registry, a, 1));
    assert(chimera_smb_namespace_identity_present_locked(&registry, b, 1));
    chimera_smb_namespace_unlock(&registry);
    chimera_smb_namespace_snapshot(pending, &observed);
    assert(!strcmp(observed.path.name, "first") && observed.delete_on_close == initial.delete_on_close &&
        observed.doc_cred.uid == initial.doc_cred.uid);
    assert(chimera_smb_namespace_detach(pending, NULL));

    chimera_smb_namespace_destroy(&registry);
    return 0;
} /* main */
