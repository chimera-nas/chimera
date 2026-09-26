// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only
#define _GNU_SOURCE
#undef NDEBUG
#include <assert.h>
#include <dlfcn.h>
#include <stdatomic.h>
#include "server/smb/smb_internal.h"
#include "server/smb/smb_compound.h"
#include "server/smb/smb_notify.h"

atomic_int                                notify_test_cleanups, notify_test_attached;
atomic_int                                notify_test_retired_session, notify_test_retired_freed;
atomic_int                                notify_test_wire_capture, notify_test_wire_count;
atomic_int                                notify_test_delete_cutoff;
static struct chimera_smb_notify_state   *wire_state;
static struct chimera_server_smb_thread  *wire_thread;
atomic_int                                notify_test_cross_capture, notify_test_cross_count, notify_test_cross_freed;
static struct chimera_smb_notify_request *cross_nr[2];
static struct chimera_smb_notify_state   *cross_state[2];
static pthread_t                          cross_thread[2];
static atomic_int                         cross_cleanup;
atomic_int                                notify_test_target_freed;
static evpl_mutex_t                       free_target_lock = EVPL_MUTEX_INITIALIZER;
static uint8_t                            free_target_file_id[16];
static bool                               free_target_enabled;

void
notify_test_track_free(const uint8_t file_id[16])
{
    evpl_mutex_lock(&free_target_lock);
    memcpy(free_target_file_id, file_id, sizeof(free_target_file_id));
    free_target_enabled = true;
    atomic_store(&notify_test_target_freed, 0);
    evpl_mutex_unlock(&free_target_lock);
} /* notify_test_track_free */


static void
test_state_put(struct chimera_smb_notify_state *state)
{
    typedef void (*fn)(
        struct chimera_smb_notify_state *);
    fn next = (fn) dlsym(RTLD_NEXT, "chimera_smb_notify_state_put");
    assert(next); next(state);
} /* test_state_put */

void
notify_test_inspect(
    void        *private_data,
    unsigned int expected)
{
    struct smb_vfs_command          *command = private_data;
    struct chimera_smb_open_file    *open    = command->open;
    unsigned int                     bucket  = open->file_id.vid & CHIMERA_SMB_OPEN_FILE_BUCKET_MASK;

    evpl_mutex_lock(&open->tree->open_files_lock[bucket]);
    struct chimera_smb_notify_state *state = open->notify_state;
    if (atomic_load(&notify_test_attached)) {
        assert(state && !(open->flags & CHIMERA_SMB_OPEN_FILE_CLOSED));
        evpl_mutex_lock(&state->lock);
        assert(!state->closing);
        evpl_mutex_unlock(&state->lock);
    } else {
        if (state) {
            fprintf(stderr, "notify inspect: unexpected attachment fid=%llx.%llx disconnecting=%u\n",
                    (unsigned long long) open->file_id.pid, (unsigned long long) open->file_id.vid,
                    command->request->compound->conn->disconnecting);
        }
        assert(!state);
    }
    evpl_mutex_unlock(&open->tree->open_files_lock[bucket]);
    unsigned int count = 0;
    for (struct chimera_smb_notify_request *nr = command->request->compound->conn->parked_notifies;
         nr; nr = nr->next) {
        if (nr->open_file != open) {
            continue;
        }
        if (atomic_load(&notify_test_attached)) {
            assert(!nr->admitting && nr->state == state);
            evpl_mutex_lock(&state->lock);
            assert(!nr->cleanup);
            evpl_mutex_unlock(&state->lock);
        } else {
            assert(nr->admitting && !nr->state);
        }
        assert(nr->session);
        evpl_mutex_lock(&nr->thread->shared->sessions_lock);
        assert(nr->session->compound_pins);
        evpl_mutex_unlock(&nr->thread->shared->sessions_lock);
        count++;
    }
    if (count != expected) {
        fprintf(stderr, "notify inspect: count=%u expected=%u fid=%llx.%llx disconnecting=%u attached=%d\n",
                count, expected, (unsigned long long) open->file_id.pid,
                (unsigned long long) open->file_id.vid,
                command->request->compound->conn->disconnecting,
                atomic_load(&notify_test_attached));
    }
    assert(count == expected);
} /* notify_test_inspect */

__attribute__((visibility("default"))) void
chimera_smb_notify_q_push(
    struct chimera_smb_notify_state   *state,
    struct chimera_smb_notify_request *nr)
{
    typedef void (*fn)(
        struct chimera_smb_notify_state *,
        struct chimera_smb_notify_request *);
    fn next = (fn) dlsym(RTLD_NEXT, "chimera_smb_notify_q_push");
    assert(next);
    next(state, nr);
    if (atomic_load(&notify_test_wire_capture)) {
        int index = atomic_load(&notify_test_wire_count);
        if (!index) {
            wire_state = state; wire_thread = nr->thread;
        } else {
            assert(index == 1 && wire_state == state && wire_thread != nr->thread);
        }
        atomic_store(&notify_test_wire_count, index + 1);
    }
    if (atomic_load(&notify_test_cross_capture)) {
        int index = atomic_load(&notify_test_cross_count);
        assert(index < 2);
        cross_nr[index]     = nr; cross_state[index] = state;
        cross_thread[index] = pthread_self();
        atomic_fetch_add(&state->refs, 1); /* fixture pin */
        if (index == 1) {
            assert(cross_nr[0]->thread != nr->thread);
        }
        atomic_store(&notify_test_cross_count, index + 1);
    }
} /* chimera_smb_notify_q_push */

__attribute__((visibility("default"))) void
chimera_smb_notify_close(
    struct chimera_vfs_notify       *vfs,
    struct chimera_smb_notify_state *state)
{
    typedef void (*fn)(
        struct chimera_vfs_notify *,
        struct chimera_smb_notify_state *);
    fn   next = (fn) dlsym(RTLD_NEXT, "chimera_smb_notify_close");
    assert(next);
    if (state) {
        atomic_fetch_add(&notify_test_cleanups, 1);
    }
    bool transfer = atomic_load(&notify_test_cross_count) == 2 && state == cross_state[0] &&
        !atomic_exchange(&cross_cleanup, 1);
    if (transfer) {
        assert(pthread_equal(pthread_self(), cross_thread[0]));
        struct chimera_smb_notify_state *other = cross_state[1];
        /* Internal lifecycle fixture, not SMB channel binding. Both requests
         * have real live owning workers/connections. Move only the second
         * request's queue membership/reference; its own open/tree stay intact. */
        evpl_mutex_lock(&state->lock);
        evpl_mutex_lock(&other->lock);
        assert(other->q_head == cross_nr[1] && other->q_tail == cross_nr[1]);
        assert(!cross_nr[1]->on_ready_queue);
        other->q_head       = other->q_tail = NULL;
        cross_nr[1]->q_next = NULL;
        cross_nr[1]->state  = state;
        atomic_fetch_add(&state->refs, 1);
        typedef void (*push_fn)(
            struct chimera_smb_notify_state *,
            struct chimera_smb_notify_request *);
        push_fn push = (push_fn) dlsym(RTLD_NEXT, "chimera_smb_notify_q_push");
        assert(push);
        push(state, cross_nr[1]);
        evpl_mutex_unlock(&other->lock);
        evpl_mutex_unlock(&state->lock);
        test_state_put(other); /* transfer old request reference */
        /* First nr is already ready on this worker when CLOSE marks cleanup.
         * The second nr must independently wake its different worker. */
        typedef void (*callback_fn)(
            struct chimera_vfs_notify_watch *,
            void *);
        callback_fn callback = (callback_fn) dlsym(RTLD_NEXT, "chimera_smb_notify_callback");
        assert(callback); callback(state->watch, state);
    }
    int cutoff = state ? atomic_exchange(&notify_test_delete_cutoff, 0) : 0;
    if (cutoff) {
        /* Retain watch storage through detach and event injection. The real
        * owning loop cannot service its queued doorbell until we return. */
        atomic_fetch_add(&state->refs, 1);
        if (cutoff == 1) {
            chimera_vfs_notify_emit_delete(vfs, state->watch->dir_fh, state->watch->dir_fh_len);
        }
    }
    next(vfs, state);
    if (cutoff) {
        if (cutoff == 2) {
            chimera_vfs_notify_emit_delete(vfs, state->watch->dir_fh, state->watch->dir_fh_len);
        }
        test_state_put(state);
    }
    if (transfer) {
        test_state_put(cross_state[0]);
        test_state_put(cross_state[1]);
    }
} /* chimera_smb_notify_close */

__attribute__((visibility("default"))) void
chimera_smb_notify_request_free(struct chimera_smb_notify_request *nr)
{
    typedef void (*fn)(
        struct chimera_smb_notify_request *);
    fn   next = (fn) dlsym(RTLD_NEXT, "chimera_smb_notify_request_free");
    assert(next);
    bool target = false;
    evpl_mutex_lock(&free_target_lock);
    if (free_target_enabled && nr->open_file &&
        !memcmp(&nr->open_file->file_id, free_target_file_id, sizeof(free_target_file_id))) {
        target              = true;
        free_target_enabled = false;
    }
    evpl_mutex_unlock(&free_target_lock);
    bool tracked = false;
    if (atomic_load(&notify_test_cross_capture)) {
        for (int i = 0; i < atomic_load(&notify_test_cross_count); i++) {
            if (cross_nr[i] == nr) {
                assert(pthread_equal(pthread_self(), cross_thread[i]));
                tracked = true;
            }
        }
    }
    bool retired = atomic_load(&notify_test_retired_session);
    if (retired) {
        assert(nr->session && nr->secure.enc_session == nr->session);
        evpl_mutex_lock(&nr->thread->shared->sessions_lock);
        assert(nr->session->compound_retired && nr->session->compound_pins);
        evpl_mutex_unlock(&nr->thread->shared->sessions_lock);
    }
    next(nr);
    if (target) {
        atomic_fetch_add(&notify_test_target_freed, 1);
    }
    if (retired) {
        atomic_fetch_add(&notify_test_retired_freed, 1);
    }
    if (tracked) {
        atomic_fetch_add(&notify_test_cross_freed, 1);
    }
} /* chimera_smb_notify_request_free */
