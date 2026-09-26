// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif /* ifndef _GNU_SOURCE */
#include <dlfcn.h>
#include <stdatomic.h>
#undef NDEBUG
#include <assert.h>
#include "smb2_mbt_common.h"
#include "vfs/vfs_compound.h"
#include "vfs/vfs_notify.h"
#include "vfs/vfs_internal_procs.h"
#include "vfs/sdk/vfs_acl.h"
#include "vfs/sdk/vfs_sid.h"
static atomic_int armed, submissions, expected_groups;
static atomic_int watch_disconnected_suffix, suffix_dispatches, suffix_backend_opens, suffix_drained;
struct chimera_smb_request;

__attribute__((visibility("default"))) void
chimera_smb_create(struct chimera_smb_request *request)
{
    typedef void (*create_fn)(
        struct chimera_smb_request *);
    create_fn next = (create_fn) dlsym(RTLD_NEXT, "chimera_smb_create");
    assert(next);
    if (atomic_load(&watch_disconnected_suffix)) {
        atomic_fetch_add(&suffix_dispatches, 1);
    }
    next(request);
} /* chimera_smb_create */

__attribute__((visibility("default"))) void
chimera_smb_complete_request(
    struct chimera_smb_request *request,
    unsigned int                status)
{
    typedef void (*complete_fn)(
        struct chimera_smb_request *,
        unsigned int);
    complete_fn next = (complete_fn) dlsym(RTLD_NEXT, "chimera_smb_complete_request");
    assert(next);
    /* This window has exactly one canceled request: the disconnected mutator
     * admission wait. Its completion synchronously runs compound_advance. */
    bool        drained = atomic_load(&watch_disconnected_suffix) && status == ST_CANCELLED;
    next(request, status);
    if (drained) {
        atomic_store(&suffix_drained, 1);
    }
} /* chimera_smb_complete_request */

static atomic_int noop_retry, noop_retry_calls;
static atomic_int hold_rename, rename_held, release_rename;
static atomic_int hold_replace;
static atomic_int hold_legacy_replace;
static atomic_int constructor_denials;
struct chimera_smb_namespace_open_token;
struct chimera_smb_namespace_registry;
__attribute__((visibility("default"))) bool
chimera_smb_namespace_open_begin(
    struct chimera_smb_namespace_open_token *token,
    struct chimera_smb_namespace_registry   *registry,
    const void                              *owner)
{
    typedef bool (*begin_fn)(
        struct chimera_smb_namespace_open_token *,
        struct chimera_smb_namespace_registry *,
        const void *);
    begin_fn next = (begin_fn) dlsym(RTLD_NEXT, "chimera_smb_namespace_open_begin");
    assert(next);
    bool     ready = next(token, registry, owner);
    if (!ready) {
        atomic_fetch_add(&constructor_denials, 1);
    }
    return ready;
} /* chimera_smb_namespace_open_begin */
static atomic_int                            checked_renames, replacing_renames;
static atomic_int                            rename_notifications, expect_clean_fallback;
static atomic_int                            scan_mode, scan_finishes, scan_recalls, scan_mutations;
static atomic_int                            scan_held, scan_release;
static chimera_vfs_compound_readdir_reset_t  scan_reset_original;
static chimera_vfs_compound_readdir_append_t scan_append_original;
static void                                 *scan_append_private;
static unsigned int                          scan_page_entries;
static char                                  scan_first_child;

/* Pause after compound parent resolution and before the atomic matched move.
 * A second connection can change the destination without any fake rollback. */
static struct {
    struct evpl_timer                timer;
    struct chimera_vfs_thread       *thread;
    const struct chimera_vfs_cred   *cred;
    const void                      *fh, *new_fh;
    const char                      *name, *new_name;
    const uint8_t                   *target_fh, *lease_key, *match_fh;
    int                              fh_len, name_len, new_fh_len, new_name_len, target_fh_len;
    unsigned int                     flags;
    uint64_t                         pre_mask, post_mask;
    struct chimera_vfs_open_handle  *handle;
    struct chimera_claim_actor       actor;
    bool                             have_actor;
    uint32_t                         match_fh_len;
    enum chimera_vfs_rename_outcome *outcome;
    chimera_vfs_rename_at_callback_t callback;
    void                            *private_data;
} rename_gate;

static void
rename_poll(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    if (!atomic_exchange(&release_rename, 0)) {
        evpl_add_oneshot_timer(evpl, timer, rename_poll, 1000); return;
    }
    __typeof__(&chimera_vfs_rename_at_checked_result_actor) next =
        (__typeof__(&chimera_vfs_rename_at_checked_result_actor))dlsym(RTLD_NEXT,
                                                                       "chimera_vfs_rename_at_checked_result_actor");
    assert(next);
    atomic_store(&rename_held, 0);
    next(rename_gate.thread, rename_gate.cred, rename_gate.fh, rename_gate.fh_len,
         rename_gate.name, rename_gate.name_len, rename_gate.new_fh, rename_gate.new_fh_len,
         rename_gate.new_name, rename_gate.new_name_len, rename_gate.target_fh,
         rename_gate.target_fh_len, rename_gate.flags, rename_gate.pre_mask, rename_gate.post_mask,
         rename_gate.lease_key, rename_gate.handle, rename_gate.have_actor ? &rename_gate.actor : NULL,
         rename_gate.match_fh, rename_gate.match_fh_len,
         rename_gate.outcome, rename_gate.callback, rename_gate.private_data);
} /* rename_poll */

__attribute__((visibility("default"))) void
chimera_vfs_rename_at_checked_result_actor(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    const void                       *fh,
    int                               fh_len,
    const char                       *name,
    int                               name_len,
    const void                       *new_fh,
    int                               new_fh_len,
    const char                       *new_name,
    int                               new_name_len,
    const uint8_t                    *target_fh,
    int                               target_fh_len,
    unsigned int                      flags,
    uint64_t                          pre_mask,
    uint64_t                          post_mask,
    const uint8_t                    *lease_key,
    struct chimera_vfs_open_handle   *handle,
    const struct chimera_claim_actor *actor,
    const uint8_t                    *match_fh,
    uint32_t                          match_fh_len,
    enum chimera_vfs_rename_outcome  *outcome,
    chimera_vfs_rename_at_callback_t  callback,
    void                             *private_data)
{
    __typeof__(&chimera_vfs_rename_at_checked_result_actor) next =
        (__typeof__(&chimera_vfs_rename_at_checked_result_actor))dlsym(RTLD_NEXT,
                                                                       "chimera_vfs_rename_at_checked_result_actor");

    assert(next);
    if (atomic_load(&scan_mode)) {
        atomic_fetch_add(&scan_mutations, 1);
    }
    if (atomic_load(&armed)) {
        if (flags & CHIMERA_VFS_RENAME_NOREPLACE) {
            assert(flags & CHIMERA_VFS_RENAME_MATCH_SOURCE_FH);
            assert(match_fh && match_fh_len);
            atomic_fetch_add(&checked_renames, 1);
        } else {
            if (atomic_load(&expect_clean_fallback)) {
                assert(!atomic_load(&rename_notifications));
            }
            atomic_fetch_add(&replacing_renames, 1);
        }
    }
    if (((flags & CHIMERA_VFS_RENAME_NOREPLACE) && atomic_exchange(&hold_rename, 0)) ||
        ((flags & CHIMERA_VFS_RENAME_MATCH_DEST_FH) && atomic_exchange(&hold_replace, 0)) ||
        (!(flags & (CHIMERA_VFS_RENAME_MATCH_DEST_FH | CHIMERA_VFS_RENAME_NOREPLACE)) &&
         atomic_exchange(&hold_legacy_replace, 0))) {
        rename_gate.thread     = thread; rename_gate.cred = cred;
        rename_gate.fh         = fh; rename_gate.fh_len = fh_len;
        rename_gate.name       = name; rename_gate.name_len = name_len;
        rename_gate.new_fh     = new_fh; rename_gate.new_fh_len = new_fh_len;
        rename_gate.new_name   = new_name; rename_gate.new_name_len = new_name_len;
        rename_gate.target_fh  = target_fh; rename_gate.target_fh_len = target_fh_len;
        rename_gate.flags      = flags; rename_gate.pre_mask = pre_mask; rename_gate.post_mask = post_mask;
        rename_gate.lease_key  = lease_key; rename_gate.handle = handle;
        rename_gate.have_actor = actor != NULL;
        if (actor) {
            rename_gate.actor = *actor;
        }
        rename_gate.match_fh = match_fh; rename_gate.match_fh_len = match_fh_len;
        rename_gate.outcome  = outcome;
        rename_gate.callback = callback; rename_gate.private_data = private_data;
        evpl_add_oneshot_timer(thread->evpl, &rename_gate.timer, rename_poll, 1000);
        atomic_store(&rename_held, 1);
        return;
    }
    next(thread, cred, fh, fh_len, name, name_len, new_fh, new_fh_len, new_name, new_name_len,
         target_fh, target_fh_len, flags, pre_mask, post_mask, lease_key, handle, actor,
         match_fh, match_fh_len, outcome, callback, private_data);
} /* chimera_vfs_rename_at_checked_result_actor */

/* Hold an actual backend OPEN result before either frontend has seen the
 * handle or reserved ACCESS. The handle ownership transfers to this callback;
 * attribute values are copied because the VFS request may now be recycled. */
static atomic_int hold_open_result, open_result_held, release_open_result;
static struct {
    struct evpl_timer               timer;
    struct chimera_vfs_thread      *thread;
    struct chimera_vfs_open_handle *handle;
    struct chimera_vfs_attrs        set, attr, pre, post;
    bool                            have_set, have_attr, have_pre, have_post;
    enum chimera_vfs_error status;
    chimera_vfs_open_at_callback_t  callback;
    void                           *private_data;
} open_gate;

static void
open_attr_copy(
    struct chimera_vfs_attrs       *dst,
    const struct chimera_vfs_attrs *src)
{
    memset(dst, 0, sizeof(*dst));
    if (!src) {
        return;
    }
    *dst        = *src;
    dst->va_acl = NULL; dst->va_owner_sid = NULL; dst->va_group_sid = NULL;
    if ((src->va_set_mask & CHIMERA_VFS_ATTR_ACL) && src->va_acl) {
        size_t bytes = chimera_acl_size(src->va_acl->num_aces);
        dst->va_acl = malloc(bytes); assert(dst->va_acl);
        memcpy(dst->va_acl, src->va_acl, bytes);
    }
    if ((src->va_set_mask & CHIMERA_VFS_ATTR_OWNER_SID) && src->va_owner_sid) {
        dst->va_owner_sid  = malloc(sizeof(*dst->va_owner_sid)); assert(dst->va_owner_sid);
        *dst->va_owner_sid = *src->va_owner_sid;
    }
    if ((src->va_set_mask & CHIMERA_VFS_ATTR_GROUP_SID) && src->va_group_sid) {
        dst->va_group_sid  = malloc(sizeof(*dst->va_group_sid)); assert(dst->va_group_sid);
        *dst->va_group_sid = *src->va_group_sid;
    }
} /* open_attr_copy */
static void
open_attr_free(struct chimera_vfs_attrs *attr)
{
    free(attr->va_acl); free(attr->va_owner_sid); free(attr->va_group_sid);
} /* open_attr_free */

static void
open_result_poll(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    if (!atomic_exchange(&release_open_result, 0)) {
        evpl_add_oneshot_timer(evpl, timer, open_result_poll, 1000); return;
    }
    atomic_store(&open_result_held, 0);
    open_gate.callback(open_gate.status, open_gate.handle,
                       open_gate.have_set ? &open_gate.set : NULL, open_gate.have_attr ? &open_gate.attr : NULL,
                       open_gate.have_pre ? &open_gate.pre : NULL, open_gate.have_post ? &open_gate.post : NULL,
                       open_gate.private_data);
    open_attr_free(&open_gate.set); open_attr_free(&open_gate.attr);
    open_attr_free(&open_gate.pre); open_attr_free(&open_gate.post);
} /* open_result_poll */

static void
open_result_done(
    enum chimera_vfs_error          status,
    struct chimera_vfs_open_handle *handle,
    struct chimera_vfs_attrs       *set,
    struct chimera_vfs_attrs       *attr,
    struct chimera_vfs_attrs       *pre,
    struct chimera_vfs_attrs       *post,
    void                           *private_data)
{
    (void) private_data;
    assert(status == CHIMERA_VFS_OK && handle);
    open_gate.status    = status;
    open_gate.handle    = handle;
    open_gate.have_set  = set != NULL; open_attr_copy(&open_gate.set, set);
    open_gate.have_attr = attr != NULL; open_attr_copy(&open_gate.attr, attr);
    open_gate.have_pre  = pre != NULL; open_attr_copy(&open_gate.pre, pre);
    open_gate.have_post = post != NULL; open_attr_copy(&open_gate.post, post);
    evpl_add_oneshot_timer(open_gate.thread->evpl, &open_gate.timer, open_result_poll, 1000);
    atomic_store(&open_result_held, 1);
} /* open_result_done */

__attribute__((visibility("default"))) void
chimera_vfs_open_at(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *parent,
    const char                     *name,
    int                             name_len,
    unsigned int                    flags,
    struct chimera_vfs_attrs       *set,
    uint64_t                        mask,
    uint64_t                        pre_mask,
    uint64_t                        post_mask,
    chimera_vfs_open_at_callback_t  callback,
    void                           *private_data)
{
    __typeof__(&chimera_vfs_open_at) next = (__typeof__(&chimera_vfs_open_at))dlsym(RTLD_NEXT, "chimera_vfs_open_at");

    assert(next);
    if (atomic_load(&watch_disconnected_suffix) &&
        name_len == (int) strlen("mutation-abandoned-suffix") &&
        !memcmp(name, "mutation-abandoned-suffix", name_len)) {
        atomic_fetch_add(&suffix_backend_opens, 1);
    }
    if (atomic_exchange(&hold_open_result, 0)) {
        assert(!(flags & CHIMERA_VFS_OPEN_CREATE));
        open_gate.thread       = thread;
        open_gate.callback     = callback;
        open_gate.private_data = private_data;
        callback               = open_result_done;
        private_data           = NULL;
    }
    next(thread, cred, parent, name, name_len, flags, set, mask, pre_mask, post_mask, callback, private_data);
} /* chimera_vfs_open_at */

static atomic_int  pending_doc_watch, pending_doc_notifications;
static const char *pending_doc_expected_name;
static uint8_t     pending_doc_parent_fh[CHIMERA_VFS_FH_SIZE];
static uint32_t    pending_doc_parent_fh_len;

__attribute__((visibility("default"))) void
chimera_vfs_notify_emit_lease(
    struct chimera_vfs_notify *notify,
    const uint8_t             *fh,
    uint16_t                   fh_len,
    uint32_t                   action,
    const char                *name,
    uint16_t                   name_len,
    const char                *old_name,
    uint16_t                   old_name_len,
    uint64_t                   skip_lo,
    uint64_t                   skip_hi,
    bool                       has_skip)
{
    typedef void (*notify_fn)(
        struct chimera_vfs_notify *,
        const uint8_t *,
        uint16_t,
        uint32_t,
        const char *,
        uint16_t,
        const char *,
        uint16_t,
        uint64_t,
        uint64_t,
        bool);
    notify_fn next = (notify_fn) dlsym(RTLD_NEXT, "chimera_vfs_notify_emit_lease");
    assert(next);
    if (atomic_load(&armed) && (action & (CHIMERA_VFS_NOTIFY_RENAMED | CHIMERA_VFS_NOTIFY_RENAMED_DIR))) {
        atomic_fetch_add(&rename_notifications, 1);
    }
    if (atomic_load(&pending_doc_watch)) {
        if ((action & (CHIMERA_VFS_NOTIFY_RENAMED | CHIMERA_VFS_NOTIFY_RENAMED_DIR)) && name &&
            name_len == strlen(pending_doc_expected_name) &&
            !memcmp(name, pending_doc_expected_name, name_len)) {
            assert(fh_len <= sizeof(pending_doc_parent_fh));
            pending_doc_parent_fh_len = fh_len;
            memcpy(pending_doc_parent_fh, fh, fh_len);
        }
        if (action & CHIMERA_VFS_NOTIFY_STREAM_NAME) {
            assert(action == CHIMERA_VFS_NOTIFY_STREAM_NAME);
            assert(name_len == strlen(pending_doc_expected_name) &&
                   !memcmp(name, pending_doc_expected_name, name_len));
            assert(pending_doc_parent_fh_len == fh_len &&
                   !memcmp(fh, pending_doc_parent_fh, fh_len));
            atomic_fetch_add(&pending_doc_notifications, 1);
        }
    }
    next(notify, fh, fh_len, action, name, name_len, old_name, old_name_len,
         skip_lo, skip_hi, has_skip);
} /* chimera_vfs_notify_emit_lease */

/* Actor and legacy entrypoints emit independently; observe each exactly once. */
__attribute__((visibility("default"))) void
chimera_vfs_notify_emit_actor(
    struct chimera_vfs_notify        *notify,
    const uint8_t                    *fh,
    uint16_t                          fh_len,
    uint32_t                          action,
    const char                       *name,
    uint16_t                          name_len,
    const char                       *old_name,
    uint16_t                          old_name_len,
    const struct chimera_claim_actor *actor)
{
    typedef void (*notify_fn)(
        struct chimera_vfs_notify *,
        const uint8_t *,
        uint16_t,
        uint32_t,
        const char *,
        uint16_t,
        const char *,
        uint16_t,
        const struct chimera_claim_actor *);
    notify_fn next = (notify_fn) dlsym(RTLD_NEXT, "chimera_vfs_notify_emit_actor");
    assert(next);
    if (atomic_load(&armed) && (action & (CHIMERA_VFS_NOTIFY_RENAMED | CHIMERA_VFS_NOTIFY_RENAMED_DIR))) {
        atomic_fetch_add(&rename_notifications, 1);
    }
    if (atomic_load(&pending_doc_watch)) {
        if ((action & (CHIMERA_VFS_NOTIFY_RENAMED | CHIMERA_VFS_NOTIFY_RENAMED_DIR)) && name &&
            name_len == strlen(pending_doc_expected_name) &&
            !memcmp(name, pending_doc_expected_name, name_len)) {
            assert(fh_len <= sizeof(pending_doc_parent_fh));
            pending_doc_parent_fh_len = fh_len;
            memcpy(pending_doc_parent_fh, fh, fh_len);
        }
        if (action & CHIMERA_VFS_NOTIFY_STREAM_NAME) {
            assert(action == CHIMERA_VFS_NOTIFY_STREAM_NAME);
            assert(name_len == strlen(pending_doc_expected_name) &&
                   !memcmp(name, pending_doc_expected_name, name_len));
            assert(pending_doc_parent_fh_len == fh_len &&
                   !memcmp(fh, pending_doc_parent_fh, fh_len));
            atomic_fetch_add(&pending_doc_notifications, 1);
        }
    }
    next(notify, fh, fh_len, action, name, name_len, old_name, old_name_len,
         actor);
} /* chimera_vfs_notify_emit_actor */

static atomic_int hold_producer, producer_held, release_producer;
static            _Thread_local struct evpl *owner_evpl;
static struct {
    struct evpl_timer            timer;
    struct evpl                 *evpl;
    struct chimera_vfs_compound *compound;
    atomic_uint                  calls;
    bool                         reject;
} producer_gate;

__attribute__((visibility("default"))) struct chimera_vfs_compound *
chimera_vfs_compound_alloc(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred)
{
    typedef struct chimera_vfs_compound *(*alloc_fn)(
        struct chimera_vfs_thread *,
        const struct chimera_vfs_cred *);
    alloc_fn next = (alloc_fn) dlsym(RTLD_NEXT, "chimera_vfs_compound_alloc");
    assert(next);
    owner_evpl = thread->evpl;
    return next(thread, cred);
} /* chimera_vfs_compound_alloc */
static void
producer_poll(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    if (!atomic_exchange(&release_producer, 0)) {
        evpl_add_oneshot_timer(evpl, timer, producer_poll, 1000); return;
    }
    atomic_store(&producer_held, 0);
    chimera_vfs_compound_finish_result(producer_gate.compound,
                                       producer_gate.reject ? CHIMERA_VFS_EAGAIN : CHIMERA_VFS_OK);
} /* producer_poll */
static void
producer_finish(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    (void) private_data;
    if (++producer_gate.calls != 1) {
        chimera_vfs_compound_finish_result(compound, CHIMERA_VFS_OK); return;
    }
    producer_gate.compound = compound;
    evpl_add_oneshot_timer(producer_gate.evpl, &producer_gate.timer, producer_poll, 1000);
    atomic_store(&producer_held, 1);
} /* producer_finish */
static void
noop_retry_finish(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    (void) private_data;
    for (uint32_t i = 0; i < chimera_vfs_compound_num_ops(compound); i++) {
        const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound, i);
        assert(op->type != CHIMERA_VFS_COMPOUND_OP_REMOVE && op->type != CHIMERA_VFS_COMPOUND_OP_CLOSE &&
               op->type != CHIMERA_VFS_COMPOUND_OP_WRITE && op->type != CHIMERA_VFS_COMPOUND_OP_SETATTR &&
               op->type != CHIMERA_VFS_COMPOUND_OP_OVERWRITE && op->type != CHIMERA_VFS_COMPOUND_OP_CREATE);
        if (op->type == CHIMERA_VFS_COMPOUND_OP_RENAME) {
            assert(op->skipped || op->rename_outcome == CHIMERA_VFS_RENAME_OUTCOME_NOOP ||
                   ((op->remove_flags & CHIMERA_VFS_RENAME_NOREPLACE) &&
                    op->rename_outcome == CHIMERA_VFS_RENAME_OUTCOME_UNKNOWN));
        }
        if (op->type == CHIMERA_VFS_COMPOUND_OP_OPEN) {
            assert(!(op->open_flags & CHIMERA_VFS_OPEN_CREATE));
        }
    }
    unsigned int call = atomic_fetch_add(&noop_retry_calls, 1);
    chimera_vfs_compound_finish_result(compound, call ? CHIMERA_VFS_OK : CHIMERA_VFS_EAGAIN);
} /* noop_retry_finish */

static struct {
    struct evpl_timer            timer;
    struct chimera_vfs_compound *compound;
} scan_gate;

static void
scan_poll(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    if (!atomic_exchange(&scan_release, 0)) {
        evpl_add_oneshot_timer(evpl, timer, scan_poll, 1000); return;
    }
    atomic_store(&scan_held, 0);
    chimera_vfs_compound_finish_result(scan_gate.compound, CHIMERA_VFS_EAGAIN);
} /* scan_poll */

static void
scan_page_reset(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    void                        *arg)
{
    (void) arg;
    scan_page_entries = 0;
    scan_reset_original(compound, index, scan_append_private);
} /* scan_page_reset */

static int
scan_page_append(
    struct chimera_vfs_compound    *compound,
    uint32_t                        index,
    uint64_t                        inum,
    uint64_t                        cookie,
    const char                     *name,
    int                             namelen,
    const struct chimera_vfs_attrs *attrs,
    void                           *arg)
{
    (void) arg;
    assert(!scan_page_entries++);
    assert(namelen == 1 && (name[0] == 'a' || name[0] == 'b'));
    if (!scan_first_child) {
        scan_first_child = name[0];
    }
    assert(scan_append_original(compound, index, inum, cookie, name, namelen,
                                attrs, scan_append_private) == 0);
    /* Memfs resumes after the callback cookie: include this entry, then stop. */
    return -1;
} /* scan_page_append */

static void
scan_finish(
    struct chimera_vfs_compound *compound,
    void                        *arg)
{
    (void) arg;
    for (uint32_t i = 0; i < chimera_vfs_compound_num_ops(compound); i++) {
        int type = chimera_vfs_compound_op(compound, i)->type;
        assert(type == CHIMERA_VFS_COMPOUND_OP_PUTHANDLE || type == CHIMERA_VFS_COMPOUND_OP_READDIR);
    }
    int call = atomic_fetch_add(&scan_finishes, 1);
    int mode = atomic_load(&scan_mode);
    assert(!atomic_load(&scan_recalls) && !atomic_load(&scan_mutations));
    if ((mode == 1 && !call) || (mode == 5 && call == 1)) {
        scan_gate.compound = compound;
        evpl_add_oneshot_timer(owner_evpl, &scan_gate.timer, scan_poll, 1000);
        atomic_store(&scan_held, 1);
        return;
    }
    chimera_vfs_compound_finish_result(compound, mode == 2 ? CHIMERA_VFS_EIO :
                                       mode == 3 ? CHIMERA_VFS_EINTR : CHIMERA_VFS_OK);
} /* scan_finish */

__attribute__((visibility("default"))) void
chimera_vfs_recall_caching_fh(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
    const uint8_t                   *fh,
    uint32_t                         fh_len,
    chimera_vfs_recall_fh_callback_t callback,
    void                            *private_data)
{
    __typeof__(&chimera_vfs_recall_caching_fh) next =
        (__typeof__(&chimera_vfs_recall_caching_fh))dlsym(RTLD_NEXT, "chimera_vfs_recall_caching_fh");

    assert(next);
    if (atomic_load(&scan_mode)) {
        atomic_fetch_add(&scan_recalls, 1);
    }
    next(thread, cred, fh, fh_len, callback, private_data);
} /* chimera_vfs_recall_caching_fh */

static atomic_int                         cancel_initial_move, canceled_initial_move;
static chimera_vfs_compound_op_callback_t before_cancel_complete;

static void
cancel_initial_move_complete(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    before_cancel_complete(compound, index, status, private_data);
    const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound, index);
    assert(*status == CHIMERA_VFS_OK && op->rename_outcome == CHIMERA_VFS_RENAME_OUTCOME_MOVED);
    assert(atomic_exchange(&cancel_initial_move, 0));
    assert(chimera_vfs_compound_cancel(compound));
    atomic_store(&canceled_initial_move, 1);
} /* cancel_initial_move_complete */

__attribute__((visibility("default"))) void
chimera_vfs_compound_submit(
    struct chimera_vfs_compound    *cp,
    chimera_vfs_compound_callback_t callback,
    void                           *private_data)
{
    typedef void (*submit_fn)(
        struct chimera_vfs_compound *,
        chimera_vfs_compound_callback_t,
        void *);
    submit_fn next = (submit_fn) dlsym(RTLD_NEXT, "chimera_vfs_compound_submit");
    assert(next);
    if (atomic_load(&armed)) {
        atomic_fetch_add(&submissions, 1);
        if (atomic_load(&expected_groups) >= 0) {
            assert(chimera_vfs_compound_num_groups(cp) == (uint32_t) atomic_load(&expected_groups));
        }
        if (atomic_load(&noop_retry)) {
            chimera_vfs_compound_set_finish_handler(cp, noop_retry_finish, NULL);
        }
        if (atomic_load(&hold_producer)) {
            /* The held producer only opens an existing object and reserves
             * private ACCESS. Never fake rollback of namespace/data changes. */
            for (uint32_t i = 0; i < chimera_vfs_compound_num_ops(cp); i++) {
                const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(cp, i);
                assert(op->type != CHIMERA_VFS_COMPOUND_OP_RENAME &&
                       op->type != CHIMERA_VFS_COMPOUND_OP_REMOVE &&
                       op->type != CHIMERA_VFS_COMPOUND_OP_CLOSE &&
                       op->type != CHIMERA_VFS_COMPOUND_OP_WRITE &&
                       op->type != CHIMERA_VFS_COMPOUND_OP_SETATTR &&
                       op->type != CHIMERA_VFS_COMPOUND_OP_OVERWRITE &&
                       op->type != CHIMERA_VFS_COMPOUND_OP_CREATE);
                if (op->type == CHIMERA_VFS_COMPOUND_OP_OPEN) {
                    assert(!(op->open_flags & CHIMERA_VFS_OPEN_CREATE));
                }
            }
            producer_gate.evpl = owner_evpl;
            chimera_vfs_compound_set_finish_handler(cp, producer_finish, NULL);
        }
    }
    if (atomic_load(&scan_mode)) {
        for (uint32_t i = 0; i < chimera_vfs_compound_num_ops(cp); i++) {
            if (chimera_vfs_compound_op(cp, i)->type == CHIMERA_VFS_COMPOUND_OP_RECALL) {
                atomic_fetch_add(&scan_recalls, 1);
            }
            /* Only legacy streaming discovery owns this page/retry fixture.
             * Native bounded READDIR may be built but skipped at admission. */
            if (chimera_vfs_compound_op(cp, i)->type == CHIMERA_VFS_COMPOUND_OP_READDIR &&
                chimera_vfs_compound_op(cp, i)->readdir_append) {
                chimera_vfs_compound_set_finish_handler(cp, scan_finish, NULL);
                if (atomic_load(&scan_mode) == 5) {
                    struct chimera_vfs_compound_op *op = chimera_vfs_compound_op_args(cp, i);
                    scan_reset_original  = op->readdir_reset;
                    scan_append_original = op->readdir_append;
                    scan_append_private  = op->readdir_private;
                    op->readdir_reset    = scan_page_reset;
                    op->readdir_append   = scan_page_append;
                }
                break;
            }
        }
    }
    if (atomic_load(&cancel_initial_move)) {
        bool installed = false;
        for (uint32_t i = 0; i < chimera_vfs_compound_num_ops(cp); i++) {
            const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(cp, i);
            if (op->type != CHIMERA_VFS_COMPOUND_OP_RENAME) {
                continue;
            }
            assert(op->remove_flags & CHIMERA_VFS_RENAME_NOREPLACE);
            assert(op->complete);
            before_cancel_complete = op->complete;
            chimera_vfs_compound_op_callback_t    prepare         = op->prepare;
            void                                 *prepare_private = op->prepare_private;
            chimera_vfs_compound_set_op_callbacks(cp, i, op->prepare,
                                                  cancel_initial_move_complete, op->callback_private);
            /* SMB binds execution inputs through its own prepare context;
             * the completion still receives the command's private context. */
            chimera_vfs_compound_set_op_prepare(cp, i, prepare, prepare_private);
            installed = true;
            break;
        }
        assert(installed);
    }
    next(cp, callback, private_data);
} /* chimera_vfs_compound_submit */
struct packet {
    uint8_t      data[4096];
    unsigned int length, previous, count;
    uint64_t     first_mid;
    uint32_t     expected[32];
};

static uint8_t *
append(
    struct packet    *p,
    struct smb2_conn *c,
    uint16_t          command,
    unsigned int      body_size)
{
    unsigned int start = (p->length + 7) & ~7u;

    if (p->count) {
        p32(p->data + p->previous, 20, start - p->previous);
    } else {
        p->first_mid = c->msg_id;
    }
    assert(start + SMB2_HDR_SIZE + body_size <= sizeof(p->data));
    uint8_t     *h = p->data + start;
    memcpy(h, "\xfeSMB", 4);
    p16(h, 4, SMB2_HDR_SIZE);
    p16(h, 6, 1);
    p16(h, 12, command);
    p16(h, 14, 32);
    p64(h, 24, p->first_mid + p->count);
    p32(h, 36, c->tree_id);
    p64(h, 40, c->session_id);
    p->count++;
    p->previous = start;
    p->length   = start + SMB2_HDR_SIZE + body_size;
    return h + SMB2_HDR_SIZE;
} /* append */

static void
query(
    struct packet    *p,
    struct smb2_conn *c,
    const uint8_t     fid[16],
    uint8_t           type,
    uint8_t           level)
{
    uint8_t *b = append(p, c, SMB2_QUERY_INFO, 40);

    p16(b, 0, 41);
    b[2] = type;
    b[3] = level;
    p32(b, 4, 2048);
    if (type == 3) {
        p32(b, 16, 7);
    }                                 /* owner/group/DACL */
    memcpy(b + 24, fid, 16);
} /* query */

static void
set(
    struct packet    *p,
    struct smb2_conn *c,
    const uint8_t     fid[16],
    uint8_t           level,
    const void       *value,
    uint32_t          length)
{
    uint8_t *b = append(p, c, SMB2_SET_INFO, 32 + length);

    p16(b, 0, 33);
    b[2] = SMB2_INFO_FILE_T;
    b[3] = level;
    p32(b, 4, length);
    p16(b, 8, SMB2_HDR_SIZE + 32);
    memcpy(b + 16, fid, 16);
    memcpy(b + 32, value, length);
} /* set */

static void
check_replies(
    struct packet    *p,
    struct smb2_conn *c)
{
    unsigned int off = 4;

    for (unsigned int i = 0; i < p->count; i++) {
        assert(off + SMB2_HDR_SIZE <= (unsigned int) c->rlen);
        const uint8_t *h = c->rbuf + off;
        fprintf(stderr, "DOC response %u command %u status %08x\n", i, g16(h, 12), g32(h, 8));
        assert(g64(h, 24) == p->first_mid + i);
        assert(g32(h, 8) == p->expected[i]);
        uint32_t       next = g32(h, 20);
        assert((i + 1 < p->count) == (next != 0));
        if (next) {
            assert(next >= SMB2_HDR_SIZE && !(next & 7));
        }
        off += next;
    }
} /* check_replies */

static void
send_runs_groups(
    struct packet    *p,
    struct smb2_conn *c,
    unsigned int      runs,
    int               groups)
{
    int replies = c->nreply_app;

    atomic_store(&submissions, 0);
    atomic_store(&checked_renames, 0);
    atomic_store(&replacing_renames, 0);
    atomic_store(&rename_notifications, 0);
    atomic_store(&expected_groups, groups);
    atomic_store(&armed, 1);
    memcpy(c->sbuf + 4, p->data, p->length);
    smb2c_send(c, p->length - SMB2_HDR_SIZE);
    c->msg_id = p->first_mid + p->count;
    (void) smb2c_wait(c);
    atomic_store(&armed, 0);
    assert(atomic_load(&submissions) == (int) runs);
    assert(c->nreply_app == replies + 1);
    check_replies(p, c);
} /* send_runs_groups */

static void
send_runs(
    struct packet    *p,
    struct smb2_conn *c,
    unsigned int      runs)
{
    send_runs_groups(p, c, runs, runs == 1 ? (int) p->count : -1);
} /* send_runs */

static void doc_send(
    struct packet    *p,
    struct smb2_conn *c) { send_runs(p, c, 1); }

static const uint8_t *
result(
    struct smb2_conn *c,
    unsigned int      index,
    uint32_t         *length)
{
    const uint8_t *h = c->rbuf + 4;

    for (unsigned int i = 0; i < index; i++) {
        h += g32(h, 20);
    }
    const uint8_t *b = h + SMB2_HDR_SIZE;
    assert(g16(h, 12) == SMB2_QUERY_INFO);
    assert(g16(b, 0) == 9);
    *length = g32(b, 4);
    const uint8_t *out = h + g16(b, 2);
    assert(out + *length <= c->rbuf + c->rlen);
    return out;
} /* result */

static void
close_op(
    struct packet    *p,
    struct smb2_conn *c,
    const uint8_t     fid[16])
{
    uint8_t *b = append(p, c, SMB2_CLOSE, 24);

    p16(b, 0, 24);
    memcpy(b + 8, fid, 16);
} /* close_op */


static void
rename_op(
    struct packet    *p,
    struct smb2_conn *c,
    const uint8_t     fid[16],
    const char       *name)
{
    unsigned int len        = strlen(name);
    uint8_t      value[512] = { 0 };

    assert(len * 2 + 20 <= sizeof(value));
    p32(value, 16, len * 2);
    for (unsigned int i = 0; i < len; i++) {
        p16(value, 20 + 2 * i, name[i]);
    }
    set(p, c, fid, 0x0A, value, 20 + len * 2);
} /* rename_op */

static void
replace_op(
    struct packet    *p,
    struct smb2_conn *c,
    const uint8_t     fid[16],
    const char       *name)
{
    rename_op(p, c, fid, name);
    p->data[p->previous + SMB2_HDR_SIZE + 32] = 1;
} /* replace_op */
static void
name_result(
    struct smb2_conn *c,
    unsigned int      index,
    const char       *name)
{
    uint32_t       length;
    const uint8_t *out   = result(c, index, &length);
    uint32_t       bytes = g32(out, 0);
    unsigned int   n     = strlen(name);

    assert(length >= 4 + bytes && bytes >= n * 2);
    const uint8_t *suffix = out + 4 + bytes - n * 2;
    for (unsigned int i = 0; i < n; i++) {
        assert(g16(suffix, i * 2) == (uint8_t) name[i]);
    }
} /* name_result */
static void
hardlink(
    struct smb2_conn *c,
    const uint8_t     fid[16],
    const char       *name)
{
    unsigned int len        = strlen(name);
    uint8_t      value[512] = { 0 };

    p32(value, 16, len * 2);
    for (unsigned int i = 0; i < len; i++) {
        p16(value, 20 + 2 * i, name[i]);
    }
    assert(smb2_set_info(c, 1, 0x0B, fid, value, 20 + len * 2) == ST_SUCCESS);
} /* hardlink */
static void
create_op(
    struct packet    *p,
    struct smb2_conn *c,
    const char       *name,
    uint32_t          disposition,
    uint32_t          access)
{
    unsigned int len = strlen(name);
    uint8_t     *b   = append(p, c, SMB2_CREATE, 56 + 2 * len);

    p16(b, 0, 57);
    p32(b, 4, 2); /* impersonation */
    p32(b, 24, access);
    p32(b, 32, MBT_FILE_SHARE_RWD);
    p32(b, 36, disposition);
    p16(b, 44, SMB2_HDR_SIZE + 56);
    p16(b, 46, 2 * len);
    for (unsigned int i = 0; i < len; i++) {
        p16(b, 56 + 2 * i, name[i]);
    }
} /* create_op */

static void
stat_open(
    struct packet    *p,
    struct smb2_conn *c,
    const char       *name)
{
    create_op(p, c, name, MBT_FILE_OPEN, 0x80u);
} /* stat_open */
static void related(struct packet *p) { p32(p->data + p->previous, 16, 4); }

static void
provisional_rename_cases(struct smb2_conn *c)
{
    struct packet          p;
    struct smb2_create_out probe;
    uint8_t                inherited[16];

    memset(inherited, 0xff, sizeof(inherited));
    for (unsigned int existing = 0; existing < 2; existing++) {
        const char *old  = existing ? "provisional-open-old" : "provisional-create-old";
        const char *next = existing ? "rename-dir\\provisional-open-new" : "rename-dir\\provisional-create-new";
        if (existing) {
            assert(smb2_create(c, old, MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD, NULL, &probe) ==
                   ST_SUCCESS);
            assert(smb2_close(c, probe.file_id) == ST_SUCCESS);
        }
        memset(&p, 0, sizeof(p));
        create_op(&p, c, old, existing ? MBT_FILE_OPEN : MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS);
        rename_op(&p, c, inherited, next); related(&p);
        query(&p, c, inherited, 1, 0x30); related(&p);
        close_op(&p, c, inherited); related(&p);
        doc_send(&p, c);
        name_result(c, 2, next);
        assert(smb2_create(c, old, MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD, NULL, &probe) ==
               ST_OBJECT_NAME_NOT_FOUND
               );
        assert(smb2_create(c, next, MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD, NULL, &probe) == ST_SUCCESS)
        ;
        assert(smb2_close(c, probe.file_id) == ST_SUCCESS);
    }
    /* Failed RENAME does not invalidate the produced handle or undo CREATE. */
    memset(&p, 0, sizeof(p));
    create_op(&p, c, "provisional-collision", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS);
    rename_op(&p, c, inherited, "rename-alias"); related(&p);
    p.expected[1] = 0xC0000035u;
    query(&p, c, inherited, 1, 0x30); related(&p);
    close_op(&p, c, inherited); related(&p);
    doc_send(&p, c);
    name_result(c, 2, "provisional-collision");

    /* A same-name no-op can safely reject finish: reset must drop late DOC
    * admission before replaying the producer's OPEN/cache coordination. */
    memset(&p, 0, sizeof(p));
    create_op(&p, c, "provisional-collision", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS);
    replace_op(&p, c, inherited, "provisional-collision"); related(&p);
    query(&p, c, inherited, 1, 0x30); related(&p);
    atomic_store(&noop_retry_calls, 0);
    atomic_store(&noop_retry, 1);
    doc_send(&p, c);
    atomic_store(&noop_retry, 0);
    assert(atomic_load(&noop_retry_calls) == 2);
    name_result(c, 2, "provisional-collision");
    memcpy(probe.file_id, c->rbuf + 4 + SMB2_HDR_SIZE + 64, 16);
    assert(smb2_close(c, probe.file_id) == ST_SUCCESS);
} /* provisional_rename_cases */

static void
replace_if_exists_cases(struct smb2_conn *c)
{
    struct smb2_create_out base, stream, alias, target, probe;
    struct packet          p = { 0 };
    uint8_t                inherited[16], data[16];
    uint32_t               bytes;

    memset(inherited, 0xff, sizeof(inherited));

    assert(smb2_create(c, "replace-absent-old", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &base) == ST_SUCCESS);
    hardlink(c, base.file_id, "replace-absent-alias");
    assert(smb2_create(c, "replace-absent-alias", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &alias) == ST_SUCCESS);
    assert(smb2_create(c, "replace-absent-old:fork:$DATA", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &stream) == ST_SUCCESS);
    assert(smb2_write(c, stream.file_id, 0, "fork", 4, &bytes) == ST_SUCCESS && bytes == 4);
    query(&p, c, base.file_id, 1, 0x30);
    replace_op(&p, c, base.file_id, "rename-dir\\replace-absent-next");
    query(&p, c, stream.file_id, 1, 0x30);
    query(&p, c, alias.file_id, 1, 0x30);
    replace_op(&p, c, base.file_id, "replace-absent-final");
    query(&p, c, base.file_id, 1, 0x30);
    doc_send(&p, c);
    assert(atomic_load(&checked_renames) == 2 && !atomic_load(&replacing_renames));
    name_result(c, 0, "replace-absent-old");
    name_result(c, 2, "rename-dir\\replace-absent-next");
    name_result(c, 3, "replace-absent-alias");
    name_result(c, 5, "replace-absent-final");
    assert(smb2_read(c, stream.file_id, 0, 4, data, &bytes) == ST_SUCCESS &&
           bytes == 4 && !memcmp(data, "fork", 4));
    assert(smb2_close(c, stream.file_id) == ST_SUCCESS);
    assert(smb2_close(c, alias.file_id) == ST_SUCCESS);
    assert(smb2_close(c, base.file_id) == ST_SUCCESS);

    /* A private CREATE prefix and its related RENAME/QUERY/CLOSE stay together
     * when replacement was permitted but is unnecessary. */
    memset(&p, 0, sizeof(p));
    create_op(&p, c, "replace-producer-old", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS);
    replace_op(&p, c, inherited, "rename-dir\\replace-producer-new"); related(&p);
    query(&p, c, inherited, 1, 0x30); related(&p);
    close_op(&p, c, inherited); related(&p);
    doc_send(&p, c);
    assert(atomic_load(&checked_renames) == 1 && !atomic_load(&replacing_renames));
    name_result(c, 2, "rename-dir\\replace-producer-new");

    /* Occupied target: the producer commits exactly once, then actual
     * replacement runs at its existing lifecycle boundary. An already-open
     * destination still reads its old inode after its name is replaced. */
    assert(smb2_create(c, "replace-occupied", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &target) == ST_SUCCESS);
    assert(smb2_write(c, target.file_id, 0, "target", 6, &bytes) == ST_SUCCESS && bytes == 6);
    memset(&p, 0, sizeof(p));
    create_op(&p, c, "replace-prefix", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS);
    replace_op(&p, c, inherited, "replace-occupied"); related(&p);
    query(&p, c, inherited, 1, 0x30); related(&p);
    close_op(&p, c, inherited); related(&p);
    atomic_store(&expect_clean_fallback, 1);
    send_runs(&p, c, 4);
    atomic_store(&expect_clean_fallback, 0);
    assert(atomic_load(&checked_renames) == 1 && atomic_load(&replacing_renames) == 1);
    name_result(c, 2, "replace-occupied");
    assert(smb2_read(c, target.file_id, 0, 6, data, &bytes) == ST_SUCCESS &&
           bytes == 6 && !memcmp(data, "target", 6));
    assert(smb2_close(c, target.file_id) == ST_SUCCESS);
    assert(smb2_create(c, "replace-prefix", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &probe) == ST_OBJECT_NAME_NOT_FOUND);

    /* A distinct occupied target without holders is now replaced in the
     * original wire batch, including a private CREATE producer and suffix. */
    assert(smb2_create(c, "replace-unheld-target", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &target) == ST_SUCCESS);
    assert(smb2_close(c, target.file_id) == ST_SUCCESS);
    memset(&p, 0, sizeof(p));
    create_op(&p, c, "replace-unheld-source", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS);
    replace_op(&p, c, inherited, "replace-unheld-target"); related(&p);
    query(&p, c, inherited, 1, 0x30); related(&p);
    close_op(&p, c, inherited); related(&p);
    doc_send(&p, c);
    assert(atomic_load(&checked_renames) == 1 && atomic_load(&replacing_renames) == 1);
    name_result(c, 2, "replace-unheld-target");
    assert(smb2_create(c, "replace-unheld-source", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &probe) == ST_OBJECT_NAME_NOT_FOUND);

    /* Same-inode aliases remain one compound. The backend's atomic NOOP
     * result preserves the original path, even across safe finish retry. */
    assert(smb2_create(c, "replace-same-inode", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &base) == ST_SUCCESS);
    hardlink(c, base.file_id, "replace-same-alias");
    assert(smb2_write(c, base.file_id, 0, "same", 4, &bytes) == ST_SUCCESS && bytes == 4);
    memset(&p, 0, sizeof(p));
    replace_op(&p, c, base.file_id, "replace-same-alias");
    query(&p, c, base.file_id, 1, 0x30);
    atomic_store(&noop_retry_calls, 0);
    atomic_store(&noop_retry, 1);
    doc_send(&p, c);
    atomic_store(&noop_retry, 0);
    assert(atomic_load(&noop_retry_calls) == 2);
    assert(atomic_load(&checked_renames) == 2 && atomic_load(&replacing_renames) == 2);
    assert(!atomic_load(&rename_notifications));
    name_result(c, 1, "replace-same-inode");
    assert(smb2_create(c, "replace-same-inode", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &probe) == ST_SUCCESS);
    assert(smb2_read(c, probe.file_id, 0, 4, data, &bytes) == ST_SUCCESS &&
           bytes == 4 && !memcmp(data, "same", 4));
    assert(smb2_close(c, probe.file_id) == ST_SUCCESS);
    assert(smb2_create(c, "replace-same-alias", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &probe) == ST_SUCCESS);
    assert(smb2_close(c, probe.file_id) == ST_SUCCESS);
    assert(smb2_close(c, base.file_id) == ST_SUCCESS);
} /* replace_if_exists_cases */

static void
same_inode_doc_cases(struct smb2_conn *c)
{
    struct chimera_vfs_module *module = dlsym(RTLD_DEFAULT, "vfs_memfs");

    assert(module);
    for (unsigned int legacy = 0; legacy < 2; legacy++) {
        const char            *source = legacy ? "legacy-alias-doc-source" : "native-alias-doc-source";
        const char            *alias  = legacy ? "legacy-alias-doc-alias" : "native-alias-doc-alias";
        struct smb2_create_out open, probe;
        assert(smb2_create(c, source, MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD,
                           NULL, &open) == ST_SUCCESS);
        hardlink(c, open.file_id, alias);
        uint64_t               caps = module->capabilities;
        if (legacy) {
            module->capabilities &= ~CHIMERA_VFS_CAP_RENAME_NOREPLACE;
        }
        struct packet          p = { 0 };
        replace_op(&p, c, open.file_id, alias);
        query(&p, c, open.file_id, 1, 0x30);
        /* The fallback parent/destination lookup is one ungrouped compound;
         * the QUERY suffix contributes the native command group. */
        if (legacy) {
            send_runs_groups(&p, c, 3, -1);
        } else {
            doc_send(&p, c);
        }
        module->capabilities = caps;
        assert(!atomic_load(&rename_notifications));
        name_result(c, 1, source);
        /* This DOC mutation runs after the accepted no-op, never in the
         * rejected finish test. It must remove the original link, not alias. */
        assert(smb2_set_disposition(c, open.file_id, 1) == ST_SUCCESS);
        assert(smb2_close(c, open.file_id) == ST_SUCCESS);
        assert(smb2_create(c, source, MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD,
                           NULL, &probe) == ST_OBJECT_NAME_NOT_FOUND);
        assert(smb2_create(c, alias, MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD,
                           NULL, &probe) == ST_SUCCESS);
        assert(smb2_close(c, probe.file_id) == ST_SUCCESS);
    }
} /* same_inode_doc_cases */

static void
cancel_after_initial_move_case(struct smb2_conn *c)
{
    struct smb2_create_out source, alias, probe;
    uint8_t                data[8]; uint32_t count;

    assert(smb2_create(c, "cancel-move-old", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &source) == ST_SUCCESS);
    assert(smb2_write(c, source.file_id, 0, "kept", 4, &count) == ST_SUCCESS && count == 4);
    hardlink(c, source.file_id, "cancel-move-alias");
    assert(smb2_create(c, "cancel-move-alias", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &alias) == ST_SUCCESS);
    struct packet          p = { 0 };
    replace_op(&p, c, source.file_id, "cancel-move-new");
    p.expected[0] = ST_CANCELLED;
    atomic_store(&canceled_initial_move, 0);
    atomic_store(&cancel_initial_move, 1);
    doc_send(&p, c);
    assert(atomic_load(&canceled_initial_move) && !atomic_load(&cancel_initial_move));
    assert(atomic_load(&checked_renames) == 1 && !atomic_load(&replacing_renames));
    assert(atomic_load(&rename_notifications) == 1);
    memset(&p, 0, sizeof(p));
    query(&p, c, source.file_id, 1, 0x30);
    query(&p, c, alias.file_id, 1, 0x30);
    doc_send(&p, c);
    name_result(c, 0, "cancel-move-new"); name_result(c, 1, "cancel-move-alias");
    assert(smb2_create(c, "cancel-move-old", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &probe) == ST_OBJECT_NAME_NOT_FOUND);
    assert(smb2_create(c, "cancel-move-new", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &probe) == ST_SUCCESS);
    assert(smb2_read(c, probe.file_id, 0, 4, data, &count) == ST_SUCCESS &&
           count == 4 && !memcmp(data, "kept", 4));
    assert(smb2_close(c, probe.file_id) == ST_SUCCESS);
    /* DOC follows the accepted new link, even though the rename command's
     * cleanup suffix was canceled. Neither old nor independent alias is used. */
    assert(smb2_set_disposition(c, source.file_id, 1) == ST_SUCCESS);
    assert(smb2_close(c, source.file_id) == ST_SUCCESS);
    assert(smb2_close(c, alias.file_id) == ST_SUCCESS);
    assert(smb2_create(c, "cancel-move-new", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &probe) == ST_OBJECT_NAME_NOT_FOUND);
    assert(smb2_create(c, "cancel-move-alias", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &probe) == ST_SUCCESS);
    assert(smb2_close(c, probe.file_id) == ST_SUCCESS);
} /* cancel_after_initial_move_case */

static void
replacement_delayed_open_cases(
    struct smb2_env  *env,
    struct smb2_conn *c)
{
    struct smb2_conn *d = smb2_conn_open(env);

    smb2_handshake(d);
    for (unsigned int legacy = 0; legacy < 2; legacy++) {
        const char            *source_name = legacy ? "replace-delayed-legacy-source" : "replace-delayed-native-source";
        const char            *target_name = legacy ? "replace-delayed-legacy-target" : "replace-delayed-native-target";
        struct smb2_create_out source, target;
        assert(smb2_create(c, source_name, MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                           MBT_FILE_SHARE_RWD, NULL, &source) == ST_SUCCESS);
        assert(smb2_create(c, target_name, MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                           MBT_FILE_SHARE_RWD, NULL, &target) == ST_SUCCESS);
        assert(smb2_close(c, target.file_id) == ST_SUCCESS);
        struct packet          create = { 0 }, rename = { 0 };
        /* Mandatory level-II caching uses the legacy CREATE path. The pause
         * precedes frontend admission and hence precedes its caching grant. */
        create_op(&create, d, target_name, MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS);
        if (legacy) {
            create.data[SMB2_HDR_SIZE + 3] = SMB2_OPLOCK_LEVEL_II;
            p32(create.data + SMB2_HDR_SIZE, 40, 0x10000);
        }
        int                    create_replies = d->nreply_app;
        atomic_store(&open_result_held, 0);
        atomic_store(&release_open_result, 0);
        atomic_store(&hold_open_result, 1);
        memcpy(d->sbuf + 4, create.data, create.length);
        smb2c_send(d, create.length - SMB2_HDR_SIZE);
        d->msg_id = create.first_mid + create.count;
        uint64_t               deadline = smb2c_now_ms() + SMB2C_HANG_MS;
        while (!atomic_load(&open_result_held)) {
            smb2_pump(env);
            assert(smb2c_now_ms() < deadline && !d->disconnected);
        }
        assert(d->nreply_app == create_replies);
        replace_op(&rename, c, source.file_id, target_name);
        query(&rename, c, source.file_id, 1, 0x30);
        int                    rename_replies = c->nreply_app;
        atomic_store(&rename_held, 0);
        atomic_store(&release_rename, 0);
        atomic_store(&hold_legacy_replace, 1);
        memcpy(c->sbuf + 4, rename.data, rename.length);
        smb2c_send(c, rename.length - SMB2_HDR_SIZE);
        c->msg_id = rename.first_mid + rename.count;
        while (!atomic_load(&rename_held)) {
            smb2_pump(env);
            assert(smb2c_now_ms() < deadline && !c->disconnected);
        }
        /* Native replacement must decline while the old backend handle is
         * still invisible. Stop the legacy fallback before mutation, then
         * drain this opener so the fixture never creates a stale publication. */
        assert(!(rename_gate.flags & CHIMERA_VFS_RENAME_MATCH_DEST_FH));
        assert(atomic_load(&open_result_held));
        atomic_store(&release_open_result, 1);
        assert(smb2c_pump_for_nreply(d, create_replies, "delayed OPEN admission"));
        check_replies(&create, d);
        uint8_t fid[16];
        memcpy(fid, d->rbuf + 4 + SMB2_HDR_SIZE + 64, sizeof(fid));
        assert(smb2_close(d, fid) == ST_SUCCESS);
        atomic_store(&release_rename, 1);
        assert(smb2c_pump_for_nreply(c, rename_replies, "replacement after delayed OPEN"));
        check_replies(&rename, c);
        assert(smb2_close(c, source.file_id) == ST_SUCCESS);
    }
} /* replacement_delayed_open_cases */

static void
replacement_constructor_races(
    struct smb2_env  *env,
    struct smb2_conn *c)
{
    struct smb2_conn *d = smb2_conn_open(env);

    smb2_handshake(d);
    for (unsigned int legacy = 0; legacy < 2; legacy++) {
        const char            *source_name = legacy ? "replace-writer-legacy-source" : "replace-writer-native-source";
        const char            *target_name = legacy ? "replace-writer-legacy-target" : "replace-writer-native-target";
        struct smb2_create_out source, target;
        uint32_t               bytes;
        uint8_t                data[8], late_fid[16];
        assert(smb2_create(c, source_name, MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                           MBT_FILE_SHARE_RWD, NULL, &source) == ST_SUCCESS);
        assert(smb2_write(c, source.file_id, 0, "new", 3, &bytes) == ST_SUCCESS && bytes == 3);
        assert(smb2_create(c, target_name, MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                           MBT_FILE_SHARE_RWD, NULL, &target) == ST_SUCCESS);
        assert(smb2_write(c, target.file_id, 0, "old", 3, &bytes) == ST_SUCCESS && bytes == 3);
        assert(smb2_close(c, target.file_id) == ST_SUCCESS);
        struct packet          p = { 0 }, late = { 0 };
        replace_op(&p, c, source.file_id, target_name);
        query(&p, c, source.file_id, 1, 0x30);
        int                    replies = c->nreply_app;
        atomic_store(&rename_held, 0);
        atomic_store(&release_rename, 0);
        atomic_store(&constructor_denials, 0);
        atomic_store(&hold_replace, 1);
        memcpy(c->sbuf + 4, p.data, p.length);
        smb2c_send(c, p.length - SMB2_HDR_SIZE);
        c->msg_id = p.first_mid + p.count;
        uint64_t               deadline = smb2c_now_ms() + SMB2C_HANG_MS;
        while (!atomic_load(&rename_held)) {
            smb2_pump(env);
            assert(smb2c_now_ms() < deadline && !c->disconnected);
        }
        assert(rename_gate.flags & CHIMERA_VFS_RENAME_MATCH_DEST_FH);
        assert(c->nreply_app == replies);
        create_op(&late, d, target_name, MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS);
        /* Create-time DOC retains the legacy CREATE entrypoint. Both paths
        * must wait before acquiring the old destination backend handle. */
        if (legacy) {
            p32(late.data + SMB2_HDR_SIZE, 40, 0x1000);
        }
        int late_replies = d->nreply_app;
        memcpy(d->sbuf + 4, late.data, late.length);
        smb2c_send(d, late.length - SMB2_HDR_SIZE);
        d->msg_id = late.first_mid + late.count;
        while (!atomic_load(&constructor_denials)) {
            smb2_pump(env);
            assert(smb2c_now_ms() < deadline && !d->disconnected);
        }
        assert(d->nreply_app == late_replies);
        atomic_store(&release_rename, 1);
        assert(smb2c_pump_for_nreply(c, replies, "replace writer completion"));
        check_replies(&p, c);
        assert(smb2c_pump_for_nreply(d, late_replies, "late constructor admission"));
        check_replies(&late, d);
        memcpy(late_fid, d->rbuf + 4 + SMB2_HDR_SIZE + 64, sizeof(late_fid));
        assert(smb2_read(d, late_fid, 0, 3, data, &bytes) == ST_SUCCESS && bytes == 3 && !memcmp(data, "new", 3));
        assert(smb2_close(d, late_fid) == ST_SUCCESS);
        assert(smb2_close(c, source.file_id) == ST_SUCCESS);
    }
} /* replacement_constructor_races */

static void
replace_destination_races(
    struct smb2_env  *env,
    struct smb2_conn *c)
{
    struct smb2_conn      *d = smb2_conn_open(env);
    struct smb2_create_out source, target, probe;
    uint8_t                data[8];
    uint32_t               bytes;

    smb2_handshake(d);
    for (unsigned int disappears = 0; disappears < 2; disappears++) {
        const char   *old  = disappears ? "replace-race-gone-old" : "replace-race-new-old";
        const char   *dest = disappears ? "rename-dir\\replace-race-gone" : "rename-dir\\replace-race-new";
        struct packet p    = { 0 };
        assert(smb2_create(c, old, MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                           MBT_FILE_SHARE_RWD, NULL, &source) == ST_SUCCESS);
        assert(smb2_write(c, source.file_id, 0, "source", 6, &bytes) == ST_SUCCESS && bytes == 6);
        if (disappears) {
            assert(smb2_create(d, dest, MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                               MBT_FILE_SHARE_RWD, NULL, &target) == ST_SUCCESS);
        }
        query(&p, c, source.file_id, 1, 0x30);
        replace_op(&p, c, source.file_id, dest);
        query(&p, c, source.file_id, 1, 0x30);
        atomic_store(&submissions, 0);
        atomic_store(&checked_renames, 0);
        atomic_store(&replacing_renames, 0);
        atomic_store(&rename_notifications, 0);
        atomic_store(&expected_groups, -1);
        atomic_store(&rename_held, 0);
        atomic_store(&release_rename, 0);
        atomic_store(&hold_rename, 1);
        atomic_store(&armed, 1);
        int      replies = c->nreply_app;
        memcpy(c->sbuf + 4, p.data, p.length);
        smb2c_send(c, p.length - SMB2_HDR_SIZE);
        c->msg_id = p.first_mid + p.count;
        uint64_t deadline = smb2c_now_ms() + SMB2C_HANG_MS;
        while (!atomic_load(&rename_held)) {
            smb2_pump(env);
            assert(smb2c_now_ms() < deadline && !c->disconnected);
        }
        assert(c->nreply_app == replies && !atomic_load(&rename_notifications));
        atomic_store(&armed, 0);
        if (disappears) {
            assert(smb2_rename(d, target.file_id, "replace-race-target-saved", 0) == ST_SUCCESS);
        } else {
            assert(smb2_create(d, dest, MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                               MBT_FILE_SHARE_RWD, NULL, &target) == ST_SUCCESS);
        }
        assert(smb2_close(d, target.file_id) == ST_SUCCESS);
        atomic_store(&expect_clean_fallback, 1);
        atomic_store(&armed, 1);
        atomic_store(&release_rename, 1);
        assert(smb2c_pump_for_nreply(c, replies, "ReplaceIfExists destination race"));
        atomic_store(&armed, 0);
        atomic_store(&expect_clean_fallback, 0);
        check_replies(&p, c);
        assert(atomic_load(&checked_renames) == 1);
        assert(atomic_load(&replacing_renames) == (disappears ? 0 : 1));
        assert(atomic_load(&submissions) == 1);
        name_result(c, 0, old);
        name_result(c, 2, dest);
        assert(smb2_create(d, dest, MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                           MBT_FILE_SHARE_RWD, NULL, &probe) == ST_SUCCESS);
        assert(smb2_read(d, probe.file_id, 0, 6, data, &bytes) == ST_SUCCESS &&
               bytes == 6 && !memcmp(data, "source", 6));
        assert(smb2_close(d, probe.file_id) == ST_SUCCESS);
        assert(smb2_close(c, source.file_id) == ST_SUCCESS);
    }

    /* An open whose original name now identifies a different inode must fail
     * strict source matching, never fall back and move the replacement. */
    assert(smb2_create(c, "replace-stale-source", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &source) == ST_SUCCESS);
    assert(smb2_create(d, "replace-stale-other", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &target) == ST_SUCCESS);
    assert(smb2_write(d, target.file_id, 0, "other", 5, &bytes) == ST_SUCCESS && bytes == 5);
    assert(smb2_rename(d, target.file_id, "replace-stale-source", 1) == ST_SUCCESS);
    struct packet p = { 0 };
    replace_op(&p, c, source.file_id, "replace-stale-unwanted");
    p.expected[0] = ST_OBJECT_NAME_NOT_FOUND;
    query(&p, c, source.file_id, 1, 0x30);
    doc_send(&p, c);
    assert(atomic_load(&checked_renames) == 1 && !atomic_load(&replacing_renames));
    assert(!atomic_load(&rename_notifications));
    name_result(c, 1, "replace-stale-source");
    assert(smb2_create(d, "replace-stale-unwanted", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &probe) == ST_OBJECT_NAME_NOT_FOUND);
    assert(smb2_read(d, target.file_id, 0, 5, data, &bytes) == ST_SUCCESS &&
           bytes == 5 && !memcmp(data, "other", 5));
    assert(smb2_close(d, target.file_id) == ST_SUCCESS);
    assert(smb2_close(c, source.file_id) == ST_SUCCESS);
} /* replace_destination_races */

/* A base-link move must update both stream participant snapshots while keeping
 * the independent hardlink alias unchanged. Every command below is part of the
 * same VFS compound, including the stream queries and ordinary closes. */
static void
stream_holder_rename_cases(struct smb2_conn *c)
{
    struct smb2_create_out base, stream, sibling, alias, probe;
    struct packet          p = { 0 };
    uint32_t               bytes;
    uint8_t                data[8];

    assert(smb2_create(c, "stream-rename-old", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &base) == ST_SUCCESS);
    hardlink(c, base.file_id, "stream-rename-alias");
    assert(smb2_create(c, "stream-rename-old:fork:$DATA", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &stream) == ST_SUCCESS);
    assert(smb2_create(c, "stream-rename-old:sibling:$DATA", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &sibling) == ST_SUCCESS);
    assert(smb2_create(c, "stream-rename-alias:fork:$DATA", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &alias) == ST_SUCCESS);
    assert(smb2_write(c, stream.file_id, 0, "stream", 6, &bytes) == ST_SUCCESS && bytes == 6);
    query(&p, c, stream.file_id, 1, 0x30);
    rename_op(&p, c, base.file_id, "rename-dir\\stream-middle");
    query(&p, c, stream.file_id, 1, 0x30);
    query(&p, c, sibling.file_id, 1, 0x30);
    query(&p, c, alias.file_id, 1, 0x30);
    rename_op(&p, c, base.file_id, "stream-rename-final");
    query(&p, c, stream.file_id, 1, 0x30);
    query(&p, c, alias.file_id, 1, 0x30);
    doc_send(&p, c);
    name_result(c, 0, "stream-rename-old");
    name_result(c, 2, "rename-dir\\stream-middle");
    name_result(c, 3, "rename-dir\\stream-middle");
    name_result(c, 4, "stream-rename-alias");
    name_result(c, 6, "stream-rename-final");
    name_result(c, 7, "stream-rename-alias");
    /* A new batch reads the accepted public snapshots, not the path overlay. */
    memset(&p, 0, sizeof(p));
    query(&p, c, stream.file_id, 1, 0x30);
    query(&p, c, sibling.file_id, 1, 0x30);
    query(&p, c, alias.file_id, 1, 0x30);
    doc_send(&p, c);
    name_result(c, 0, "stream-rename-final");
    name_result(c, 1, "stream-rename-final");
    name_result(c, 2, "stream-rename-alias");
    assert(smb2_read(c, stream.file_id, 0, 6, data, &bytes) == ST_SUCCESS &&
           bytes == 6 && !memcmp(data, "stream", 6));
    assert(smb2_create(c, "stream-rename-old:fork:$DATA", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &probe) == ST_OBJECT_NAME_NOT_FOUND);
    assert(smb2_create(c, "stream-rename-final:fork:$DATA", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &probe) == ST_SUCCESS);
    memset(&p, 0, sizeof(p));
    close_op(&p, c, probe.file_id);
    close_op(&p, c, stream.file_id);
    close_op(&p, c, sibling.file_id);
    close_op(&p, c, alias.file_id);
    doc_send(&p, c);
    /* The base's ordinary DOC-capable CLOSE uses preflight admission. Stream
     * CLOSE uses late dual-identity fences, so the runtime intentionally ends
     * the first VFS run before mixing those two admission modes. */
    assert(smb2_close(c, base.file_id) == ST_SUCCESS);

    /* CREATE-DOC remains a boundary, but its accepted stream name anchor must
     * follow a native base rename and delete only that stream at close. */
    assert(smb2_create(c, "stream-doc-base", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &base) == ST_SUCCESS);
    assert(smb2_create_opts(c, "stream-doc-base:fork:$DATA", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                            MBT_FILE_SHARE_RWD, MBT_FILE_DELETE_ON_CLOSE, NULL, &stream) == ST_SUCCESS);
    memset(&p, 0, sizeof(p));
    rename_op(&p, c, base.file_id, "rename-dir\\stream-doc-moved");
    query(&p, c, stream.file_id, 1, 0x30);
    doc_send(&p, c);
    name_result(c, 1, "rename-dir\\stream-doc-moved");
    assert(smb2_close(c, stream.file_id) == ST_SUCCESS);
    assert(smb2_create(c, "rename-dir\\stream-doc-moved:fork:$DATA", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &probe) == ST_OBJECT_NAME_NOT_FOUND);
    assert(smb2_create(c, "rename-dir\\stream-doc-moved", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &probe) == ST_SUCCESS);
    assert(smb2_close(c, probe.file_id) == ST_SUCCESS);
    assert(smb2_close(c, base.file_id) == ST_SUCCESS);

    /* A readonly, same-name retry is safe even with live stream peers. */
    assert(smb2_create(c, "stream-noop", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &base) == ST_SUCCESS);
    assert(smb2_create(c, "stream-noop:fork:$DATA", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &stream) == ST_SUCCESS);
    memset(&p, 0, sizeof(p));
    rename_op(&p, c, base.file_id, "stream-noop");
    query(&p, c, stream.file_id, 1, 0x30);
    atomic_store(&noop_retry_calls, 0);
    atomic_store(&noop_retry, 1);
    doc_send(&p, c);
    atomic_store(&noop_retry, 0);
    assert(atomic_load(&noop_retry_calls) == 2);
    name_result(c, 1, "stream-noop");
    assert(smb2_close(c, stream.file_id) == ST_SUCCESS);
    assert(smb2_close(c, base.file_id) == ST_SUCCESS);
} /* stream_holder_rename_cases */

/* A closed DOC opener owns the pending event's old link, even if every
 * surviving stream peer uses another hardlink. Accepted rename must repath that
 * event separately from the public peers' exact-link path updates. */
static void
closed_stream_doc_rename_cases(struct smb2_conn *c)
{
    for (int legacy = 0; legacy < 2; legacy++) {
        const char            *old        = legacy ? "pending-doc-legacy" : "pending-doc-native";
        const char            *alias_name = legacy ? "pending-doc-legacy-alias" : "pending-doc-native-alias";
        const char            *moved      = legacy ? "rename-dir\\pending-doc-legacy-moved" :
            "rename-dir\\pending-doc-native-moved";
        char                   stream_name[128], alias_stream[128], moved_stream[128];
        snprintf(stream_name, sizeof(stream_name), "%s:fork:$DATA", old);
        snprintf(alias_stream, sizeof(alias_stream), "%s:fork:$DATA", alias_name);
        snprintf(moved_stream, sizeof(moved_stream), "%s:fork:$DATA", moved);
        struct smb2_create_out base, doc, survivor, probe;
        struct packet          p = { 0 };
        assert(smb2_create(c, old, MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                           MBT_FILE_SHARE_RWD, NULL, &base) == ST_SUCCESS);
        hardlink(c, base.file_id, alias_name);
        assert(smb2_create_opts(c, stream_name, MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                                MBT_FILE_SHARE_RWD, MBT_FILE_DELETE_ON_CLOSE, NULL, &doc) == ST_SUCCESS);
        assert(smb2_create(c, alias_stream, MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                           MBT_FILE_SHARE_RWD, NULL, &survivor) == ST_SUCCESS);
        assert(smb2_close(c, doc.file_id) == ST_SUCCESS);
        if (legacy) {
            /* Create the actual replacement target before watching the DOC
             * removal: its ordinary CREATE can emit broad metadata events. */
            assert(smb2_create(c, moved, MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                               MBT_FILE_SHARE_RWD, NULL, &probe) == ST_SUCCESS);
            assert(smb2_close(c, probe.file_id) == ST_SUCCESS);
        }
        pending_doc_expected_name = legacy ? "pending-doc-legacy-moved" : "pending-doc-native-moved";
        pending_doc_parent_fh_len = 0;
        atomic_store(&pending_doc_notifications, 0);
        atomic_store(&pending_doc_watch, 1);
        if (legacy) {
            /* Actual target replacement retains the legacy publisher. Merely
             * permitting replacement now stays native for an absent target. */
            assert(smb2_rename(c, base.file_id, moved, 1) == ST_SUCCESS);
        } else {
            rename_op(&p, c, base.file_id, moved);
            query(&p, c, survivor.file_id, 1, 0x30);
            doc_send(&p, c);
            name_result(c, 1, alias_name);
        }
        assert(pending_doc_parent_fh_len != 0);
        assert(smb2_close(c, survivor.file_id) == ST_SUCCESS);
        atomic_store(&pending_doc_watch, 0);
        assert(atomic_load(&pending_doc_notifications) == 1);
        assert(smb2_create(c, moved_stream, MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                           MBT_FILE_SHARE_RWD, NULL, &probe) == ST_OBJECT_NAME_NOT_FOUND);
        assert(smb2_create(c, alias_stream, MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                           MBT_FILE_SHARE_RWD, NULL, &probe) == ST_OBJECT_NAME_NOT_FOUND);
        assert(smb2_create(c, moved, MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                           MBT_FILE_SHARE_RWD, NULL, &probe) == ST_SUCCESS);
        assert(smb2_close(c, probe.file_id) == ST_SUCCESS);
        assert(smb2_close(c, base.file_id) == ST_SUCCESS);
    }
} /* closed_stream_doc_rename_cases */

static void
pending_stream_rename_case(
    struct smb2_env  *env,
    struct smb2_conn *c)
{
    struct smb2_conn      *d = smb2_conn_open(env);
    struct smb2_create_out base, stream;
    struct packet          p = { 0 };

    smb2_handshake(d);
    assert(smb2_create(c, "pending-stream-old", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &base) == ST_SUCCESS);
    assert(smb2_create(c, "pending-stream-old:fork:$DATA", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &stream) == ST_SUCCESS);
    assert(smb2_close(c, stream.file_id) == ST_SUCCESS);
    create_op(&p, d, "pending-stream-old:fork:$DATA", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS);
    producer_gate.calls  = 0;
    producer_gate.reject = false;
    atomic_store(&producer_held, 0);
    atomic_store(&release_producer, 0);
    atomic_store(&submissions, 0);
    atomic_store(&expected_groups, 1);
    atomic_store(&armed, 1);
    atomic_store(&hold_producer, 1);
    int      replies = d->nreply_app;
    memcpy(d->sbuf + 4, p.data, p.length);
    smb2c_send(d, p.length - SMB2_HDR_SIZE);
    d->msg_id = p.first_mid + p.count;
    uint64_t deadline = smb2c_now_ms() + SMB2C_HANG_MS;
    while (!atomic_load(&producer_held)) {
        smb2_pump(env);
        assert(smb2c_now_ms() < deadline && !d->disconnected);
    }
    atomic_store(&armed, 0);
    atomic_store(&hold_producer, 0);
    assert(atomic_load(&submissions) == 1 && d->nreply_app == replies);
    memset(&p, 0, sizeof(p));
    rename_op(&p, c, base.file_id, "rename-dir\\pending-stream-moved");
    query(&p, c, base.file_id, 1, 0x30);
    doc_send(&p, c);
    name_result(c, 1, "rename-dir\\pending-stream-moved");
    atomic_store(&release_producer, 1);
    assert(smb2c_pump_for_nreply(d, replies, "pending stream name token acceptance"));
    assert(g32(d->rbuf + 4, 8) == ST_SUCCESS && producer_gate.calls == 1);
    memcpy(stream.file_id, d->rbuf + 4 + SMB2_HDR_SIZE + 64, 16);
    memset(&p, 0, sizeof(p));
    query(&p, d, stream.file_id, 1, 0x30);
    doc_send(&p, d);
    name_result(d, 0, "rename-dir\\pending-stream-moved");
    /* Revisit after pending base promotion: a stale base participant must not
     * pull the stream back to its original spelling on another accepted move. */
    memset(&p, 0, sizeof(p));
    rename_op(&p, c, base.file_id, "pending-stream-final");
    query(&p, c, base.file_id, 1, 0x30);
    doc_send(&p, c);
    memset(&p, 0, sizeof(p));
    query(&p, d, stream.file_id, 1, 0x30);
    close_op(&p, d, stream.file_id);
    doc_send(&p, d);
    name_result(d, 0, "pending-stream-final");
    assert(smb2_close(c, base.file_id) == ST_SUCCESS);
} /* pending_stream_rename_case */

/* Root-child directory rename can remain in a single wire batch with unrelated
* root-child handles live too. Closed descendants need no path journal:
* their actual subtree and data survive, while source peer paths are staged. */
static void
directory_native_cases(
    struct smb2_env  *env,
    struct smb2_conn *c)
{
    struct smb2_create_out dir, peer, nested, child, probe, sibling;
    struct packet          p = { 0 };
    uint32_t               bytes;
    uint8_t                data[16];

    assert(smb2_create(c, "native-directory-unrelated", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &sibling) == ST_SUCCESS);
    assert(smb2_create_opts(c, "native-directory-old", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                            MBT_FILE_SHARE_RWD, MBT_FILE_DIRECTORY_FILE, NULL, &dir) == ST_SUCCESS);
    assert(smb2_create_opts(c, "native-directory-old\\nested", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                            MBT_FILE_SHARE_RWD, MBT_FILE_DIRECTORY_FILE, NULL, &nested) == ST_SUCCESS);
    assert(smb2_create(c, "native-directory-old\\nested\\contents", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &child) == ST_SUCCESS);
    assert(smb2_write(c, child.file_id, 0, "subtree", 7, &bytes) == ST_SUCCESS && bytes == 7);
    assert(smb2_close(c, child.file_id) == ST_SUCCESS);
    assert(smb2_close(c, nested.file_id) == ST_SUCCESS);
    assert(smb2_create_opts(c, "native-directory-old", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                            MBT_FILE_SHARE_RWD, MBT_FILE_DIRECTORY_FILE, NULL, &peer) == ST_SUCCESS);
    rename_op(&p, c, dir.file_id, "native-directory-middle");
    query(&p, c, peer.file_id, 1, 0x30);
    rename_op(&p, c, peer.file_id, "native-directory-final");
    query(&p, c, dir.file_id, 1, 0x30);
    doc_send(&p, c);
    assert(atomic_load(&checked_renames) == 2 && !atomic_load(&replacing_renames));
    assert(atomic_load(&rename_notifications) == 2);
    name_result(c, 1, "native-directory-middle");
    name_result(c, 3, "native-directory-final");
    /* Public directory CLOSE still has its separate fence admission run;
     * this assertion targets both renames and peer queries in one compound. */
    assert(smb2_close(c, dir.file_id) == ST_SUCCESS);
    assert(smb2_close(c, peer.file_id) == ST_SUCCESS);
    assert(smb2_create(c, "native-directory-final\\nested\\contents", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &probe) == ST_SUCCESS);
    assert(smb2_read(c, probe.file_id, 0, 7, data, &bytes) == ST_SUCCESS && bytes == 7 &&
           !memcmp(data, "subtree", 7));
    assert(smb2_close(c, probe.file_id) == ST_SUCCESS);
    assert(smb2_create_opts(c, "native-directory-old", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                            MBT_FILE_SHARE_RWD, MBT_FILE_DIRECTORY_FILE, NULL, &probe) == ST_OBJECT_NAME_NOT_FOUND);

    /* An earlier invisible root-child producer is permitted by the same
     * predicate as a published sibling. Its constructor token can promote to
     * this wire's directory writer without discarding the private handle. */
    assert(smb2_create_opts(c, "native-directory-final", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                            MBT_FILE_SHARE_RWD, MBT_FILE_DIRECTORY_FILE, NULL, &dir) == ST_SUCCESS);
    memset(&p, 0, sizeof(p));
    create_op(&p, c, "native-directory-private-sibling", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS);
    rename_op(&p, c, dir.file_id, "native-directory-private-moved");
    query(&p, c, dir.file_id, 1, 0x30);
    doc_send(&p, c);
    assert(atomic_load(&checked_renames) == 1);
    name_result(c, 2, "native-directory-private-moved");
    memcpy(child.file_id, c->rbuf + 4 + SMB2_HDR_SIZE + 64, 16);
    assert(smb2_close(c, child.file_id) == ST_SUCCESS);
    assert(smb2_close(c, dir.file_id) == ST_SUCCESS);

    /* Producer-relative directory identity/path is usable before publication;
     * a same-wire OPEN -> RENAME -> QUERY -> CLOSE is one VFS compound. */
    uint8_t inherited[16];
    memset(inherited, 0xff, sizeof(inherited));
    memset(&p, 0, sizeof(p));
    create_op(&p, c, "native-directory-private-moved", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS);
    p32(p.data + p.previous + SMB2_HDR_SIZE, 40, MBT_FILE_DIRECTORY_FILE);
    rename_op(&p, c, inherited, "native-directory-producer"); related(&p);
    query(&p, c, inherited, 1, 0x30); related(&p);
    close_op(&p, c, inherited); related(&p);
    doc_send(&p, c);
    assert(atomic_load(&checked_renames) == 1);
    name_result(c, 2, "native-directory-producer");

    /* Exact same-link no-op can reject a finish and reacquire its constructor
     * writer safely. No namespace mutation is ever rolled back by this test. */
    assert(smb2_create_opts(c, "native-directory-producer", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                            MBT_FILE_SHARE_RWD, MBT_FILE_DIRECTORY_FILE, NULL, &dir) == ST_SUCCESS);
    memset(&p, 0, sizeof(p));
    rename_op(&p, c, dir.file_id, "native-directory-producer");
    query(&p, c, dir.file_id, 1, 0x30);
    atomic_store(&noop_retry_calls, 0);
    atomic_store(&noop_retry, 1);
    doc_send(&p, c);
    atomic_store(&noop_retry, 0);
    assert(atomic_load(&noop_retry_calls) == 2 && !atomic_load(&rename_notifications));
    name_result(c, 1, "native-directory-producer");
    assert(smb2_close(c, dir.file_id) == ST_SUCCESS);

    /* A late backend constructor cannot acquire the old subtree spelling
    * between directory validation and atomic rename/path publication. */
    struct smb2_conn *d = smb2_conn_open(env);
    smb2_handshake(d);
    assert(smb2_create_opts(c, "native-directory-producer", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                            MBT_FILE_SHARE_RWD, MBT_FILE_DIRECTORY_FILE, NULL, &dir) == ST_SUCCESS);
    memset(&p, 0, sizeof(p));
    rename_op(&p, c, dir.file_id, "native-directory-gated");
    atomic_store(&hold_rename, 1);
    atomic_store(&rename_held, 0);
    atomic_store(&release_rename, 0);
    atomic_store(&constructor_denials, 0);
    int           rename_replies = c->nreply_app;
    memcpy(c->sbuf + 4, p.data, p.length);
    smb2c_send(c, p.length - SMB2_HDR_SIZE);
    c->msg_id = p.first_mid + p.count;
    uint64_t      deadline = smb2c_now_ms() + SMB2C_HANG_MS;
    while (!atomic_load(&rename_held)) {
        smb2_pump(env);
        assert(smb2c_now_ms() < deadline && !c->disconnected);
    }
    struct packet late = { 0 };
    create_op(&late, d, "native-directory-producer\\nested\\contents", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS);
    late.expected[0] = 0xC000003Au; /* the old ancestor is gone after acceptance */
    int           late_replies = d->nreply_app;
    memcpy(d->sbuf + 4, late.data, late.length);
    smb2c_send(d, late.length - SMB2_HDR_SIZE);
    d->msg_id = late.first_mid + late.count;
    while (!atomic_load(&constructor_denials)) {
        smb2_pump(env);
        assert(smb2c_now_ms() < deadline && !d->disconnected);
    }
    assert(c->nreply_app == rename_replies && d->nreply_app == late_replies);
    atomic_store(&release_rename, 1);
    assert(smb2c_pump_for_nreply(c, rename_replies, "directory rename publication"));
    check_replies(&p, c);
    assert(smb2c_pump_for_nreply(d, late_replies, "directory constructor admission"));
    check_replies(&late, d);
    assert(smb2_close(c, dir.file_id) == ST_SUCCESS);

    /* An earlier invisible child producer must prevent directory admission.
     * The accepted CREATE prefix then enters the legacy contained-open check. */
    assert(smb2_create_opts(c, "native-directory-gated", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                            MBT_FILE_SHARE_RWD, MBT_FILE_DIRECTORY_FILE, NULL, &dir) == ST_SUCCESS);
    memset(&p, 0, sizeof(p));
    create_op(&p, c, "native-directory-gated\\private-child", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS);
    rename_op(&p, c, dir.file_id, "native-directory-forbidden");
    p.expected[1] = ST_ACCESS_DENIED;
    /* This command references the last CREATE despite an explicit FileId on
     * the intervening rename. Keep its FileId from the actual CREATE reply. */
    int replies = c->nreply_app;
    memcpy(c->sbuf + 4, p.data, p.length);
    smb2c_send(c, p.length - SMB2_HDR_SIZE);
    c->msg_id = p.first_mid + p.count;
    (void) smb2c_wait(c);
    assert(c->nreply_app == replies + 1);
    check_replies(&p, c);
    memcpy(child.file_id, c->rbuf + 4 + SMB2_HDR_SIZE + 64, 16);
    assert(smb2_close(c, child.file_id) == ST_SUCCESS);
    assert(smb2_close(c, dir.file_id) == ST_SUCCESS);
    assert(smb2_close(c, sibling.file_id) == ST_SUCCESS);
} /* directory_native_cases */

/* All namespace mutators are readers of directory/target admission, including
 * their legacy continuation. Hold the actual writer just before backend RENAME
 * and prove neither RENAME nor LINK can enter the old subtree behind its scan. */
static void
directory_mutator_races(
    struct smb2_env  *env,
    struct smb2_conn *c)
{
    for (unsigned int mode = 0; mode < 5; mode++) {
        struct smb2_conn      *d = smb2_conn_open(env);
        smb2_handshake(d);
        char                   old[64], moved[64], source_name[64], target[96];
        snprintf(old, sizeof(old), "mutation-dir-%u", mode);
        snprintf(moved, sizeof(moved), "mutation-moved-%u", mode);
        snprintf(source_name, sizeof(source_name), "mutation-source-%u", mode);
        snprintf(target, sizeof(target), "%s\\entered", old);
        struct smb2_create_out dir, source, probe;
        assert(smb2_create_opts(c, old, MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                                MBT_FILE_SHARE_RWD, MBT_FILE_DIRECTORY_FILE, NULL, &dir) == ST_SUCCESS);
        assert(smb2_create_opts(d, source_name, MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                                MBT_FILE_SHARE_RWD, mode == 2 ? MBT_FILE_DIRECTORY_FILE : 0, NULL, &source) ==
               ST_SUCCESS);
        struct packet          p = { 0 }, late = { 0 };
        rename_op(&p, c, dir.file_id, moved);
        atomic_store(&armed, 1);
        atomic_store(&expected_groups, -1);
        atomic_store(&checked_renames, 0);
        atomic_store(&rename_notifications, 0);
        atomic_store(&hold_rename, 1);
        atomic_store(&rename_held, 0);
        atomic_store(&release_rename, 0);
        atomic_store(&constructor_denials, 0);
        int      replies = c->nreply_app;
        memcpy(c->sbuf + 4, p.data, p.length);
        smb2c_send(c, p.length - SMB2_HDR_SIZE);
        c->msg_id = p.first_mid + p.count;
        uint64_t deadline = smb2c_now_ms() + SMB2C_HANG_MS;
        while (!atomic_load(&rename_held)) {
            smb2_pump(env);
            assert(smb2c_now_ms() < deadline && !c->disconnected);
        }
        assert(rename_gate.flags & CHIMERA_VFS_RENAME_SRC_IS_DIR);
        if (mode == 1 || mode == 4) {
            uint8_t      value[256] = { 0 };
            unsigned int len        = strlen(target);
            p32(value, 16, 2 * len);
            for (unsigned int i = 0; i < len; i++) {
                p16(value, 20 + 2 * i, target[i]);
            }
            set(&late, d, source.file_id, 0x0B, value, 20 + 2 * len);
        } else {
            rename_op(&late, d, source.file_id, target);
        }
        late.expected[0] = mode == 3 ? ST_CANCELLED : ST_OBJECT_PATH_NOT_FOUND;
        if (mode == 4) {
            /* Force legacy CREATE so the old batch-only disconnect cutoff
             * cannot hide an incorrectly dispatched mutating wire suffix. */
            create_op(&late, d, "mutation-abandoned-suffix", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS);
            late.data[late.previous + SMB2_HDR_SIZE + 3] = SMB2_OPLOCK_LEVEL_II;
            p32(late.data + late.previous + SMB2_HDR_SIZE, 40, 0x10000);
            atomic_store(&suffix_dispatches, 0);
            atomic_store(&suffix_backend_opens, 0);
            atomic_store(&suffix_drained, 0);
            atomic_store(&watch_disconnected_suffix, 1);
        }
        int late_replies = d->nreply_app;
        int interim      = d->ninterim;
        memcpy(d->sbuf + 4, late.data, late.length);
        smb2c_send(d, late.length - SMB2_HDR_SIZE);
        d->msg_id = late.first_mid + late.count;
        /* Native contention defers untouched. Only the legacy pre-resolve
         * admission wrapper sends this interim and owns the polling timer. */
        while (d->ninterim == interim) {
            smb2_pump(env);
            assert(smb2c_now_ms() < deadline && !d->disconnected);
        }
        assert(atomic_load(&constructor_denials));
        assert(c->nreply_app == replies && d->nreply_app == late_replies);
        assert(atomic_load(&checked_renames) == 1 && !atomic_load(&rename_notifications));
        if (mode == 3) {
            smb2_cancel_post(d, late.first_mid, d->last_async_id);
            assert(smb2c_pump_for_nreply(d, late_replies, "cancel namespace admission"));
            check_replies(&late, d);
            assert(atomic_load(&rename_held) && !atomic_load(&rename_notifications));
        } else if (mode == 4) {
            smb2_conn_disconnect(d);
            while (!atomic_load(&suffix_drained)) {
                smb2_pump(env);
                assert(smb2c_now_ms() < deadline);
            }
            assert(!atomic_load(&suffix_dispatches) && !atomic_load(&suffix_backend_opens));
            atomic_store(&watch_disconnected_suffix, 0);
        }
        atomic_store(&release_rename, 1);
        assert(smb2c_pump_for_nreply(c, replies, "directory writer vs mutator"));
        check_replies(&p, c);
        if (mode < 3) {
            assert(smb2c_pump_for_nreply(d, late_replies, "mutator after directory publication"));
            check_replies(&late, d);
        }
        atomic_store(&armed, 0);
        if (mode == 4) {
            assert(smb2_create(c, "mutation-abandoned-suffix", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                               MBT_FILE_SHARE_RWD, NULL, &probe) == ST_OBJECT_NAME_NOT_FOUND);
        }
        /* The blocked/canceled mutator did not move its existing source. */
        assert(smb2_create_opts(c, source_name, MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                                MBT_FILE_SHARE_RWD, mode == 2 ? MBT_FILE_DIRECTORY_FILE : 0, NULL, &probe) == ST_SUCCESS
               );
        assert(smb2_close(c, probe.file_id) == ST_SUCCESS);
        if (mode != 4) {
            assert(smb2_close(d, source.file_id) == ST_SUCCESS);
        }
        assert(smb2_close(c, dir.file_id) == ST_SUCCESS);
    }
} /* directory_mutator_races */

static void
directory_failed_move_case(struct smb2_conn *c)
{
    struct smb2_create_out dir, source;

    assert(smb2_create_opts(c, "private-move-dir", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                            MBT_FILE_SHARE_RWD, MBT_FILE_DIRECTORY_FILE, NULL, &dir) == ST_SUCCESS);
    assert(smb2_create(c, "private-move-source", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &source) == ST_SUCCESS);
    struct packet          p = { 0 };
    rename_op(&p, c, source.file_id, "private-move-dir\\child");
    rename_op(&p, c, dir.file_id, "private-move-denied");
    /* The destination directory's DELETE-access holder rejects the incoming
     * regular move. Its unchanged private state must not poison the following
     * independent directory rename. */
    p.expected[0] = 0xC0000043u; /* SHARING_VIOLATION */
    int                    replies = c->nreply_app;
    atomic_store(&armed, 1);
    atomic_store(&expected_groups, -1);
    atomic_store(&checked_renames, 0);
    atomic_store(&replacing_renames, 0);
    memcpy(c->sbuf + 4, p.data, p.length);
    smb2c_send(c, p.length - SMB2_HDR_SIZE);
    c->msg_id = p.first_mid + p.count;
    (void) smb2c_wait(c);
    atomic_store(&armed, 0);
    assert(c->nreply_app == replies + 1);
    check_replies(&p, c);
    /* Only the directory rename reached the backend. The failed incoming
     * move staged no descendant path in either the registry or private view. */
    assert(atomic_load(&checked_renames) == 1 && !atomic_load(&replacing_renames));
    memset(&p, 0, sizeof(p));
    query(&p, c, source.file_id, 1, 0x30);
    doc_send(&p, c);
    name_result(c, 0, "private-move-source");
    assert(smb2_close(c, source.file_id) == ST_SUCCESS);
    assert(smb2_close(c, dir.file_id) == ST_SUCCESS);
} /* directory_failed_move_case */

/* Directory discovery remains a legacy rename boundary, but every scan page
 * has a retry-safe compound. Remove the held child after the rejected scan:
 * retaining its first-attempt entry would trigger an observable stale recall. */
static void
directory_scan_cases(
    struct smb2_env  *env,
    struct smb2_conn *c)
{
    struct smb2_conn *d = smb2_conn_open(env);

    smb2_handshake(d);
    for (int mode = 1; mode <= 5; mode++) {
        char                   old[64], moved[64], child_name[96];
        snprintf(old, sizeof(old), "scan-dir-%d", mode);
        snprintf(moved, sizeof(moved), "scan-moved-%d", mode);
        snprintf(child_name, sizeof(child_name), mode == 5 ? "%s\\a" : "%s\\child", old);
        struct smb2_create_out directory, child, other, probe;
        assert(smb2_create_opts(c, old, MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                                MBT_FILE_SHARE_RWD, MBT_FILE_DIRECTORY_FILE, NULL, &directory) == ST_SUCCESS);
        assert(smb2_create(d, child_name, MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                           MBT_FILE_SHARE_RWD, NULL, &child) == ST_SUCCESS);
        if (mode == 5) {
            snprintf(child_name, sizeof(child_name), "%s\\b", old);
            assert(smb2_create(d, child_name, MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS,
                               MBT_FILE_SHARE_RWD, NULL, &other) == ST_SUCCESS);
        }
        scan_first_child = 0;
        atomic_store(&scan_finishes, 0);
        atomic_store(&scan_recalls, 0);
        atomic_store(&scan_mutations, 0);
        atomic_store(&scan_held, 0);
        atomic_store(&scan_release, 0);
        atomic_store(&scan_mode, mode);
        uint32_t expected = mode == 1 ? ST_SUCCESS : mode == 2 ? 0xC00000E5u :
            mode == 3 ? 0xC0000120u : ST_ACCESS_DENIED;
        if (mode == 1 || mode == 5) {
            struct packet p = { 0 };
            rename_op(&p, c, directory.file_id, moved);
            int           replies = c->nreply_app;
            memcpy(c->sbuf + 4, p.data, p.length);
            smb2c_send(c, p.length - SMB2_HDR_SIZE);
            c->msg_id = p.first_mid + p.count;
            uint64_t      deadline = smb2c_now_ms() + SMB2C_HANG_MS;
            while (!atomic_load(&scan_held)) {
                smb2_pump(env);
                assert(smb2c_now_ms() < deadline && !c->disconnected);
            }
            assert(c->nreply_app == replies && !atomic_load(&scan_recalls) && !atomic_load(&scan_mutations));
            assert(mode != 5 || scan_first_child == 'a' || scan_first_child == 'b');
            assert(smb2_close(d, mode == 5 && scan_first_child == 'a' ? other.file_id : child.file_id) == ST_SUCCESS);
            atomic_store(&scan_release, 1);
            assert(smb2c_pump_for_nreply(c, replies, "directory scan retry"));
            assert(g32(c->rbuf + 4, 8) == expected);
            assert(atomic_load(&scan_finishes) == (mode == 5 ? 4 : 3));
            assert(atomic_load(&scan_recalls) == (mode == 5 ? 1 : 0));
            assert(atomic_load(&scan_mutations) == (mode == 5 ? 0 : 1));
            if (mode == 5) {
                assert(smb2_close(d, scan_first_child == 'a' ? child.file_id : other.file_id) == ST_SUCCESS);
            }
        } else {
            assert(smb2_rename(c, directory.file_id, moved, 0) == expected);
            assert(atomic_load(&scan_finishes) == 1 && !atomic_load(&scan_mutations));
            assert(atomic_load(&scan_recalls) == (mode == 4 ? 1 : 0));
            assert(smb2_close(d, child.file_id) == ST_SUCCESS);
        }
        atomic_store(&scan_mode, 0);
        assert(smb2_create_opts(d, mode == 1 ? old : moved, MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                                MBT_FILE_SHARE_RWD, MBT_FILE_DIRECTORY_FILE, NULL, &probe) == ST_OBJECT_NAME_NOT_FOUND);
        assert(smb2_create_opts(d, mode == 1 ? moved : old, MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                                MBT_FILE_SHARE_RWD, MBT_FILE_DIRECTORY_FILE, NULL, &probe) == ST_SUCCESS);
        assert(smb2_close(d, probe.file_id) == ST_SUCCESS);
        assert(smb2_close(c, directory.file_id) == ST_SUCCESS);
    }
} /* directory_scan_cases */

int
main(void)
{
    struct smb2_env        env;
    struct smb2_create_out a, b, alias, probe;
    struct packet          p = { 0 };

    /* The delayed legacy constructor uses a mandatory level-II oplock to
     * select its lifecycle path; enable that feature for a valid request. */
    struct smb2_env_opts   opts = { .named_streams = 1, .oplocks = 1 };

    smb2_env_start_opts(&env, &opts);
    struct smb2_conn      *c = smb2_conn_open(&env);
    smb2_handshake(c);
    /* The first operation has no published tree-root cache yet. RENAME must
     * bind both source parent and share root from the producer's private path. */
    uint8_t                cold_inherited[16];
    memset(cold_inherited, 0xff, sizeof(cold_inherited));
    create_op(&p, c, "cold-producer-old", MBT_FILE_CREATE, MBT_FILE_ALL_ACCESS);
    rename_op(&p, c, cold_inherited, "cold-producer-new"); related(&p);
    query(&p, c, cold_inherited, 1, 0x30); related(&p);
    close_op(&p, c, cold_inherited); related(&p);
    doc_send(&p, c);
    name_result(c, 2, "cold-producer-new");
    directory_native_cases(&env, c);
    directory_mutator_races(&env, c);
    directory_failed_move_case(c);
    memset(&p, 0, sizeof(p));
    assert(smb2_create(c, "rename-old", MBT_FILE_OPEN_IF, MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD, NULL, &a) ==
           ST_SUCCESS);
    assert(smb2_create(c, "rename-old", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD, NULL, &b) == ST_SUCCESS)
    ;
    hardlink(c, a.file_id, "rename-alias");
    assert(smb2_create(c, "rename-alias", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD, NULL, &alias) ==
           ST_SUCCESS);
    query(&p, c, a.file_id, 1, 0x30);
    rename_op(&p, c, a.file_id, "rename-next");
    query(&p, c, b.file_id, 1, 0x30);
    query(&p, c, alias.file_id, 1, 0x30);
    rename_op(&p, c, b.file_id, "rename-final");
    query(&p, c, a.file_id, 1, 0x30);
    query(&p, c, alias.file_id, 1, 0x30);
    doc_send(&p, c);
    name_result(c, 0, "rename-old");
    name_result(c, 2, "rename-next");
    name_result(c, 3, "rename-alias");
    name_result(c, 5, "rename-final");
    name_result(c, 6, "rename-alias");
    assert(smb2_create(c, "rename-old", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD, NULL, &probe) ==
           ST_OBJECT_NAME_NOT_FOUND);
    assert(smb2_create(c, "rename-next", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD, NULL, &probe) ==
           ST_OBJECT_NAME_NOT_FOUND);

    /* Existing destination errors do not abort an independent later command,
     * while exact same-link rename remains a successful no-op. */
    memset(&p, 0, sizeof(p));
    rename_op(&p, c, a.file_id, "rename-alias");
    p.expected[0] = 0xC0000035u;
    query(&p, c, b.file_id, 1, 0x30);
    rename_op(&p, c, b.file_id, "rename-final");
    close_op(&p, c, a.file_id);
    close_op(&p, c, b.file_id);
    close_op(&p, c, alias.file_id);
    doc_send(&p, c);
    name_result(c, 1, "rename-final");
    assert(smb2_create(c, "rename-alias", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD, NULL, &probe) ==
           ST_SUCCESS);
    assert(smb2_close(c, probe.file_id) == ST_SUCCESS);
    assert(smb2_create(c, "rename-final", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD, NULL, &probe) ==
           ST_SUCCESS);
    assert(smb2_close(c, probe.file_id) == ST_SUCCESS);

    /* Nested parents are resolved through typed LOOKUP_PATH and compared to
    * the source parent identity; a basename alone would target the root. */
    struct smb2_create_out directory;
    assert(smb2_create_opts(c, "rename-dir", MBT_FILE_OPEN_IF, MBT_FILE_ALL_ACCESS,
                            MBT_FILE_SHARE_RWD, MBT_FILE_DIRECTORY_FILE, NULL, &directory) == ST_SUCCESS);
    assert(smb2_close(c, directory.file_id) == ST_SUCCESS);
    assert(smb2_create(c, "rename-dir\\old", MBT_FILE_OPEN_IF, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &a) == ST_SUCCESS);
    assert(smb2_create(c, "rename-dir\\old", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &b) == ST_SUCCESS);
    memset(&p, 0, sizeof(p));
    query(&p, c, a.file_id, 1, 0x30);
    rename_op(&p, c, a.file_id, "rename-dir\\middle");
    query(&p, c, b.file_id, 1, 0x30);
    rename_op(&p, c, b.file_id, "rename-dir\\last");
    query(&p, c, a.file_id, 1, 0x30);
    close_op(&p, c, a.file_id);
    close_op(&p, c, b.file_id);
    doc_send(&p, c);
    name_result(c, 0, "rename-dir\\old");
    name_result(c, 2, "rename-dir\\middle");
    name_result(c, 4, "rename-dir\\last");
    assert(smb2_create(c, "rename-dir\\last", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &probe) == ST_SUCCESS);
    assert(smb2_close(c, probe.file_id) == ST_SUCCESS);

    /* Cross-parent moves preserve both peer path snapshots and hardlink
     * aliases. A missing target parent fails only its own command group. */
    assert(smb2_create_opts(c, "rename-other", MBT_FILE_OPEN_IF, MBT_FILE_ALL_ACCESS,
                            MBT_FILE_SHARE_RWD, MBT_FILE_DIRECTORY_FILE, NULL, &directory) == ST_SUCCESS);
    assert(smb2_close(c, directory.file_id) == ST_SUCCESS);
    assert(smb2_create(c, "rename-dir\\last", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &a) == ST_SUCCESS);
    assert(smb2_create(c, "rename-dir\\last", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &b) == ST_SUCCESS);
    hardlink(c, a.file_id, "rename-move-alias");
    assert(smb2_create(c, "rename-move-alias", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &alias) == ST_SUCCESS);
    memset(&p, 0, sizeof(p));
    rename_op(&p, c, a.file_id, "missing-parent\\nope");
    p.expected[0] = 0xC000003Au;
    query(&p, c, b.file_id, 1, 0x30);
    rename_op(&p, c, a.file_id, "rename-other\\moved");
    query(&p, c, b.file_id, 1, 0x30);
    query(&p, c, alias.file_id, 1, 0x30);
    rename_op(&p, c, b.file_id, "rename-returned");
    query(&p, c, a.file_id, 1, 0x30);
    close_op(&p, c, a.file_id);
    close_op(&p, c, b.file_id);
    close_op(&p, c, alias.file_id);
    doc_send(&p, c);
    name_result(c, 1, "rename-dir\\last");
    name_result(c, 3, "rename-other\\moved");
    name_result(c, 4, "rename-move-alias");
    name_result(c, 6, "rename-returned");
    assert(smb2_create(c, "rename-returned", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS,
                       MBT_FILE_SHARE_RWD, NULL, &probe) == ST_SUCCESS);
    assert(smb2_close(c, probe.file_id) == ST_SUCCESS);

    provisional_rename_cases(c);
    replace_if_exists_cases(c);
    same_inode_doc_cases(c);
    cancel_after_initial_move_case(c);
    replace_destination_races(&env, c);
    replacement_constructor_races(&env, c);
    replacement_delayed_open_cases(&env, c);
    directory_scan_cases(&env, c);
    stream_holder_rename_cases(c);
    pending_stream_rename_case(&env, c);
    closed_stream_doc_rename_cases(c);

    /* A CREATE-DOC opener follows the renamed link through its peer close.
     * Rebinding the old name must not delete the replacement inode. */
    assert(smb2_create_opts(c, "rename-doc-old", MBT_FILE_OPEN_IF, MBT_FILE_ALL_ACCESS,
                            MBT_FILE_SHARE_RWD, MBT_FILE_DELETE_ON_CLOSE, NULL, &a) == ST_SUCCESS);
    assert(smb2_create(c, "rename-doc-old", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD, NULL, &b) ==
           ST_SUCCESS);
    memset(&p, 0, sizeof(p));
    rename_op(&p, c, b.file_id, "rename-dir\\rename-doc-new");
    query(&p, c, a.file_id, 1, 0x30);
    doc_send(&p, c);
    name_result(c, 1, "rename-dir\\rename-doc-new");
    assert(smb2_create(c, "rename-doc-old", MBT_FILE_OPEN_IF, MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD, NULL, &probe) ==
           ST_SUCCESS);
    assert(smb2_close(c, probe.file_id) == ST_SUCCESS);
    memset(&p, 0, sizeof(p));
    close_op(&p, c, a.file_id);
    close_op(&p, c, b.file_id);
    doc_send(&p, c);
    assert(smb2_create(c, "rename-dir\\rename-doc-new", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD, NULL, &
                       probe) ==
           ST_OBJECT_NAME_NOT_FOUND);
    assert(smb2_create(c, "rename-doc-old", MBT_FILE_OPEN, MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD, NULL, &probe) ==
           ST_SUCCESS);
    assert(smb2_close(c, probe.file_id) == ST_SUCCESS);
    /* A foreign unpublished producer must follow accepted external renames
     * without exposing its private open to another request. On finish retry,
     * reopening the original spelling on a different inode drops that delta. */
    struct smb2_conn *d = smb2_conn_open(&env);
    smb2_handshake(d);
    for (int retry = 0; retry < 2; retry++) {
        const char *old    = retry ? "producer-retry-a" : "producer-accept-a";
        const char *middle = retry ? "rename-dir\\producer-retry-b" : "rename-dir\\producer-accept-b";
        const char *last   = retry ? "rename-dir\\producer-retry-c" : "rename-dir\\producer-accept-c";
        assert(smb2_create(c, old, MBT_FILE_OPEN_IF, MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD, NULL, &a) == ST_SUCCESS);
        memset(&p, 0, sizeof(p));
        stat_open(&p, d, old);
        producer_gate.calls  = 0;
        producer_gate.reject = retry;
        atomic_store(&producer_held, 0);
        atomic_store(&release_producer, 0);
        atomic_store(&submissions, 0);
        atomic_store(&expected_groups, 1);
        atomic_store(&armed, 1);
        atomic_store(&hold_producer, 1);
        int      replies = d->nreply_app;
        memcpy(d->sbuf + 4, p.data, p.length);
        smb2c_send(d, p.length - SMB2_HDR_SIZE);
        d->msg_id = p.first_mid + p.count;
        uint64_t deadline = smb2c_now_ms() + SMB2C_HANG_MS;
        while (!atomic_load(&producer_held)) {
            smb2_pump(&env);
            assert(smb2c_now_ms() < deadline && !d->disconnected);
        }
        atomic_store(&armed, 0);
        atomic_store(&hold_producer, 0);
        assert(atomic_load(&submissions) == 1 && d->nreply_app == replies);
        assert(smb2_rename(c, a.file_id, middle, 0) == ST_SUCCESS);
        assert(smb2_rename(c, a.file_id, last, 0) == ST_SUCCESS);
        assert(smb2_create(c, old, MBT_FILE_OPEN_IF, MBT_FILE_ALL_ACCESS, MBT_FILE_SHARE_RWD, NULL, &probe) ==
               ST_SUCCESS);
        atomic_store(&release_producer, 1);
        assert(smb2c_pump_for_nreply(d, replies, "pending name token acceptance"));
        assert(g32(d->rbuf + 4, 8) == ST_SUCCESS);
        assert(producer_gate.calls == (retry ? 2u : 1u));
        uint8_t  pending_fid[16];
        memcpy(pending_fid, d->rbuf + 4 + SMB2_HDR_SIZE + 64, 16);
        memset(&p, 0, sizeof(p));
        query(&p, d, pending_fid, 1, 0x30);
        doc_send(&p, d);
        name_result(d, 0, retry ? old : "rename-dir\\producer-accept-c");
        uint8_t  object[16], expected_object[16];
        uint32_t object_len, expected_len;
        assert(smb2_query_info(d, 1, 0x06, pending_fid, 0, object, sizeof(object), &object_len) == ST_SUCCESS);
        assert(smb2_query_info(c, 1, 0x06, retry ? probe.file_id : a.file_id,
                               0, expected_object, sizeof(expected_object), &expected_len) == ST_SUCCESS);
        assert(object_len == 8 && expected_len == 8 && !memcmp(object, expected_object, 8));
        assert(smb2_close(d, pending_fid) == ST_SUCCESS);
        assert(smb2_close(c, a.file_id) == ST_SUCCESS);
        assert(smb2_close(c, probe.file_id) == ST_SUCCESS);
    }
    smb2_env_fs_teardown(&env, "fs0");
    smb2_env_stop(&env);
    return 0;
} /* main */
