// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: Unlicense

/* Test-only LD_PRELOAD fault injector. No production environment switch or
* backend rollback is implied: every filesystem operation is read-only, and
* protocol/claim reservations are attempt-private and reversible. Check every
* operation before rejection, including any dynamically appended suffix. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif /* ifndef _GNU_SOURCE */
#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>
#include <stdarg.h>
#include <stddef.h>
#include <unistd.h>
#undef NDEBUG
#include <assert.h>

#include "vfs/vfs_compound.h"

struct retry_fixture {
    chimera_vfs_compound_callback_t callback;
    void                           *private_data;
    unsigned                        attempts;
    unsigned long long              id;
    unsigned                        named_opens;
    unsigned                        reads;
    char                            first_name[256];
    struct evpl                    *evpl;
    struct evpl_timer               gate_timer;
    struct chimera_vfs_compound    *gate_compound;
    unsigned                        gate_polls;
    char                            gate_release[1024];
};

static _Atomic unsigned long long next_fixture_id;

/* A compound is allocated/submitted on its owning VFS event-loop thread.
 * Retain that thread's evpl for asynchronous test-only finish rendezvous. */
static                            _Thread_local struct evpl *owner_evpl;

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
fixture_log(
    const char *format,
    ...)
{
    char    record[1024];
    va_list args;

    va_start(args, format);
    int     length = vsnprintf(record, sizeof(record), format, args);
    va_end(args);
    assert(length > 0 && (size_t) length < sizeof(record));

    /* Share the server logger's stdout stream, buffer and stdio lock.
     * A separate stderr write can bisect a multi-write stdout flush even
     * when the fixture emits its entire record in one write. */
    assert(fwrite(record, 1, (size_t) length, stdout) == (size_t) length);
    assert(fflush(stdout) == 0);
} /* fixture_log */

static int
eligible(struct chimera_vfs_compound *compound)
{
    const char *prefix   = getenv("CHIMERA_COMPOUND_RETRY_PREFIX");
    const char *feature  = getenv("CHIMERA_COMPOUND_FEATURE");
    int         metadata = feature && (!strcmp(feature, "metadata") || !strncmp(feature, "readdir", 7));
    int         marker   = 0;

    if (!prefix) {
        prefix = "retry-";
    }
    for (uint32_t i = 0; i < chimera_vfs_compound_num_ops(compound); i++) {
        const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound, i);
        /* A deferred namespace suffix may contain mutations that never ran.
         * Only executed operations determine whether rejection is safe. */
        if (op->status == CHIMERA_VFS_UNSET || op->skipped) {
            marker |= 2;
            continue;
        }
        if (op->set_attr.va_set_mask ||
            (op->open_flags & (CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_TRUNCATE))) {
            return 0;
        }
        switch (op->type) {
            case CHIMERA_VFS_COMPOUND_OP_OPEN_STREAM:
                if (!metadata) {
                    return 0;
                }
                marker |= 4;
            /* fall through */
            case CHIMERA_VFS_COMPOUND_OP_OPEN:
                if (op->name_len >= strlen(prefix) &&
                    memcmp(op->name, prefix, strlen(prefix)) == 0) {
                    marker |= 1;
                }
                break;
            case CHIMERA_VFS_COMPOUND_OP_SETATTR:
                /* LAYOUTCOMMIT starts with empty attributes and computes them
                * during prepare. Recheck at finish: only an unexecuted or
                * skipped metadata update is safe without backend rollback. */
                if (!op->setattr_after_write || (op->completed && !op->skipped)) {
                    return 0;
                }
                marker |= 2;
                break;
            case CHIMERA_VFS_COMPOUND_OP_COPY_RANGE:
                /* Only a zero-byte COPY is read-only. The frontend resolves
                 * count=0 to the source's remaining size during execution;
                 * eligibility is checked again at finish after that prepare.
                 * Nonempty copies never receive synthetic finish rejection. */
                if (op->length) {
                    return 0;
                }
                marker |= 2;
                break;
            case CHIMERA_VFS_COMPOUND_OP_CHECKPOINT:
            /* Coordination only queries/recalls peers, never mutates the
             * filesystem. Its result is retained across rejected attempts. */
            case CHIMERA_VFS_COMPOUND_OP_COORDINATE:
                marker |= 2;
                break;
            case CHIMERA_VFS_COMPOUND_OP_RESERVE:
                if (!op->claim || op->claim->backend_token ||
                    (op->claim->klass != CHIMERA_CLAIM_CLASS_ACCESS &&
                     op->claim->klass != CHIMERA_CLAIM_CLASS_RANGE)) {
                    return 0;
                }
                break;
            case CHIMERA_VFS_COMPOUND_OP_LOOKUP:
            case CHIMERA_VFS_COMPOUND_OP_LOOKUP_PATH:
                if (getenv("CHIMERA_COMPOUND_JUNCTION_GATE")) {
                    marker |= 2;
                }
                break;
            case CHIMERA_VFS_COMPOUND_OP_PUTROOT:
                if (feature && !strcmp(feature, "cold_root")) {
                    marker |= 2;
                }
                break;
            case CHIMERA_VFS_COMPOUND_OP_PUTFH:
            case CHIMERA_VFS_COMPOUND_OP_LOOKUPP:
            case CHIMERA_VFS_COMPOUND_OP_GETFH:
            case CHIMERA_VFS_COMPOUND_OP_SAVEFH:
            case CHIMERA_VFS_COMPOUND_OP_RESTOREFH:
            case CHIMERA_VFS_COMPOUND_OP_READ:
            case CHIMERA_VFS_COMPOUND_OP_SEEK:
            case CHIMERA_VFS_COMPOUND_OP_READ_PLUS:
            case CHIMERA_VFS_COMPOUND_OP_PUTHANDLE:
            case CHIMERA_VFS_COMPOUND_OP_OPEN_CURRENT:
            case CHIMERA_VFS_COMPOUND_OP_GETHANDLE:
            case CHIMERA_VFS_COMPOUND_OP_CLOSE:
            case CHIMERA_VFS_COMPOUND_OP_SAVEHANDLE:
            case CHIMERA_VFS_COMPOUND_OP_RESTOREHANDLE:
                break;
            /* COMMIT may repeat a flush, but changes neither namespace nor
             * file contents; mutation operations remain excluded below. */
            case CHIMERA_VFS_COMPOUND_OP_COMMIT:
            case CHIMERA_VFS_COMPOUND_OP_READLINK:
            case CHIMERA_VFS_COMPOUND_OP_GETXATTR:
            case CHIMERA_VFS_COMPOUND_OP_LISTXATTRS:
            case CHIMERA_VFS_COMPOUND_OP_GETATTR:
            case CHIMERA_VFS_COMPOUND_OP_GET_LAYOUT:
            case CHIMERA_VFS_COMPOUND_OP_ACCESS:
            case CHIMERA_VFS_COMPOUND_OP_READDIR:
                if (metadata) {
                    marker |= 4;
                }
                break;
            case CHIMERA_VFS_COMPOUND_OP_LIST_STREAMS:
                if (!metadata) {
                    return 0;
                }
                marker |= 4;
                break;
            default:
                return 0;
        } /* switch */
    }
    return marker;
} /* eligible */

static void
reject_attempt(
    struct chimera_vfs_compound *compound,
    struct retry_fixture        *fixture)
{
    unsigned stream_lists = 0, stream_pages = 0, readlinks = 0, xattrs = 0, commits = 0, layout_noops = 0, paths = 0;
    unsigned readdirs = 0;

    for (uint32_t i = 0; i < chimera_vfs_compound_num_ops(compound); i++) {
        const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound, i);
        paths        += op->type == CHIMERA_VFS_COMPOUND_OP_LOOKUP_PATH && op->completed;
        layout_noops += op->type == CHIMERA_VFS_COMPOUND_OP_SETATTR && op->setattr_after_write &&
            op->completed && op->skipped;
        readlinks += op->type == CHIMERA_VFS_COMPOUND_OP_READLINK && op->completed;
        readdirs  += op->type == CHIMERA_VFS_COMPOUND_OP_READDIR && op->completed;
        commits   += op->type == CHIMERA_VFS_COMPOUND_OP_COMMIT && op->completed && op->status == CHIMERA_VFS_OK;
        xattrs    += (op->type == CHIMERA_VFS_COMPOUND_OP_GETXATTR ||
                      op->type == CHIMERA_VFS_COMPOUND_OP_LISTXATTRS) && op->completed;
        if (op->type == CHIMERA_VFS_COMPOUND_OP_LIST_STREAMS && op->completed) {
            stream_lists++;
            stream_pages += op->status == CHIMERA_VFS_OK && !op->eof;
        }
    }
    fixture_log(
        "NFS4_FINISH_RETRY injected compound=%p execution=%d open=%d id=%llu name=%s named_opens=%u reads=%u metadata=%d stream_lists=%u stream_pages=%u readlinks=%u xattrs=%u commits=%u layout_noops=%u paths=%u readdirs=%u\n",
        (void *) compound, chimera_vfs_compound_execution_status(compound),
        !!(eligible(compound) & 1), fixture->id,
        fixture->first_name[0] ? fixture->first_name : "-", fixture->named_opens, fixture->reads,
        !!(eligible(compound) & 4), stream_lists, stream_pages, readlinks, xattrs, commits, layout_noops, paths,
        readdirs);
    chimera_vfs_compound_finish_result(compound, CHIMERA_VFS_EAGAIN);
} /* reject_attempt */

static void
finish_gate_poll(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    struct retry_fixture *fixture = (struct retry_fixture *)
        ((char *) timer - offsetof(struct retry_fixture, gate_timer));

    if (access(fixture->gate_release, F_OK)) {
        assert(++fixture->gate_polls < 1000);
        evpl_add_oneshot_timer(evpl, timer, finish_gate_poll, 10000);
        return;
    }
    FILE                 *release = fopen(fixture->gate_release, "r");
    assert(release);
    int                   action       = fgetc(release);
    int                   close_status = fclose(release);
    assert(close_status == 0);
    (void) close_status;
    /* The Python peer creates the file before writing its action. Seeing
     * that brief empty state must not turn a terminal error into a retry. */
    if (action == EOF) {
        assert(++fixture->gate_polls < 1000);
        evpl_add_oneshot_timer(evpl, timer, finish_gate_poll, 10000);
        return;
    }
    /* A one-shot timer is removed before entry; completion may free its fixture. */
    if (action == 'a') {
        fixture_log("NFS4_FINISH_GATE accepted compound=%p id=%llu\n",
                    (void *) fixture->gate_compound, fixture->id);
        chimera_vfs_compound_finish_result(fixture->gate_compound, CHIMERA_VFS_OK);
    } else if (action == 'e') {
        fixture_log("NFS4_FINISH_GATE failed compound=%p id=%llu\n",
                    (void *) fixture->gate_compound, fixture->id);
        chimera_vfs_compound_finish_result(fixture->gate_compound, CHIMERA_VFS_EIO);
    } else {
        reject_attempt(fixture->gate_compound, fixture);
    }
} /* finish_gate_poll */

/* Let REST or LAYOUTRETURN run on this event loop while finish is pending.
 * The client changes protocol state before releasing the rejected attempt. */
static int
revalidation_gate(
    struct chimera_vfs_compound *compound,
    struct retry_fixture        *fixture)
{
    const char *gate   = getenv("CHIMERA_COMPOUND_JUNCTION_GATE");
    const char *prefix = "retry-junction-";
    char        ready[1024];
    bool        layout     = false;
    bool        retirement = false;

    if (!gate) {
        gate   = getenv("CHIMERA_COMPOUND_LAYOUT_GATE");
        prefix = "retry-layoutcommit-return";
        layout = true;
    }
    /* LAYOUTGET must keep its public grant/version unchanged while finish is
     * pending. The Python peer inspects it, then accepts/rejects/fails finish.
     * A mapped object avoids pretending DS materialization can be rolled back. */
    const char *layoutget_gate = getenv("CHIMERA_COMPOUND_LAYOUTGET_GATE");
    if (layoutget_gate) {
        const char *layoutget_prefix = "retry-layoutget-pending";
        for (uint32_t i = 0; i < chimera_vfs_compound_num_ops(compound); i++) {
            const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound, i);
            if (op->type == CHIMERA_VFS_COMPOUND_OP_LOOKUP && op->completed &&
                op->name_len >= strlen(layoutget_prefix) &&
                !memcmp(op->name, layoutget_prefix, strlen(layoutget_prefix))) {
                gate   = layoutget_gate;
                prefix = layoutget_prefix;
                layout = false;
                break;
            }
        }
    }
    const char *retirement_gate = getenv("CHIMERA_COMPOUND_RETIREMENT_GATE");
    if (retirement_gate) {
        const char *retirement_prefix = "retry-retirement-terminal-";
        for (uint32_t i = 0; i < chimera_vfs_compound_num_ops(compound); i++) {
            const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound, i);
            if (op->type == CHIMERA_VFS_COMPOUND_OP_OPEN && op->completed &&
                op->name_len >= strlen(retirement_prefix) &&
                !memcmp(op->name, retirement_prefix, strlen(retirement_prefix))) {
                gate       = retirement_gate;
                prefix     = retirement_prefix;
                layout     = false;
                retirement = true;
                break;
            }
        }
    }
    if (!gate) {
        return 0;
    }
    if (layout) {
        bool noop = false;
        for (uint32_t i = 0; i < chimera_vfs_compound_num_ops(compound); i++) {
            const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound, i);
            noop |= op->type == CHIMERA_VFS_COMPOUND_OP_SETATTR && op->setattr_after_write &&
                op->completed && op->skipped;
        }
        if (!noop) {
            return 0;
        }
    }
    for (uint32_t i = 0; i < chimera_vfs_compound_num_ops(compound); i++) {
        const struct chimera_vfs_compound_op *op      = chimera_vfs_compound_op(compound, i);
        bool                                  matched = (op->type == CHIMERA_VFS_COMPOUND_OP_LOOKUP ||
                                                         (retirement && op->type == CHIMERA_VFS_COMPOUND_OP_OPEN)) &&
            op->name_len >= strlen(prefix) && !memcmp(op->name, prefix, strlen(prefix));
        if (!layout && op->type == CHIMERA_VFS_COMPOUND_OP_LOOKUP_PATH) {
            const char *prefixes[] = { "rootfs/retry-export-", "share/retry-export-" };
            const char *path       = op->path;
            size_t      length     = op->path_len;
            while (length && *path == '/') {
                path++;
                length--;
            }
            for (uint32_t n = 0; n < sizeof(prefixes) / sizeof(prefixes[0]); n++) {
                matched |= length >= strlen(prefixes[n]) &&
                    !memcmp(path, prefixes[n], strlen(prefixes[n]));
            }
        }
        if (!matched) {
            continue;
        }
        assert(snprintf(ready, sizeof(ready), "%s.ready", gate) < (int) sizeof(ready));
        assert(snprintf(fixture->gate_release, sizeof(fixture->gate_release), "%s.release", gate) <
               (int) sizeof(fixture->gate_release));
        FILE *marker = fopen(ready, "w");
        assert(marker);
        assert(fclose(marker) == 0);
        assert(fixture->evpl);
        fixture->gate_compound = compound;
        evpl_add_oneshot_timer(fixture->evpl, &fixture->gate_timer, finish_gate_poll, 10000);
        return 1;
    }
    return 0;
} /* revalidation_gate */

static void
finish_attempt(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct retry_fixture *fixture = private_data;

    fixture->attempts++;
    if (fixture->attempts == 1 && chimera_vfs_compound_num_completed(compound) > 0 &&
        eligible(compound)) {
        /* Even completed OPEN reservations/handles cannot be published while
         * finish is pending. The frontend must retry before taking them. */
        for (uint32_t i = 0; i < chimera_vfs_compound_num_ops(compound); i++) {
            const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound, i);
            if (op->type == CHIMERA_VFS_COMPOUND_OP_RESERVE) {
                assert(chimera_vfs_compound_take_reservation(compound, i) == NULL);
            }
            assert(chimera_vfs_compound_take_handle(compound, i) == NULL);
        }
        if (!revalidation_gate(compound, fixture)) {
            reject_attempt(compound, fixture);
        }
        return;
    }
    if (fixture->attempts == 2) {
        fixture_log("NFS4_FINISH_RETRY accepted compound=%p execution=%d attempts=2 id=%llu\n",
                    (void *) compound, chimera_vfs_compound_execution_status(compound), fixture->id);
    }
    chimera_vfs_compound_finish_result(compound, CHIMERA_VFS_OK);
} /* finish_attempt */

static void
complete_attempt(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct retry_fixture           *fixture        = private_data;
    chimera_vfs_compound_callback_t callback       = fixture->callback;
    void                           *caller_private = fixture->private_data;

    /* EAGAIN calls back into retry on the same compound; its finish context
     * survives. Accepted completion may synchronously free the compound. */
    if (chimera_vfs_compound_finish_status(compound) != CHIMERA_VFS_EAGAIN) {
        free(fixture);
    }
    callback(compound, caller_private);
} /* complete_attempt */

__attribute__((visibility("default"))) void
chimera_vfs_compound_submit(
    struct chimera_vfs_compound    *compound,
    chimera_vfs_compound_callback_t callback,
    void                           *private_data)
{
    typedef void (*submit_fn)(
        struct chimera_vfs_compound *,
        chimera_vfs_compound_callback_t,
        void *);
    submit_fn             next = (submit_fn) dlsym(RTLD_NEXT, "chimera_vfs_compound_submit");
    struct retry_fixture *fixture;

    assert(next);
    /* Eligibility is decided at finish: groups and namespace callouts can
     * leave an entire mutation suffix unexecuted. */
    fixture = calloc(1, sizeof(*fixture));
    assert(fixture);
    fixture->id = atomic_fetch_add_explicit(&next_fixture_id, 1, memory_order_relaxed) + 1;
    for (uint32_t i = 0; i < chimera_vfs_compound_num_ops(compound); i++) {
        const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound, i);
        if (op->type == CHIMERA_VFS_COMPOUND_OP_READ) {
            fixture->reads++;
        }
        if ((op->type == CHIMERA_VFS_COMPOUND_OP_OPEN || op->type == CHIMERA_VFS_COMPOUND_OP_OPEN_STREAM) && op->
            name_len) {
            if (!fixture->named_opens) {
                size_t len = op->name_len;
                if (len >= sizeof(fixture->first_name)) {
                    len = sizeof(fixture->first_name) - 1;
                }
                memcpy(fixture->first_name, op->name, len);
            }
            fixture->named_opens++;
        }
    }
    fixture->evpl         = owner_evpl;
    fixture->callback     = callback;
    fixture->private_data = private_data;
    chimera_vfs_compound_set_finish_handler(compound, finish_attempt, fixture);
    next(compound, complete_attempt, fixture);
} /* chimera_vfs_compound_submit */
