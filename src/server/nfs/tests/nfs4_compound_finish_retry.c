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
#include <errno.h>
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

    /* stderr's fprintf can issue the body and newline separately. The
     * server's asynchronous stdout logger shares this descriptor's file
     * offset, so that split can splice a log line into our final field.
     * Emit the complete bounded record atomically, including its newline. */
    ssize_t written;
    do {
        written = write(STDERR_FILENO, record, (size_t) length);
    } while (written < 0 && errno == EINTR);
    assert(written == length);
} /* fixture_log */

static int
eligible(struct chimera_vfs_compound *compound)
{
    const char *prefix = getenv("CHIMERA_COMPOUND_RETRY_PREFIX");
    int         marker = 0;

    if (!prefix) {
        prefix = "retry-";
    }
    for (uint32_t i = 0; i < chimera_vfs_compound_num_ops(compound); i++) {
        const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound, i);
        if (op->set_attr.va_set_mask ||
            (op->open_flags & (CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_TRUNCATE))) {
            return 0;
        }
        switch (op->type) {
            case CHIMERA_VFS_COMPOUND_OP_OPEN:
                if (op->name_len >= strlen(prefix) &&
                    memcmp(op->name, prefix, strlen(prefix)) == 0) {
                    marker |= 1;
                }
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
                if (getenv("CHIMERA_COMPOUND_JUNCTION_GATE")) {
                    marker |= 2;
                }
                break;
            case CHIMERA_VFS_COMPOUND_OP_PUTFH:
            case CHIMERA_VFS_COMPOUND_OP_LOOKUPP:
            case CHIMERA_VFS_COMPOUND_OP_LOOKUP_PATH:
            case CHIMERA_VFS_COMPOUND_OP_GETATTR:
            case CHIMERA_VFS_COMPOUND_OP_ACCESS:
            case CHIMERA_VFS_COMPOUND_OP_GETFH:
            case CHIMERA_VFS_COMPOUND_OP_READLINK:
            case CHIMERA_VFS_COMPOUND_OP_SAVEFH:
            case CHIMERA_VFS_COMPOUND_OP_RESTOREFH:
            case CHIMERA_VFS_COMPOUND_OP_READDIR:
            case CHIMERA_VFS_COMPOUND_OP_READ:
            case CHIMERA_VFS_COMPOUND_OP_GETXATTR:
            case CHIMERA_VFS_COMPOUND_OP_LISTXATTRS:
            case CHIMERA_VFS_COMPOUND_OP_SEEK:
            case CHIMERA_VFS_COMPOUND_OP_READ_PLUS:
            case CHIMERA_VFS_COMPOUND_OP_PUTHANDLE:
            case CHIMERA_VFS_COMPOUND_OP_PUTROOT:
            case CHIMERA_VFS_COMPOUND_OP_OPEN_CURRENT:
            case CHIMERA_VFS_COMPOUND_OP_GETHANDLE:
            case CHIMERA_VFS_COMPOUND_OP_CLOSE:
            case CHIMERA_VFS_COMPOUND_OP_SAVEHANDLE:
            case CHIMERA_VFS_COMPOUND_OP_RESTOREHANDLE:
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
    fixture_log(
        "NFS4_FINISH_RETRY injected compound=%p execution=%d open=%d id=%llu name=%s named_opens=%u reads=%u\n",
        (void *) compound, chimera_vfs_compound_execution_status(compound),
        !!(eligible(compound) & 1), fixture->id,
        fixture->first_name[0] ? fixture->first_name : "-", fixture->named_opens, fixture->reads);
    chimera_vfs_compound_finish_result(compound, CHIMERA_VFS_EAGAIN);
} /* reject_attempt */

static void
junction_gate_poll(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    struct retry_fixture *fixture = (struct retry_fixture *)
        ((char *) timer - offsetof(struct retry_fixture, gate_timer));

    if (access(fixture->gate_release, F_OK)) {
        assert(++fixture->gate_polls < 1000);
        evpl_add_oneshot_timer(evpl, timer, junction_gate_poll, 10000);
        return;
    }
    /* A one-shot timer is removed before entry; retry may free its fixture. */
    reject_attempt(fixture->gate_compound, fixture);
} /* junction_gate_poll */

/* Let REST run even when its connection uses this same event-loop thread.
* The finish stays pending while a timer waits for the client's update. */
static int
junction_update_gate(
    struct chimera_vfs_compound *compound,
    struct retry_fixture        *fixture)
{
    const char *gate   = getenv("CHIMERA_COMPOUND_JUNCTION_GATE");
    const char *prefix = "retry-junction-";
    char        ready[1024];

    if (!gate) {
        return 0;
    }
    for (uint32_t i = 0; i < chimera_vfs_compound_num_ops(compound); i++) {
        const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound, i);
        if (op->type != CHIMERA_VFS_COMPOUND_OP_LOOKUP ||
            op->name_len < strlen(prefix) || memcmp(op->name, prefix, strlen(prefix))) {
            continue;
        }
        assert(snprintf(ready, sizeof(ready), "%s.ready", gate) < (int) sizeof(ready));
        assert(snprintf(fixture->gate_release, sizeof(fixture->gate_release), "%s.release", gate) <
               (int) sizeof(fixture->gate_release));
        FILE                                 *marker = fopen(ready, "w");
        assert(marker);
        assert(fclose(marker) == 0);
        assert(fixture->evpl);
        fixture->gate_compound = compound;
        evpl_add_oneshot_timer(fixture->evpl, &fixture->gate_timer, junction_gate_poll, 10000);
        return 1;
    }
    return 0;
} /* junction_update_gate */

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
        if (!junction_update_gate(compound, fixture)) {
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
    if (!eligible(compound)) {
        next(compound, callback, private_data);
        return;
    }
    fixture = calloc(1, sizeof(*fixture));
    assert(fixture);
    fixture->id = atomic_fetch_add_explicit(&next_fixture_id, 1, memory_order_relaxed) + 1;
    for (uint32_t i = 0; i < chimera_vfs_compound_num_ops(compound); i++) {
        const struct chimera_vfs_compound_op *op = chimera_vfs_compound_op(compound, i);
        if (op->type == CHIMERA_VFS_COMPOUND_OP_READ) {
            fixture->reads++;
        }
        if (op->type == CHIMERA_VFS_COMPOUND_OP_OPEN && op->name_len) {
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
