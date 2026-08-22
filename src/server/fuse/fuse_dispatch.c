// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/uio.h>

#include "fuse_internal.h"
#include "fuse_attr.h"
#include "common/macros.h"
#include "vfs/vfs_release.h"

/*
 * Channel read loop and reply writers.
 *
 * Each /dev/fuse read returns exactly one complete request, and each reply
 * is one atomic writev to the channel the request was read from -- the
 * kernel tracks a request on the per-device queue of the fd that read it,
 * so a reply on any other channel would not find it.  Requests are
 * dispatched on the thread that owns the channel and every VFS completion
 * fires on the issuing thread, so a request never changes threads.
 */

static struct chimera_fuse_request *
chimera_fuse_request_alloc(
    struct chimera_fuse_thread  *thread,
    struct chimera_fuse_channel *channel)
{
    struct chimera_fuse_request *req;
    int                          niov;

    if (thread->free_requests) {
        req                   = thread->free_requests;
        thread->free_requests = req->next;
        thread->num_free_requests--;
    } else {
        req         = calloc(1, sizeof(*req));
        req->thread = thread;
    }

    if (!req->buf_allocated) {
        niov = evpl_iovec_alloc(thread->evpl, CHIMERA_FUSE_BUFSZ, 4096, 1, 0,
                                &req->buf);
        chimera_fuse_abort_if(niov != 1,
                              "fuse request buffer allocation failed (%d)", niov);
        req->buf_allocated = 1;
    }

    req->channel = channel;
    req->handle  = NULL;
    req->file    = NULL;

    thread->active_requests++;

    return req;
} /* chimera_fuse_request_alloc */

void
chimera_fuse_request_free(
    struct chimera_fuse_thread  *thread,
    struct chimera_fuse_request *req)
{
    thread->active_requests--;

    if (thread->num_free_requests >= CHIMERA_FUSE_MAX_POOLED_REQS) {
        if (req->buf_allocated) {
            evpl_iovec_release(thread->evpl, &req->buf);
        }
        free(req);
        return;
    }

    req->next             = thread->free_requests;
    thread->free_requests = req;
    thread->num_free_requests++;
} /* chimera_fuse_request_free */

/*
 * Deliver a reply.  Returns 0 once the kernel has taken it, -1 when it never
 * will: the request was aborted (ENOENT), the channel is dead (ENODEV), or
 * the write failed outright.  Callers whose reply hands the kernel a
 * reference (an entry's lookup count, an open's fh) undo it on -1.
 */
static int
chimera_fuse_send(
    struct chimera_fuse_request *req,
    int                          error,
    const void                  *payload,
    size_t                       payload_len,
    struct evpl_iovec           *data_iov,
    int                          data_niov,
    size_t                       data_len)
{
    struct fuse_out_header hdr;
    struct iovec           iov[2 + CHIMERA_FUSE_IOV_MAX];
    int                    niov = 0, i;
    size_t                 total;
    ssize_t                rc;

    if (req->channel->dead) {
        return -1;
    }

    hdr.error  = -error;
    hdr.unique = req->unique;

    iov[niov].iov_base = &hdr;
    iov[niov].iov_len  = sizeof(hdr);
    niov++;

    total = sizeof(hdr);

    if (error == 0 && payload_len) {
        iov[niov].iov_base = (void *) payload;
        iov[niov].iov_len  = payload_len;
        niov++;
        total += payload_len;
    }

    if (error == 0 && data_len) {
        size_t remain = data_len;

        for (i = 0; i < data_niov && remain; i++) {
            size_t chunk = evpl_iovec_length(&data_iov[i]);

            if (chunk > remain) {
                chunk = remain;
            }

            iov[niov].iov_base = evpl_iovec_data(&data_iov[i]);
            iov[niov].iov_len  = chunk;
            niov++;
            remain -= chunk;
        }

        total += data_len;
    }

    hdr.len = total;

    do {
        rc = writev(req->channel->fd, iov, niov);
    } while (rc < 0 && errno == EINTR);

    if (rc < 0) {
        switch (errno) {
            case ENOENT:
                /* The request was interrupted/aborted before we replied. */
                break;
            case ENODEV:
                chimera_fuse_channel_dead(req->channel);
                break;
            default:
                chimera_fuse_error("fuse reply write failed (opcode %u unique %llu): %s",
                                   req->opcode,
                                   (unsigned long long) req->unique,
                                   strerror(errno));
                break;
        } /* switch */
        return -1;
    }

    return 0;
} /* chimera_fuse_send */

void
chimera_fuse_request_finish(struct chimera_fuse_request *req)
{
    struct chimera_fuse_thread *thread = req->thread;

    if (req->handle) {
        chimera_vfs_release(thread->vfs_thread, req->handle);
        req->handle = NULL;
    }

    chimera_fuse_request_free(thread, req);
} /* chimera_fuse_request_finish */

int
chimera_fuse_reply(
    struct chimera_fuse_request *req,
    int                          error,
    const void                  *payload,
    size_t                       payload_len)
{
    int rc;

    rc = chimera_fuse_send(req, error, payload, payload_len, NULL, 0, 0);
    chimera_fuse_request_finish(req);

    return rc;
} /* chimera_fuse_reply */

int
chimera_fuse_send_only(
    struct chimera_fuse_request *req,
    int                          error,
    const void                  *payload,
    size_t                       payload_len)
{
    return chimera_fuse_send(req, error, payload, payload_len, NULL, 0, 0);
} /* chimera_fuse_send_only */

void
chimera_fuse_reply_read(
    struct chimera_fuse_request *req,
    int                          error,
    struct evpl_iovec           *iov,
    int                          niov,
    size_t                       data_len)
{
    struct evpl *evpl = req->thread->evpl;

    chimera_fuse_send(req, error, NULL, 0, iov, niov,
                      error == 0 ? data_len : 0);

    /* The iovec array lives inside the request, so the backend's buffers are
     * dropped before the request is recycled. */
    evpl_iovecs_release(evpl, iov, niov);

    chimera_fuse_request_finish(req);
} /* chimera_fuse_reply_read */

/*
 * Entry-shaped reply (LOOKUP, CREATE, MKDIR, MKNOD, SYMLINK, LINK): registers
 * the child in the nodeid table and undoes the lookup-count bump if the
 * kernel never saw the entry.  `extra` follows the fuse_entry_out in the
 * reply (CREATE's fuse_open_out).  Requires attr to carry a file handle.
 */
int
chimera_fuse_reply_entry(
    struct chimera_fuse_request    *req,
    const struct chimera_vfs_attrs *attr,
    const void                     *extra,
    size_t                          extra_len)
{
    struct chimera_fuse_mount *mount = req->channel->mount;
    struct fuse_entry_out      entry;
    uint8_t                    payload[sizeof(entry) + 64];
    int                        rc;

    if (!(attr->va_set_mask & CHIMERA_VFS_ATTR_FH)) {
        chimera_fuse_reply(req, EIO, NULL, 0);
        return -1;
    }

    memset(&entry, 0, sizeof(entry));

    entry.nodeid = chimera_fuse_node_insert(mount->node_table,
                                            attr->va_fh, attr->va_fh_len);
    entry.generation       = 1;
    entry.entry_valid      = mount->entry_timeout_ms / 1000;
    entry.entry_valid_nsec = (mount->entry_timeout_ms % 1000) * 1000000;
    entry.attr_valid       = mount->attr_timeout_ms / 1000;
    entry.attr_valid_nsec  = (mount->attr_timeout_ms % 1000) * 1000000;

    chimera_fuse_attr_from_vfs(&entry.attr, attr);

    chimera_fuse_abort_if(extra_len > 64, "fuse entry reply extra too large");

    memcpy(payload, &entry, sizeof(entry));

    if (extra_len) {
        memcpy(payload + sizeof(entry), extra, extra_len);
    }

    rc = chimera_fuse_send(req, 0, payload, sizeof(entry) + extra_len,
                           NULL, 0, 0);

    if (rc != 0) {
        chimera_fuse_node_forget(mount->node_table, entry.nodeid, 1);
    }

    chimera_fuse_request_finish(req);

    return rc;
} /* chimera_fuse_reply_entry */

void
chimera_fuse_channel_dead(struct chimera_fuse_channel *channel)
{
    if (channel->dead) {
        return;
    }

    channel->dead        = 1;
    channel->mount->dead = 1;

    if (channel->armed) {
        evpl_remove_fd_event(channel->thread->evpl, &channel->event);
        channel->armed = 0;
    }

    /* The fd stays open until the protocol destroy hook: in-flight requests
     * may still attempt replies on it, and closing early would let the
     * number be reused by an unrelated descriptor.  Open VFS handles the
     * kernel will now never RELEASE are swept at shutdown. */

    chimera_fuse_info("fuse mount %s: connection closed by kernel",
                      channel->mount->mountpoint);
} /* chimera_fuse_channel_dead */

static void
chimera_fuse_op_interrupt(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    /* ENOSYS to an INTERRUPT tells the kernel interrupts are unsupported so
     * it stops sending them; interrupted requests simply complete. */
    chimera_fuse_reply(req, ENOSYS, NULL, 0);
} /* chimera_fuse_op_interrupt */

static void
chimera_fuse_op_destroy(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen)
{
    chimera_fuse_reply(req, 0, NULL, 0);
} /* chimera_fuse_op_destroy */

const chimera_fuse_handler_t chimera_fuse_handlers[CHIMERA_FUSE_OPCODE_MAX] = {
    [FUSE_LOOKUP]       = chimera_fuse_op_lookup,
    [FUSE_FORGET]       = chimera_fuse_op_forget,
    [FUSE_BATCH_FORGET] = chimera_fuse_op_batch_forget,
    [FUSE_GETATTR]      = chimera_fuse_op_getattr,
    [FUSE_SETATTR]      = chimera_fuse_op_setattr,
    [FUSE_READLINK]     = chimera_fuse_op_readlink,
    [FUSE_STATFS]       = chimera_fuse_op_statfs,
    [FUSE_ACCESS]       = chimera_fuse_op_access,
    [FUSE_OPENDIR]      = chimera_fuse_op_opendir,
    [FUSE_READDIR]      = chimera_fuse_op_readdir,
    [FUSE_READDIRPLUS]  = chimera_fuse_op_readdir,
    [FUSE_RELEASEDIR]   = chimera_fuse_op_releasedir,
    [FUSE_FSYNCDIR]     = chimera_fuse_op_fsyncdir,
    [FUSE_OPEN]         = chimera_fuse_op_open,
    [FUSE_CREATE]       = chimera_fuse_op_create,
    [FUSE_READ]         = chimera_fuse_op_read,
    [FUSE_WRITE]        = chimera_fuse_op_write,
    [FUSE_FLUSH]        = chimera_fuse_op_flush,
    [FUSE_FSYNC]        = chimera_fuse_op_fsync,
    [FUSE_RELEASE]      = chimera_fuse_op_release,
    [FUSE_FALLOCATE]    = chimera_fuse_op_fallocate,
    [FUSE_LSEEK]        = chimera_fuse_op_lseek,
    [FUSE_MKDIR]        = chimera_fuse_op_mkdir,
    [FUSE_MKNOD]        = chimera_fuse_op_mknod,
    [FUSE_SYMLINK]      = chimera_fuse_op_symlink,
    [FUSE_LINK]         = chimera_fuse_op_link,
    [FUSE_UNLINK]       = chimera_fuse_op_unlink,
    [FUSE_RMDIR]        = chimera_fuse_op_rmdir,
    [FUSE_RENAME]       = chimera_fuse_op_rename,
    [FUSE_RENAME2]      = chimera_fuse_op_rename,
    [FUSE_GETXATTR]     = chimera_fuse_op_getxattr,
    [FUSE_SETXATTR]     = chimera_fuse_op_setxattr,
    [FUSE_LISTXATTR]    = chimera_fuse_op_listxattr,
    [FUSE_REMOVEXATTR]  = chimera_fuse_op_removexattr,
    [FUSE_INTERRUPT]    = chimera_fuse_op_interrupt,
    [FUSE_DESTROY]      = chimera_fuse_op_destroy,
};

static void
chimera_fuse_dispatch(
    struct chimera_fuse_request *req,
    uint32_t                     len)
{
    const struct fuse_in_header *hdr = evpl_iovec_data(&req->buf);
    chimera_fuse_handler_t       handler;

    if (len < sizeof(*hdr) || hdr->len != len) {
        chimera_fuse_error("fuse request framing mismatch (read %u, header %u)",
                           len, len >= sizeof(*hdr) ? hdr->len : 0);
        chimera_fuse_request_free(req->thread, req);
        return;
    }

    req->unique  = hdr->unique;
    req->opcode  = hdr->opcode;
    req->nodeid  = hdr->nodeid;
    req->buf_len = len;

    chimera_fuse_map_cred(&req->cred, hdr);

    handler = hdr->opcode < CHIMERA_FUSE_OPCODE_MAX ?
        chimera_fuse_handlers[hdr->opcode] : NULL;

    if (!handler) {
        chimera_fuse_reply(req, ENOSYS, NULL, 0);
        return;
    }

    handler(req, hdr, hdr + 1, len - sizeof(*hdr));
} /* chimera_fuse_dispatch */

void
chimera_fuse_channel_readable(
    struct evpl          *evpl,
    struct evpl_fd_event *event)
{
    struct chimera_fuse_channel *channel = container_of(event, struct chimera_fuse_channel, event);
    struct chimera_fuse_thread  *thread  = channel->thread;
    struct chimera_fuse_request *req;
    ssize_t                      len;
    int                          i;

    for (i = 0; i < CHIMERA_FUSE_READ_BATCH; i++) {

        req = chimera_fuse_request_alloc(thread, channel);

        len = read(channel->fd, evpl_iovec_data(&req->buf), CHIMERA_FUSE_BUFSZ);

        if (len < 0) {
            chimera_fuse_request_free(thread, req);

            switch (errno) {
                case EINTR:
                    continue;
                case EAGAIN:
                    evpl_fd_event_mark_unreadable(evpl, event);
                    return;
                case ENOENT:
                    /* Request aborted between wakeup and read. */
                    continue;
                case ENODEV:
                    chimera_fuse_channel_dead(channel);
                    return;
                default:
                    chimera_fuse_error("fuse channel read failed: %s",
                                       strerror(errno));
                    evpl_fd_event_mark_unreadable(evpl, event);
                    return;
            } /* switch */
        }

        chimera_fuse_dispatch(req, len);
    }

    /* Batch cap reached: the event stays readable, so the loop re-invokes us
     * on the next pass after giving other work a turn. */
} /* chimera_fuse_channel_readable */

void
chimera_fuse_channel_error(
    struct evpl          *evpl,
    struct evpl_fd_event *event)
{
    struct chimera_fuse_channel *channel = container_of(event, struct chimera_fuse_channel, event);

    chimera_fuse_channel_dead(channel);
} /* chimera_fuse_channel_error */
