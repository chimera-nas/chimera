// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <inttypes.h>
/*
 * S3 GetObject / HeadObject / GetObjectAttributes.
 *
 * GET and HEAD share one head sequence: PUTFH(bucket) -> LOOKUP_PATH(key)
 * -> OPEN_CURRENT -> GETHANDLE -> LISTXATTRS.  The lookup's attributes give
 * the ETag, Last-Modified and the size the range is resolved against; the
 * handle is taken for the body reads; the xattr names feed the metadata
 * headers (a GETXATTR fan-out, as a following sequence) and, for HEAD, the
 * x-amz-tagging-count -- counted from the same list, so the HEAD's open is
 * its only open.  The body is then one READ per io_size chunk, each its
 * own sequence on the taken handle, reassembled in file order.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>
#ifdef _WIN32
#include "common/platform.h"
#endif /* ifdef _WIN32 */
#include "common/format.h"
#include "vfs/vfs.h"
#include "vfs/vfs_compound.h"
#include "vfs/vfs_release.h"
#include "s3_internal.h"
#include "s3_etag.h"
#include "s3_procs.h"
#include "s3_metadata.h"
#include "s3_tagging.h"

/* Op indexes in the head sequence. */
#define CHIMERA_S3_GET_OP_LOOKUP    1
#define CHIMERA_S3_GET_OP_OPEN      2
#define CHIMERA_S3_GET_OP_GETHANDLE 3
#define CHIMERA_S3_GET_OP_LIST      4

/* HEAD object: metadata + tag-count headers attached, release the object handle
 * and finish the (bodyless) response. */
static void
chimera_s3_head_respond(
    struct evpl               *evpl,
    struct chimera_s3_request *request)
{
    struct chimera_server_s3_thread *thread = request->thread;

    if (request->file_handle) {
        chimera_vfs_release(thread->vfs, request->file_handle);
        request->file_handle = NULL;
    }

    request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;

    if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
        s3_server_respond(evpl, request);
    }
} /* chimera_s3_head_respond */

static void
chimera_s3_get_finish(struct chimera_s3_request *request)
{
    struct chimera_server_s3_thread *thread = request->thread;

    if (request->file_handle) {
        chimera_vfs_release(thread->vfs, request->file_handle);
        request->file_handle = NULL;
    }

    request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;

} /* chimera_s3_get_finish */

/*
 * Append every read at the head of the queue that has completed, stopping at
 * the first one still outstanding.
 *
 * The body of a GET response is an ordered byte stream: evpl_http_request_add_datav()
 * appends, and carries no file offset.  chimera_s3_get_send() issues all of an
 * object's reads before any of them completes, and nothing orders those
 * completions -- io_uring in particular reaps CQEs in whatever order the kernel
 * finishes them, which for reads that miss page cache and go async is not
 * submission order.  Appending as each read landed therefore permuted the
 * object's bytes, silently, with the correct total length.
 *
 * So the queue is drained from the head, oldest offset first, and a read that
 * completes early waits for its predecessors.  io_pending is decremented here
 * rather than in the callback so the request is not finished until every byte
 * has actually been handed to the response.
 */
static void
chimera_s3_get_drain(struct chimera_s3_request *request)
{
    struct chimera_server_s3_thread *thread = request->thread;
    struct evpl                     *evpl   = thread->evpl;
    struct chimera_s3_io            *io;

    while ((io = request->read_queue) != NULL && io->ready) {

        request->read_queue = io->queue_next;

        if (request->read_queue == NULL) {
            request->read_queue_tail = NULL;
        }

        if (io->r_niov) {
            chimera_s3_response_add_datav(evpl, request, io->iov, io->r_niov);
        }

        request->io_pending--;

        chimera_s3_io_free(thread, io);
    }

    if (request->io_pending == 0 &&
        request->vfs_state == CHIMERA_S3_VFS_STATE_SENT) {
        chimera_s3_get_finish(request);
    }
} /* chimera_s3_get_drain */

static void
chimera_s3_get_read_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_s3_io      *io      = private_data;
    struct chimera_s3_request *request = io->request;
    enum chimera_vfs_error     error_code;
    struct evpl_iovec         *iov  = NULL;
    int                        niov = 0;

    error_code = chimera_vfs_compound_status(compound);

    if (error_code) {
        request->status    = chimera_s3_status_from_vfs(error_code, CHIMERA_S3_STATUS_INTERNAL_ERROR);
        request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
        /* The sequence released the buffers on the error leg; there is
         * nothing to append for this chunk, but it still has to leave the
         * queue in order so the reads behind it are not stranded. */
        io->r_niov = 0;
    } else {
        /* The data is taken from the sequence: the descriptors were written
         * into this io's own iov[] -- the array handed to the READ -- so
         * only the returned count needs recording. */
        chimera_vfs_compound_take_iov(compound, 0, &iov, &niov);
        io->r_niov = niov;
    }

    chimera_vfs_compound_free(compound);

    io->ready = 1;

    chimera_s3_get_drain(request);
} /* chimera_s3_get_read_complete */

void
chimera_s3_get_send(
    struct evpl               *evpl,
    struct chimera_s3_request *request)
{
    struct chimera_server_s3_thread *thread = request->thread;
    struct chimera_server_s3_shared *shared = thread->shared;
    struct chimera_s3_config        *config = shared->config;
    struct chimera_s3_io            *io;
    struct chimera_vfs_compound     *compound;
    uint64_t                         left;

 again:

    left = request->file_left;

    if (left == 0) {
        request->vfs_state = CHIMERA_S3_VFS_STATE_SENT;

        if (request->io_pending == 0) {
            chimera_s3_get_finish(request);
        }
        return;
    }

    if (left > config->io_size) {
        left = config->io_size;
    }

    io = chimera_s3_io_alloc(thread, request);

    io->niov       = CHIMERA_S3_IOV_MAX;
    io->r_niov     = 0;
    io->ready      = 0;
    io->queue_next = NULL;

    /* Queue before dispatching: the sequence may complete inline (memfs and
     * the other non-blocking backends do), and the callback drains from this
     * queue. */
    if (request->read_queue_tail) {
        request->read_queue_tail->queue_next = io;
    } else {
        request->read_queue = io;
    }
    request->read_queue_tail = io;

    request->io_pending++;

    compound = chimera_vfs_compound_alloc(thread->vfs, &request->cred);

    chimera_vfs_compound_add_read(compound,
                                  request->file_handle,
                                  request->file_cur_offset,
                                  left,
                                  io->iov,
                                  io->niov,
                                  0,
                                  NULL,
                                  NULL, 0);

    chimera_vfs_compound_submit(compound, chimera_s3_get_read_complete, io);

    request->file_cur_offset += left;
    request->file_left       -= left;

    goto again;

} /* chimera_s3_get_send */

/*
 * Metadata xattrs have been read and their response headers attached. Dispatch
 * the response now (if the request body has been fully received). For HEAD the
 * request is complete; for GET the body is streamed once libevpl asks for data.
 */
static void
chimera_s3_get_metadata_done(
    struct chimera_s3_request *request,
    int                        error,
    void                      *private_data)
{
    struct chimera_server_s3_thread *thread = request->thread;
    struct evpl                     *evpl   = thread->evpl;
    int                              is_head;

    is_head = (evpl_http_request_type(request->http_request) ==
               EVPL_HTTP_REQUEST_TYPE_HEAD);

    if (is_head) {
        /* HEAD: no body.  The x-amz-tagging-count header was attached from
         * the head sequence's own xattr list; release the handle and finish
         * in chimera_s3_head_respond. */
        chimera_s3_head_respond(evpl, request);
        return;
    }

    request->vfs_state = CHIMERA_S3_VFS_STATE_SEND;

    if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
        s3_server_respond(evpl, request);
    }

    if (request->http_state == CHIMERA_S3_HTTP_STATE_SEND) {
        chimera_s3_get_send(evpl, request);
    }
} /* chimera_s3_get_metadata_done */

/*
 * Only regular files are S3 objects. A key can resolve to a directory
 * (chimera stores hierarchical keys as a real directory tree) or to an
 * entry whose lookup did not return the size/mtime/fh the object ETag is
 * built from. Either way it is not a readable object: report NoSuchKey
 * instead of asserting in chimera_s3_compute_etag.
 */
static int
chimera_s3_get_is_object(const struct chimera_vfs_attrs *attr)
{
    const uint64_t need = CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_SIZE |
        CHIMERA_VFS_ATTR_MTIME;
    int            is_dir = (attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) &&
        (attr->va_mode & S_IFMT) == S_IFDIR;

    return !is_dir && (attr->va_set_mask & need) == need;
} /* chimera_s3_get_is_object */

/*
 * The veto on the head sequence: what the lookup found has to be an object
 * before it is opened.  Answered from the lookup's own result, so it is the
 * same answer however many times it is asked.
 */
static void
chimera_s3_get_head_gate(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    const struct chimera_vfs_compound_op *op;

    (void) private_data;

    if (index != CHIMERA_S3_GET_OP_LOOKUP || *status != CHIMERA_VFS_OK) {
        return;
    }

    op = chimera_vfs_compound_op(compound, index);

    if (!chimera_s3_get_is_object(&op->attr)) {
        *status = CHIMERA_VFS_ENOENT;
    }
} /* chimera_s3_get_head_gate */

static void
chimera_s3_get_head_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    CHIMERA_S3_HOLD_REQUEST(private_data);
    struct chimera_s3_request            *request = private_data;
    struct chimera_server_s3_thread      *thread  = request->thread;
    struct evpl                          *evpl    = thread->evpl;
    const struct chimera_vfs_compound_op *op;
    struct chimera_vfs_attrs              attr;
    enum chimera_vfs_error                error_code;
    uint32_t                              completed;
    char                                 *names     = NULL;
    uint32_t                              names_len = 0;
    int                                   is_head;

    error_code = chimera_vfs_compound_status(compound);
    completed  = chimera_vfs_compound_num_completed(compound);

    /* The lookup or the open failed (or the gate refused what the lookup
     * found): no handle was made, nothing to release. */
    if (completed <= CHIMERA_S3_GET_OP_OPEN) {
        chimera_vfs_compound_free(compound);
        request->status    = chimera_s3_status_from_vfs(error_code, CHIMERA_S3_STATUS_NO_SUCH_KEY);
        request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
        if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
            s3_server_respond(evpl, request);
        }
        return;
    }

    attr = chimera_vfs_compound_op(compound, CHIMERA_S3_GET_OP_LOOKUP)->attr;

    request->file_handle = chimera_vfs_compound_take_handle(compound, CHIMERA_S3_GET_OP_GETHANDLE);

    /* A list that failed (no xattrs, or a backend without them) leaves the
     * defaults in place, as it did. */
    op = chimera_vfs_compound_op(compound, CHIMERA_S3_GET_OP_LIST);

    if (op->status == CHIMERA_VFS_OK && op->buffer_len) {
        names_len = op->buffer_len;
        names     = malloc(names_len);
        memcpy(names, op->buffer, names_len);
    }

    chimera_vfs_compound_free(compound);

    is_head = (evpl_http_request_type(request->http_request) ==
               EVPL_HTTP_REQUEST_TYPE_HEAD);

    if (is_head) {
        /* S3 reports the object's tag count on HEAD; the names came with the
         * open, so there is no second open for the count. */
        char hdr[16];

        snprintf(hdr, sizeof(hdr), "%d",
                 chimera_s3_tagging_count_names(names, names_len));
        chimera_s3_response_add_header(request, "x-amz-tagging-count", hdr);
    }

    chimera_s3_attach_etag(request->http_request, &attr);
    chimera_s3_attach_last_modified(request->http_request, &attr);

    request->file_real_length = attr.va_size;

    /* Reject an unsatisfiable range with 416 before resolving it. A range was
     * requested iff a sentinel is present (suffix carries file_offset < 0, any
     * explicit byte range carries file_length != 0; the no-range case is
     * file_offset 0 / file_length 0). A closed or open-ended range whose start
     * is at or past EOF is unsatisfiable, and any range against a zero-length
     * object is unsatisfiable. A range that merely extends past EOF but starts
     * within the object is clamped below and returns 206 (AWS semantics). */
    if (request->file_offset < 0 || request->file_length != 0) {
        if (request->file_real_length == 0 ||
            (request->file_offset >= 0 &&
             request->file_offset >= request->file_real_length)) {
            free(names);
            chimera_vfs_release(thread->vfs, request->file_handle);
            request->file_handle = NULL;
            request->status      = CHIMERA_S3_STATUS_INVALID_RANGE;
            request->vfs_state   = CHIMERA_S3_VFS_STATE_COMPLETE;
            if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
                s3_server_respond(evpl, request);
            }
            return;
        }
    }

    /* Resolve the requested byte range now that the object size is known. The
     * range was parsed before the size was available, so open-ended and suffix
     * forms still carry sentinels:
     *   no range          -> file_offset 0, file_length 0
     *   bytes=N-M (closed) -> file_offset N, file_length M-N+1
     *   bytes=N-  (open)   -> file_offset N, file_length -1
     *   bytes=-N  (suffix) -> file_offset -1, file_length N
     * Leaving a negative length (or offset) in place makes file_left wrap to a
     * huge unsigned value and the read loop in chimera_s3_get_send spins
     * forever. */
    if (request->file_offset < 0) {
        /* suffix: last file_length bytes */
        int64_t n = request->file_length;
        if (n > request->file_real_length) {
            n = request->file_real_length;
        }
        request->file_offset = request->file_real_length - n;
        request->file_length = n;
    } else if (request->file_length < 0) {
        /* open-ended: from file_offset to EOF */
        request->file_length = request->file_real_length - request->file_offset;
    } else if (request->file_length == 0) {
        /* whole object */
        request->file_offset = 0;
        request->file_length = request->file_real_length;
    } else {
        /* closed range: clamp to EOF */
        if (request->file_offset > request->file_real_length) {
            request->file_offset = request->file_real_length;
        }
        if (request->file_offset + request->file_length > request->file_real_length) {
            request->file_length = request->file_real_length - request->file_offset;
        }
    }

    if (request->file_length < 0) {
        request->file_length = 0;
    }

    request->file_left       = request->file_length;
    request->file_cur_offset = request->file_offset;

    /* Re-emit the object's stored metadata xattrs as response headers before
     * the response is dispatched.  The body is only streamed for GET. */
    chimera_s3_metadata_attach_from_list(request, request->file_handle,
                                         names, names_len,
                                         chimera_s3_get_metadata_done, NULL);

    free(names);
} /* chimera_s3_get_head_complete */

void
chimera_s3_get(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request)
{
    struct chimera_vfs_compound *compound;

    request->io_pending = 0;

    compound = chimera_vfs_compound_alloc(thread->vfs, &request->cred);

    chimera_vfs_compound_add_putfh(compound, request->bucket_fh,
                                   request->bucket_fhlen);
    chimera_vfs_compound_add_lookup_path(compound,
                                         request->path, request->path_len,
                                         CHIMERA_VFS_ATTR_FH |
                                         CHIMERA_VFS_ATTR_MASK_STAT,
                                         CHIMERA_VFS_LOOKUP_FOLLOW);
    chimera_vfs_compound_add_open_current(compound, 0, 0);
    chimera_vfs_compound_add_gethandle(compound);
    chimera_vfs_compound_add_listxattrs(compound, 0, 16384);

    chimera_vfs_compound_set_gate(compound, chimera_s3_get_head_gate, request);

    chimera_s3_request_get(request);

    chimera_vfs_compound_submit(compound, chimera_s3_get_head_complete, request);
} /* chimera_s3_get */

/*
 * GetObjectAttributes: GET /bucket/<key>?attributes with an
 * x-amz-object-attributes header. Returns a small XML document carrying the
 * attributes the filesystem can supply trivially (ETag, ObjectSize, and a
 * static StorageClass). Checksum and ObjectParts are not implemented and are
 * intentionally omitted; clients that request only those attributes still get
 * a well-formed 200 response.  PUTFH(bucket) -> LOOKUP_PATH(key).
 */
static void
chimera_s3_get_object_attributes_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_s3_request       *request = private_data;
    struct chimera_server_s3_thread *thread  = request->thread;
    struct evpl                     *evpl    = thread->evpl;
    enum chimera_vfs_error           error_code;
    struct chimera_vfs_attrs         attr;
    uint64_t                         etag[2];
    char                             etag_hex[80];
    char                            *bp, *body_start;

    error_code = chimera_vfs_compound_status(compound);

    if (error_code == CHIMERA_VFS_OK) {
        attr = chimera_vfs_compound_op(compound, 1)->attr;
    }

    chimera_vfs_compound_free(compound);

    if (error_code) {
        request->status    = chimera_s3_status_from_vfs(error_code, CHIMERA_S3_STATUS_NO_SUCH_KEY);
        request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
        if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
            s3_server_respond(evpl, request);
        }
        return;
    }

    /* Mirror the regular-object guard of the GET head sequence: only a
     * regular file with the attributes the ETag is built from is an object. */
    if (!chimera_s3_get_is_object(&attr)) {
        request->status    = CHIMERA_S3_STATUS_NO_SUCH_KEY;
        request->vfs_state = CHIMERA_S3_VFS_STATE_COMPLETE;
        if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
            s3_server_respond(evpl, request);
        }
        return;
    }

    /* ETag without surrounding quotes (the GetObjectAttributes API returns the
     * raw value, unlike the HTTP ETag header). */
    chimera_s3_compute_etag(etag, &attr);
    format_hex(etag_hex, sizeof(etag_hex), etag, sizeof(etag));

    chimera_s3_attach_last_modified(request->http_request, &attr);

    evpl_iovec_alloc(evpl, 4096, 0, 1, 0, &request->multipart.response);

    bp = body_start = evpl_iovec_data(&request->multipart.response);

    bp += sprintf(bp, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    bp += sprintf(bp, "<GetObjectAttributesOutput xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n");
    bp += sprintf(bp, "  <ETag>%s</ETag>\n", etag_hex);
    bp += sprintf(bp, "  <StorageClass>STANDARD</StorageClass>\n");
    bp += sprintf(bp, "  <ObjectSize>%" PRIu64 "</ObjectSize>\n", attr.va_size);
    bp += sprintf(bp, "</GetObjectAttributesOutput>\n");

    evpl_iovec_set_length(&request->multipart.response, bp - body_start);
    chimera_s3_response_add_datav(evpl, request,
                                  &request->multipart.response, 1);

    request->file_length      = bp - body_start;
    request->file_real_length = request->file_length;
    request->file_offset      = 0;
    request->is_list          = 1; /* triggers application/xml Content-Type */
    request->status           = CHIMERA_S3_STATUS_OK;
    request->vfs_state        = CHIMERA_S3_VFS_STATE_COMPLETE;

    if (request->http_state == CHIMERA_S3_HTTP_STATE_RECVED) {
        s3_server_respond(evpl, request);
    }
} /* chimera_s3_get_object_attributes_complete */

void
chimera_s3_get_object_attributes(
    struct evpl                     *evpl,
    struct chimera_server_s3_thread *thread,
    struct chimera_s3_request       *request)
{
    struct chimera_vfs_compound *compound;

    request->io_pending = 0;

    compound = chimera_vfs_compound_alloc(thread->vfs, &request->cred);

    chimera_vfs_compound_add_putfh(compound, request->bucket_fh,
                                   request->bucket_fhlen);
    chimera_vfs_compound_add_lookup_path(compound,
                                         request->path, request->path_len,
                                         CHIMERA_VFS_ATTR_FH |
                                         CHIMERA_VFS_ATTR_MASK_STAT,
                                         CHIMERA_VFS_LOOKUP_FOLLOW);

    chimera_s3_request_get(request);

    chimera_vfs_compound_submit(compound,
                                chimera_s3_get_object_attributes_complete,
                                request);
} /* chimera_s3_get_object_attributes */
