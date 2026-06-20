// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Mount/unmount lifecycle: module init/destroy and config parsing, format
 * (mkfs) and bootstrap, crash recovery (intent-log replay), the mount-time
 * synchronous block-I/O pump, per-worker thread context setup/teardown and
 * metrics registration.
 */

#include "diskfs_internal.h"


static const char *diskfs_metric_inode_cache_op_names[] = {
    "hit",
    "miss",
    "stale",
    "load",
    "insert",
    "wait",
};


static const char *diskfs_metric_block_cache_op_names[] = {
    "hit",
    "miss",
    "new",
    "wait",
    "cow",
    "recycle",
};


static const char *diskfs_metric_mtime_op_names[] = {
    "deferred",
    "flushed",
    "skip_not_inplace",
    "skip_size_grew",
    "skip_filesync",
};


static const char *diskfs_metric_io_dir_names[] = {
    "read",
    "write",
};


static const char *diskfs_metric_io_class_names[] = {
    "data",
    "rmw",
    "inode",
    "btree",
    "metadata",
    "intent_log",
    "tail_push",
};


static const char *diskfs_metric_txn_phase_names[] = {
    "queue_to_submit",
    "submit_to_durable",
    "queue_to_durable",
    "durable_to_callback",
    "queue_to_callback",
};

/* Forward declarations (definitions below, in call-graph order) */

static void
diskfs_metrics_init(
    struct diskfs_shared      *shared,
    struct prometheus_metrics *metrics);

static void
diskfs_thread_metrics_init(
    struct diskfs_thread *thread);

static void
diskfs_mount_io_complete(
    struct evpl *evpl,
    int          status,
    void        *private_data);

static int
diskfs_mount_io_write(
    void       *user,
    uint32_t    device_id,
    const void *buf,
    uint64_t    length,
    uint64_t    offset);

static int
diskfs_mount_io_write_many(
    void                     *user,
    const struct sm_io_write *writes,
    uint32_t                  count);

static int
diskfs_mount_io_flush(
    void    *user,
    uint32_t device_id);

static int
diskfs_mount_io_discard(
    void    *user,
    uint32_t device_id,
    uint64_t offset,
    uint64_t length);

static struct sm_io
diskfs_mount_sm_io(
    struct diskfs_mount_io *io);

static int
diskfs_recover_log(
    struct diskfs_shared   *shared,
    struct diskfs_mount_io *io);

static uint32_t
diskfs_parse_hex(
    const char *hex,
    uint8_t    *out,
    uint32_t    max);

static void
diskfs_inode_cache_release(
    struct rb_node *node,
    void           *private_data);


static void
diskfs_metrics_init(
    struct diskfs_shared      *shared,
    struct prometheus_metrics *metrics)
{
    struct diskfs_metrics *m               = &shared->metrics;
    static const char     *op_label[]      = { "op" };
    static const char     *phase_label[]   = { "phase" };
    static const char     *io_labels[]     = { "direction", "class" };
    static const char     *io_dev_labels[] = { "direction", "class", "device" };
    static const char     *intent_label[]  = { "name" };
    static const char     *txn_label[]     = { "name" };
    static const char     *txn_names[]     = { "write", "blocks", "bytes" };
    static const char     *intent_names[]  = {
        "redo_inflight",
        "iocbs_inflight",
        "push_outstanding",
        "log_used_bytes",
        "registered_channels",
        "redo_inflight_high_water",
        "iocbs_inflight_high_water",
        "push_outstanding_high_water",
        "log_used_bytes_high_water",
    };

    if (!metrics) {
        return;
    }

    m->metrics     = metrics;
    m->num_devices = shared->num_devices;
    m->inode_cache = prometheus_metrics_create_counter(
        metrics, "chimera_diskfs_inode_cache",
        "Diskfs inode cache events");
    m->block_cache = prometheus_metrics_create_counter(
        metrics, "chimera_diskfs_block_cache",
        "Diskfs block cache events");
    m->mtime = prometheus_metrics_create_counter(
        metrics, "chimera_diskfs_mtime",
        "Diskfs deferred-mtime accounting (deferred/flushed/skip reasons)");
    m->block_io_ops = prometheus_metrics_create_counter(
        metrics, "chimera_diskfs_block_io_ops",
        "Diskfs classified block I/O submissions");
    m->block_io_bytes = prometheus_metrics_create_counter(
        metrics, "chimera_diskfs_block_io_bytes",
        "Diskfs classified block I/O submitted bytes");
    m->block_io_device_ops = prometheus_metrics_create_counter(
        metrics, "chimera_diskfs_block_io_device_ops",
        "Diskfs classified block I/O submissions by device");
    m->block_io_device_bytes = prometheus_metrics_create_counter(
        metrics, "chimera_diskfs_block_io_device_bytes",
        "Diskfs classified block I/O submitted bytes by device");
    m->txn = prometheus_metrics_create_counter(
        metrics, "chimera_diskfs_txn",
        "Diskfs transaction counters");
    m->txn_blocks = prometheus_metrics_create_histogram_exponential(
        metrics, "chimera_diskfs_txn_blocks",
        "Diskfs dirty blocks per transaction", 24);
    m->txn_bytes = prometheus_metrics_create_histogram_exponential(
        metrics, "chimera_diskfs_txn_bytes",
        "Diskfs dirty bytes per transaction", 32);
    m->txn_latency = prometheus_metrics_create_histogram_time(
        metrics, "chimera_diskfs_txn_latency_nanoseconds",
        "Diskfs transaction latency in nanoseconds", 34);
    m->pending_io = prometheus_metrics_create_gauge(
        metrics, "chimera_diskfs_pending_io",
        "Diskfs outstanding worker block I/O");
    m->intent_log = prometheus_metrics_create_gauge(
        metrics, "chimera_diskfs_intent_log",
        "Diskfs intent-log pressure gauges");
    for (int i = 0; i < DISKFS_METRIC_INODE_CACHE_NUM; i++) {
        m->inode_cache_series[i] = prometheus_counter_create_series(
            m->inode_cache, op_label, &diskfs_metric_inode_cache_op_names[i], 1);
    }
    for (int i = 0; i < DISKFS_METRIC_BLOCK_CACHE_NUM; i++) {
        m->block_cache_series[i] = prometheus_counter_create_series(
            m->block_cache, op_label, &diskfs_metric_block_cache_op_names[i], 1);
    }
    for (int i = 0; i < DISKFS_METRIC_MTIME_NUM; i++) {
        m->mtime_series[i] = prometheus_counter_create_series(
            m->mtime, op_label, &diskfs_metric_mtime_op_names[i], 1);
    }
    for (int d = 0; d < DISKFS_METRIC_IO_NUM_DIRS; d++) {
        for (int c = 0; c < DISKFS_METRIC_IO_NUM_CLASSES; c++) {
            const char *values[] = {
                diskfs_metric_io_dir_names[d],
                diskfs_metric_io_class_names[c],
            };

            m->block_io_ops_series[d][c] = prometheus_counter_create_series(
                m->block_io_ops, io_labels, values, 2);
            m->block_io_bytes_series[d][c] = prometheus_counter_create_series(
                m->block_io_bytes, io_labels, values, 2);
        }
    }
    m->block_io_device_ops_series = calloc(
        (size_t) shared->num_devices * DISKFS_METRIC_IO_NUM_DIRS *
        DISKFS_METRIC_IO_NUM_CLASSES, sizeof(*m->block_io_device_ops_series));
    m->block_io_device_bytes_series = calloc(
        (size_t) shared->num_devices * DISKFS_METRIC_IO_NUM_DIRS *
        DISKFS_METRIC_IO_NUM_CLASSES, sizeof(*m->block_io_device_bytes_series));
    for (int dev = 0; dev < shared->num_devices; dev++) {
        for (int d = 0; d < DISKFS_METRIC_IO_NUM_DIRS; d++) {
            for (int c = 0; c < DISKFS_METRIC_IO_NUM_CLASSES; c++) {
                size_t      idx = ((size_t) dev * DISKFS_METRIC_IO_NUM_DIRS + d) *
                    DISKFS_METRIC_IO_NUM_CLASSES + c;
                const char *values[] = {
                    diskfs_metric_io_dir_names[d],
                    diskfs_metric_io_class_names[c],
                    shared->devices[dev].name,
                };

                m->block_io_device_ops_series[idx] = prometheus_counter_create_series(
                    m->block_io_device_ops, io_dev_labels, values, 3);
                m->block_io_device_bytes_series[idx] = prometheus_counter_create_series(
                    m->block_io_device_bytes, io_dev_labels, values, 3);
            }
        }
    }
    for (int i = 0; i < 3; i++) {
        m->txn_series[i] = prometheus_counter_create_series(
            m->txn, txn_label, &txn_names[i], 1);
    }
    m->txn_blocks_series = prometheus_histogram_create_series(m->txn_blocks, NULL, NULL, 0);
    m->txn_bytes_series  = prometheus_histogram_create_series(m->txn_bytes, NULL, NULL, 0);
    for (int i = 0; i < DISKFS_METRIC_TXN_NUM_PHASES; i++) {
        m->txn_latency_series[i] = prometheus_histogram_create_series(
            m->txn_latency, phase_label, &diskfs_metric_txn_phase_names[i], 1);
    }
    m->pending_io_series = prometheus_gauge_create_series(m->pending_io, NULL, NULL, 0);
    for (int i = 0; i < 9; i++) {
        m->intent_log_series[i] = prometheus_gauge_create_series(
            m->intent_log, intent_label, &intent_names[i], 1);
    }
} /* diskfs_metrics_init */


static void
diskfs_thread_metrics_init(struct diskfs_thread *thread)
{
    struct diskfs_metrics        *m  = &thread->shared->metrics;
    struct diskfs_thread_metrics *tm = &thread->metrics;

    if (!m->metrics) {
        return;
    }

    for (int i = 0; i < DISKFS_METRIC_INODE_CACHE_NUM; i++) {
        tm->inode_cache[i] = prometheus_counter_series_create_instance(m->inode_cache_series[i]);
    }
    for (int i = 0; i < DISKFS_METRIC_BLOCK_CACHE_NUM; i++) {
        tm->block_cache[i] = prometheus_counter_series_create_instance(m->block_cache_series[i]);
    }
    for (int i = 0; i < DISKFS_METRIC_MTIME_NUM; i++) {
        tm->mtime[i] = prometheus_counter_series_create_instance(m->mtime_series[i]);
    }
    for (int d = 0; d < DISKFS_METRIC_IO_NUM_DIRS; d++) {
        for (int c = 0; c < DISKFS_METRIC_IO_NUM_CLASSES; c++) {
            tm->block_io_ops[d][c] =
                prometheus_counter_series_create_instance(m->block_io_ops_series[d][c]);
            tm->block_io_bytes[d][c] =
                prometheus_counter_series_create_instance(m->block_io_bytes_series[d][c]);
        }
    }
    tm->block_io_device_ops = calloc(
        (size_t) m->num_devices * DISKFS_METRIC_IO_NUM_DIRS *
        DISKFS_METRIC_IO_NUM_CLASSES, sizeof(*tm->block_io_device_ops));
    tm->block_io_device_bytes = calloc(
        (size_t) m->num_devices * DISKFS_METRIC_IO_NUM_DIRS *
        DISKFS_METRIC_IO_NUM_CLASSES, sizeof(*tm->block_io_device_bytes));
    for (int dev = 0; dev < m->num_devices; dev++) {
        for (int d = 0; d < DISKFS_METRIC_IO_NUM_DIRS; d++) {
            for (int c = 0; c < DISKFS_METRIC_IO_NUM_CLASSES; c++) {
                size_t idx = ((size_t) dev * DISKFS_METRIC_IO_NUM_DIRS + d) *
                    DISKFS_METRIC_IO_NUM_CLASSES + c;

                tm->block_io_device_ops[idx] =
                    prometheus_counter_series_create_instance(
                        m->block_io_device_ops_series[idx]);
                tm->block_io_device_bytes[idx] =
                    prometheus_counter_series_create_instance(
                        m->block_io_device_bytes_series[idx]);
            }
        }
    }
    for (int i = 0; i < 3; i++) {
        tm->txn[i] = prometheus_counter_series_create_instance(m->txn_series[i]);
    }
    tm->txn_blocks = prometheus_histogram_series_create_instance(m->txn_blocks_series);
    tm->txn_bytes  = prometheus_histogram_series_create_instance(m->txn_bytes_series);
    for (int i = 0; i < DISKFS_METRIC_TXN_NUM_PHASES; i++) {
        tm->txn_latency[i] = prometheus_histogram_series_create_instance(m->txn_latency_series[i]);
    }
    tm->pending_io = prometheus_gauge_series_create_instance(m->pending_io_series);
} /* diskfs_thread_metrics_init */


void
diskfs_intent_log_metrics_init(struct diskfs_intent_log *il)
{
    struct diskfs_shared  *shared = container_of(il, struct diskfs_shared, intent_log);
    struct diskfs_metrics *m      = &shared->metrics;

    if (!m->metrics) {
        return;
    }

    for (int d = 0; d < DISKFS_METRIC_IO_NUM_DIRS; d++) {
        for (int c = 0; c < DISKFS_METRIC_IO_NUM_CLASSES; c++) {
            il->metrics.block_io_ops[d][c] =
                prometheus_counter_series_create_instance(m->block_io_ops_series[d][c]);
            il->metrics.block_io_bytes[d][c] =
                prometheus_counter_series_create_instance(m->block_io_bytes_series[d][c]);
        }
    }
    il->metrics.block_io_device_ops = calloc(
        (size_t) m->num_devices * DISKFS_METRIC_IO_NUM_DIRS *
        DISKFS_METRIC_IO_NUM_CLASSES, sizeof(*il->metrics.block_io_device_ops));
    il->metrics.block_io_device_bytes = calloc(
        (size_t) m->num_devices * DISKFS_METRIC_IO_NUM_DIRS *
        DISKFS_METRIC_IO_NUM_CLASSES, sizeof(*il->metrics.block_io_device_bytes));
    for (int dev = 0; dev < m->num_devices; dev++) {
        for (int d = 0; d < DISKFS_METRIC_IO_NUM_DIRS; d++) {
            for (int c = 0; c < DISKFS_METRIC_IO_NUM_CLASSES; c++) {
                size_t idx = ((size_t) dev * DISKFS_METRIC_IO_NUM_DIRS + d) *
                    DISKFS_METRIC_IO_NUM_CLASSES + c;

                il->metrics.block_io_device_ops[idx] =
                    prometheus_counter_series_create_instance(
                        m->block_io_device_ops_series[idx]);
                il->metrics.block_io_device_bytes[idx] =
                    prometheus_counter_series_create_instance(
                        m->block_io_device_bytes_series[idx]);
            }
        }
    }
    for (int i = 0; i < DISKFS_METRIC_TXN_NUM_PHASES; i++) {
        il->metrics.txn_latency[i] =
            prometheus_histogram_series_create_instance(m->txn_latency_series[i]);
    }
    il->metrics.redo_inflight =
        prometheus_gauge_series_create_instance(m->intent_log_series[0]);
    il->metrics.iocbs_inflight =
        prometheus_gauge_series_create_instance(m->intent_log_series[1]);
    il->metrics.push_outstanding =
        prometheus_gauge_series_create_instance(m->intent_log_series[2]);
    il->metrics.log_used_bytes =
        prometheus_gauge_series_create_instance(m->intent_log_series[3]);
    il->metrics.registered_channels =
        prometheus_gauge_series_create_instance(m->intent_log_series[4]);
    il->metrics.redo_inflight_high_water =
        prometheus_gauge_series_create_instance(m->intent_log_series[5]);
    il->metrics.iocbs_inflight_high_water =
        prometheus_gauge_series_create_instance(m->intent_log_series[6]);
    il->metrics.push_outstanding_high_water =
        prometheus_gauge_series_create_instance(m->intent_log_series[7]);
    il->metrics.log_used_bytes_high_water =
        prometheus_gauge_series_create_instance(m->intent_log_series[8]);
} /* diskfs_intent_log_metrics_init */


static void
diskfs_mount_io_complete(
    struct evpl *evpl,
    int          status,
    void        *private_data)
{
    struct diskfs_mount_io_wait *w = private_data;

    (void) evpl;
    w->status = status;
    w->done   = 1;
} /* diskfs_mount_io_complete */


struct diskfs_mount_io *
diskfs_mount_io_open(struct diskfs_shared *shared)
{
    struct diskfs_mount_io *io = calloc(1, sizeof(*io));
    int                     i;

    io->shared = shared;
    io->evpl   = evpl_create(NULL);
    io->queue  = calloc(shared->num_devices, sizeof(*io->queue));
    for (i = 0; i < shared->num_devices; i++) {
        io->queue[i] = shared->devices[i].bdev ?
            evpl_block_open_queue(io->evpl, shared->devices[i].bdev) : NULL;
    }
    return io;
} /* diskfs_mount_io_open */


void
diskfs_mount_io_close(struct diskfs_mount_io *io)
{
    int i;

    for (i = 0; i < io->shared->num_devices; i++) {
        if (io->queue[i]) {
            evpl_block_close_queue(io->evpl, io->queue[i]);
        }
    }
    free(io->queue);
    evpl_destroy(io->evpl);
    free(io);
} /* diskfs_mount_io_close */


/*
 * sm_io read bridge.  offset must be block-aligned; the device transfer is
 * rounded up to a whole block and only the requested bytes are copied out, so
 * callers may ask for a sub-block struct.  Chunked at the device max request
 * size.
 */
int
diskfs_mount_io_read(
    void    *user,
    uint32_t device_id,
    void    *buf,
    uint64_t length,
    uint64_t offset)
{
    struct diskfs_mount_io *io = user;
    uint64_t                maxreq;
    uint64_t                done = 0;

    if (device_id >= (uint32_t) io->shared->num_devices ||
        !io->queue[device_id]) {
        return -1;
    }
    maxreq = io->shared->devices[device_id].max_request_size;
    if (maxreq == 0) {
        return -1;
    }

    while (done < length) {
        struct evpl_iovec           iov;
        struct diskfs_mount_io_wait w    = { 0, 0 };
        uint64_t                    want = length - done;
        uint64_t                    xfer;

        if (want > maxreq) {
            want = maxreq;
        }
        xfer = (want + DISKFS_BLOCK_SIZE - 1) & ~((uint64_t) DISKFS_BLOCK_SIZE - 1);

        evpl_iovec_alloc(io->evpl, xfer, DISKFS_BLOCK_SIZE, 1, 0, &iov);
        evpl_block_read(io->evpl, io->queue[device_id], &iov, 1, offset + done,
                        diskfs_mount_io_complete, &w);
        while (!w.done) {
            evpl_continue(io->evpl);
        }
        if (w.status) {
            evpl_iovec_release(io->evpl, &iov);
            return -1;
        }
        memcpy((char *) buf + done, iov.data, want);
        evpl_iovec_release(io->evpl, &iov);
        done += want;
    }
    return 0;
} /* diskfs_mount_io_read */


/* sm_io write bridge.  offset and length must be block-aligned (callers write
 * whole blocks / the block-padded superblock + condensed log slots). */
static int
diskfs_mount_io_write(
    void       *user,
    uint32_t    device_id,
    const void *buf,
    uint64_t    length,
    uint64_t    offset)
{
    struct diskfs_mount_io *io = user;
    uint64_t                maxreq;
    uint64_t                done = 0;

    if (device_id >= (uint32_t) io->shared->num_devices ||
        !io->queue[device_id]) {
        return -1;
    }
    maxreq = io->shared->devices[device_id].max_request_size;
    if (maxreq == 0) {
        return -1;
    }

    while (done < length) {
        struct evpl_iovec           iov;
        struct diskfs_mount_io_wait w    = { 0, 0 };
        uint64_t                    want = length - done;

        if (want > maxreq) {
            want = maxreq;
        }
        evpl_iovec_alloc(io->evpl, want, DISKFS_BLOCK_SIZE, 1, 0, &iov);
        memcpy(iov.data, (const char *) buf + done, want);
        evpl_block_write(io->evpl, io->queue[device_id], &iov, 1, offset + done,
                         !io->shared->unsafe_async, diskfs_mount_io_complete, &w);
        while (!w.done) {
            evpl_continue(io->evpl);
        }
        evpl_iovec_release(io->evpl, &iov);
        if (w.status) {
            return -1;
        }
        done += want;
    }
    return 0;
} /* diskfs_mount_io_write */


static int
diskfs_mount_io_write_many(
    void                     *user,
    const struct sm_io_write *writes,
    uint32_t                  count)
{
    struct diskfs_mount_io      *io = user;
    struct diskfs_mount_io_wait *waits;
    struct evpl_iovec           *iovs;
    uint32_t                     i, done = 0;
    int                          rc = 0;

    if (count == 0) {
        return 0;
    }

    waits = calloc(count, sizeof(*waits));
    iovs  = calloc(count, sizeof(*iovs));
    if (!waits || !iovs) {
        free(waits);
        free(iovs);
        return -1;
    }

    for (i = 0; i < count; i++) {
        struct diskfs_device *dev;

        if (writes[i].device_id >= (uint32_t) io->shared->num_devices ||
            !io->queue[writes[i].device_id]) {
            rc    = -1;
            count = i;
            goto out;
        }
        dev = &io->shared->devices[writes[i].device_id];

        if (dev->max_request_size == 0 || writes[i].length > dev->max_request_size) {
            rc    = -1;
            count = i;
            goto out;
        }

        evpl_iovec_alloc(io->evpl, writes[i].length, DISKFS_BLOCK_SIZE, 1, 0,
                         &iovs[i]);
        memcpy(iovs[i].data, writes[i].buf, writes[i].length);
        evpl_block_write(io->evpl, io->queue[writes[i].device_id], &iovs[i],
                         1, writes[i].offset, !io->shared->unsafe_async,
                         diskfs_mount_io_complete, &waits[i]);
    }

    while (done < count) {
        evpl_continue(io->evpl);
        done = 0;
        for (i = 0; i < count; i++) {
            if (waits[i].done) {
                done++;
            }
        }
    }

    for (i = 0; i < count; i++) {
        if (waits[i].status) {
            rc = -1;
            break;
        }
    }

 out:
    for (i = 0; i < count; i++) {
        evpl_iovec_release(io->evpl, &iovs[i]);
    }
    free(iovs);
    free(waits);
    return rc;
} /* diskfs_mount_io_write_many */


static int
diskfs_mount_io_flush(
    void    *user,
    uint32_t device_id)
{
    struct diskfs_mount_io     *io = user;
    struct diskfs_mount_io_wait w  = { 0, 0 };

    if (device_id >= (uint32_t) io->shared->num_devices ||
        !io->queue[device_id]) {
        return -1;
    }

    evpl_block_flush(io->evpl, io->queue[device_id], diskfs_mount_io_complete, &w);
    while (!w.done) {
        evpl_continue(io->evpl);
    }
    return w.status ? -1 : 0;
} /* diskfs_mount_io_flush */


/* Discard (deallocate) a device byte range, pumping the mount-time evpl to
 * completion.  Used at mkfs to clear whole devices.  Backends without native
 * discard treat it as a no-op success. */
static int
diskfs_mount_io_discard(
    void    *user,
    uint32_t device_id,
    uint64_t offset,
    uint64_t length)
{
    struct diskfs_mount_io     *io = user;
    struct diskfs_mount_io_wait w  = { 0, 0 };

    if (device_id >= (uint32_t) io->shared->num_devices ||
        !io->queue[device_id]) {
        return -1;
    }

    evpl_block_discard(io->evpl, io->queue[device_id], offset, length,
                       diskfs_mount_io_complete, &w);
    while (!w.done) {
        evpl_continue(io->evpl);
    }
    return w.status ? -1 : 0;
} /* diskfs_mount_io_discard */


static struct sm_io
diskfs_mount_sm_io(struct diskfs_mount_io *io)
{
    struct sm_io smio = {
        .read       = diskfs_mount_io_read,
        .write      = diskfs_mount_io_write,
        .write_many = diskfs_mount_io_write_many,
        .flush      = diskfs_mount_io_flush,
        .user       = io,
    };

    return smio;
} /* diskfs_mount_sm_io */


/*
 * Crash recovery (synchronous replay): the previous instance did not unmount
 * cleanly, so logged-but-not-yet-pushed redo records may still sit in the
 * intent log while their home locations hold stale data.  Sweep the log for
 * intact records -- a 4 KiB-aligned magic whose XXH3-128 over reclen bytes
 * verifies (rejecting torn/partially-overwritten records) -- and write each
 * block image to its home location in seq order (latest image of a block
 * wins), then flush.  After this the on-disk b+tree / inodes / data are
 * consistent with the last acknowledged write, exactly as the tail-pusher
 * would have left them.
 *
 * Replaying every intact record (rather than just [tail, head]) is safe: in a
 * FIFO circular log a superseding record outlives every record it supersedes,
 * so seq-ordered replay always lands the latest image, and re-writing an
 * already-current block is idempotent.  Runs at mount before worker threads
 * exist, so it drives the device through the mount-time evpl pump.
 */
static int
diskfs_recover_log(
    struct diskfs_shared   *shared,
    struct diskfs_mount_io *io)
{
    char                      *log;
    uint64_t                   o;
    struct diskfs_recover_rec *recs;
    uint32_t                   nrec = 0, cap = 4096, i;

    uint64_t                   intent_log_size = shared->intent_log_size;

    log = malloc(intent_log_size);
    if (diskfs_mount_io_read(io, SM_INTENT_LOG_DEVICE, log, intent_log_size,
                             SM_INTENT_LOG_OFFSET) != 0) {
        free(log);
        return -1;
    }

    recs = malloc(cap * sizeof(*recs));

    for (o = 0; o + sizeof(struct diskfs_redo_header) <= intent_log_size;
         o += DISKFS_BLOCK_SIZE) {
        struct diskfs_redo_header *hdr = (struct diskfs_redo_header *) (log + o);
        uint64_t                   lo, hi;
        XXH128_hash_t              h;

        if (hdr->magic != DISKFS_REDO_MAGIC) {
            continue;
        }
        if (hdr->reclen < sizeof(*hdr) ||
            (hdr->reclen & (DISKFS_BLOCK_SIZE - 1)) ||
            hdr->reclen != diskfs_il_hdr_len(hdr->num_blocks) + (uint64_t) hdr->num_blocks * DISKFS_BLOCK_SIZE ||
            o + hdr->reclen > intent_log_size) {
            continue;
        }
        lo           = hdr->csum_lo;
        hi           = hdr->csum_hi;
        hdr->csum_lo = 0;
        hdr->csum_hi = 0;
        h            = XXH3_128bits(log + o, diskfs_il_hdr_len(hdr->num_blocks));
        hdr->csum_lo = lo;
        hdr->csum_hi = hi;
        if (h.low64 != lo || h.high64 != hi) {
            continue;
        }
        {
            char    *bhp  = log + o + sizeof(*hdr);
            char    *data = log + o + diskfs_il_hdr_len(hdr->num_blocks);
            uint32_t b;
            int      ok = 1;

            for (b = 0; b < hdr->num_blocks; b++) {
                struct diskfs_redo_block_header *bh =
                    (struct diskfs_redo_block_header *) (bhp + (size_t) b * sizeof(*bh));
                char                            *img   = data + (size_t) b * DISKFS_BLOCK_SIZE;
                XXH128_hash_t                    bhash = XXH3_128bits(img, DISKFS_BLOCK_SIZE);

                if (bhash.low64 != bh->block_csum_lo ||
                    bhash.high64 != bh->block_csum_hi) {
                    ok = 0;
                    break;
                }
            }
            if (!ok) {
                continue;
            }
        }

        if (nrec == cap) {
            cap *= 2;
            recs = realloc(recs, cap * sizeof(*recs));
        }
        recs[nrec].seq    = hdr->seq;
        recs[nrec].offset = o;
        nrec++;
    }

    qsort(recs, nrec, sizeof(*recs), diskfs_recover_rec_cmp);

    for (i = 0; i < nrec; i++) {
        struct diskfs_redo_header *hdr  = (struct diskfs_redo_header *) (log + recs[i].offset);
        char                      *bhp  = log + recs[i].offset + sizeof(*hdr);
        char                      *data = log + recs[i].offset + diskfs_il_hdr_len(hdr->num_blocks);
        uint32_t                   b;

        /* New layout: all per-block headers are grouped after the redo header,
         * and the block images follow the 4 KiB-aligned header region. */
        for (b = 0; b < hdr->num_blocks; b++) {
            struct diskfs_redo_block_header *bh =
                (struct diskfs_redo_block_header *) (bhp + (size_t) b * sizeof(*bh));
            char                            *img = data + (size_t) b * DISKFS_BLOCK_SIZE;

            if (bh->device_id < (uint32_t) shared->num_devices) {
                int wr = diskfs_mount_io_write(io, bh->device_id, img,
                                               DISKFS_BLOCK_SIZE,
                                               bh->device_offset);

                chimera_diskfs_abort_if(wr != 0,
                                        "recovery replay write failed");
            }
        }
    }

    for (i = 0; i < (uint32_t) shared->num_devices; i++) {
        diskfs_mount_io_flush(io, i);
    }

    free(recs);
    free(log);
    chimera_diskfs_info("crash recovery: replayed %u intact intent-log records", nrec);
    return 0;
} /* diskfs_recover_log */


/*
 * Decode a hex string (e.g. "deadbeef" or "de:ad:be:ef") into up to `max`
 * bytes; returns the number of bytes decoded.  Used for the block-mode device
 * id and SIMPLE-volume signature, which are configured as hex.
 */
static uint32_t
diskfs_parse_hex(
    const char *hex,
    uint8_t    *out,
    uint32_t    max)
{
    uint32_t n  = 0;
    int      hi = -1;

    if (!hex) {
        return 0;
    }
    for (; *hex && n < max; hex++) {
        int v;

        if (*hex >= '0' && *hex <= '9') {
            v = *hex - '0';
        } else if (*hex >= 'a' && *hex <= 'f') {
            v = *hex - 'a' + 10;
        } else if (*hex >= 'A' && *hex <= 'F') {
            v = *hex - 'A' + 10;
        } else {
            continue;   /* skip separators (':', '-', spaces) */
        }
        if (hi < 0) {
            hi = v;
        } else {
            out[n++] = (uint8_t) ((hi << 4) | v);
            hi       = -1;
        }
    }
    return n;
} /* diskfs_parse_hex */


void *
diskfs_init(
    const char                *cfgdata,
    struct prometheus_metrics *metrics)
{
    struct diskfs_shared       *shared = calloc(1, sizeof(*shared));
    struct diskfs_device       *device;
    enum evpl_block_protocol_id protocol_id;
    const char                 *protocol_name, *device_path;
    char                       *device0_path = NULL;
    int                         i, fd, rc;
    struct stat                 st;
    int64_t                     size;
    struct sm_device_cfg       *dev_cfg;
    json_t                     *cfg, *devices_cfg, *device_cfg;
    json_error_t                json_error;
    int                         initialize;


    cfg = json_loads(cfgdata, 0, &json_error);

    chimera_diskfs_abort_if(cfg == NULL, "Error parsing config: %s", json_error.text);

    devices_cfg = json_object_get(cfg, "devices");

    shared->num_devices  = json_array_size(devices_cfg);
    shared->devices      = calloc(shared->num_devices, sizeof(*shared->devices));
    shared->device_paths = calloc(shared->num_devices, sizeof(char *));

    json_array_foreach(devices_cfg, i, device_cfg)
    {
        const char *role_name;

        device     = &shared->devices[i];
        device->id = i;

        protocol_name = json_string_value(json_object_get(device_cfg, "type"));
        device_path   = json_string_value(json_object_get(device_cfg, "path"));
        size          = json_integer_value(json_object_get(device_cfg, "size"));
        role_name     = json_string_value(json_object_get(device_cfg, "role"));

        /* A "remote" device models pNFS-block data storage outside this system:
         * diskfs tracks its free space but never opens it.  Its size, stable
         * deviceid and SIMPLE-volume signature come from config; its AG logs are
         * relocated onto the local metadata device by the allocator. */
        if (role_name && strcmp(role_name, "remote") == 0) {
            json_t *sig_cfg  = json_object_get(device_cfg, "signature");
            json_t *scsi_cfg = json_object_get(device_cfg, "scsi");

            chimera_diskfs_abort_if(i == 0, "device 0 must be a local metadata device");

            device->role             = SM_DEV_REMOTE;
            device->bdev             = NULL;
            device->size             = size;
            device->max_request_size = 0;
            shared->device_paths[i]  = strdup(device_path ? device_path : "");
            snprintf(device->name, sizeof(device->name), "%s",
                     device_path ? device_path : "remote");

            diskfs_parse_hex(json_string_value(json_object_get(device_cfg, "deviceid")),
                             device->deviceid, SM_DEVICEID_SIZE);
            /* A remote device carries either a SIMPLE-volume content signature
             * (block layout, RFC 5663) or a SCSI VPD-0x83 designator (SCSI
             * layout, RFC 8154); the share-level mode flag selects which. */
            if (json_is_object(sig_cfg)) {
                device->sig_offset = json_integer_value(json_object_get(sig_cfg, "offset"));
                device->sig_len    = diskfs_parse_hex(
                    json_string_value(json_object_get(sig_cfg, "bytes")),
                    device->sig, SM_SIG_MAX);
            }
            if (json_is_object(scsi_cfg)) {
                const char *dtype = json_string_value(
                    json_object_get(scsi_cfg, "designator_type"));
                const char *cset = json_string_value(
                    json_object_get(scsi_cfg, "code_set"));

                /* Default binary NAA -- the common SAS/FC/iSCSI WWID form. */
                device->scsi_desig_type = 3; /* NAA */
                if (dtype && strcmp(dtype, "eui64") == 0) {
                    device->scsi_desig_type = 2;
                } else if (dtype && strcmp(dtype, "t10") == 0) {
                    device->scsi_desig_type = 1;
                }
                device->scsi_code_set  = (cset && strcmp(cset, "ascii") == 0) ? 2 : 1;
                device->scsi_desig_len = diskfs_parse_hex(
                    json_string_value(json_object_get(scsi_cfg, "id")),
                    device->scsi_desig, sizeof(device->scsi_desig));
                device->scsi_pr_key = json_integer_value(
                    json_object_get(scsi_cfg, "pr_key"));
            }

            chimera_diskfs_info(
                "Remote data device %s size %lu sig_len %u scsi_desig_len %u",
                device->name, device->size, device->sig_len,
                device->scsi_desig_len);
            continue;
        }

        device->role            = SM_DEV_LOCAL;
        shared->device_paths[i] = strdup(device_path);
        snprintf(device->name, sizeof(device->name), "%s", device_path);

        if (strcmp(protocol_name, "io_uring") == 0) {
            protocol_id = EVPL_BLOCK_PROTOCOL_IO_URING;
        } else if (strcmp(protocol_name, "libaio") == 0) {
            protocol_id = EVPL_BLOCK_PROTOCOL_LIBAIO;
        } else if (strcmp(protocol_name, "vfio") == 0) {
            protocol_id = EVPL_BLOCK_PROTOCOL_VFIO;
        } else {
            chimera_diskfs_abort("Unsupported protocol: %s", protocol_name);
        }

        /* For the file-backed backends a missing path is auto-created + sized.
         * A vfio device's "path" is a PCI BDF (e.g. "01:00.0"), not a file:
         * stat'ing it ENOENTs, so skip the create -- otherwise we'd drop a
         * stray zero-length file named after the BDF in the CWD. */
        if (protocol_id != EVPL_BLOCK_PROTOCOL_VFIO) {
            rc = stat(device_path, &st);

            if (rc < 0 && errno == ENOENT) {

                fd = open(device_path, O_CREAT | O_RDWR, 0644);

                chimera_diskfs_abort_if(fd < 0, "Failed to open device %s: %s", device_path, strerror(errno));

                rc = ftruncate(fd, size);

                chimera_diskfs_abort_if(rc < 0, "Failed to truncate device %s: %s", device_path, strerror(errno));

                close(fd);
            }
        }

        device->bdev = evpl_block_open_device(protocol_id, device_path);

        device->size             = evpl_block_size(device->bdev);
        device->max_request_size = evpl_block_max_request_size(device->bdev);

        chimera_diskfs_info("Device %s size %lu max_request_size %lu",
                            device_path, device->size, device->max_request_size);

        if (i == 0) {
            device0_path = strdup(device_path);
        }
    }

    /* Opt-in unsafe async I/O: when set, block writes are submitted without
     * FUA/sync, so diskfs runs lighter at the cost of crash safety.  Off by
     * default; tests that do not exercise crash recovery enable it to run more
     * efficiently. */
    initialize           = json_object_get(cfg, "initialize") != NULL;
    shared->unsafe_async = json_is_true(json_object_get(cfg, "unsafe_async"));
    shared->noatime      = json_is_true(json_object_get(cfg, "noatime"));
    {
        /* Deferred-mtime coalescing window (ms in config); 0 disables it. */
        json_t *mdv = json_object_get(cfg, "mtime_defer_ms");
        shared->mtime_defer_us = mdv ? (uint64_t) json_integer_value(mdv) * 1000 : 1000000;
    }
    /* Opt-in pNFS block / SCSI layout mode: diskfs sources RFC 5663 block or
     * RFC 8154 SCSI layouts and keeps file data on remote (data-only) devices.
     * Both share the same remote-device data path and allocator; they differ
     * only in how the client identifies the disk (content signature vs. SCSI
     * hardware designator) and the encoded layout type. */
    shared->block_layout       = json_is_true(json_object_get(cfg, "block_layout"));
    shared->scsi_layout        = json_is_true(json_object_get(cfg, "scsi_layout"));
    shared->block_cache_blocks = (uint32_t) json_integer_value(
        json_object_get(cfg, "block_cache_blocks"));

    /* Intent-log size (bytes).  Larger pipelines more redo records before the
     * ring laps (throughput on big devices); small test devices need a small
     * log so the AG 0 metadata reservation fits.  A remount overrides this with
     * the value the superblock recorded at format time. */
    {
        json_t *ils = json_object_get(cfg, "intent_log_size");

        shared->intent_log_size = ils ? (uint64_t) json_integer_value(ils) :
            SM_INTENT_LOG_SIZE_DEFAULT;
        if (shared->intent_log_size < SM_INTENT_LOG_SIZE_MIN) {
            shared->intent_log_size = SM_INTENT_LOG_SIZE_MIN;
        }
        shared->intent_log_size = (shared->intent_log_size + SM_BLOCK_MASK) &
            ~(uint64_t) SM_BLOCK_MASK;
    }

    chimera_diskfs_abort_if(shared->block_layout && shared->scsi_layout,
                            "block_layout and scsi_layout are mutually exclusive");

    /* Layout-sourcing mode and orchestrated flex-files are mutually exclusive
     * per module (the NFS server routes on CAP_LAYOUT_SOURCE before CAP_LAYOUT).
     * A diskfs module is loaded once per daemon with one config, so switch the
     * module's advertised layout capability here: a sourcing mode SOURCEs block
     * or SCSI layouts, otherwise we persist the orchestrated flex blob. */
    if (shared->block_layout || shared->scsi_layout) {
        vfs_diskfs.capabilities &= ~(uint64_t) CHIMERA_VFS_CAP_LAYOUT;
        vfs_diskfs.capabilities |= CHIMERA_VFS_CAP_LAYOUT_SOURCE |
            (shared->scsi_layout ? CHIMERA_VFS_CAP_LAYOUT_CLASS_SCSI
                                 : CHIMERA_VFS_CAP_LAYOUT_CLASS_BLOCK);
    }
    shared->inode_cache_inodes = (uint32_t) json_integer_value(
        json_object_get(cfg, "inode_cache_inodes"));
    shared->reclaim_threads = (uint32_t) json_integer_value(
        json_object_get(cfg, "reclaim_threads"));

    json_decref(cfg);


    pthread_mutex_init(&shared->lock, NULL);
    pthread_mutex_init(&shared->gen_lock, NULL);
    diskfs_metrics_init(shared, metrics);

    /* Decide mkfs vs clean-mount vs crash-recovery from the superblock, just as
     * a real filesystem would:
     *   - initialize present -> mkfs, regardless of any existing contents.
     *   - valid + CLEAN      -> previous instance unmounted cleanly: reload
     *                           the persisted free-space map and mount.
     *   - valid + !CLEAN     -> crash: replay the intent log to home, then
     *                           mount the now-consistent image.
     *   - no/garbage         -> fail; callers must opt in to mkfs.
     */
    {
        struct sm_superblock    sb;
        int                     have_sb;
        int                     mode; /* 0 = mkfs, 1 = clean mount, 2 = recover */
        struct diskfs_mount_io *mio  = diskfs_mount_io_open(shared);
        struct sm_io            smio = diskfs_mount_sm_io(mio);

        have_sb = space_map_read_superblock(&smio, &sb) == 0;

        if (initialize) {
            if (have_sb) {
                chimera_diskfs_info(
                    "initialize=true: ignoring existing superblock and formatting fresh");
            }
            mode         = 0;
            shared->fsid = chimera_rand64();
        } else if (have_sb && (sb.flags & SM_SB_CLEAN)) {
            mode         = 1;
            shared->fsid = sb.fsid;
        } else if (have_sb) {
            mode         = 2;
            shared->fsid = sb.fsid;
        } else {
            chimera_diskfs_abort(
                "diskfs superblock missing or invalid; specify initialize to format");
        }

        /* On a remount the intent-log size is whatever was formatted, not the
         * configured value -- the on-disk layout is fixed by it. */
        if (mode != 0) {
            chimera_diskfs_abort_if(
                sb.intent_log_size == 0 || (sb.intent_log_size & SM_BLOCK_MASK),
                "superblock intent_log_size %lu is invalid", sb.intent_log_size);
            shared->intent_log_size = sb.intent_log_size;
        }

        dev_cfg = calloc(shared->num_devices, sizeof(*dev_cfg));
        for (i = 0; i < shared->num_devices; i++) {
            struct diskfs_device *dv = &shared->devices[i];

            dev_cfg[i].size = dv->size;
            dev_cfg[i].role = dv->role;
            if (dv->role == SM_DEV_REMOTE) {
                memcpy(dev_cfg[i].deviceid, dv->deviceid, SM_DEVICEID_SIZE);
                dev_cfg[i].sig_offset = dv->sig_offset;
                dev_cfg[i].sig_len    = dv->sig_len;
                memcpy(dev_cfg[i].sig, dv->sig, dv->sig_len);
            }
        }
        shared->space_map = space_map_create(dev_cfg, shared->num_devices,
                                             shared->intent_log_size);
        free(dev_cfg);

        /* On a persistent remount the relocated-log map is recomputed from the
         * device cfg; it must match what the superblock recorded or the remote
         * AG logs would be read from the wrong offsets. */
        if (mode != 0) {
            chimera_diskfs_abort_if(
                sb.num_remote_devices != shared->space_map->num_remote_devices ||
                sb.remote_log_offset != shared->space_map->remote_log_offset ||
                sb.remote_log_size != shared->space_map->remote_log_size,
                "device configuration changed since last mount (remote-log region "
                "mismatch): refusing to mount to avoid corrupting relocated AG logs");
        }

        if (mode == 2) {
            chimera_diskfs_info("superblock not clean: running crash recovery");
            diskfs_recover_log(shared, mio);
        }

        /* Reload the persisted free-space map.  The allocator is authoritative
         * for future writes; after recovery, mounting without it would let new
         * allocations overlap live metadata/data until namespace-walk rebuild
         * exists. */
        if (mode != 0 &&
            space_map_load(shared->space_map, &smio) != 0) {
            if (mode == 1) {
                chimera_diskfs_abort("space-map reload failed");
            } else {
                chimera_diskfs_abort(
                    "post-recovery space-map reload failed; refusing unsafe mount "
                    "until namespace-walk reconstruction is implemented");
            }
        }

        shared->mounted = (mode != 0);

        /* Seed the generation epoch: a remount continues from the durable
         * floor (>= every generation ever issued); a fresh format starts
         * above the format-created inodes' generation (1).  The dirty
         * superblock write below persists the extended floor. */
        shared->gen_next = (mode != 0 && sb.gen_floor > DISKFS_GEN_FIRST)
            ? sb.gen_floor : DISKFS_GEN_FIRST;
        shared->gen_floor = shared->gen_next + DISKFS_GEN_RESERVE;

        if (mode != 0) {
            uint8_t              fsid_buf[CHIMERA_VFS_FSID_SIZE] = { 0 };
            uint64_t             rinum                           = sb.root_inum ? sb.root_inum : 2;
            uint32_t             rgen                            = sb.root_gen;
            uint32_t             rdev;
            uint64_t             roff;
            struct diskfs_dinode rdi;

            /* The superblock's root generation is only refreshed at clean
             * unmount, so after a crash it can be stale (0).  Read the
             * authoritative generation from the root inode's on-disk dinode
             * (consistent post-replay) so the mount handle matches. */
            roff = sm_inum_to_device_offset(shared->space_map, rinum, &rdev);
            if (diskfs_mount_io_read(mio, rdev, &rdi, sizeof(rdi), roff) == 0 &&
                rdi.inum == rinum) {
                rgen = rdi.gen;
            }

            shared->root_inum = rinum;
            shared->root_gen  = rgen;
            memcpy(fsid_buf, &shared->fsid, sizeof(shared->fsid));
            shared->root_fhlen = chimera_vfs_encode_fh_inum_mount(fsid_buf,
                                                                  rinum,
                                                                  rgen,
                                                                  shared->root_fh);
        }

        /* Fresh format: deallocate every device before writing any metadata,
         * so the filesystem starts on a clean slate.  This drops the drives'
         * stale FTL mappings (resetting GC/wear state) and makes cold-cache
         * behavior reproducible run to run.  Deallocate is only a hint and the
         * post-discard read value is unspecified, but diskfs never relies on
         * device-provided zeros (unwritten extents read as zeros from the
         * b+tree), so a device that ignores it is harmless.  Backends without
         * native discard treat it as a no-op success. */
        if (mode == 0) {
            for (i = 0; i < shared->num_devices; i++) {
                if (!shared->devices[i].bdev) {
                    continue;
                }
                if (diskfs_mount_io_discard(mio, i, 0,
                                            shared->devices[i].size) != 0) {
                    chimera_diskfs_info(
                        "device %d: full-device discard failed or unsupported; "
                        "continuing format", i);
                }
            }
        }

        /* Clear the CLEAN flag for this session: an unclean teardown then
         * leaves it clear, so the next mount won't mistake a crash for a
         * clean shutdown. */
        rc = space_map_write_superblock(shared->space_map, &smio,
                                        shared->fsid, 0,
                                        mode != 0 ? shared->root_inum : 0,
                                        mode != 0 ? shared->root_gen : 0,
                                        mode != 0 ? sb.log_seq : 0,
                                        shared->gen_floor);
        chimera_diskfs_abort_if(rc != 0, "Failed to write superblock");

        /* mkfs: write an initial condensed AG-log base so each slot has a valid
         * header before any runtime delta is journaled -- otherwise a crash
         * right after format would leave the allocator log unreadable. */
        if (mode == 0) {
            rc = space_map_persist(shared->space_map, &smio);
            chimera_diskfs_abort_if(rc != 0, "Failed to persist initial space map");
        }

        diskfs_mount_io_close(mio);
    }
    free(device0_path);

    /* Inode cache: sharded rb-trees keyed by inum, with per-shard LRU eviction
     * of idle inodes (recycle candidates). */
    shared->inode_cache            = calloc(1, sizeof(*shared->inode_cache));
    shared->inode_cache->shard_cap = (shared->inode_cache_inodes ?
                                      shared->inode_cache_inodes :
                                      DISKFS_INODE_CACHE_DEFAULT_INODES) /
        DISKFS_INODE_CACHE_SHARDS;
    if (shared->inode_cache->shard_cap == 0) {
        shared->inode_cache->shard_cap = 1;
    }
    for (i = 0; i < DISKFS_INODE_CACHE_SHARDS; i++) {
        rb_tree_init(&shared->inode_cache->shards[i].inodes);
        pthread_mutex_init(&shared->inode_cache->shards[i].lock, NULL);
    }

    /* Block cache: sharded RCU hash of 4 KiB device blocks. */
    diskfs_block_cache_create(shared);

    /* Initialize KV shards */
    shared->num_kv_shards = 256;
    shared->kv_shards     = calloc(shared->num_kv_shards, sizeof(*shared->kv_shards));

    for (i = 0; i < shared->num_kv_shards; i++) {
        rb_tree_init(&shared->kv_shards[i].entries);
        pthread_mutex_init(&shared->kv_shards[i].lock, NULL);
    }

    /* Bring up the intent log thread.  Spin until its init has registered
     * the wake doorbell, since workers will start ringing it as soon as
     * they begin processing requests. */
    shared->intent_log.ready        = 0;
    shared->intent_log.push_ready   = 0;
    shared->intent_log.shutdown     = 0;
    shared->intent_log.commit_alive = 1;
    shared->intent_log.num_channels = 0;
    shared->intent_log.pending_head = NULL;
    pthread_mutex_init(&shared->intent_log.registration_lock, NULL);

    /* Commit thread first: it allocates the cross-thread hand-off ring the
     * push thread consumes, and opens the intent-log device queue. */
    shared->intent_log.thread = evpl_thread_create(NULL,
                                                   diskfs_intent_log_thread_init,
                                                   diskfs_intent_log_thread_shutdown,
                                                   &shared->intent_log);
    while (!__atomic_load_n(&shared->intent_log.ready, __ATOMIC_ACQUIRE)) {
        /* spin briefly */
    }

    shared->intent_log.push_thread = evpl_thread_create(NULL,
                                                        diskfs_il_push_thread_init,
                                                        diskfs_il_push_thread_shutdown,
                                                        &shared->intent_log);
    while (!__atomic_load_n(&shared->intent_log.push_ready, __ATOMIC_ACQUIRE)) {
        /* spin briefly */
    }

    /* Per-(device, AG) park lists for journaling stalled behind an AG-log
     * condensation. */
    shared->agw = calloc(shared->num_devices, sizeof(*shared->agw));
    for (int d = 0; d < shared->num_devices; d++) {
        uint32_t nags = shared->space_map->devices[d].num_ags;

        shared->agw[d] = calloc(nags, sizeof(**shared->agw));
        for (uint32_t a = 0; a < nags; a++) {
            pthread_mutex_init(&shared->agw[d][a].lock, NULL);
        }
    }

    /* Reclaim workers last: their thread contexts register intent-log
     * channels, so the commit thread must already be up. */
    diskfs_reclaim_create(shared);

    return shared;
} /* diskfs_init */


void
diskfs_bootstrap(struct diskfs_thread *thread)
{
    struct diskfs_shared   *shared = thread->shared;
    struct timespec         now;
    struct diskfs_inode    *inode;
    uint32_t                device_id;
    uint64_t                device_offset, inum;
    int                     rc;
    struct diskfs_mount_io *mio;

    /* Guard against concurrent first-touch from multiple workers. */
    pthread_mutex_lock(&shared->lock);
    if (shared->root_fhlen != 0) {
        pthread_mutex_unlock(&shared->lock);
        return;
    }

    /* Bootstrap writes the root + orphan inode blocks to their home locations
     * synchronously (they must be re-readable from disk before becoming
     * evictable CLEAN blocks).  All device access goes through evpl_block, so
     * drive it with a transient mount-time pump rather than this worker's own
     * event loop (avoids re-entering the dispatch loop mid-request). */
    mio = diskfs_mount_io_open(shared);

    clock_gettime(CLOCK_REALTIME, &now);

    /* The root inode lives at the statically-reserved block_idx 2 of AG 0 /
     * disk 0 (inum 2).  It is reserved (not allocated through the allocator)
     * so it is excluded from every condensed free set -- no alloc delta is
     * needed and it can never be re-handed-out after a crash. */
    inum          = 2;
    device_id     = 0;
    device_offset = sm_inum_to_device_offset(shared->space_map, inum, &device_id);
    (void) rc;

    inode = diskfs_inode_struct_new(inum);

    inode->size       = 4096;
    inode->space_used = 4096;
    inode->uid        = 0;
    inode->gid        = 0;
    inode->nlink      = 2;
    /* World-writable fresh root: with VFS-layer ADD_FILE/ADD_SUBDIRECTORY
     * enforcement a root-owned 0755 root would refuse all creation by non-root
     * clients on this engine-authoritative backend (matches memfs/cairn).
     * Subdirs are still created owned by their creator with 0755. */
    inode->mode       = S_IFDIR | 0777;
    inode->atime_sec  = now.tv_sec;
    inode->atime_nsec = now.tv_nsec;
    inode->mtime_sec  = now.tv_sec;
    inode->mtime_nsec = now.tv_nsec;
    inode->ctime_sec  = now.tv_sec;
    inode->ctime_nsec = now.tv_nsec;
    inode->change++;
    inode->btime_sec      = now.tv_sec;
    inode->btime_nsec     = now.tv_nsec;
    inode->dos_attributes = 0;

    /* Root directory's parent is itself for ".." lookup */
    inode->parent_inum = inode->inum;
    inode->parent_gen  = inode->gen;

    diskfs_inode_cache_insert(shared, inode);

    /* Create the root inode's block: an embedded empty b+tree root plus the
     * dinode.  Bootstrap is not a transaction, so write the block to its home
     * location synchronously -- otherwise it would be CLEAN-but-not-home and
     * eviction could discard it before the first write op logs it (CLEAN
     * blocks must be re-readable from disk).  Then detach it, evictable. */
    inode->block = diskfs_block_claim(thread, device_id, device_offset, 1);
    diskfs_bt_node_init(inode->block->iov.data, DISKFS_BT_ROOT_BASE,
                        DISKFS_BT_ROOT_CAP, 0);
    diskfs_inode_flush(inode);
    rc = diskfs_mount_io_write(mio, device_id, inode->block->iov.data,
                               DISKFS_BLOCK_SIZE, device_offset);
    chimera_diskfs_abort_if(rc != 0, "bootstrap root write failed");
    diskfs_mount_io_flush(mio, device_id);
    inode->block->state = DISKFS_BLOCK_CLEAN;
    diskfs_block_unpin(thread, inode->block, DISKFS_BLOCK_CLEAN);
    inode->block = NULL;

    /* Statically-reserved orphan-list shard inodes (inums 3..): empty
     * directories whose b+tree keys are the inums of deleted-but-not-fully-
     * reclaimed inodes, sharded by deleted inum.  Created at format alongside
     * root (persist; loaded from disk on remount); the reclaim workers scan
     * them on mount and empty them. */
    for (int s = 0; s < DISKFS_ORPHAN_SHARDS; s++) {
        uint64_t             oinum = DISKFS_ORPHAN_INUM_BASE + s;
        uint32_t             odev;
        uint64_t             ooff = sm_inum_to_device_offset(shared->space_map,
                                                             oinum, &odev);
        struct diskfs_inode *oin = diskfs_inode_struct_new(oinum);

        oin->size       = 4096;
        oin->space_used = 4096;
        oin->nlink      = 1;
        oin->mode       = S_IFDIR | 0700;
        oin->atime_sec  = now.tv_sec;
        oin->atime_nsec = now.tv_nsec;
        oin->mtime_sec  = now.tv_sec;
        oin->mtime_nsec = now.tv_nsec;
        oin->ctime_sec  = now.tv_sec;
        oin->ctime_nsec = now.tv_nsec;
        oin->change++;
        oin->btime_sec      = now.tv_sec;
        oin->btime_nsec     = now.tv_nsec;
        oin->dos_attributes = 0;
        oin->parent_inum    = oinum;
        oin->parent_gen     = oin->gen;

        diskfs_inode_cache_insert(shared, oin);

        oin->block = diskfs_block_claim(thread, odev, ooff, 1);
        diskfs_bt_node_init(oin->block->iov.data, DISKFS_BT_ROOT_BASE,
                            DISKFS_BT_ROOT_CAP, 0);
        diskfs_inode_flush(oin);
        rc = diskfs_mount_io_write(mio, odev, oin->block->iov.data,
                                   DISKFS_BLOCK_SIZE, ooff);
        chimera_diskfs_abort_if(rc != 0, "bootstrap orphan write failed");
        diskfs_mount_io_flush(mio, odev);
        oin->block->state = DISKFS_BLOCK_CLEAN;
        diskfs_block_unpin(thread, oin->block, DISKFS_BLOCK_CLEAN);
        oin->block = NULL;
    }

    /* Create 16-byte fsid buffer for root FH encoding (8-byte fsid + 8 bytes padding) */
    {
        uint8_t fsid_buf[CHIMERA_VFS_FSID_SIZE] = { 0 };
        memcpy(fsid_buf, &shared->fsid, sizeof(shared->fsid));
        shared->root_fhlen = chimera_vfs_encode_fh_inum_mount(fsid_buf,
                                                              inode->inum,
                                                              inode->gen,
                                                              shared->root_fh);
    }
    shared->root_inum       = inode->inum;
    shared->root_gen        = inode->gen;
    shared->orphans_scanned = 1;

    diskfs_mount_io_close(mio);

    pthread_mutex_unlock(&shared->lock);
} /* diskfs_bootstrap */


static void
diskfs_inode_cache_release(
    struct rb_node *node,
    void           *private_data)
{
    struct diskfs_inode *inode = container_of(node, struct diskfs_inode, node);

    (void) private_data;

    /* All inode contents live in b+tree blocks freed via the block cache;
     * we only own the inode struct itself and the record mirrors riding it. */
    diskfs_inode_struct_free(inode);
} /* diskfs_inode_cache_release */


void
diskfs_destroy(void *private_data)
{
    struct diskfs_shared *shared = private_data;
    int                   i;

    /* Reclaim workers first: their shutdown finishes the queued drains, which
     * need the inode cache and the intent-log threads still alive. */
    diskfs_reclaim_destroy(shared);

    for (i = 0; i < DISKFS_INODE_CACHE_SHARDS; i++) {
        rb_tree_destroy(&shared->inode_cache->shards[i].inodes,
                        diskfs_inode_cache_release, NULL);
        pthread_mutex_destroy(&shared->inode_cache->shards[i].lock);
    }

    /* Shut down the intent-log threads before tearing down anything they
     * might still touch.  Worker threads have already unregistered their
     * channels via the unregister handshake at this point.  Order matters: the
     * commit thread first (it drains all redo writes and hands every record to
     * the push thread), then the push thread (it flushes every record home and
     * trims the log).  Only then are the shared rings and device-metric arrays
     * safe to free. */
    __atomic_store_n(&shared->intent_log.shutdown, 1, __ATOMIC_RELEASE);
    /* Stop the push thread from ringing the commit thread's wake_doorbell:
     * destroying the commit thread closes that fd, and the push thread (torn
     * down afterwards, to drain what the commit thread handed off) would
     * otherwise abort writing to it. */
    __atomic_store_n(&shared->intent_log.commit_alive, 0, __ATOMIC_RELEASE);
    evpl_thread_destroy(shared->intent_log.thread);
    evpl_thread_destroy(shared->intent_log.push_thread);
    pthread_mutex_destroy(&shared->intent_log.registration_lock);
    free(shared->intent_log.handoff);
    free(shared->intent_log.metrics.block_io_device_ops);
    free(shared->intent_log.metrics.block_io_device_bytes);

    /* Clean unmount: the intent-log thread already drained every logged block
     * to its home location, so persist the free-space map and stamp the
     * superblock CLEAN (with the root + next log seq) so the next mount
     * reloads instead of re-handing-out in-use space.  Driven through the
     * mount-time evpl pump while the devices are still open -- the IL thread
     * (the only other device user) is already gone.  Only mark clean if a root
     * actually exists (an untouched mkfs has nothing to preserve). */
    {
        struct diskfs_mount_io *mio  = diskfs_mount_io_open(shared);
        struct sm_io            smio = diskfs_mount_sm_io(mio);

        if (space_map_persist(shared->space_map, &smio) != 0) {
            chimera_diskfs_error("space-map persist at unmount failed");
        } else if (shared->root_fhlen != 0) {
            /* Clean unmount: persist the exact next generation (no reserve
             * needed -- nothing was issued past gen_next). */
            int rc = space_map_write_superblock(shared->space_map, &smio,
                                                shared->fsid, SM_SB_CLEAN,
                                                shared->root_inum, shared->root_gen,
                                                shared->intent_log.log_seq,
                                                __atomic_load_n(&shared->gen_next,
                                                                __ATOMIC_ACQUIRE));
            if (rc != 0) {
                chimera_diskfs_error("clean-superblock write at unmount failed");
            }
        }
        diskfs_mount_io_close(mio);
    }

    for (int i = 0; i < shared->num_devices; i++) {
        if (shared->devices[i].bdev) {
            evpl_block_close_device(shared->devices[i].bdev);
        }
    }

    diskfs_block_cache_destroy(shared);

    if (shared->agw) {
        for (i = 0; i < shared->num_devices; i++) {
            uint32_t nags = shared->space_map->devices[i].num_ags;

            for (uint32_t a = 0; a < nags; a++) {
                pthread_mutex_destroy(&shared->agw[i][a].lock);
            }
            free(shared->agw[i]);
        }
        free(shared->agw);
    }

    space_map_destroy(shared->space_map);

    for (int i = 0; i < shared->num_devices; i++) {
        free(shared->device_paths[i]);
    }
    free(shared->device_paths);
    free(shared->metrics.block_io_device_ops_series);
    free(shared->metrics.block_io_device_bytes_series);

    pthread_mutex_destroy(&shared->lock);
    free(shared->devices);
    free(shared->inode_cache);

    /* Clean up KV shards */
    for (i = 0; i < shared->num_kv_shards; i++) {
        rb_tree_destroy(&shared->kv_shards[i].entries, diskfs_kv_entry_release, NULL);
        pthread_mutex_destroy(&shared->kv_shards[i].lock);
    }
    free(shared->kv_shards);

    free(shared);
} /* diskfs_destroy */ /* diskfs_destroy */


void *
diskfs_thread_init(
    struct evpl *evpl,
    void        *private_data)
{
    struct diskfs_shared *shared = private_data;
    struct diskfs_thread *thread = calloc(1, sizeof(*thread));


    thread->allocator = slab_allocator_create(4096, 1024 * 1024 * 1024);

    diskfs_block_cache_prealloc(shared, evpl);

    evpl_iovec_alloc(evpl, 4096, 4096, 1, 0, &thread->zero);
    memset(thread->zero.data, 0, 4096);  // Zero buffer must contain zeros!
    evpl_iovec_alloc(evpl, 4096, 4096, 1, 0, &thread->pad);

    thread->shared = shared;
    thread->evpl   = evpl;

    /* Allocate this worker's intent-log channel and register the CQ
     * doorbell on the worker's own evpl. */
    thread->iq_channel         = calloc(1, sizeof(*thread->iq_channel));
    thread->iq_channel->worker = thread;
    thread->commits_inflight   = 0;
    evpl_add_doorbell(evpl, &thread->iq_channel->cq_doorbell,
                      diskfs_iq_cq_doorbell_cb);
    /* Reap intent-log completions every loop iteration (the worker is pinned in
     * poll mode while a commit is outstanding); the doorbell is only a backstop. */
    thread->cq_poll = evpl_add_poll(evpl, NULL, NULL, diskfs_iq_cq_poll,
                                    thread->iq_channel);

    /* Inode lock-grant delivery queue + doorbell.  Register its poll before
     * the block-device queue polls so granted inode waiters are resumed before
     * the worker spends a loop iteration polling every VFIO queue. */
    pthread_mutex_init(&thread->grant_lock, NULL);
    thread->grant_head = NULL;
    thread->grant_tail = NULL;
    __atomic_store_n(&thread->grant_pending, 0, __ATOMIC_RELAXED);
    evpl_add_doorbell(evpl, &thread->grant_doorbell, diskfs_grant_doorbell_cb);
    thread->grant_poll = evpl_add_poll(evpl, NULL, NULL, diskfs_grant_poll, thread);

    thread->queue = calloc(shared->num_devices, sizeof(*thread->queue));

    for (int i = 0; i < shared->num_devices; i++) {
        /* Remote (pNFS data) devices have no local handle: leave queue NULL. */
        thread->queue[i] = shared->devices[i].bdev ?
            evpl_block_open_queue(evpl, shared->devices[i].bdev) : NULL;
    }

    /* B+tree op resume queue: doorbell (cross-thread) + deferral (same-thread). */
    pthread_mutex_init(&thread->resume_lock, NULL);
    thread->resume_head            = NULL;
    thread->resume_tail            = NULL;
    thread->bt_op_free_list        = NULL;
    thread->block_waiter_free_list = NULL;
    __atomic_store_n(&thread->resume_pending, 0, __ATOMIC_RELAXED);
    evpl_add_doorbell(evpl, &thread->resume_doorbell, diskfs_bt_resume_doorbell_cb);
    evpl_deferral_init(&thread->resume_deferral, diskfs_bt_resume_deferral_cb, thread);
    thread->resume_poll = evpl_add_poll(evpl, NULL, NULL, diskfs_bt_resume_poll,
                                        thread);

    pthread_mutex_lock(&shared->lock);
    thread->thread_id = shared->num_active_threads++;
    pthread_mutex_unlock(&shared->lock);
    diskfs_thread_metrics_init(thread);

    /* Deferred-mtime coalescing flusher: scan from this worker's first owned
     * shard, and fire the flush driver every coalescing window. */
    thread->mtime_scan_shard = (uint32_t) thread->thread_id & DISKFS_INODE_CACHE_MASK;
    if (shared->mtime_defer_us > 0) {
        evpl_add_timer(evpl, &thread->mtime_timer, diskfs_mtime_flush_timer_cb,
                       shared->mtime_defer_us);
    }

    /* Hand the channel to the intent log thread via the pending list. */
    pthread_mutex_lock(&shared->intent_log.registration_lock);
    thread->iq_channel->next_pending = shared->intent_log.pending_head;
    shared->intent_log.pending_head  = thread->iq_channel;
    pthread_mutex_unlock(&shared->intent_log.registration_lock);

    /* Publish "registration pending" before the doorbell: the commit thread
     * services this from its per-iteration poll (reg_dirty) when awake, or from
     * the wake doorbell when asleep. */
    __atomic_store_n(&shared->intent_log.reg_dirty, 1, __ATOMIC_SEQ_CST);
    evpl_ring_doorbell(&shared->intent_log.wake_doorbell);

    return thread;
} /* diskfs_thread_init */


void
diskfs_thread_destroy(void *private_data)
{
    struct diskfs_thread *thread = private_data;
    struct diskfs_shared *shared = thread->shared;

    /* Flush every deferred-mtime inode this worker owns so the latest timestamps
     * are durable before teardown (clean unmount => no replay).  The intent-log
     * thread is still alive to complete the flush txns.  Stop the periodic timer
     * and drive the flusher directly, ignoring the age gate. */
    if (shared->mtime_defer_us > 0) {
        evpl_remove_timer(thread->evpl, &thread->mtime_timer);
    }
    thread->mtime_flush_all = 1;
    diskfs_mtime_flush_kick(thread);
    while (thread->mtime_flushing || diskfs_mtime_any_dirty(thread)) {
        diskfs_mtime_flush_kick(thread);
        evpl_continue(thread->evpl);
    }

    /* Quiesce background inode drains first: their transactions reference this
     * thread (and, unlike VFS ops, nothing else waits for them), so they must
     * finish before we tear the thread down.  Pump our event loop until the
     * queue empties; the intent-log thread is still alive to complete the
     * drain transactions we issue. */
    while (thread->draining || thread->drain_head) {
        evpl_continue(thread->evpl);
    }

    /* Drain pending block I/O before closing queues */
    if (thread->pending_io > 0) {
        chimera_diskfs_debug("diskfs_thread_destroy: draining %d pending I/O operations",
                             thread->pending_io);
        while (thread->pending_io > 0) {
            evpl_continue(thread->evpl);
        }
        chimera_diskfs_debug("diskfs_thread_destroy: drain complete");
    }

    /* Wait for every commit this thread handed to the intent log to become
     * durable and be reaped from the CQ.  The IL's retire path walks each
     * txn's pinned blocks and dereferences txn->thread
     * (diskfs_txn_unpin_blocks -> diskfs_block_unpin -> thread->shared), so
     * tearing this thread down with a commit still in flight is a
     * use-after-free on both the thread struct and the iq channel.  The
     * drains above do not cover it: with unsafe_async a VFS request
     * completes before its commit is durable, so the caller can legitimately
     * reach thread teardown with redo writes still queued at the IL (seen as
     * an elbencho-teardown ASan UAF in diskfs_block_unpin).
     * commits_inflight counts every commit from submission (or SQ-full park)
     * until its CQE is reaped, which is strictly after the IL's last touch
     * of the txn and the channel, so zero here means quiescent.
     *
     * Drain the CQ directly rather than pumping the event loop: the IL posts
     * commit CQEs WITHOUT ringing the cq_doorbell (it relies on the pinned
     * worker reaping via the per-iteration poll), and evpl_continue stops
     * running polls once its poll-iteration budget expires without activity
     * and parks in the OS wait -- with no doorbell ever coming, that is a
     * deadlock (seen as mass diskfs test timeouts in CI, where loaded
     * runners reach teardown with a commit still in flight).  drain_cq also
     * resubmits commits parked on the SQ-full FIFO, so parked entries make
     * progress too. */
    while (thread->commits_inflight > 0) {
        if (diskfs_iq_drain_cq(thread->iq_channel) == 0) {
            usleep(100);
        }
    }

    evpl_iovec_release(thread->evpl, &thread->zero);
    evpl_iovec_release(thread->evpl, &thread->pad);

    slab_allocator_destroy(thread->allocator);

    for (int i = 0; i < shared->num_devices; i++) {
        if (!thread->queue[i]) {
            continue;
        }
        evpl_block_close_queue(thread->evpl, thread->queue[i]);
    }

    /* No txn at thread teardown; the unused metadata reservation tail returns to
     * the in-memory free set and is captured by the condense at clean unmount.
     * (File-data reservations live on the inode and are returned on close.) */
    space_map_thread_cache_return(shared->space_map, NULL, &thread->space_cache);

    /* Unregister the intent-log channel.  Caller must have quiesced all
     * in-flight VFS ops on this thread first. */
    if (thread->iq_channel) {
        struct diskfs_iq_channel *ch = thread->iq_channel;

        __atomic_store_n(&ch->unregister_requested, 1, __ATOMIC_RELEASE);
        __atomic_store_n(&shared->intent_log.reg_dirty, 1, __ATOMIC_SEQ_CST);
        evpl_ring_doorbell(&shared->intent_log.wake_doorbell);

        /* Spin (not evpl_continue: with nothing left in flight there is no
         * event to wake the loop, and the IL acks via a plain store with no
         * doorbell).  The commits_inflight drain above guarantees the channel
         * is quiescent, so the IL acks on its next registration sweep. */
        while (!__atomic_load_n(&ch->unregister_done, __ATOMIC_ACQUIRE)) {
            usleep(100);
        }

        evpl_remove_poll(thread->evpl, thread->cq_poll);
        evpl_remove_doorbell(thread->evpl, &ch->cq_doorbell);
        free(ch);
        thread->iq_channel = NULL;
    }

    if (thread->grant_poll) {
        evpl_remove_poll(thread->evpl, thread->grant_poll);
    }
    evpl_remove_doorbell(thread->evpl, &thread->grant_doorbell);
    pthread_mutex_destroy(&thread->grant_lock);

    if (thread->resume_poll) {
        evpl_remove_poll(thread->evpl, thread->resume_poll);
    }
    evpl_remove_doorbell(thread->evpl, &thread->resume_doorbell);
    pthread_mutex_destroy(&thread->resume_lock);

    while (thread->bt_op_free_list) {
        struct diskfs_bt_op *op = thread->bt_op_free_list;
        thread->bt_op_free_list = op->next;
        free(op);
    }

    while (thread->block_waiter_free_list) {
        struct diskfs_block_waiter *w = thread->block_waiter_free_list;
        thread->block_waiter_free_list = w->next;
        free(w);
    }

    while (thread->txn_free_list) {
        struct diskfs_txn *txn = thread->txn_free_list;
        thread->txn_free_list = txn->next;
        free(txn);
    }

    while (thread->waiter_free_list) {
        struct diskfs_inode_waiter *w = thread->waiter_free_list;
        thread->waiter_free_list = w->next;
        free(w);
    }

    free(thread->metrics.block_io_device_ops);
    free(thread->metrics.block_io_device_bytes);
    free(thread->queue);
    free(thread);
} /* diskfs_thread_destroy */
