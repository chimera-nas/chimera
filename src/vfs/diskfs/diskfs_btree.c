// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Per-inode b+tree over 4 KiB slotted nodes: node search/insert/remove and
 * rebalance (split, merge, borrow), iteration, and the async operation
 * driver that suspends on block-cache misses and space-map journal parks
 * and resumes to completion.
 */

#include "diskfs_internal.h"

/* Forward declarations (definitions below, in call-graph order) */

static inline int
diskfs_bt_key_cmp(
    const struct diskfs_bt_key *a,
    const struct diskfs_bt_key *b);

static inline uint32_t
diskfs_bt_leaf_free(
    void    *buf,
    uint32_t base);

static inline uint32_t
diskfs_bt_interior_free(
    void    *buf,
    uint32_t base);

static inline struct diskfs_bt_key
diskfs_bt_node_min_key(
    void    *buf,
    uint32_t base);

static void
diskfs_bt_leaf_append(
    void                       *buf,
    uint32_t                    base,
    const struct diskfs_bt_key *key,
    const void                 *rec,
    uint32_t                    reclen);

static struct diskfs_block *
diskfs_bt_alloc_node(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    uint16_t              level,
    uint64_t             *r_bptr);

static void
diskfs_bt_leaf_split(
    struct diskfs_thread       *thread,
    struct diskfs_txn          *txn,
    void                       *buf,
    uint32_t                    base,
    uint32_t                    cap,
    uint64_t                    self_bptr,
    int                         insert_idx,
    const struct diskfs_bt_key *nkey,
    const void                 *nrec,
    uint32_t                    nreclen,
    struct diskfs_bt_key       *sep_key,
    uint64_t                   *sep_bptr);

static int
diskfs_bt_leaf_insert(
    struct diskfs_thread       *thread,
    struct diskfs_txn          *txn,
    void                       *buf,
    uint32_t                    base,
    uint32_t                    cap,
    uint64_t                    self_bptr,
    const struct diskfs_bt_key *key,
    const void                 *rec,
    uint32_t                    reclen,
    struct diskfs_bt_key       *sep_key,
    uint64_t                   *sep_bptr);

static int
diskfs_bt_interior_insert(
    struct diskfs_thread       *thread,
    struct diskfs_txn          *txn,
    void                       *buf,
    uint32_t                    base,
    uint32_t                    cap,
    const struct diskfs_bt_key *key,
    uint64_t                    child,
    struct diskfs_bt_key       *sep_key,
    uint64_t                   *sep_bptr);

static int
diskfs_bt_insert_rec(
    struct diskfs_thread       *thread,
    struct diskfs_txn          *txn,
    void                       *buf,
    uint32_t                    base,
    uint32_t                    cap,
    uint64_t                    self_bptr,
    const struct diskfs_bt_key *key,
    const void                 *rec,
    uint32_t                    reclen,
    struct diskfs_bt_key       *sep_key,
    uint64_t                   *sep_bptr);

static void
diskfs_bt_insert_locked(
    struct diskfs_thread       *thread,
    struct diskfs_txn          *txn,
    struct diskfs_inode        *inode,
    const struct diskfs_bt_key *key,
    const void                 *rec,
    uint32_t                    reclen);

static void
diskfs_orphan_op_done_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *priv);

static void
diskfs_orphan_op_acquired_cb(
    struct diskfs_inode *orphan_dir,
    int                  status,
    void                *priv);

static inline int
diskfs_bt_op_emit(
    struct diskfs_bt_op *op,
    void                *buf,
    uint32_t             base,
    int                  idx);


/* ------------------------------------------------------------------ */
/* Per-inode b+tree                                                    */
/* ------------------------------------------------------------------ */

static inline int
diskfs_bt_key_cmp(
    const struct diskfs_bt_key *a,
    const struct diskfs_bt_key *b)
{
    if (a->type != b->type) {
        return a->type < b->type ? -1 : 1;
    }
    if (a->subkey != b->subkey) {
        return a->subkey < b->subkey ? -1 : 1;
    }
    return 0;
} /* diskfs_bt_key_cmp */


/* Free bytes available in a leaf for one more (slot + record). */
static inline uint32_t
diskfs_bt_leaf_free(
    void    *buf,
    uint32_t base)
{
    struct diskfs_bt_node_hdr *h          = diskfs_bt_hdr(buf, base);
    uint32_t                   free_start = sizeof(*h) + h->nitems * sizeof(struct diskfs_bt_lslot);

    return h->free_end - free_start;
} /* diskfs_bt_leaf_free */


static inline uint32_t
diskfs_bt_interior_free(
    void    *buf,
    uint32_t base)
{
    struct diskfs_bt_node_hdr *h          = diskfs_bt_hdr(buf, base);
    uint32_t                   free_start = sizeof(*h) + h->nitems * sizeof(struct diskfs_bt_islot);

    return h->capacity - free_start;
} /* diskfs_bt_interior_free */


/*
 * Binary search a leaf for key.  Returns the index of the first slot whose
 * key is >= the search key; sets *exact if that slot's key matches.
 */
int
diskfs_bt_leaf_search(
    void                       *buf,
    uint32_t                    base,
    const struct diskfs_bt_key *key,
    int                        *exact)
{
    struct diskfs_bt_lslot *sl = diskfs_bt_lslots(buf, base);
    int                     n  = diskfs_bt_hdr(buf, base)->nitems;
    int                     lo = 0, hi = n;

    *exact = 0;
    while (lo < hi) {
        int mid = (lo + hi) >> 1;
        int c   = diskfs_bt_key_cmp(&sl[mid].key, key);

        if (c < 0) {
            lo = mid + 1;
        } else if (c > 0) {
            hi = mid;
        } else {
            *exact = 1;
            return mid;
        }
    }
    return lo;
} /* diskfs_bt_leaf_search */


/* Index of the child subtree that may contain key (largest key <= search). */
int
diskfs_bt_interior_search(
    void                       *buf,
    uint32_t                    base,
    const struct diskfs_bt_key *key)
{
    struct diskfs_bt_islot *sl = diskfs_bt_islots(buf, base);
    int                     n  = diskfs_bt_hdr(buf, base)->nitems;
    int                     lo = 0, hi = n, ans = 0;

    while (lo < hi) {
        int mid = (lo + hi) >> 1;

        if (diskfs_bt_key_cmp(&sl[mid].key, key) <= 0) {
            ans = mid;
            lo  = mid + 1;
        } else {
            hi = mid;
        }
    }
    return ans;
} /* diskfs_bt_interior_search */


/*
 * Mount-pump exact b+tree lookup: descend inode's tree from the embedded root
 * in `home` (the freshly-read dinode block image), reading interior/leaf
 * blocks synchronously through the mount-time pump `io` (VFIO-safe).  Returns
 * the record's full length (the copy into out truncated to cap) or -1 when
 * absent.  Mount-time only: runs single-threaded before concurrent load, so
 * the blocking pump reads are fine.
 */
int
diskfs_bt_lookup_pump(
    struct diskfs_shared       *shared,
    struct diskfs_mount_io     *io,
    void                       *home,
    const struct diskfs_bt_key *key,
    void                       *out,
    uint32_t                    cap)
{
    uint8_t  scratch[DISKFS_BLOCK_SIZE];
    void    *buf  = home;
    uint32_t base = DISKFS_BT_ROOT_BASE;

    for (;; ) {
        struct diskfs_bt_node_hdr *h = diskfs_bt_hdr(buf, base);

        if (h->level > 0) {
            int      ci    = diskfs_bt_interior_search(buf, base, key);
            uint64_t child = diskfs_bt_islots(buf, base)[ci].child;
            uint32_t dev;
            uint64_t off = sm_inum_to_device_offset(shared->space_map, child,
                                                    &dev);

            if (diskfs_mount_io_read(io, dev, scratch, sizeof(scratch),
                                     off) != 0) {
                return -1;
            }
            buf  = scratch;
            base = 0;
            continue;
        }

        {
            int                     exact = 0;
            int                     idx   = h->nitems ? diskfs_bt_leaf_search(buf, base, key,
                                                                              &exact) : 0;
            struct diskfs_bt_lslot *sl = diskfs_bt_lslots(buf, base);
            uint32_t                len;

            if (!exact) {
                return -1;
            }
            len = sl[idx].len;
            if (len > cap) {
                len = cap;
            }
            memcpy(out, (char *) buf + base + sl[idx].off, len);
            return (int) sl[idx].len;
        }
    }
} /* diskfs_bt_lookup_pump */


static inline struct diskfs_bt_key
diskfs_bt_node_min_key(
    void    *buf,
    uint32_t base)
{
    struct diskfs_bt_node_hdr *h = diskfs_bt_hdr(buf, base);

    chimera_diskfs_abort_if(h->nitems == 0, "b+tree empty node has no minimum key");
    return h->level == 0 ? diskfs_bt_lslots(buf, base)[0].key :
           diskfs_bt_islots(buf, base)[0].key;
} /* diskfs_bt_node_min_key */


/* Append a leaf record at the end (caller guarantees sorted order + room). */
static void
diskfs_bt_leaf_append(
    void                       *buf,
    uint32_t                    base,
    const struct diskfs_bt_key *key,
    const void                 *rec,
    uint32_t                    reclen)
{
    struct diskfs_bt_node_hdr *h  = diskfs_bt_hdr(buf, base);
    struct diskfs_bt_lslot    *sl = diskfs_bt_lslots(buf, base);

    h->free_end -= reclen;
    memcpy((char *) buf + base + h->free_end, rec, reclen);
    sl[h->nitems].key = *key;
    sl[h->nitems].off = h->free_end;
    sl[h->nitems].len = reclen;
    h->nitems++;
} /* diskfs_bt_leaf_append */


/* Allocate a fresh b+tree node block; returns the (pinned, txn-attached)
 * block and its bptr.  Buffer is zeroed and initialized as an empty node. */
static struct diskfs_block *
diskfs_bt_alloc_node(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    uint16_t              level,
    uint64_t             *r_bptr)
{
    struct diskfs_shared *shared = thread->shared;
    struct diskfs_block  *blk;
    uint32_t              device_id;
    uint64_t              device_offset;
    int                   rc;

    /* Runs inside the synchronous b+tree modify, which pre-reserves enough
     * space (bt_run's RESERVE phase) so this draws from the thread cache and
     * never journals -- hence no_suspend and the rc != 0 abort. */
    DISKFS_SM_JNL(jnl, thread, txn, diskfs_sm_no_suspend, NULL);
    rc = space_map_alloc(shared->space_map, &thread->space_cache, &jnl,
                         SM_DEV_LOCAL, DISKFS_BLOCK_SIZE, SM_RESERVATION_MIN,
                         &device_id, &device_offset);
    chimera_diskfs_abort_if(rc != 0, "b+tree node allocation failed (ENOSPC)");

    blk = diskfs_block_claim(thread, device_id, device_offset, 1);
    diskfs_txn_add_block(txn, blk);

    diskfs_bt_node_init(blk->iov.data, 0, DISKFS_BT_NODE_CAP, level);

    *r_bptr = sm_inum_from_device_offset(shared->space_map, device_id, device_offset);
    return blk;
} /* diskfs_bt_alloc_node */


/* Resolve a child bptr to its block buffer for writing (claim+pin+attach). */
void *
diskfs_bt_node_for_write(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    uint64_t              bptr)
{
    uint32_t             device_id;
    uint64_t             device_offset;
    struct diskfs_block *blk;

    device_offset = sm_inum_to_device_offset(thread->shared->space_map, bptr, &device_id);
    blk           = diskfs_block_claim(thread, device_id, device_offset, 0);
    diskfs_txn_add_block(txn, blk);
    return blk->iov.data;
} /* diskfs_bt_node_for_write */


/*
 * Split a full leaf (current node at buf/base/cap) while inserting
 * (nkey,nrec).  The lower half stays in place; the upper half plus the new
 * right sibling's bptr are returned via *sep_key / *sep_bptr.
 */
static void
diskfs_bt_leaf_split(
    struct diskfs_thread       *thread,
    struct diskfs_txn          *txn,
    void                       *buf,
    uint32_t                    base,
    uint32_t                    cap,
    uint64_t                    self_bptr,
    int                         insert_idx,
    const struct diskfs_bt_key *nkey,
    const void                 *nrec,
    uint32_t                    nreclen,
    struct diskfs_bt_key       *sep_key,
    uint64_t                   *sep_bptr)
{
    struct diskfs_bt_node_hdr *h     = diskfs_bt_hdr(buf, base);
    struct diskfs_bt_lslot    *sl    = diskfs_bt_lslots(buf, base);
    int                        n     = h->nitems;
    int                        total = n + 1;

    struct {
        struct diskfs_bt_key key;
        uint32_t             len;
        uint32_t             scratch_off;
    } *items;
    char                      *scratch;
    uint32_t                   sp = 0, total_bytes = 0, half, acc = 0;
    int                        i, oi, split_i;
    struct diskfs_block       *right;
    void                      *rbuf;
    uint64_t                   old_next, old_prev;

    items   = malloc(total * sizeof(*items));
    scratch = malloc(cap + nreclen);

    for (i = 0, oi = 0; i < total; i++) {
        if (i == insert_idx) {
            memcpy(scratch + sp, nrec, nreclen);
            items[i].key = *nkey;
            items[i].len = nreclen;
        } else {
            memcpy(scratch + sp, (char *) buf + base + sl[oi].off, sl[oi].len);
            items[i].key = sl[oi].key;
            items[i].len = sl[oi].len;
            oi++;
        }
        items[i].scratch_off = sp;
        sp                  += items[i].len;
        total_bytes         += items[i].len;
    }

    half    = total_bytes / 2;
    split_i = 1;
    for (i = 0; i < total; i++) {
        if (acc >= half && i > 0) {
            split_i = i;
            break;
        }
        acc    += items[i].len;
        split_i = i + 1;
    }
    if (split_i < 1) {
        split_i = 1;
    }
    if (split_i > total - 1) {
        split_i = total - 1;
    }

    old_next = h->next_leaf;
    old_prev = h->prev_leaf;

    right = diskfs_bt_alloc_node(thread, txn, 0, sep_bptr);
    rbuf  = right->iov.data;

    /* Rebuild the left node in place from scratch (no aliasing).  node_init
     * clears the leaf links, so they are restored explicitly below. */
    diskfs_bt_node_init(buf, base, cap, 0);
    for (i = 0; i < split_i; i++) {
        diskfs_bt_leaf_append(buf, base, &items[i].key,
                              scratch + items[i].scratch_off, items[i].len);
    }
    for (i = split_i; i < total; i++) {
        diskfs_bt_leaf_append(rbuf, 0, &items[i].key,
                              scratch + items[i].scratch_off, items[i].len);
    }

    /* Splice the new right sibling into the doubly-linked leaf chain:
     *   self <-> right <-> old_next
     * (self keeps its own bptr; for the embedded-root-grow case the caller
     * fixes right->prev_leaf to the new left block afterward.) */
    diskfs_bt_hdr(rbuf, 0)->next_leaf = old_next;
    diskfs_bt_hdr(rbuf, 0)->prev_leaf = self_bptr;
    h->next_leaf                      = *sep_bptr;
    h->prev_leaf                      = old_prev;

    if (old_next) {
        void *nbuf = diskfs_bt_node_for_write(thread, txn, old_next);
        diskfs_bt_hdr(nbuf, 0)->prev_leaf = *sep_bptr;
    }

    *sep_key = items[split_i].key;

    chimera_diskfs_abort_if(diskfs_bt_leaf_free(buf, base) > cap ||
                            diskfs_bt_leaf_free(rbuf, 0) > DISKFS_BT_NODE_CAP,
                            "b+tree leaf split overflow");

    free(items);
    free(scratch);
} /* diskfs_bt_leaf_split */


/* Insert (key,rec) into a leaf, splitting if needed.  Returns 1 on split. */
static int
diskfs_bt_leaf_insert(
    struct diskfs_thread       *thread,
    struct diskfs_txn          *txn,
    void                       *buf,
    uint32_t                    base,
    uint32_t                    cap,
    uint64_t                    self_bptr,
    const struct diskfs_bt_key *key,
    const void                 *rec,
    uint32_t                    reclen,
    struct diskfs_bt_key       *sep_key,
    uint64_t                   *sep_bptr)
{
    struct diskfs_bt_node_hdr *h = diskfs_bt_hdr(buf, base);
    struct diskfs_bt_lslot    *sl;
    int                        idx, exact, j;

    idx = diskfs_bt_leaf_search(buf, base, key, &exact);
    chimera_diskfs_abort_if(exact, "b+tree duplicate key insert");

    if (diskfs_bt_leaf_free(buf, base) >= sizeof(struct diskfs_bt_lslot) + reclen) {
        sl = diskfs_bt_lslots(buf, base);
        for (j = h->nitems; j > idx; j--) {
            sl[j] = sl[j - 1];
        }
        h->free_end -= reclen;
        memcpy((char *) buf + base + h->free_end, rec, reclen);
        sl[idx].key = *key;
        sl[idx].off = h->free_end;
        sl[idx].len = reclen;
        h->nitems++;
        return 0;
    }

    diskfs_bt_leaf_split(thread, txn, buf, base, cap, self_bptr, idx, key, rec,
                         reclen, sep_key, sep_bptr);
    return 1;
} /* diskfs_bt_leaf_insert */


/* Insert (key,child) into an interior node, splitting if needed. */
static int
diskfs_bt_interior_insert(
    struct diskfs_thread       *thread,
    struct diskfs_txn          *txn,
    void                       *buf,
    uint32_t                    base,
    uint32_t                    cap,
    const struct diskfs_bt_key *key,
    uint64_t                    child,
    struct diskfs_bt_key       *sep_key,
    uint64_t                   *sep_bptr)
{
    struct diskfs_bt_node_hdr *h  = diskfs_bt_hdr(buf, base);
    struct diskfs_bt_islot    *sl = diskfs_bt_islots(buf, base);
    int                        n  = h->nitems;
    int                        idx, j, split_i;
    struct diskfs_block       *right;
    struct diskfs_bt_islot    *rsl;
    struct diskfs_bt_node_hdr *rh;

    /* Find sorted insert position (keys are unique separators). */
    idx = 0;
    while (idx < n && diskfs_bt_key_cmp(&sl[idx].key, key) < 0) {
        idx++;
    }

    if (diskfs_bt_interior_free(buf, base) >= sizeof(struct diskfs_bt_islot)) {
        for (j = n; j > idx; j--) {
            sl[j] = sl[j - 1];
        }
        sl[idx].key   = *key;
        sl[idx].child = child;
        h->nitems++;
        return 0;
    }

    /* Split: build the full set of n+1 slots in order, distribute halves. */
    {
        struct diskfs_bt_islot all[ (DISKFS_BT_NODE_CAP / sizeof(struct diskfs_bt_islot)) + 2 ];
        int                    total = n + 1;
        int                    p = 0, ins = 0;

        for (j = 0; j < n; j++) {
            if (!ins && diskfs_bt_key_cmp(key, &sl[j].key) < 0) {
                all[p].key = *key; all[p].child = child; p++; ins = 1;
            }
            all[p++] = sl[j];
        }
        if (!ins) {
            all[p].key = *key; all[p].child = child; p++;
        }

        split_i = total / 2;

        right = diskfs_bt_alloc_node(thread, txn, h->level, sep_bptr);
        rsl   = diskfs_bt_islots(right->iov.data, 0);
        rh    = diskfs_bt_hdr(right->iov.data, 0);

        h->nitems = split_i;
        for (j = 0; j < split_i; j++) {
            sl[j] = all[j];
        }
        for (j = split_i; j < total; j++) {
            rsl[j - split_i] = all[j];
        }
        rh->nitems = total - split_i;

        *sep_key = rsl[0].key;
        return 1;
    }
} /* diskfs_bt_interior_insert */


static int
diskfs_bt_insert_rec(
    struct diskfs_thread       *thread,
    struct diskfs_txn          *txn,
    void                       *buf,
    uint32_t                    base,
    uint32_t                    cap,
    uint64_t                    self_bptr,
    const struct diskfs_bt_key *key,
    const void                 *rec,
    uint32_t                    reclen,
    struct diskfs_bt_key       *sep_key,
    uint64_t                   *sep_bptr)
{
    struct diskfs_bt_node_hdr *h = diskfs_bt_hdr(buf, base);
    struct diskfs_bt_islot    *sl;
    int                        ci, csplit;
    uint64_t                   child_bptr;
    void                      *child_buf;
    struct diskfs_bt_key       csep;
    uint64_t                   cbptr;

    if (h->level == 0) {
        return diskfs_bt_leaf_insert(thread, txn, buf, base, cap, self_bptr, key,
                                     rec, reclen, sep_key, sep_bptr);
    }

    ci         = diskfs_bt_interior_search(buf, base, key);
    sl         = diskfs_bt_islots(buf, base);
    child_bptr = sl[ci].child;
    child_buf  = diskfs_bt_node_for_write(thread, txn, child_bptr);

    csplit = diskfs_bt_insert_rec(thread, txn, child_buf, 0, DISKFS_BT_NODE_CAP,
                                  child_bptr, key, rec, reclen, &csep, &cbptr);
    sl[ci].key = diskfs_bt_node_min_key(child_buf, 0);
    if (!csplit) {
        return 0;
    }

    return diskfs_bt_interior_insert(thread, txn, buf, base, cap, &csep, cbptr,
                                     sep_key, sep_bptr);
} /* diskfs_bt_insert_rec */


/*
 * Insert a record into an inode's b+tree.  The inode must be write-locked and
 * its root block pinned, and the descent path must already be resident (the
 * async driver faults it in first).  Synchronous structural modify.
 */
static void
diskfs_bt_insert_locked(
    struct diskfs_thread       *thread,
    struct diskfs_txn          *txn,
    struct diskfs_inode        *inode,
    const struct diskfs_bt_key *key,
    const void                 *rec,
    uint32_t                    reclen)
{
    void                *root = inode->block->iov.data;
    struct diskfs_bt_key sep;
    uint64_t             sep_bptr;
    int                  split;

    split = diskfs_bt_insert_rec(thread, txn, root, DISKFS_BT_ROOT_BASE,
                                 DISKFS_BT_ROOT_CAP, inode->inum, key, rec, reclen,
                                 &sep, &sep_bptr);
    if (!split) {
        return;
    }

    /* Root overflowed: grow the tree.  Move the (post-split lower-half)
     * root contents into a new left child, then re-form the root as an
     * interior node pointing at the new left child and the split's right
     * sibling. */
    {
        struct diskfs_bt_node_hdr *rh        = diskfs_bt_hdr(root, DISKFS_BT_ROOT_BASE);
        uint16_t                   old_level = rh->level;
        uint64_t                   left_bptr;
        struct diskfs_block       *left;
        struct diskfs_bt_key       left_min;
        struct diskfs_bt_islot    *isl;

        left = diskfs_bt_alloc_node(thread, txn, old_level, &left_bptr);

        /* Copy the entire embedded root node into the new left block. */
        memcpy((char *) left->iov.data, (char *) root + DISKFS_BT_ROOT_BASE, DISKFS_BT_ROOT_CAP);
        diskfs_bt_hdr(left->iov.data, 0)->capacity = DISKFS_BT_NODE_CAP;

        if (old_level == 0) {
            left_min = diskfs_bt_lslots(left->iov.data, 0)[0].key;
        } else {
            left_min = diskfs_bt_islots(left->iov.data, 0)[0].key;
        }

        diskfs_bt_node_init(root, DISKFS_BT_ROOT_BASE, DISKFS_BT_ROOT_CAP,
                            old_level + 1);
        rh           = diskfs_bt_hdr(root, DISKFS_BT_ROOT_BASE);
        rh->nitems   = 2;
        isl          = diskfs_bt_islots(root, DISKFS_BT_ROOT_BASE);
        isl[0].key   = left_min;
        isl[0].child = left_bptr;
        isl[1].key   = sep;
        isl[1].child = sep_bptr;

        if (old_level == 0) {     /* leaf-root grow: fix right sibling back-link */
            /* The lower half migrated from the (now interior) embedded root
             * into the new left block, so the right sibling's back-link must
             * point at the left block rather than the inode's own bptr. */
            void *rbuf = diskfs_bt_node_for_write(thread, txn, sep_bptr);
            diskfs_bt_hdr(rbuf, 0)->prev_leaf = left_bptr;
        }
    }
} /* diskfs_bt_insert_locked */


/* Repack a leaf's live records into a fresh heap, reclaiming the dead space
 * left by prior slot removals.  Leaf-chain links are preserved. */
void
diskfs_bt_leaf_compact(
    void    *buf,
    uint32_t base,
    uint32_t cap)
{
    struct diskfs_bt_node_hdr *h  = diskfs_bt_hdr(buf, base);
    struct diskfs_bt_lslot    *sl = diskfs_bt_lslots(buf, base);
    int                        n    = h->nitems, i;
    uint64_t                   next = h->next_leaf, prev = h->prev_leaf;
    struct diskfs_bt_key      *keys    = malloc(n * sizeof(*keys) + 1);
    uint32_t                  *lens    = malloc(n * sizeof(uint32_t) + 1);
    char                      *scratch = malloc(cap);
    uint32_t                   o       = 0;

    for (i = 0; i < n; i++) {
        keys[i] = sl[i].key;
        lens[i] = sl[i].len;
        memcpy(scratch + o, (char *) buf + base + sl[i].off, sl[i].len);
        o += sl[i].len;
    }

    diskfs_bt_node_init(buf, base, cap, 0);
    o = 0;
    for (i = 0; i < n; i++) {
        diskfs_bt_leaf_append(buf, base, &keys[i], scratch + o, lens[i]);
        o += lens[i];
    }
    h            = diskfs_bt_hdr(buf, base);
    h->next_leaf = next;
    h->prev_leaf = prev;

    free(keys);
    free(lens);
    free(scratch);
} /* diskfs_bt_leaf_compact */


/*
 * Rebalance an underflowing leaf (child index ci of the interior parent at
 * pbuf/pbase) against an adjacent sibling: merge the two leaves if their
 * combined contents fit in one node, otherwise redistribute evenly.  Returns
 * 1 if the merge dropped a slot from the parent (which may now underflow), 0
 * otherwise.  Freed leaf blocks are orphaned (reclaim deferred).
 */
int
diskfs_bt_rebalance_leaf(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    void                 *pbuf,
    uint32_t              pbase,
    int                   ci)
{
    struct diskfs_bt_islot    *psl = diskfs_bt_islots(pbuf, pbase);
    int                        pn  = diskfs_bt_hdr(pbuf, pbase)->nitems;
    int                        lidx, ridx, ln, rn, total, i, merged;
    uint64_t                   l_bptr, r_bptr, l_prev, r_next;
    void                      *lbuf, *rbuf;
    struct diskfs_bt_node_hdr *lh, *rh;
    struct diskfs_bt_key      *keys;
    uint32_t                  *lens;
    char                      *scratch;
    uint32_t                   o, need;

    if (pn < 2) {
        return 0;     /* sole child of a degenerate root: collapse handles it */
    }

    if (ci + 1 < pn) {
        lidx = ci;
        ridx = ci + 1;
    } else {
        lidx = ci - 1;
        ridx = ci;
    }

    l_bptr = psl[lidx].child;
    r_bptr = psl[ridx].child;
    lbuf   = diskfs_bt_node_for_write(thread, txn, l_bptr);
    rbuf   = diskfs_bt_node_for_write(thread, txn, r_bptr);
    lh     = diskfs_bt_hdr(lbuf, 0);
    rh     = diskfs_bt_hdr(rbuf, 0);
    ln     = lh->nitems;
    rn     = rh->nitems;
    total  = ln + rn;
    l_prev = lh->prev_leaf;
    r_next = rh->next_leaf;

    keys    = malloc((total + 1) * sizeof(*keys));
    lens    = malloc((total + 1) * sizeof(uint32_t));
    scratch = malloc(2 * DISKFS_BT_NODE_CAP);

    o = 0;
    for (i = 0; i < ln; i++) {
        struct diskfs_bt_lslot *s = diskfs_bt_lslots(lbuf, 0);
        keys[i] = s[i].key;
        lens[i] = s[i].len;
        memcpy(scratch + o, (char *) lbuf + s[i].off, s[i].len);
        o += s[i].len;
    }
    for (i = 0; i < rn; i++) {
        struct diskfs_bt_lslot *s = diskfs_bt_lslots(rbuf, 0);
        keys[ln + i] = s[i].key;
        lens[ln + i] = s[i].len;
        memcpy(scratch + o, (char *) rbuf + s[i].off, s[i].len);
        o += s[i].len;
    }

    need = sizeof(struct diskfs_bt_node_hdr) +
        total * sizeof(struct diskfs_bt_lslot) + o;

    if (need <= DISKFS_BT_NODE_CAP) {
        /* Merge everything into L; orphan R and unlink it from the chain. */
        uint32_t off = 0;

        diskfs_bt_node_init(lbuf, 0, DISKFS_BT_NODE_CAP, 0);
        for (i = 0; i < total; i++) {
            diskfs_bt_leaf_append(lbuf, 0, &keys[i], scratch + off, lens[i]);
            off += lens[i];
        }
        lh            = diskfs_bt_hdr(lbuf, 0);
        lh->prev_leaf = l_prev;
        lh->next_leaf = r_next;
        if (r_next) {
            void *nn = diskfs_bt_node_for_write(thread, txn, r_next);
            diskfs_bt_hdr(nn, 0)->prev_leaf = l_bptr;
        }

        for (i = ridx; i < pn - 1; i++) {
            psl[i] = psl[i + 1];
        }
        diskfs_bt_hdr(pbuf, pbase)->nitems = pn - 1;
        if (total > 0) {
            psl[lidx].key = diskfs_bt_lslots(lbuf, 0)[0].key;
        }
        merged = 1;

        /* R is merged away: return its node block to the allocator (pending
         * free, applied when this txn commits). */
        {
            uint32_t fdev;
            uint64_t foff = sm_inum_to_device_offset(thread->shared->space_map,
                                                     r_bptr, &fdev);

            diskfs_txn_free_space(thread, txn, fdev, foff, DISKFS_BLOCK_SIZE);
        }
    } else {
        /* Redistribute evenly across L and R. */
        uint32_t half = o / 2, acc = 0, off = 0;
        int      split = 1;

        for (i = 0; i < total; i++) {
            if (acc >= half && i > 0) {
                split = i;
                break;
            }
            acc  += lens[i];
            split = i + 1;
        }
        if (split < 1) {
            split = 1;
        }
        if (split > total - 1) {
            split = total - 1;
        }

        diskfs_bt_node_init(lbuf, 0, DISKFS_BT_NODE_CAP, 0);
        for (i = 0; i < split; i++) {
            diskfs_bt_leaf_append(lbuf, 0, &keys[i], scratch + off, lens[i]);
            off += lens[i];
        }
        diskfs_bt_node_init(rbuf, 0, DISKFS_BT_NODE_CAP, 0);
        for (i = split; i < total; i++) {
            diskfs_bt_leaf_append(rbuf, 0, &keys[i], scratch + off, lens[i]);
            off += lens[i];
        }

        lh            = diskfs_bt_hdr(lbuf, 0);
        rh            = diskfs_bt_hdr(rbuf, 0);
        lh->prev_leaf = l_prev;
        lh->next_leaf = r_bptr;
        rh->prev_leaf = l_bptr;
        rh->next_leaf = r_next;

        psl[lidx].key = diskfs_bt_lslots(lbuf, 0)[0].key;
        psl[ridx].key = diskfs_bt_lslots(rbuf, 0)[0].key;
        merged        = 0;
    }

    free(keys);
    free(lens);
    free(scratch);
    return merged;
} /* diskfs_bt_rebalance_leaf */


/*
 * Rebalance an underflowing interior node (child index ci of parent
 * pbuf/pbase) against a sibling.  B+tree separators are routing copies, so a
 * merge is a plain concatenation of the two children's slots.  Returns 1 if a
 * parent slot was dropped, 0 otherwise.
 */
int
diskfs_bt_rebalance_interior(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    void                 *pbuf,
    uint32_t              pbase,
    int                   ci)
{
    struct diskfs_bt_islot *psl = diskfs_bt_islots(pbuf, pbase);
    int                     pn  = diskfs_bt_hdr(pbuf, pbase)->nitems;
    int                     lidx, ridx, ln, rn, total, i, merged;
    void                   *lbuf, *rbuf;
    struct diskfs_bt_islot  all[(2 * DISKFS_BT_NODE_CAP / sizeof(struct diskfs_bt_islot)) + 2];
    uint32_t                maxi;

    if (pn < 2) {
        return 0;
    }

    if (ci + 1 < pn) {
        lidx = ci;
        ridx = ci + 1;
    } else {
        lidx = ci - 1;
        ridx = ci;
    }

    lbuf  = diskfs_bt_node_for_write(thread, txn, psl[lidx].child);
    rbuf  = diskfs_bt_node_for_write(thread, txn, psl[ridx].child);
    ln    = diskfs_bt_hdr(lbuf, 0)->nitems;
    rn    = diskfs_bt_hdr(rbuf, 0)->nitems;
    total = ln + rn;

    for (i = 0; i < ln; i++) {
        all[i] = diskfs_bt_islots(lbuf, 0)[i];
    }
    for (i = 0; i < rn; i++) {
        all[ln + i] = diskfs_bt_islots(rbuf, 0)[i];
    }

    maxi = (DISKFS_BT_NODE_CAP - sizeof(struct diskfs_bt_node_hdr)) /
        sizeof(struct diskfs_bt_islot);

    if ((uint32_t) total <= maxi) {
        uint64_t r_child = psl[ridx].child;   /* captured before the slot shift */
        uint32_t fdev;
        uint64_t foff = sm_inum_to_device_offset(thread->shared->space_map, r_child, &fdev);

        for (i = 0; i < total; i++) {
            diskfs_bt_islots(lbuf, 0)[i] = all[i];
        }
        diskfs_bt_hdr(lbuf, 0)->nitems = total;

        for (i = ridx; i < pn - 1; i++) {
            psl[i] = psl[i + 1];
        }
        diskfs_bt_hdr(pbuf, pbase)->nitems = pn - 1;
        if (total > 0) {
            psl[lidx].key = diskfs_bt_islots(lbuf, 0)[0].key;
        }
        merged = 1;

        /* R is merged away: pending-free its node block. */
        diskfs_txn_free_space(thread, txn, fdev, foff, DISKFS_BLOCK_SIZE);
    } else {
        int split = total / 2;

        for (i = 0; i < split; i++) {
            diskfs_bt_islots(lbuf, 0)[i] = all[i];
        }
        diskfs_bt_hdr(lbuf, 0)->nitems = split;
        for (i = split; i < total; i++) {
            diskfs_bt_islots(rbuf, 0)[i - split] = all[i];
        }
        diskfs_bt_hdr(rbuf, 0)->nitems = total - split;

        psl[lidx].key = diskfs_bt_islots(lbuf, 0)[0].key;
        psl[ridx].key = diskfs_bt_islots(rbuf, 0)[0].key;
        merged        = 0;
    }

    return merged;
} /* diskfs_bt_rebalance_interior */


/*
 * Collapse the tree when the embedded root interior shrinks to a single
 * child: pull that child up into the embedded root, provided its contents fit
 * in the (smaller) embedded area.  Otherwise keep the degenerate one-child
 * root until later removals make it fit.
 */
void
diskfs_bt_collapse_root(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    struct diskfs_inode  *inode)
{
    void    *root = inode->block->iov.data;
    uint32_t base = DISKFS_BT_ROOT_BASE;

    for (;; ) {
        struct diskfs_bt_node_hdr *rh = diskfs_bt_hdr(root, base);
        uint64_t                   cbptr;
        void                      *cbuf;
        struct diskfs_bt_node_hdr *ch;
        uint32_t                   need;
        int                        i, n;

        if (rh->level == 0 || rh->nitems != 1) {
            break;
        }

        cbptr = diskfs_bt_islots(root, base)[0].child;
        cbuf  = diskfs_bt_node_for_write(thread, txn, cbptr);
        ch    = diskfs_bt_hdr(cbuf, 0);
        n     = ch->nitems;

        if (ch->level == 0) {
            need = sizeof(struct diskfs_bt_node_hdr) +
                n * sizeof(struct diskfs_bt_lslot) +
                (DISKFS_BT_NODE_CAP - ch->free_end);
        } else {
            need = sizeof(struct diskfs_bt_node_hdr) +
                n * sizeof(struct diskfs_bt_islot);
        }

        if (need > DISKFS_BT_ROOT_CAP) {
            break;     /* keep the one-child root */
        }

        if (ch->level == 0) {
            struct diskfs_bt_lslot *cs = diskfs_bt_lslots(cbuf, 0);
            uint64_t                cnext = ch->next_leaf, cprev = ch->prev_leaf;
            struct diskfs_bt_key   *keys    = malloc((n + 1) * sizeof(*keys));
            uint32_t               *lens    = malloc((n + 1) * sizeof(uint32_t));
            char                   *scratch = malloc(DISKFS_BT_NODE_CAP);
            uint32_t                o       = 0;

            for (i = 0; i < n; i++) {
                keys[i] = cs[i].key;
                lens[i] = cs[i].len;
                memcpy(scratch + o, (char *) cbuf + cs[i].off, cs[i].len);
                o += cs[i].len;
            }
            diskfs_bt_node_init(root, base, DISKFS_BT_ROOT_CAP, 0);
            o = 0;
            for (i = 0; i < n; i++) {
                diskfs_bt_leaf_append(root, base, &keys[i], scratch + o, lens[i]);
                o += lens[i];
            }
            rh            = diskfs_bt_hdr(root, base);
            rh->next_leaf = cnext;
            rh->prev_leaf = cprev;
            free(keys);
            free(lens);
            free(scratch);
        } else {
            struct diskfs_bt_islot tmp[(DISKFS_BT_NODE_CAP / sizeof(struct diskfs_bt_islot))];
            uint16_t               clevel = ch->level;

            for (i = 0; i < n; i++) {
                tmp[i] = diskfs_bt_islots(cbuf, 0)[i];
            }
            diskfs_bt_node_init(root, base, DISKFS_BT_ROOT_CAP, clevel);
            for (i = 0; i < n; i++) {
                diskfs_bt_islots(root, base)[i] = tmp[i];
            }
            diskfs_bt_hdr(root, base)->nitems = n;
        }
        /* The child was pulled into the embedded root: pending-free its block. */
        {
            uint32_t fdev;
            uint64_t foff = sm_inum_to_device_offset(thread->shared->space_map,
                                                     cbptr, &fdev);

            diskfs_txn_free_space(thread, txn, fdev, foff, DISKFS_BLOCK_SIZE);
        }
    }
} /* diskfs_bt_collapse_root */


static void
diskfs_orphan_op_done_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *priv)
{
    struct diskfs_orphan_op *o = priv;

    void                     (*done)(
        void *) = o->done;
    void                    *dpriv = o->priv;

    (void) result;
    diskfs_bt_op_free(o->thread, op);
    free(o);
    done(dpriv);
} /* diskfs_orphan_op_done_cb */


static void
diskfs_orphan_op_acquired_cb(
    struct diskfs_inode *orphan_dir,
    int                  status,
    void                *priv)
{
    struct diskfs_orphan_op *o = priv;
    struct diskfs_bt_op     *op;
    struct diskfs_bt_key     key = { .type = DISKFS_REC_ORPHAN, .subkey = o->inum };

    chimera_diskfs_abort_if(status != CHIMERA_VFS_OK,
                            "orphan-list inode acquire failed: %d", status);

    op = diskfs_bt_op_alloc(o->thread);
    if (o->remove) {
        if (diskfs_bt_remove_async(op, o->thread, o->txn, orphan_dir, &key,
                                   diskfs_orphan_op_done_cb, o)) {
            diskfs_orphan_op_done_cb(op, op->result, o);
        }
    } else {
        if (diskfs_bt_insert_async(op, o->thread, o->txn, orphan_dir, &key,
                                   &o->gen, sizeof(o->gen),
                                   diskfs_orphan_op_done_cb, o)) {
            diskfs_orphan_op_done_cb(op, op->result, o);
        }
    }
} /* diskfs_orphan_op_acquired_cb */


void
diskfs_orphan_op_start(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    uint64_t              inum,
    uint32_t              gen,
    int                   remove,
    void (               *done )(void *priv),
    void                 *priv)
{
    struct diskfs_orphan_op *o = malloc(sizeof(*o));

    o->thread = thread;
    o->txn    = txn;
    o->inum   = inum;
    o->gen    = gen;
    o->remove = remove;
    o->done   = done;
    o->priv   = priv;

    diskfs_inode_acquire(thread, txn, DISKFS_ORPHAN_SHARD_INUM(inum),
                         DISKFS_ORPHAN_GEN, DISKFS_INODE_LOCK_WRITE,
                         diskfs_orphan_op_acquired_cb, o);
} /* diskfs_orphan_op_start */


/* ------------------------------------------------------------------ */
/* Async b+tree operation driver                                       */
/* ------------------------------------------------------------------ */

/* Copy a leaf slot's record + key into the op's output; returns true length. */
static inline int
diskfs_bt_op_emit(
    struct diskfs_bt_op *op,
    void                *buf,
    uint32_t             base,
    int                  idx)
{
    struct diskfs_bt_lslot *sl  = diskfs_bt_lslots(buf, base);
    uint32_t                len = sl[idx].len;

    if (op->r_key) {
        *op->r_key = sl[idx].key;
    }
    if (len > op->out_cap) {
        len = op->out_cap;
    }
    if (op->out) {
        memcpy(op->out, (char *) buf + base + sl[idx].off, len);
    }
    return (int) sl[idx].len;
} /* diskfs_bt_op_emit */


/*
 * Drive (or resume) an async b+tree operation.  The traversal suspends and
 * returns whenever a needed node is not resident (diskfs_bt_block_get parks
 * the op on the block's waiter list); it is re-entered here from the resume
 * queue once the block loads.  All per-step state lives in *op so the loop is
 * safe to re-enter from the top of the current phase.
 */
void
diskfs_bt_run(struct diskfs_bt_op *op)
{
    struct diskfs_thread *thread = op->thread;
    struct diskfs_inode  *inode  = op->inode;
    struct diskfs_block  *blk;
    void                 *buf;
    uint32_t              base;
    uint32_t              dev;
    uint64_t              off;

    for (;; ) {
        struct diskfs_bt_node_hdr *h;

        /*
         * Pre-reserve worst-case split space (one new node per tree level)
         * before the descent.  The refill journals and may park on a cold
         * AG-log block -- suspendable here (we re-enter this phase on resume) --
         * so the later synchronous modify's bt_alloc_node calls are guaranteed
         * pure cache draws that never journal.
         */
        if (op->phase == DISKFS_BT_PHASE_RESERVE) {
            /* Worst case a split allocates one node per tree level plus a new
             * root (height+1 <= DISKFS_BT_MAX_DEPTH+1); +2 for margin so the
             * modify can never exhaust the reservation and journal. */
            DISKFS_SM_JNL(jnl, thread, op->txn, diskfs_bt_op_resume_cb, op);
            int rrc = space_map_reserve(thread->shared->space_map,
                                        &thread->space_cache, &jnl, SM_DEV_LOCAL,
                                        (uint64_t) (DISKFS_BT_MAX_DEPTH + 2) * DISKFS_BLOCK_SIZE,
                                        SM_RESERVATION_MIN);

            if (rrc == SM_AGAIN) {
                /* Parked on a cold journal block.  Mark the op suspended so
                 * completion is delivered via the callback even when the rest
                 * of the traversal never parks again: a fully-resident descent
                 * after a reserve-only park would otherwise complete into
                 * `done` with no caller left to read it, dropping the op (and
                 * the request) on the floor. */
                op->suspended = 1;
                return;     /* resumes back into this phase */
            }
            /* ENOSPC here is left to the modify's allocation to surface; just
             * proceed to the descent. */
            op->phase = DISKFS_BT_PHASE_DESCEND;
            continue;
        }

        /*
         * Remove rebalance can touch the immediate siblings of every node on
         * the descent path; fault them all in here, then run the synchronous
         * modify which is guaranteed not to miss.
         */
        if (op->phase == DISKFS_BT_PHASE_REBALANCE) {
            while (op->reb_level < op->depth) {
                struct diskfs_bt_path_ent *pe = &op->path[op->reb_level];
                struct diskfs_bt_node_hdr *ph;
                int                        ci, pn;
                void                      *pbuf;

                off = (pe->bptr == 0)
                ? sm_inum_to_device_offset(thread->shared->space_map, inode->inum, &dev)
                : sm_inum_to_device_offset(thread->shared->space_map, pe->bptr, &dev);
                blk = diskfs_bt_block_get(op, dev, off);
                if (!blk) {
                    return;
                }
                pbuf = blk->iov.data;
                ph   = diskfs_bt_hdr(pbuf, pe->base);
                ci   = pe->ci;
                pn   = ph->nitems;

                if (op->removed_idx == 0) {
                    if (ci - 1 >= 0) {
                        uint64_t sb = diskfs_bt_islots(pbuf, pe->base)[ci - 1].child;
                        off = sm_inum_to_device_offset(thread->shared->space_map, sb, &dev);
                        if (!diskfs_bt_block_get(op, dev, off)) {
                            return;
                        }
                    }
                    op->removed_idx = 1;
                }
                if (op->removed_idx == 1) {
                    if (ci + 1 < pn) {
                        uint64_t sb = diskfs_bt_islots(pbuf, pe->base)[ci + 1].child;
                        off = sm_inum_to_device_offset(thread->shared->space_map, sb, &dev);
                        if (!diskfs_bt_block_get(op, dev, off)) {
                            return;
                        }
                    }
                    op->removed_idx = 2;
                }
                if (op->removed_idx == 2 && op->reb_level == op->depth - 1) {
                    /* Leaf-parent level: a leaf merge rewrites the right
                     * participant's next-neighbour back-link.  That neighbour
                     * is reachable only through the leaf chain (it is not a
                     * child of any node on the descent path), so the merge's
                     * synchronous node_for_write would miss it on a cold cache
                     * (remount).  Fault it in here.  The right participant is
                     * ridx = (ci+1 < pn) ? ci+1 : ci (matching
                     * diskfs_bt_rebalance_leaf) and is already resident (faulted
                     * by the descent or the ci+1 step above). */
                    int                  ridx = (ci + 1 < pn) ? ci + 1 : ci;
                    uint64_t             rb   = diskfs_bt_islots(pbuf, pe->base)[ridx].child;
                    struct diskfs_block *rblk;
                    uint64_t             rnext;

                    off  = sm_inum_to_device_offset(thread->shared->space_map, rb, &dev);
                    rblk = diskfs_bt_block_get(op, dev, off);
                    if (!rblk) {
                        return;
                    }
                    rnext = diskfs_bt_hdr(rblk->iov.data, 0)->next_leaf;
                    if (rnext) {
                        off = sm_inum_to_device_offset(thread->shared->space_map, rnext, &dev);
                        if (!diskfs_bt_block_get(op, dev, off)) {
                            return;
                        }
                    }
                }
                op->reb_level++;
                op->removed_idx = 0;
            }

            diskfs_bt_complete(op, diskfs_bt_remove_locked(thread, op->txn, inode, &op->key));
            return;
        }

        if (op->phase == DISKFS_BT_PHASE_DESCEND && op->use_root) {
            off  = sm_inum_to_device_offset(thread->shared->space_map, inode->inum, &dev);
            base = DISKFS_BT_ROOT_BASE;
        } else {
            off  = sm_inum_to_device_offset(thread->shared->space_map, op->cur_bptr, &dev);
            base = 0;
        }

        blk = diskfs_bt_block_get(op, dev, off);
        if (!blk) {
            return;     /* suspended; resumed when the block loads */
        }
        buf = blk->iov.data;
        h   = diskfs_bt_hdr(buf, base);

        if (op->phase == DISKFS_BT_PHASE_DESCEND) {
            if (h->level > 0) {
                struct diskfs_bt_islot *isl = diskfs_bt_islots(buf, base);
                int                     ci  = diskfs_bt_interior_search(buf, base, &op->key);

                op->last_parent_valid      = 1;
                op->last_parent_ci         = ci;
                op->last_parent_nitems     = h->nitems;
                op->last_parent_key        = isl[ci].key;
                op->last_parent_child      = isl[ci].child;
                op->last_parent_next_child = 0;
                if (ci + 1 < h->nitems) {
                    op->last_parent_next_key   = isl[ci + 1].key;
                    op->last_parent_next_child = isl[ci + 1].child;
                }

                if (op->opcode == DISKFS_BT_OP_INSERT ||
                    op->opcode == DISKFS_BT_OP_REMOVE) {
                    chimera_diskfs_abort_if(op->depth >= DISKFS_BT_MAX_DEPTH,
                                            "b+tree op: path too deep");
                    op->path[op->depth].bptr = op->use_root ? 0 : op->cur_bptr;
                    op->path[op->depth].base = base;
                    op->path[op->depth].ci   = ci;
                    op->depth++;
                }
                op->cur_bptr = isl[ci].child;
                op->use_root = 0;
                continue;
            }

            /* At the leaf. */
            if (op->opcode == DISKFS_BT_OP_INSERT) {
                /* A leaf split rewrites the right sibling's prev_leaf link.
                 * That sibling is off the descent path, so on a cold cache
                 * (remount) the synchronous split's node_for_write would miss
                 * it.  Fault it in first via the async evpl_block path (parks +
                 * resumes into this phase if not resident); a warm cache hits. */
                if (h->next_leaf) {
                    off = sm_inum_to_device_offset(thread->shared->space_map,
                                                   h->next_leaf, &dev);
                    if (!diskfs_bt_block_get(op, dev, off)) {
                        return;
                    }
                }
                diskfs_bt_insert_locked(thread, op->txn, inode, &op->key,
                                        op->rec, op->reclen);
                diskfs_bt_complete(op, 0);
                return;
            } else if (op->opcode == DISKFS_BT_OP_REMOVE) {
                /* Path faulted in; now fault in rebalance siblings. */
                op->phase       = DISKFS_BT_PHASE_REBALANCE;
                op->reb_level   = 0;
                op->removed_idx = 0;
                continue;
            } else if (op->opcode == DISKFS_BT_OP_LOOKUP_EXACT) {
                int exact, idx = diskfs_bt_leaf_search(buf, base, &op->key, &exact);

                if (unlikely(!exact)) {
                    diskfs_bt_complete(op, -1);
                    return;
                }
                diskfs_bt_complete(op, diskfs_bt_op_emit(op, buf, base, idx));
                return;
            } else if (op->opcode == DISKFS_BT_OP_LOOKUP_GE) {
                int exact, idx = h->nitems ? diskfs_bt_leaf_search(buf, base, &op->key, &exact) : 0;

                if (idx < h->nitems) {
                    if (unlikely(diskfs_bt_key_cmp(&diskfs_bt_lslots(buf, base)[idx].key,
                                                   &op->key) < 0)) {
                        chimera_diskfs_error("b+tree lookup_ge routed backwards");
                        diskfs_bt_complete(op, -1);
                        return;
                    }
                    diskfs_bt_complete(op, diskfs_bt_op_emit(op, buf, base, idx));
                    return;
                }
                op->cur_bptr = h->next_leaf;
                op->use_root = 0;
                op->phase    = DISKFS_BT_PHASE_WALK_NEXT;
                if (op->cur_bptr == 0) {
                    diskfs_bt_complete(op, -1);
                    return;
                }
                continue;
            } else {     /* LOOKUP_LE */
                int exact = 0;
                int idx   = h->nitems ? diskfs_bt_leaf_search(buf, base, &op->key, &exact) : 0;
                int fidx  = h->nitems ? (exact ? idx : idx - 1) : -1;

                if (fidx >= 0) {
                    diskfs_bt_complete(op, diskfs_bt_op_emit(op, buf, base, fidx));
                    return;
                }
                op->cur_bptr = h->prev_leaf;
                op->use_root = 0;
                op->phase    = DISKFS_BT_PHASE_WALK_PREV;
                if (op->cur_bptr == 0) {
                    diskfs_bt_complete(op, -1);
                    return;
                }
                continue;
            }
        } else if (op->phase == DISKFS_BT_PHASE_WALK_NEXT) {
            if (h->nitems > 0) {
                if (unlikely(diskfs_bt_key_cmp(&diskfs_bt_lslots(buf, base)[0].key,
                                               &op->key) < 0)) {
                    chimera_diskfs_error("b+tree leaf chain moved backwards during lookup_ge");
                    diskfs_bt_complete(op, -1);
                    return;
                }
                diskfs_bt_complete(op, diskfs_bt_op_emit(op, buf, 0, 0));
                return;
            }
            op->cur_bptr = h->next_leaf;
            if (op->cur_bptr == 0) {
                diskfs_bt_complete(op, -1);
                return;
            }
            continue;
        } else {     /* DISKFS_BT_PHASE_WALK_PREV */
            if (h->nitems > 0) {
                diskfs_bt_complete(op, diskfs_bt_op_emit(op, buf, 0, h->nitems - 1));
                return;
            }
            op->cur_bptr = h->prev_leaf;
            if (op->cur_bptr == 0) {
                diskfs_bt_complete(op, -1);
                return;
            }
            continue;
        }
    }
} /* diskfs_bt_run */


/*
 * Start an async lookup on a caller-owned op.  Returns 1 if it completed
 * synchronously (result in op->result, outputs already written into
 * out/r_key), 0 if it suspended (op->cb will be invoked with the result once
 * the deferring I/O completes).
 */
int
diskfs_bt_lookup_async(
    struct diskfs_bt_op        *op,
    struct diskfs_thread       *thread,
    struct diskfs_inode        *inode,
    enum diskfs_bt_opcode       opcode,
    const struct diskfs_bt_key *key,
    struct diskfs_bt_key       *r_key,
    void                       *out,
    uint32_t                    out_cap,
    diskfs_bt_cb_t              cb,
    void                       *private_data)
{
    memset(op, 0, sizeof(*op));
    op->thread       = thread;
    op->inode        = inode;
    op->opcode       = opcode;
    op->phase        = DISKFS_BT_PHASE_DESCEND;
    op->key          = *key;
    op->r_key        = r_key;
    op->out          = out;
    op->out_cap      = out_cap;
    op->use_root     = 1;
    op->cb           = cb;
    op->private_data = private_data;

    diskfs_bt_run(op);
    return op->done;
} /* diskfs_bt_lookup_async */


int
diskfs_bt_insert_async(
    struct diskfs_bt_op        *op,
    struct diskfs_thread       *thread,
    struct diskfs_txn          *txn,
    struct diskfs_inode        *inode,
    const struct diskfs_bt_key *key,
    const void                 *rec,
    uint32_t                    reclen,
    diskfs_bt_cb_t              cb,
    void                       *private_data)
{
    /* No record can exceed a single node; callers (e.g. the symlink path)
     * reject oversized payloads before reaching here, so this is a true
     * invariant. */
    chimera_diskfs_abort_if(reclen > DISKFS_BT_NODE_CAP, "b+tree record too large");

    memset(op, 0, sizeof(*op));
    op->thread = thread;
    op->txn    = txn;
    op->inode  = inode;
    op->opcode = DISKFS_BT_OP_INSERT;
    /* Reserve worst-case split space before descending, so the synchronous
    * modify's node allocs are pure cache draws (never journal/suspend). */
    op->phase        = DISKFS_BT_PHASE_RESERVE;
    op->key          = *key;
    op->reclen       = reclen;
    op->use_root     = 1;
    op->cb           = cb;
    op->private_data = private_data;
    /* Stage the payload in op-owned storage so it survives suspension; recbuf
     * for the common (small) case, a heap buffer for an oversized one. */
    op->rec = (reclen > sizeof(op->recbuf)) ? malloc(reclen) : op->recbuf;
    memcpy(op->rec, rec, reclen);

    diskfs_bt_run(op);
    return op->done;
} /* diskfs_bt_insert_async */


int
diskfs_bt_remove_async(
    struct diskfs_bt_op        *op,
    struct diskfs_thread       *thread,
    struct diskfs_txn          *txn,
    struct diskfs_inode        *inode,
    const struct diskfs_bt_key *key,
    diskfs_bt_cb_t              cb,
    void                       *private_data)
{
    memset(op, 0, sizeof(*op));
    op->thread       = thread;
    op->txn          = txn;
    op->inode        = inode;
    op->opcode       = DISKFS_BT_OP_REMOVE;
    op->phase        = DISKFS_BT_PHASE_DESCEND;
    op->key          = *key;
    op->use_root     = 1;
    op->cb           = cb;
    op->private_data = private_data;

    diskfs_bt_run(op);
    return op->done;
} /* diskfs_bt_remove_async */
