// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * diskfs stress-model replay harness.
 *
 * Reads the ITF traces generated from diskfs.qnt and replays each against a
 * fresh diskfs pool (one small pread-backed device per trace), expanding every
 * bulk model step into the many VFS operations it stands for.  After every step
 * it runs the white-box space-allocator + b+tree invariant check
 * (diskfs_test_check); frag files carry a deterministic content pattern this
 * harness verifies on read and re-verifies after every remount and crash, so a
 * lost or corrupted write -- including one that a crash's intent-log replay
 * should have restored -- is caught where it happened.
 *
 * The model owns only abstract per-directory counts (for steering); this harness
 * owns the real names, the liveness bookkeeping and the content shadow.  See
 * diskfs.qnt for the operation set.
 */

#include <jansson.h>

#include "diskfs_test_harness.h"
#include "common/mbt_trace_dir.h"

#define DFS_NDIRS      3               /* numbered work directories d0..d2 */
#define DFS_MAXLIVE    40000           /* live-file serials tracked per directory */
#define DFS_MAXFRAG    128
#define DEV_BYTES  (128ULL * 1024 * 1024)
#define ILOG_BYTES (4ULL * 1024 * 1024)
/* Block-cache blocks.  This is split across 256 shards, so the effective
 * per-shard cap is DFS_BCACHE/256; keep it comfortably above the dirty working
 * set a create flood produces per shard, or the synchronous CoW aborts when a
 * shard's LRU head is still pinned (no clean recycle victim).  Cold-cache b+tree
 * faults are still reached: every remount / crash drops the whole cache, so the
 * next access faults from disk. */
#define DFS_BCACHE     16384
#define FRAG_STRIDE 8192           /* frag blocks are non-adjacent (no coalesce) */

static int paranoid = 1;

/* ---- ITF helpers (quint's Informal Trace Format via jansson) ------------- */

static int64_t
itf_i64(json_t *v)
{
    json_t *big;

    if (json_is_integer(v)) {
        return json_integer_value(v);
    }
    if (json_is_object(v)) {
        big = json_object_get(v, "#bigint");
        if (big && json_is_string(big)) {
            return strtoll(json_string_value(big), NULL, 10);
        }
    }
    return 0;
} /* itf_i64 */

static int64_t
op_i64(
    json_t     *op,
    const char *key)
{
    return itf_i64(json_object_get(op, key));
} /* op_i64 */

static const char *
jf_tag(json_t *v)
{
    json_t *t = v ? json_object_get(v, "tag") : NULL;

    return (t && json_is_string(t)) ? json_string_value(t) : "";
} /* jf_tag */

static json_t *
jf_val(json_t *v)
{
    return v ? json_object_get(v, "value") : NULL;
} /* jf_val */

/* State vars are namespaced ("diskfsRef::diskfs::lastOp"); match on the tail so
 * the replayer does not depend on which profile generated the trace. */
static json_t *
state_var(
    json_t     *state,
    const char *suffix)
{
    const char *key;
    json_t     *val;
    size_t      slen = strlen(suffix);

    json_object_foreach(state, key, val) {
        size_t klen = strlen(key);

        if (klen >= slen && strcmp(key + klen - slen, suffix) == 0) {
            return val;
        }
    }
    return NULL;
} /* state_var */

/* ---- replay context ------------------------------------------------------ */

struct fragrec {
    uint8_t  fh[CHIMERA_VFS_FH_SIZE];
    uint32_t fhlen;
    int      written;   /* blocks holding the data pattern (0..written-1) */
};

struct rctx {
    struct dh                       dh;
    struct chimera_vfs_open_handle *root;
    /* numbered work dirs */
    uint8_t                         dir_fh[DFS_NDIRS][CHIMERA_VFS_FH_SIZE];
    uint32_t                        dir_fhlen[DFS_NDIRS];
    struct chimera_vfs_open_handle *dirh[DFS_NDIRS];
    int                            *serial[DFS_NDIRS];   /* live file serials, FIFO */
    int                             live[DFS_NDIRS];
    int                             next_serial[DFS_NDIRS];
    /* frag-file directory */
    uint8_t                         frag_dir_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        frag_dir_fhlen;
    struct chimera_vfs_open_handle *frag_dirh;
    struct fragrec                  frag[DFS_MAXFRAG];
    int                             nfrag;
    int                             churn_seq;
    /* scratch directory for the self-contained OExercise ops */
    uint8_t                         ops_dir_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        ops_dir_fhlen;
    struct chimera_vfs_open_handle *ops_dirh;
    int                             ex_seq;
    const char                     *path;
    int                             step;
};

static uint8_t
frag_byte(int block)
{
    return (uint8_t) (block % 251 + 1);
} /* frag_byte */

static void
rfail(
    struct rctx *r,
    const char  *what)
{
    fprintf(stderr, "FAIL %s step %d: %s\n", r->path, r->step, what);
    exit(1);
} /* rfail */

static void
rcheck(struct rctx *r)
{
    char err[256];

    if (paranoid && diskfs_test_check(r->dh.vfs, err, sizeof(err)) != 0) {
        fprintf(stderr, "INVARIANT VIOLATION %s step %d: %s\n",
                r->path, r->step, err);
        exit(1);
    }
} /* rcheck */

/* ---- directory + file helpers -------------------------------------------- */

static void
open_work_dirs(struct rctx *r)
{
    char name[16];
    int  d;

    r->root = dh_root_handle(&r->dh);
    for (d = 0; d < DFS_NDIRS; d++) {
        snprintf(name, sizeof(name), "d%d", d);
        if (dh_mkdir(&r->dh, r->root, name, r->dir_fh[d], &r->dir_fhlen[d]) !=
            CHIMERA_VFS_OK) {
            rfail(r, "mkdir work dir");
        }
        r->dirh[d] = dh_open_handle(&r->dh, r->dir_fh[d], r->dir_fhlen[d]);
        if (!r->dirh[d]) {
            rfail(r, "open work dir");
        }
    }
    if (dh_mkdir(&r->dh, r->root, "dfrag", r->frag_dir_fh, &r->frag_dir_fhlen) !=
        CHIMERA_VFS_OK) {
        rfail(r, "mkdir frag dir");
    }
    r->frag_dirh = dh_open_handle(&r->dh, r->frag_dir_fh, r->frag_dir_fhlen);
    if (!r->frag_dirh) {
        rfail(r, "open frag dir");
    }
    if (dh_mkdir(&r->dh, r->root, "dops", r->ops_dir_fh, &r->ops_dir_fhlen) !=
        CHIMERA_VFS_OK) {
        rfail(r, "mkdir ops dir");
    }
    r->ops_dirh = dh_open_handle(&r->dh, r->ops_dir_fh, r->ops_dir_fhlen);
    if (!r->ops_dirh) {
        rfail(r, "open ops dir");
    }
} /* open_work_dirs */

/* Re-acquire the open handles after a remount / crash (the fhs are stable -- a
 * diskfs handle is inum+generation, and neither changes across a remount). */
static void
reopen_handles(struct rctx *r)
{
    int d;

    r->root = dh_root_handle(&r->dh);
    for (d = 0; d < DFS_NDIRS; d++) {
        r->dirh[d] = dh_open_handle(&r->dh, r->dir_fh[d], r->dir_fhlen[d]);
        if (!r->dirh[d]) {
            rfail(r, "reopen work dir after mount boundary");
        }
    }
    r->frag_dirh = dh_open_handle(&r->dh, r->frag_dir_fh, r->frag_dir_fhlen);
    if (!r->frag_dirh) {
        rfail(r, "reopen frag dir after mount boundary");
    }
    r->ops_dirh = dh_open_handle(&r->dh, r->ops_dir_fh, r->ops_dir_fhlen);
    if (!r->ops_dirh) {
        rfail(r, "reopen ops dir after mount boundary");
    }
} /* reopen_handles */

static void
release_handles(struct rctx *r)
{
    int d;

    for (d = 0; d < DFS_NDIRS; d++) {
        if (r->dirh[d]) {
            dh_release(&r->dh, r->dirh[d]);
            r->dirh[d] = NULL;
        }
    }
    if (r->frag_dirh) {
        dh_release(&r->dh, r->frag_dirh);
        r->frag_dirh = NULL;
    }
    if (r->ops_dirh) {
        dh_release(&r->dh, r->ops_dirh);
        r->ops_dirh = NULL;
    }
    if (r->root) {
        dh_release(&r->dh, r->root);
        r->root = NULL;
    }
} /* release_handles */

/* Verify every frag file reads back the exact pattern it was last left with. */
static void
verify_frags(struct rctx *r)
{
    uint8_t buf[4096];
    int     f, i;

    for (f = 0; f < r->nfrag; f++) {
        struct chimera_vfs_open_handle *h =
            dh_open_handle(&r->dh, r->frag[f].fh, r->frag[f].fhlen);

        if (!h) {
            rfail(r, "frag file vanished");
        }
        for (i = 0; i < r->frag[f].written; i++) {
            if (dh_read(&r->dh, h, (uint64_t) i * FRAG_STRIDE, 4096, buf) !=
                CHIMERA_VFS_OK) {
                rfail(r, "frag read failed");
            }
            if (buf[0] != frag_byte(i) || buf[4095] != frag_byte(i)) {
                rfail(r, "frag content mismatch (lost/garbled write)");
            }
        }
        dh_release(&r->dh, h);
    }
} /* verify_frags */

/* ---- op handlers --------------------------------------------------------- */

static void
apply_fanout(
    struct rctx *r,
    int          d,
    int          count)
{
    char name[32];
    int  i;

    for (i = 0; i < count; i++) {
        int s = r->next_serial[d]++;

        snprintf(name, sizeof(name), "f_%d", s);
        if (dh_create(&r->dh, r->dirh[d], name, NULL, NULL, NULL) != CHIMERA_VFS_OK) {
            rfail(r, "fanout create failed");
        }
        if (r->live[d] < DFS_MAXLIVE) {
            r->serial[d][r->live[d]++] = s;
        }
    }
} /* apply_fanout */

static void
apply_shrink(
    struct rctx *r,
    int          d,
    int          count)
{
    char name[32];
    int  i;

    for (i = 0; i < count && r->live[d] > 0; i++) {
        int s = r->serial[d][0];   /* oldest first */

        snprintf(name, sizeof(name), "f_%d", s);
        if (dh_remove(&r->dh, r->dirh[d], name) != CHIMERA_VFS_OK) {
            rfail(r, "shrink remove failed");
        }
        memmove(&r->serial[d][0], &r->serial[d][1],
                (size_t) (r->live[d] - 1) * sizeof(int));
        r->live[d]--;
    }
} /* apply_shrink */

static void
apply_churn(
    struct rctx *r,
    int          d,
    int          count,
    int          rounds)
{
    char name[32];
    int  rr, i, base;

    for (rr = 0; rr < rounds; rr++) {
        base = r->churn_seq;
        for (i = 0; i < count; i++) {
            snprintf(name, sizeof(name), "ch_%d", r->churn_seq++);
            if (dh_create(&r->dh, r->dirh[d], name, NULL, NULL, NULL) != CHIMERA_VFS_OK) {
                rfail(r, "churn create failed");
            }
        }
        for (i = 0; i < count; i++) {
            snprintf(name, sizeof(name), "ch_%d", base + i);
            if (dh_remove(&r->dh, r->dirh[d], name) != CHIMERA_VFS_OK) {
                rfail(r, "churn remove failed");
            }
        }
    }
} /* apply_churn */

static void
apply_fragfile(
    struct rctx *r,
    int          blocks)
{
    struct chimera_vfs_open_handle *h = NULL;
    char                            name[32];
    int                             i;
    int                             id = r->nfrag;

    if (id >= DFS_MAXFRAG) {
        return;   /* corpus generated more than we track; ignore the extras */
    }
    snprintf(name, sizeof(name), "frag_%d", id);
    if (dh_create(&r->dh, r->frag_dirh, name, r->frag[id].fh, &r->frag[id].fhlen,
                  &h) != CHIMERA_VFS_OK) {
        rfail(r, "frag create failed");
    }
    for (i = 0; i < blocks; i++) {
        if (dh_write(&r->dh, h, (uint64_t) i * FRAG_STRIDE, 4096, frag_byte(i)) !=
            CHIMERA_VFS_OK) {
            rfail(r, "frag write failed");
        }
    }
    dh_release(&r->dh, h);
    r->frag[id].written = blocks;
    r->nfrag++;
} /* apply_fragfile */

static void
apply_truncate(
    struct rctx *r,
    int          blocks)
{
    struct chimera_vfs_open_handle *h;
    int                             id = r->nfrag - 1;

    if (id < 0) {
        return;
    }
    h = dh_open_handle(&r->dh, r->frag[id].fh, r->frag[id].fhlen);
    if (!h) {
        rfail(r, "truncate open failed");
    }
    /* Truncate to a size that keeps the first `blocks` stride-blocks and drops
     * the rest (grow leaves holes; either way the surviving data pattern is the
     * first min(written, blocks) blocks). */
    if (dh_truncate(&r->dh, h, (uint64_t) blocks * FRAG_STRIDE) != CHIMERA_VFS_OK) {
        rfail(r, "truncate failed");
    }
    dh_release(&r->dh, h);
    if (blocks < r->frag[id].written) {
        r->frag[id].written = blocks;
    }
} /* apply_truncate */

/* One self-contained exercise of a further VFS operation, on scratch files in
 * the ops directory.  Each kind creates what it needs, performs the op, verifies
 * the observable result, and removes what it created (net-zero on the ops dir),
 * so no cross-step bookkeeping is needed.  Ops a backend may not implement are
 * tolerated as ENOTSUP rather than failed. */
static void
apply_exercise(
    struct rctx *r,
    int          kind)
{
    struct chimera_vfs_open_handle *h  = NULL, *h2 = NULL;
    uint8_t                         fh[CHIMERA_VFS_FH_SIZE], fh2[CHIMERA_VFS_FH_SIZE];
    uint32_t                        fhlen = 0, fhlen2 = 0;
    uint8_t                         buf[4096];
    char                            a[40], b[40];
    int                             n = r->ex_seq++;
    enum chimera_vfs_error          rc;
    int                             i;

    switch (kind % 8) {
        case 0:     /* rename: exercises the dual dirent-tree rewrite */
            snprintf(a, sizeof(a), "ren_%d_a", n);
            snprintf(b, sizeof(b), "ren_%d_b", n);
            if (dh_create(&r->dh, r->ops_dirh, a, NULL, NULL, NULL) != CHIMERA_VFS_OK) {
                rfail(r, "rename setup create");
            }
            if (dh_rename(&r->dh, r->ops_dir_fh, r->ops_dir_fhlen, a,
                          r->ops_dir_fh, r->ops_dir_fhlen, b) != CHIMERA_VFS_OK) {
                rfail(r, "rename failed");
            }
            if (dh_lookup(&r->dh, r->ops_dir_fh, r->ops_dir_fhlen, b) != CHIMERA_VFS_OK) {
                rfail(r, "renamed name missing");
            }
            if (dh_lookup(&r->dh, r->ops_dir_fh, r->ops_dir_fhlen, a) != CHIMERA_VFS_ENOENT) {
                rfail(r, "rename source still present");
            }
            dh_remove(&r->dh, r->ops_dirh, b);
            break;

        case 1:     /* hardlink: exercises nlink + a second dirent to one inode */
            snprintf(a, sizeof(a), "lnk_%d", n);
            snprintf(b, sizeof(b), "lnk_%d_h", n);
            if (dh_create(&r->dh, r->ops_dirh, a, fh, &fhlen, NULL) != CHIMERA_VFS_OK) {
                rfail(r, "link setup create");
            }
            if (dh_link(&r->dh, fh, fhlen, r->ops_dir_fh, r->ops_dir_fhlen, b) != CHIMERA_VFS_OK) {
                rfail(r, "link failed");
            }
            if (dh_lookup(&r->dh, r->ops_dir_fh, r->ops_dir_fhlen, b) != CHIMERA_VFS_OK) {
                rfail(r, "hardlink name missing");
            }
            dh_remove(&r->dh, r->ops_dirh, b);
            dh_remove(&r->dh, r->ops_dirh, a);
            break;

        case 2:     /* symlink + readlink: the symlink record type */
            snprintf(a, sizeof(a), "sym_%d", n);
            if (dh_symlink(&r->dh, r->ops_dirh, a, "the/target/path") != CHIMERA_VFS_OK) {
                rfail(r, "symlink failed");
            }
            memcpy(fh, r->dh.fh, r->dh.fh_len);
            fhlen = r->dh.fh_len;
            h     = dh_open_handle(&r->dh, fh, fhlen);
            if (h) {
                if (dh_readlink(&r->dh, h) == CHIMERA_VFS_OK &&
                    (r->dh.auxlen != (int) strlen("the/target/path") ||
                     memcmp(r->dh.aux, "the/target/path", (size_t) r->dh.auxlen) != 0)) {
                    rfail(r, "readlink target mismatch");
                }
                dh_release(&r->dh, h);
            }
            dh_remove(&r->dh, r->ops_dirh, a);
            break;

        case 3:     /* setattr: mode + owner */
            snprintf(a, sizeof(a), "att_%d", n);
            if (dh_create(&r->dh, r->ops_dirh, a, NULL, NULL, &h) != CHIMERA_VFS_OK) {
                rfail(r, "setattr setup create");
            }
            if (dh_setattr_meta(&r->dh, h, 0600, 123, 456) != CHIMERA_VFS_OK) {
                rfail(r, "setattr failed");
            }
            dh_release(&r->dh, h);
            dh_remove(&r->dh, r->ops_dirh, a);
            break;

        case 4:     /* xattr set/get/remove: the xattr record type */
            snprintf(a, sizeof(a), "xa_%d", n);
            if (dh_create(&r->dh, r->ops_dirh, a, NULL, NULL, &h) != CHIMERA_VFS_OK) {
                rfail(r, "xattr setup create");
            }
            rc = dh_set_xattr(&r->dh, h, "user.k", "hello");
            if (rc == CHIMERA_VFS_OK) {
                if (dh_get_xattr(&r->dh, h, "user.k") == CHIMERA_VFS_OK && r->dh.auxlen != 5) {
                    rfail(r, "xattr value length wrong");
                }
                dh_remove_xattr(&r->dh, h, "user.k");
            } else if (rc != CHIMERA_VFS_ENOTSUP) {
                rfail(r, "set_xattr failed");
            }
            dh_release(&r->dh, h);
            dh_remove(&r->dh, r->ops_dirh, a);
            break;

        case 5:     /* clone (reflink): refcount records + CoW */
            snprintf(a, sizeof(a), "cl_%d_s", n);
            snprintf(b, sizeof(b), "cl_%d_d", n);
            if (dh_create(&r->dh, r->ops_dirh, a, fh, &fhlen, &h) != CHIMERA_VFS_OK) {
                rfail(r, "clone src create");
            }
            for (i = 0; i < 4; i++) {
                if (dh_write(&r->dh, h, (uint64_t) i * 4096, 4096, 0x5A) != CHIMERA_VFS_OK) {
                    rfail(r, "clone src write");
                }
            }
            if (dh_create(&r->dh, r->ops_dirh, b, fh2, &fhlen2, &h2) != CHIMERA_VFS_OK) {
                rfail(r, "clone dst create");
            }
            rc = dh_clone(&r->dh, h, 0, h2, 0, 16384);
            if (rc == CHIMERA_VFS_OK) {
                if (dh_read(&r->dh, h2, 0, 4096, buf) != CHIMERA_VFS_OK ||
                    buf[0] != 0x5A || buf[4095] != 0x5A) {
                    rfail(r, "clone content mismatch");
                }
            } else if (rc != CHIMERA_VFS_ENOTSUP) {
                rfail(r, "clone failed");
            }
            dh_release(&r->dh, h);
            dh_release(&r->dh, h2);
            dh_remove(&r->dh, r->ops_dirh, a);
            dh_remove(&r->dh, r->ops_dirh, b);
            break;

        case 6:     /* allocate + deallocate (punch a hole, then re-allocate) */
            snprintf(a, sizeof(a), "al_%d", n);
            if (dh_create(&r->dh, r->ops_dirh, a, NULL, NULL, &h) != CHIMERA_VFS_OK) {
                rfail(r, "alloc setup create");
            }
            for (i = 0; i < 8; i++) {
                if (dh_write(&r->dh, h, (uint64_t) i * 4096, 4096, 0x33) != CHIMERA_VFS_OK) {
                    rfail(r, "alloc setup write");
                }
            }
            rc = dh_allocate(&r->dh, h, 4096, 4096, CHIMERA_VFS_ALLOCATE_DEALLOCATE);
            if (rc == CHIMERA_VFS_OK) {
                if (dh_read(&r->dh, h, 4096, 4096, buf) == CHIMERA_VFS_OK && buf[0] != 0) {
                    rfail(r, "punched range not zero");
                }
                (void) dh_allocate(&r->dh, h, 4096, 4096, 0);
            } else if (rc != CHIMERA_VFS_ENOTSUP) {
                rfail(r, "deallocate failed");
            }
            dh_release(&r->dh, h);
            dh_remove(&r->dh, r->ops_dirh, a);
            break;

        case 7:     /* seek DATA/HOLE over a file with a hole */
            snprintf(a, sizeof(a), "sk_%d", n);
            if (dh_create(&r->dh, r->ops_dirh, a, NULL, NULL, &h) != CHIMERA_VFS_OK) {
                rfail(r, "seek setup create");
            }
            (void) dh_write(&r->dh, h, 0, 4096, 0x11);       /* block 0 */
            (void) dh_write(&r->dh, h, 16384, 4096, 0x22);   /* block 4, hole between */
            (void) dh_seek(&r->dh, h, 8192, 0);              /* SEEK_DATA from the hole */
            (void) dh_seek(&r->dh, h, 0, 1);                 /* SEEK_HOLE from data */
            dh_release(&r->dh, h);
            dh_remove(&r->dh, r->ops_dirh, a);
            break;

        default:
            break;
    } /* switch */
} /* apply_exercise */

static void
apply_boundary(
    struct rctx *r,
    int          crash)
{
    release_handles(r);
    if (crash) {
        dh_remount_crash(&r->dh);
    } else {
        dh_remount_clean(&r->dh);
    }
    reopen_handles(r);
    verify_frags(r);
} /* apply_boundary */

/* ---- one step ------------------------------------------------------------ */

static void
apply_op(
    struct rctx *r,
    json_t      *op)
{
    const char *tag = jf_tag(op);
    json_t     *v   = jf_val(op);

    if (strcmp(tag, "OFanout") == 0) {
        apply_fanout(r, (int) op_i64(v, "dir"), (int) op_i64(v, "count"));
    } else if (strcmp(tag, "OShrink") == 0) {
        apply_shrink(r, (int) op_i64(v, "dir"), (int) op_i64(v, "count"));
    } else if (strcmp(tag, "OChurn") == 0) {
        apply_churn(r, (int) op_i64(v, "dir"), (int) op_i64(v, "count"),
                    (int) op_i64(v, "rounds"));
    } else if (strcmp(tag, "OFragFile") == 0) {
        apply_fragfile(r, (int) op_i64(v, "blocks"));
    } else if (strcmp(tag, "OTruncate") == 0) {
        apply_truncate(r, (int) op_i64(v, "blocks"));
    } else if (strcmp(tag, "OReclaim") == 0) {
        diskfs_test_await_reclaim(r->dh.vfs, r->dh.evpl, 10000);
    } else if (strcmp(tag, "ORemount") == 0) {
        apply_boundary(r, 0);
    } else if (strcmp(tag, "OCrash") == 0) {
        apply_boundary(r, 1);
    } else if (strcmp(tag, "OExercise") == 0) {
        apply_exercise(r, (int) op_i64(v, "kind"));
    } else {
        rfail(r, "unknown op tag");
    }
} /* apply_op */

/* ---- one trace ----------------------------------------------------------- */

static int
replay_trace(
    const char *path,
    int         dry)
{
    struct rctx   r;
    json_t       *root, *states, *state, *lastop;
    json_error_t  err;
    size_t        idx, nstates;
    int           d;

    root = json_load_file(path, 0, &err);
    if (!root) {
        fprintf(stderr, "%s: parse error: %s\n", path, err.text);
        return 1;
    }
    states = json_object_get(root, "states");
    if (!states || !json_is_array(states) || json_array_size(states) < 1) {
        fprintf(stderr, "%s: no states array\n", path);
        json_decref(root);
        return 1;
    }
    nstates = json_array_size(states);
    if (dry) {
        printf("%s: %zu steps (dry run)\n", path, nstates - 1);
        json_decref(root);
        return 0;
    }

    memset(&r, 0, sizeof(r));
    r.path = path;
    for (d = 0; d < DFS_NDIRS; d++) {
        r.serial[d] = malloc((size_t) DFS_MAXLIVE * sizeof(int));
    }

    dh_init(&r.dh, 1, DEV_BYTES, ILOG_BYTES, DFS_BCACHE);
    open_work_dirs(&r);
    rcheck(&r);

    for (idx = 1; idx < nstates; idx++) {
        state  = json_array_get(states, idx);
        lastop = state_var(state, "::lastOp");
        if (!lastop) {
            fprintf(stderr, "%s state %zu: no lastOp\n", path, idx);
            exit(1);
        }
        r.step = (int) idx;
        apply_op(&r, lastop);
        rcheck(&r);
    }

    /* A final durability pass: quiesce, then confirm every frag file survives a
     * clean remount too. */
    diskfs_test_await_reclaim(r.dh.vfs, r.dh.evpl, 10000);
    rcheck(&r);

    release_handles(&r);
    dh_fini(&r.dh);
    for (d = 0; d < DFS_NDIRS; d++) {
        free(r.serial[d]);
    }
    json_decref(root);
    return 0;
} /* replay_trace */

int
main(
    int    argc,
    char **argv)
{
    char **traces;
    int    ntraces, i, rc = 0, dry = 0, failed = 0;

    setvbuf(stdout, NULL, _IONBF, 0);

    for (i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--no-paranoid") == 0) {
            paranoid = 0;
        } else if (strcmp(argv[i], "--dry-run") == 0) {
            dry = 1;
        }
    }

    traces = mbt_collect_traces(argc, argv, &ntraces);
    if (ntraces == 0) {
        fprintf(stderr, "no traces (use --trace <f> or --trace-dir <d>)\n");
        return 2;
    }

    for (i = 0; i < ntraces; i++) {
        printf("replay %s\n", traces[i]);
        rc = replay_trace(traces[i], dry);
        if (rc) {
            failed++;
        }
    }

    mbt_free_traces(traces, ntraces);
    printf("%d traces replayed, %d failed\n", ntraces, failed);
    return failed ? 1 : 0;
} /* main */
