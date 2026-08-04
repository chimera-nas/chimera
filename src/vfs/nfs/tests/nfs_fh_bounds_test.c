// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Bounds test for the NFS proxy's file-handle re-encoders.
 *
 * Inside src/vfs/nfs/ chimera is the NFS *client*: it dials an upstream server
 * and re-encodes the handle that server returns into a chimera handle.  The
 * length comes off the wire and the generated XDR decoders do not enforce the
 * `<NFS3_FHSIZE>` / `<NFS4_FHSIZE>` bounds their schemas declare, so each
 * re-encoder has to bound it itself:
 *
 *   chimera_nfs3_unmarshall_fh        (nfs_common/nfs3_attr.h)
 *   chimera_nfs4_unmarshall_fh        (vfs/nfs/nfs_internal.h)
 *   chimera_nfs4_readdir_parse_attrs  (vfs/nfs/nfs4_readdir_attr.h)
 *
 * All three build [mount_id : 16][server_index : 1][remote_fh : N] and so
 * accept N up to CHIMERA_NFS_PROXY_REMOTE_FH_MAX (47), the point at which the
 * encoded handle exactly fills CHIMERA_VFS_FH_SIZE.  (The two mount paths,
 * nfs3_mount.c and nfs4_mount.c, open-code the same check against the same
 * ceiling; they need a live upstream to reach, so they are covered by
 * inspection and the pynfs/kvm proxy suites rather than here.)
 *
 * Bands 1-3 drive the two re-encoders, split by symptom rather than by input:
 *
 *   band 1  N <= 47   accepted; encoded handle fits CHIMERA_VFS_FH_SIZE
 *   band 2  48..63    over the ceiling but inside the fragment buffer.  A
 *                     helper that bounds only the fragment (fh_len > FH_SIZE-1)
 *                     accepts these and yields va_fh_len of 65..80.  That still
 *                     fits va_fh -- which carries 16 bytes of XXH3 SIMD padding
 *                     -- so nothing traps; the overrun lands on the caller that
 *                     copies va_fh_len bytes into a bare uint8_t[FH_SIZE].
 *   band 3  >= 64     past the fragment buffer itself: an unchecked memcpy here
 *                     is a stack overflow inside the helper.
 *
 * Band 4 drives the readdir attr parser, which reports "no handle" by leaving
 * *fh_len at 0 instead of by return code, so it is checked separately.  Band 5
 * drives the same parser but checks the other half of its contract: skipping a
 * handle it cannot use must not stop it parsing the rest of that entry.
 *
 * Each case reports rather than asserts, so one run shows every band rather
 * than stopping at the first failure.  Band 3 against an unchecked helper is
 * the exception: it traps inside the memcpy (ASAN in Debug, _FORTIFY_SOURCE in
 * Release) before it can report anything, which is the point.
 */

#include <stdio.h>
#include <string.h>
#include <stdint.h>

#include "nfs_internal.h"
#include "nfs4_readdir_attr.h"
#include "nfs_common/nfs3_attr.h"
#include "nfs_common/nfs_fh_limits.h"

/*
 * The immediate sink is attr->va_fh.  In the daemon the attrs sit inside a
 * pooled chimera_vfs_request and va_fh is its final member, so a write past it
 * lands on the request's callback pointers and private_data; the canary stands
 * in for that.
 */
struct probe {
    struct chimera_vfs_attrs attr;
    uint8_t                  canary[64];
};

/*
 * What consumers of va_fh do next: memcpy va_fh_len bytes into a bare
 * uint8_t[CHIMERA_VFS_FH_SIZE] with no SIMD padding behind it
 * (vfs_proc_lookup.c, vfs_proc_rename.c, vfs_proc_mkdir.c, vfs_proc_mknod.c,
 * vfs_proc_link.c).  This is the buffer band 2 overruns.
 */
struct sink {
    uint8_t fh[CHIMERA_VFS_FH_SIZE];
    uint8_t canary[64];
};

#define CANARY_BYTE 0xAB

static uint8_t remote_fh_data[2048];

/* Backing store for a synthetic one-attribute fattr4 blob (band 4). */
static uint8_t attr_blob[sizeof(uint32_t) + sizeof(remote_fh_data)];

static int     failures;

/*
 * Deliberately opaque to the optimizer: taking void * and staying out of line
 * keeps __builtin_object_size from seeing the destination, so an oversized copy
 * is caught by the canary and reported rather than aborting the run under
 * _FORTIFY_SOURCE.  Real callers pass a visibly-sized array and would abort;
 * here the goal is a full report of every band.
 */
static void __attribute__((noinline))
copy_to_fixed_fh(
    void       *dst,
    const void *src,
    uint32_t    len)
{
    memcpy(dst, src, len);
} /* copy_to_fixed_fh */

static int
canary_intact(
    const uint8_t *canary,
    size_t         len)
{
    for (size_t i = 0; i < len; i++) {
        if (canary[i] != CANARY_BYTE) {
            return 0;
        }
    }
    return 1;
} /* canary_intact */

struct outcome {
    int      accepted;
    uint32_t va_fh_len;
    int      fh_attr_set;
    int      attr_canary_ok;
    int      sink_canary_ok;
};

/*
 * Drive one helper with one remote handle length and collect what happened.
 * `version` selects the helper; both take the same shape of arguments, only
 * the handle type differs (struct nfs_fh3 vs xdr_opaque).
 */
static struct outcome
run_case(
    int      version,
    uint32_t remote_len)
{
    /* A valid parent handle: the 16-byte mount_id is all encode_fh_parent reads. */
    uint8_t        parent_fh[CHIMERA_VFS_FH_SIZE + 16];
    struct probe   probe;
    struct sink    sink;
    struct outcome out;
    int            rc;

    memset(parent_fh, 0x11, sizeof(parent_fh));

    memset(&probe, 0, sizeof(probe));
    memset(probe.canary, CANARY_BYTE, sizeof(probe.canary));

    memset(&sink, 0, sizeof(sink));
    memset(sink.canary, CANARY_BYTE, sizeof(sink.canary));

    if (version == 3) {
        struct nfs_fh3 fh;

        fh.data.len  = remote_len;
        fh.data.data = remote_fh_data;

        rc = chimera_nfs3_unmarshall_fh(&fh, 7 /* server_index */, parent_fh, &probe.attr);
    } else {
        xdr_opaque fh;

        fh.len  = remote_len;
        fh.data = (char *) remote_fh_data;

        rc = chimera_nfs4_unmarshall_fh(&fh, 7 /* server_index */, parent_fh, &probe.attr);
    }

    out.accepted       = (rc == 0);
    out.fh_attr_set    = (probe.attr.va_set_mask & CHIMERA_VFS_ATTR_FH) != 0;
    out.va_fh_len      = out.fh_attr_set ? probe.attr.va_fh_len : 0;
    out.attr_canary_ok = canary_intact(probe.canary, sizeof(probe.canary));

    /* Only an accepted handle reaches a caller, so only then is the downstream
     * fixed-size copy exercised. */
    if (out.fh_attr_set) {
        copy_to_fixed_fh(sink.fh, probe.attr.va_fh, probe.attr.va_fh_len);
    }
    out.sink_canary_ok = canary_intact(sink.canary, sizeof(sink.canary));

    return out;
} /* run_case */

static void
check_case(
    const char *band,
    int         version,
    uint32_t    remote_len,
    int         expect_accept)
{
    struct outcome out;
    const char    *verdict = "ok";

    out = run_case(version, remote_len);

    if (out.accepted != expect_accept) {
        verdict = expect_accept ? "FAIL: rejected a handle that fits"
            : "FAIL: accepted an oversized handle";
        failures++;
    } else if (out.fh_attr_set != expect_accept) {
        /* A rejected handle must leave ATTR_FH unset so the caller fails the
         * operation instead of returning a stale or partial handle. */
        verdict = "FAIL: va_set_mask disagrees with the return code";
        failures++;
    } else if (expect_accept &&
               out.va_fh_len != CHIMERA_VFS_MOUNT_ID_SIZE +
               CHIMERA_NFS_PROXY_FH_SERVER_IDX_SIZE + remote_len) {
        verdict = "FAIL: unexpected encoded length";
        failures++;
    } else if (expect_accept && out.va_fh_len > CHIMERA_VFS_FH_SIZE) {
        verdict = "FAIL: encoded handle exceeds CHIMERA_VFS_FH_SIZE";
        failures++;
    }

    if (!out.attr_canary_ok) {
        verdict = "FAIL: wrote past attr.va_fh (pooled request corrupted)";
        failures++;
    }

    if (!out.sink_canary_ok) {
        verdict = "FAIL: overran a caller's uint8_t[CHIMERA_VFS_FH_SIZE]";
        failures++;
    }

    printf("%-8s v%d remote_len=%-5u accepted=%d va_fh_len=%-3u %s\n",
           band, version, remote_len, out.accepted, out.va_fh_len, verdict);
    fflush(stdout);
} /* check_case */

/*
 * The destination chimera_nfs4_readdir_parse_attrs is contracted to fill:
 * exactly CHIMERA_NFS_PROXY_REMOTE_FH_MAX bytes, no SIMD padding behind it.
 */
struct fh_probe {
    uint8_t fh[CHIMERA_NFS_PROXY_REMOTE_FH_MAX];
    uint8_t canary[64];
};

/*
 * Band 4: chimera_nfs4_readdir_parse_attrs (vfs/nfs/nfs4_readdir_attr.h).
 *
 * Unlike the two re-encoders this one has no return value -- it signals "no
 * handle for this entry" by leaving *fh_len at 0, which its caller tests before
 * building a chimera handle.  So the bound is only as good as that zeroing, and
 * *fh_len is deliberately poisoned here: a parser that skips an oversized
 * handle without writing *fh_len would leave the caller reading a stale length
 * from the previous directory entry and re-emitting the previous entry's handle.
 */
static void
check_readdir_case(
    uint32_t remote_len,
    int      expect_handle)
{
    struct fattr4            fattr;
    struct chimera_vfs_attrs attr;
    struct fh_probe          probe;
    uint32_t                 attrmask[1];
    uint64_t                 fileid;
    uint32_t                 padded;
    int                      fh_len;
    const char              *verdict = "ok";

    memset(&probe, 0, sizeof(probe));
    memset(probe.canary, CANARY_BYTE, sizeof(probe.canary));
    memset(&attr, 0, sizeof(attr));

    /* A one-attribute fattr4 blob: [len : 4][handle][XDR pad to 4]. */
    padded                  = remote_len % 4 ? remote_len + 4 - (remote_len % 4) : remote_len;
    *(uint32_t *) attr_blob = chimera_nfs_hton32(remote_len);
    memcpy(attr_blob + sizeof(uint32_t), remote_fh_data, remote_len);

    attrmask[0]          = 1U << FATTR4_FILEHANDLE;
    fattr.num_attrmask   = 1;
    fattr.attrmask       = attrmask;
    fattr.attr_vals.data = attr_blob;
    fattr.attr_vals.len  = sizeof(uint32_t) + padded;

    fh_len = -1;   /* poison: the parser owns this, on every path */

    chimera_nfs4_readdir_parse_attrs(&fattr, &attr, &fileid, probe.fh, &fh_len);

    if (fh_len < 0) {
        verdict = "FAIL: *fh_len left unwritten (caller reuses a stale handle)";
        failures++;
    } else if (expect_handle && fh_len != (int) remote_len) {
        verdict = "FAIL: dropped a handle that fits";
        failures++;
    } else if (!expect_handle && fh_len != 0) {
        verdict = "FAIL: accepted a handle the caller cannot re-encode";
        failures++;
    } else if (expect_handle &&
               memcmp(probe.fh, remote_fh_data, remote_len) != 0) {
        verdict = "FAIL: handle bytes not copied intact";
        failures++;
    }

    if (!canary_intact(probe.canary, sizeof(probe.canary))) {
        verdict = "FAIL: overran the caller's fh_data buffer";
        failures++;
    }

    printf("%-8s v4 remote_len=%-5u fh_len=%-3d %s\n",
           "band4", remote_len, fh_len, verdict);
    fflush(stdout);
} /* check_readdir_case */

#define BAND5_FILEID 0x0123456789ABCDEFull

/*
 * Band 5: skipping an unusable handle must not cost the entry the attributes
 * that come after it.
 *
 * The parser has two rejections that look alike and are not.  A length running
 * past the attribute blob is malformed -- the next attribute's offset is
 * unknowable, so it has to stop.  A length that merely exceeds what chimera can
 * re-encode is well-formed: the bytes are all there, so the handle can be
 * stepped over and FILEID/MODE/NUMLINKS parsed as usual.  Folding the second
 * into the first (which a `len > NFS4_FHSIZE` test in the malformed check did)
 * silently drops the rest of the entry's attributes for any upstream whose
 * handles run past 128 -- a listing degraded well beyond the missing handle.
 *
 * So: a two-attribute blob, FILEHANDLE then FILEID, and the fileid has to
 * survive whatever happens to the handle.
 */
static void
check_readdir_continues(
    uint32_t remote_len,
    int      expect_handle)
{
    struct fattr4            fattr;
    struct chimera_vfs_attrs attr;
    struct fh_probe          probe;
    uint32_t                 attrmask[1];
    uint64_t                 fileid;
    uint32_t                 padded;
    int                      fh_len;
    const char              *verdict = "ok";

    memset(&probe, 0, sizeof(probe));
    memset(probe.canary, CANARY_BYTE, sizeof(probe.canary));
    memset(&attr, 0, sizeof(attr));

    /* [len : 4][handle][XDR pad to 4][fileid : 8] */
    padded                  = remote_len % 4 ? remote_len + 4 - (remote_len % 4) : remote_len;
    *(uint32_t *) attr_blob = chimera_nfs_hton32(remote_len);
    memcpy(attr_blob + sizeof(uint32_t), remote_fh_data, remote_len);
    *(uint64_t *) (attr_blob + sizeof(uint32_t) + padded) = chimera_nfs_hton64(BAND5_FILEID);

    attrmask[0]          = (1U << FATTR4_FILEHANDLE) | (1U << FATTR4_FILEID);
    fattr.num_attrmask   = 1;
    fattr.attrmask       = attrmask;
    fattr.attr_vals.data = attr_blob;
    fattr.attr_vals.len  = sizeof(uint32_t) + padded + sizeof(uint64_t);

    fh_len = -1;
    fileid = 0;

    chimera_nfs4_readdir_parse_attrs(&fattr, &attr, &fileid, probe.fh, &fh_len);

    if (fh_len < 0) {
        verdict = "FAIL: *fh_len left unwritten";
        failures++;
    } else if (expect_handle != (fh_len != 0)) {
        verdict = expect_handle ? "FAIL: dropped a handle that fits"
            : "FAIL: accepted a handle the caller cannot re-encode";
        failures++;
    } else if (fileid != BAND5_FILEID) {
        verdict = "FAIL: stopped parsing -- entry lost the attributes after its handle";
        failures++;
    } else if (!(attr.va_set_mask & CHIMERA_VFS_ATTR_INUM)) {
        verdict = "FAIL: fileid parsed but ATTR_INUM not set";
        failures++;
    }

    if (!canary_intact(probe.canary, sizeof(probe.canary))) {
        verdict = "FAIL: overran the caller's fh_data buffer";
        failures++;
    }

    printf("%-8s v4 remote_len=%-5u fh_len=%-3d fileid=%s %s\n",
           "band5", remote_len, fh_len,
           fileid == BAND5_FILEID ? "kept" : "LOST", verdict);
    fflush(stdout);
} /* check_readdir_continues */

int
main(void)
{
    /* Band 1: within the ceiling.  47 is the largest remote length whose
     * encoded handle still fits CHIMERA_VFS_FH_SIZE, so it is the boundary. */
    const uint32_t within[] = { 0, 1, 15, 31, 42, CHIMERA_NFS_PROXY_REMOTE_FH_MAX - 1,
                                CHIMERA_NFS_PROXY_REMOTE_FH_MAX };

    /* Band 2: over the ceiling, still inside the fragment buffer.  Legal on
     * the wire (NFS3_FHSIZE is 64) and the band a fragment-only bounds check
     * lets through. */
    const uint32_t over_ceiling[] = { CHIMERA_NFS_PROXY_REMOTE_FH_MAX + 1, 55,
                                      CHIMERA_VFS_FH_SIZE - 1 };

    /* Band 3: past the fragment buffer.  Still legal on the wire for v4, whose
     * NFS4_FHSIZE is 128. */
    const uint32_t over_fragment[] = { CHIMERA_VFS_FH_SIZE, CHIMERA_VFS_FH_SIZE + 1, 128, 1024 };

    memset(remote_fh_data, 0x55, sizeof(remote_fh_data));

    printf("CHIMERA_VFS_FH_SIZE=%d CHIMERA_NFS_PROXY_REMOTE_FH_MAX=%d\n\n",
           CHIMERA_VFS_FH_SIZE, CHIMERA_NFS_PROXY_REMOTE_FH_MAX);

    for (size_t i = 0; i < sizeof(within) / sizeof(within[0]); i++) {
        check_case("band1", 3, within[i], 1);
        check_case("band1", 4, within[i], 1);
    }

    for (size_t i = 0; i < sizeof(over_ceiling) / sizeof(over_ceiling[0]); i++) {
        check_case("band2", 3, over_ceiling[i], 0);
        check_case("band2", 4, over_ceiling[i], 0);
    }

    for (size_t i = 0; i < sizeof(over_fragment) / sizeof(over_fragment[0]); i++) {
        check_case("band3", 3, over_fragment[i], 0);
        check_case("band3", 4, over_fragment[i], 0);
    }

    /* Band 4: the same ceiling as bands 1-3, enforced by the v4 readdir attr
     * parser.  The ceiling (47) is below NFS4_FHSIZE (128), so it subsumes the
     * protocol bound -- past it and past NFS4_FHSIZE are the same skip, and both
     * are checked. */
    for (size_t i = 0; i < sizeof(within) / sizeof(within[0]); i++) {
        check_readdir_case(within[i], 1);
    }

    for (size_t i = 0; i < sizeof(over_ceiling) / sizeof(over_ceiling[0]); i++) {
        check_readdir_case(over_ceiling[i], 0);
    }

    check_readdir_case(CHIMERA_VFS_FH_SIZE, 0);
    check_readdir_case(NFS4_FHSIZE, 0);
    check_readdir_case(NFS4_FHSIZE + 1, 0);
    check_readdir_case(1024, 0);

    /* Band 5: a skipped handle must not take the rest of the entry's attributes
     * with it.  Spans a handle that fits, one over the ceiling but under
     * NFS4_FHSIZE, and two past NFS4_FHSIZE -- the last of which is the case
     * that used to abort the parse. */
    check_readdir_continues(CHIMERA_NFS_PROXY_REMOTE_FH_MAX, 1);
    check_readdir_continues(CHIMERA_NFS_PROXY_REMOTE_FH_MAX + 1, 0);
    check_readdir_continues(CHIMERA_VFS_FH_SIZE, 0);
    check_readdir_continues(NFS4_FHSIZE, 0);
    check_readdir_continues(NFS4_FHSIZE + 1, 0);
    check_readdir_continues(1024, 0);

    if (failures) {
        printf("\nnfs_fh_bounds_test: %d failure(s)\n", failures);
        return 1;
    }

    printf("\nnfs_fh_bounds_test: all bands passed\n");
    return 0;
} /* main */
