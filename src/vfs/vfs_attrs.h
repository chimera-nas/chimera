// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once
#include <stdint.h>
#include <time.h>

struct chimera_acl;

/*
 * Maximum chimera VFS file-handle length.  Backends produce much smaller
 * handles (mount_id + a short fragment, typically ~28-36 bytes), but the NFS
 * client/proxy (vfs_nfs) nests a remote server's handle inside its own handle,
 * and the NFS server wraps handles with an export id + signing MAC, so the cap
 * is set above the worst-case nested+wrapped size rather than the bare backend
 * size.  Kept at/below NFS3_FHSIZE (64) so a backend handle still fits an
 * NFSv3 wire handle once wrapped.
 */
#define CHIMERA_VFS_FH_SIZE                 64

#define CHIMERA_VFS_ATTR_DEV                (1UL << 0)
#define CHIMERA_VFS_ATTR_INUM               (1UL << 1)
#define CHIMERA_VFS_ATTR_MODE               (1UL << 2)
#define CHIMERA_VFS_ATTR_NLINK              (1UL << 3)
#define CHIMERA_VFS_ATTR_UID                (1UL << 4)
#define CHIMERA_VFS_ATTR_GID                (1UL << 5)
#define CHIMERA_VFS_ATTR_RDEV               (1UL << 6)
#define CHIMERA_VFS_ATTR_SIZE               (1UL << 7)
#define CHIMERA_VFS_ATTR_ATIME              (1UL << 8)
#define CHIMERA_VFS_ATTR_MTIME              (1UL << 9)
#define CHIMERA_VFS_ATTR_CTIME              (1UL << 10)
#define CHIMERA_VFS_ATTR_SPACE_USED         (1UL << 11)

#define CHIMERA_VFS_ATTR_SPACE_AVAIL        (1UL << 12)
#define CHIMERA_VFS_ATTR_SPACE_FREE         (1UL << 13)
#define CHIMERA_VFS_ATTR_SPACE_TOTAL        (1UL << 14)
#define CHIMERA_VFS_ATTR_FILES_TOTAL        (1UL << 15)
#define CHIMERA_VFS_ATTR_FILES_FREE         (1UL << 16)
#define CHIMERA_VFS_ATTR_FILES_AVAIL        (1UL << 17)

#define CHIMERA_VFS_ATTR_FH                 (1UL << 18)
#define CHIMERA_VFS_ATTR_ATOMIC             (1UL << 19)
#define CHIMERA_VFS_ATTR_FSID               (1UL << 20)

/* Windows/SMB DOS attribute bits (FILE_ATTRIBUTE_*).  Optional: a backend
 * sets this bit in va_set_mask only if it actually persists the value. */
#define CHIMERA_VFS_ATTR_DOS_ATTRIBUTES     (1UL << 21)

/* Birth/creation time (SMB create time, statx btime).  POSIX has no such
 * concept, so this is optional: a backend sets this bit in va_set_mask only
 * if it actually tracks the value. */
#define CHIMERA_VFS_ATTR_BTIME              (1UL << 22)

/* Opaque per-file pNFS layout state (va_pnfs/va_pnfs_len).  The contents are
 * defined and interpreted solely by the NFS server (it packs the data-server
 * deviceid + backing file handle); a backend just persists and returns the
 * blob verbatim, which is all it takes to become a pNFS metadata-server
 * backend.  A backend advertises CHIMERA_VFS_CAP_LAYOUT iff it persists it. */
#define CHIMERA_VFS_ATTR_PNFS_LAYOUT        (1UL << 23)
#define CHIMERA_VFS_PNFS_LAYOUT_MAX         96

/* Canonical (NFSv4/Windows) ACL, carried via va_acl.  Deliberately excluded
 * from the cacheable mask: the attr cache stays fixed-size and ACLs are fetched
 * fresh, mask-gated, only when a protocol asks for them. */
#define CHIMERA_VFS_ATTR_ACL                (1UL << 24)

/* Native change attribute (va_change).  Optional and cacheable, modeled on
 * BTIME: a backend sets this bit in va_set_mask only if it tracks a real
 * monotonic change counter (CHIMERA_VFS_CAP_CHANGE).  Deliberately NOT part of
 * MASK_STAT (which every backend must supply); backends without a native
 * counter simply leave it unset and the NFS server derives change from ctime. */
#define CHIMERA_VFS_ATTR_CHANGE             (1UL << 25)

/* SMB AllocationSize reservation (va_alloc_size): the minimum allocated size a
 * file reserves independent of its EOF, set via a CREATE AllocationSize context
 * or FileAllocationInformation.  Optional: a backend sets this bit in
 * va_set_mask only if it persists the reservation, and folds it into the
 * reported va_space_used (max of real usage and the reservation). */
#define CHIMERA_VFS_ATTR_ALLOC_SIZE         (1UL << 26)

/* SMB/OS-2 EaSize: the combined byte length of the object's user-namespace
 * extended attributes, in the OS/2 FEALIST encoding (see
 * chimera_vfs_xattr_ea_entry_size in vfs_xattr_name.h).  Optional and computed
 * by enumeration only when requested, so it is deliberately NOT in MASK_STAT or
 * MASK_CACHEABLE -- only the SMB server asks for it, and only for the info
 * levels that carry EaSize.  A backend sets the bit in va_set_mask iff it
 * supports xattrs (CHIMERA_VFS_CAP_XATTR). */
#define CHIMERA_VFS_ATTR_EA_SIZE            (1UL << 27)

#define CHIMERA_VFS_ATTR_MASK_STAT          ( \
            CHIMERA_VFS_ATTR_DEV | \
            CHIMERA_VFS_ATTR_INUM | \
            CHIMERA_VFS_ATTR_MODE | \
            CHIMERA_VFS_ATTR_NLINK | \
            CHIMERA_VFS_ATTR_UID | \
            CHIMERA_VFS_ATTR_GID | \
            CHIMERA_VFS_ATTR_RDEV | \
            CHIMERA_VFS_ATTR_SIZE | \
            CHIMERA_VFS_ATTR_SPACE_USED | \
            CHIMERA_VFS_ATTR_ATIME | \
            CHIMERA_VFS_ATTR_MTIME | \
            CHIMERA_VFS_ATTR_CTIME)

#define CHIMERA_VFS_ATTR_MASK_STATFS        ( \
            CHIMERA_VFS_ATTR_SPACE_AVAIL | \
            CHIMERA_VFS_ATTR_SPACE_FREE | \
            CHIMERA_VFS_ATTR_SPACE_TOTAL | \
            CHIMERA_VFS_ATTR_FILES_TOTAL | \
            CHIMERA_VFS_ATTR_FILES_FREE | \
            CHIMERA_VFS_ATTR_FILES_AVAIL | \
            CHIMERA_VFS_ATTR_FSID)

/* The statfs *value* fields only -- the free/available/total space and file
 * counts -- with FSID deliberately excluded.  FSID is dual-purpose: it is part
 * of the statfs response set, but it is also an ordinary per-file attribute
 * that ordinary stat-style requests carry (e.g. NFSv3 post-op attrs include
 * FSID).  A backend that uses "do I need to do full statfs work?" as a gate
 * must test against this mask, NOT against MASK_STATFS: testing MASK_STATFS
 * makes the gate fire for every FSID-carrying request, turning a cold statfs
 * path (which may iterate allocation groups / take per-group locks) into a hot
 * path on every LOOKUP/CREATE/WRITE reply.  FSID alone is satisfied cheaply by
 * each backend's dedicated FSID handler. */
#define CHIMERA_VFS_ATTR_MASK_STATFS_VALUES ( \
            CHIMERA_VFS_ATTR_MASK_STATFS & ~CHIMERA_VFS_ATTR_FSID)

/* Birth time is cacheable and is requested alongside the stat set, but it is
 * deliberately NOT part of MASK_STAT: MASK_STAT is the set every backend is
 * required to supply, and the attr cache's "complete entry" gate and the
 * remove_at hardlink-invalidation both rely on that.  Backends that don't
 * track btime (linux/io_uring/cairn) must still satisfy MASK_STAT.
 *
 * FSID is included so the attr cache can satisfy a GETATTR (the hottest op):
 * NFSv3 GETATTR carries FSID in its requested mask, so without FSID here the
 * cache-consult gate (req_mask subset of {FH|CACHEABLE}) never fired and every
 * GETATTR fell through to the backend.  FSID is per-filehandle (bound to one
 * inode on one filesystem), so it is safe to cache; every backend fills it
 * cheaply (native: a constant fsid; linux/io_uring: a per-mount fsid).  The
 * cache's complete-entry gate (vfs_attr_cache.h) requires FSID for exactly this
 * reason -- an entry that lacks it must not be served to an FSID request. */
#define CHIMERA_VFS_ATTR_MASK_CACHEABLE     ( \
            CHIMERA_VFS_ATTR_MASK_STAT | \
            CHIMERA_VFS_ATTR_BTIME | \
            CHIMERA_VFS_ATTR_CHANGE | \
            CHIMERA_VFS_ATTR_FSID)

#define CHIMERA_VFS_TIME_NOW                ((1l << 30) - 3l)

/* Sentinel: "preserve the existing value, don't bump implicitly".  Used by
 * SMB SetInfo(FileBasicInformation) to convey MS-FSCC 2.4.7's "a zero
 * timestamp means don't change" semantics: the SMB layer must always carry
 * the time-attr bit in set_mask (so the implicit ctime bump that would
 * otherwise fire on the co-present DOS-attribute change is suppressed), but
 * a value of TIME_OMIT means the backend should leave that timestamp alone. */
#define CHIMERA_VFS_TIME_OMIT               ((1l << 30) - 4l)

/*
 * Resolve a settable timestamp against the TIME_NOW / TIME_OMIT sentinels that
 * the SMB and NFS layers carry in a settable atime/mtime/btime/ctime field.
 * Writes the resolved value to *out and returns 1 when the field should be
 * stored (TIME_NOW -> the pre-sampled `now`, any concrete value -> itself), or
 * returns 0 without touching *out when the caller asked to preserve the
 * existing value (TIME_OMIT).  Leaving *out untouched on the 0 return lets a
 * caller pass a pointer to the stored field directly and have TIME_OMIT be a
 * no-op.  This is the single source of truth for that three-way decision; the
 * native-storage backends (memfs/cairn/diskfs) all route through it.
 */
static inline int
chimera_vfs_resolve_set_time(
    const struct timespec *in,
    const struct timespec *now,
    struct timespec       *out)
{
    if (in->tv_nsec == CHIMERA_VFS_TIME_NOW) {
        *out = *now;
        return 1;
    } else if (in->tv_nsec != CHIMERA_VFS_TIME_OMIT) {
        *out = *in;
        return 1;
    }

    return 0;
} /* chimera_vfs_resolve_set_time */

struct chimera_vfs_attrs {
    uint64_t            va_req_mask;
    uint64_t            va_set_mask;

    uint64_t            va_dev;
    uint64_t            va_ino;
    uint64_t            va_mode;
    uint64_t            va_nlink;
    uint64_t            va_uid;
    uint64_t            va_gid;
    uint64_t            va_rdev;
    uint64_t            va_size;
    uint64_t            va_space_used;
    uint64_t            va_alloc_size;
    struct timespec     va_atime;
    struct timespec     va_mtime;
    struct timespec     va_ctime;
    struct timespec     va_btime;

    /* Native change attribute (CHIMERA_VFS_ATTR_CHANGE): a monotonically
     * increasing per-object version counter.  Optional: a backend sets this bit
     * in va_set_mask only if it supplies a real counter (CHIMERA_VFS_CAP_CHANGE),
     * letting the NFS server report change_attr_type MONOTONIC_INCR instead of
     * falling back to a ctime-derived change value. */
    uint64_t            va_change;

    /* SMB/OS-2 EaSize (CHIMERA_VFS_ATTR_EA_SIZE): combined OS/2-FEALIST byte
     * length of this object's user-namespace xattrs.  Optional; filled only when
     * the bit is requested. */
    uint64_t            va_ea_size;

    uint64_t            va_fs_space_avail;
    uint64_t            va_fs_space_free;
    uint64_t            va_fs_space_total;
    uint64_t            va_fs_space_used;
    uint64_t            va_fs_files_total;
    uint64_t            va_fs_files_free;
    uint64_t            va_fs_files_avail;
    uint64_t            va_fsid;

    uint32_t            va_dos_attributes;

    /* Canonical ACL (CHIMERA_VFS_ATTR_ACL).  On getattr, the backend points
     * this at storage valid only for the duration of the completion callback
     * (same contract as va_fh).  On setattr, the caller owns the buffer. */
    struct chimera_acl *va_acl;

    /* Opaque pNFS layout state, owned by the NFS server (see
     * CHIMERA_VFS_ATTR_PNFS_LAYOUT). */
    uint32_t            va_pnfs_len;
    uint8_t             va_pnfs[CHIMERA_VFS_PNFS_LAYOUT_MAX];

    uint32_t            va_fh_len;
    uint64_t            va_fh_hash;

    /* XXH3 uses SIMD memory loads that may read beyond the end
     * of the actual data, so we need to provide enough padding
     * to prevent this from causing compiler complaints?
     */
    uint8_t             va_fh[CHIMERA_VFS_FH_SIZE + 16];
};

/* relatime: update atime on a read at most once per this period when the file
 * is otherwise idle (matches the Linux default). */
#define CHIMERA_VFS_RELATIME_PERIOD_SEC (24 * 60 * 60)

static inline int
chimera_vfs_timespec_ge(
    const struct timespec *a,
    const struct timespec *b)
{
    if (a->tv_sec != b->tv_sec) {
        return a->tv_sec > b->tv_sec;
    }
    return a->tv_nsec >= b->tv_nsec;
} /* chimera_vfs_timespec_ge */

/*
 * relatime policy: should a read bump atime?  Update only if the file was
 * modified or its metadata changed since the last access (mtime/ctime >= atime),
 * or the recorded atime is more than a day stale.  Mirrors the Linux kernel's
 * relatime_need_update.  The resulting bump must touch atime ONLY -- never ctime
 * -- so that after a bump conditions 1 and 2 are false until the next write.
 */
static inline int
chimera_vfs_relatime_needs_update(
    const struct timespec *atime,
    const struct timespec *mtime,
    const struct timespec *ctime,
    const struct timespec *now)
{
    if (chimera_vfs_timespec_ge(mtime, atime)) {
        return 1;
    }
    if (chimera_vfs_timespec_ge(ctime, atime)) {
        return 1;
    }
    if (now->tv_sec - atime->tv_sec >= CHIMERA_VFS_RELATIME_PERIOD_SEC) {
        return 1;
    }
    return 0;
} /* chimera_vfs_relatime_needs_update */
