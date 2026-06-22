// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/*
 * Stable per-filesystem FSID for the passthrough backends (linux / io_uring).
 *
 * The NFS/SMB FSID must be (a) distinct per underlying filesystem -- otherwise
 * fileids (st_ino) collide across submounts and clients alias unrelated files
 * -- and (b) stable across server restarts, so clients don't invalidate their
 * caches.  Raw st_dev satisfies (a) within a boot but not (b): device numbers
 * are assigned at mount time and shuffle across reboots.
 *
 * This mirrors nfs-ganesha's FSAL_VFS: enumerate the filesystems beneath the
 * export from /proc/self/mountinfo, key them by device number (major:minor),
 * and derive a stable FSID from each filesystem's persistent identity -- its
 * UUID where one exists (block-backed fs, via the /dev/disk/by-uuid links), or
 * a hash of type+source as a best-effort fallback for virtual filesystems.
 * st_dev is mapped to that stable FSID on the getattr hot path.
 *
 * The map is a process-global singleton, built lazily on first use and rebuilt
 * on a lookup miss (a filesystem mounted after the map was last built); a
 * device that is never found (anonymous/detached) is cached with a synthesized
 * fsid so it can't thrash rebuilds.
 */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/sysmacros.h>

struct chimera_vfs_fsid_ent {
    uint64_t devkey;   /* (major << 32) | minor */
    uint64_t fsid;
};

static struct {
    pthread_mutex_t              lock;
    struct chimera_vfs_fsid_ent *ents;
    int                          num;
    int                          cap;
    int                          built;
} chimera_vfs_fsid = { .lock = PTHREAD_MUTEX_INITIALIZER };

static inline uint64_t
chimera_vfs_fsid_hash(
    const void *p,
    size_t      len)
{
    const unsigned char *s = p;
    uint64_t             h = 1469598103934665603ULL;  /* FNV-1a 64 */

    for (size_t i = 0; i < len; i++) {
        h ^= s[i];
        h *= 1099511628211ULL;
    }
    return h ? h : 1;   /* never 0 (0 is the "unknown" sentinel) */
} // chimera_vfs_fsid_hash

static inline uint64_t
chimera_vfs_fsid_devkey(
    uint32_t maj,
    uint32_t min)
{
    return ((uint64_t) maj << 32) | min;
} // chimera_vfs_fsid_devkey

static inline void
chimera_vfs_fsid_append(
    uint64_t devkey,
    uint64_t fsid)
{
    if (chimera_vfs_fsid.num == chimera_vfs_fsid.cap) {
        chimera_vfs_fsid.cap  = chimera_vfs_fsid.cap ? chimera_vfs_fsid.cap * 2 : 32;
        chimera_vfs_fsid.ents = realloc(chimera_vfs_fsid.ents,
                                        chimera_vfs_fsid.cap * sizeof(*chimera_vfs_fsid.ents));
    }
    chimera_vfs_fsid.ents[chimera_vfs_fsid.num].devkey = devkey;
    chimera_vfs_fsid.ents[chimera_vfs_fsid.num].fsid   = fsid;
    chimera_vfs_fsid.num++;
} // chimera_vfs_fsid_append

static inline uint64_t
chimera_vfs_fsid_lookup_locked(uint64_t devkey)
{
    for (int i = 0; i < chimera_vfs_fsid.num; i++) {
        if (chimera_vfs_fsid.ents[i].devkey == devkey) {
            return chimera_vfs_fsid.ents[i].fsid;
        }
    }
    return 0;
} // chimera_vfs_fsid_lookup_locked

/*
 * Snapshot /dev/disk/by-uuid into a (devkey -> uuid-hash) array.  The symlink
 * name is the filesystem UUID; its target is the block device whose st_rdev
 * gives the device number.  Returns the count and fills *out (caller frees).
 */
static inline int
chimera_vfs_fsid_scan_uuids(struct chimera_vfs_fsid_ent **out)
{
    DIR                         *d = opendir("/dev/disk/by-uuid");
    struct dirent               *de;
    struct chimera_vfs_fsid_ent *ents = NULL;
    int                          num = 0, cap = 0;
    char                         path[512];
    struct stat                  st;

    if (!d) {
        *out = NULL;
        return 0;
    }
    while ((de = readdir(d))) {
        if (de->d_name[0] == '.') {
            continue;
        }
        snprintf(path, sizeof(path), "/dev/disk/by-uuid/%s", de->d_name);
        if (stat(path, &st) != 0 || !S_ISBLK(st.st_mode)) {
            continue;
        }
        if (num == cap) {
            cap  = cap ? cap * 2 : 32;
            ents = realloc(ents, cap * sizeof(*ents));
        }
        ents[num].devkey = chimera_vfs_fsid_devkey(major(st.st_rdev), minor(st.st_rdev));
        ents[num].fsid   = chimera_vfs_fsid_hash(de->d_name, strlen(de->d_name));
        num++;
    }
    closedir(d);
    *out = ents;
    return num;
} // chimera_vfs_fsid_scan_uuids

/*
 * Slurp a small /proc-style file into a malloc'd, NUL-terminated buffer.  This
 * does blocking I/O, so callers run it WITHOUT the fsid lock held.  Returns
 * NULL on error (caller treats it as an empty map).
 */
static inline char *
chimera_vfs_fsid_slurp(const char *path)
{
    FILE  *f   = fopen(path, "re");
    char  *buf = NULL;
    size_t len = 0, cap = 0;

    if (!f) {
        return NULL;
    }
    for ( ;; ) {
        size_t n;

        if (len + 4096 + 1 > cap) {
            cap = cap ? cap * 2 : 8192;
            buf = realloc(buf, cap);
        }
        n    = fread(buf + len, 1, 4096, f);
        len += n;
        if (n < 4096) {
            break;
        }
    }
    fclose(f);
    if (buf) {
        buf[len] = '\0';
    }
    return buf;
} // chimera_vfs_fsid_slurp

/*
 * (Re)build the dev -> fsid map from a previously-slurped mountinfo image and a
 * pre-scanned by-uuid snapshot.  Pure in-memory parsing -- no blocking I/O --
 * so it is safe under the fsid lock.  Caller holds the lock and owns (frees)
 * both inputs; mountinfo is consumed in place (tokenized).
 */
static inline void
chimera_vfs_fsid_rebuild_locked(
    char                              *mountinfo,
    const struct chimera_vfs_fsid_ent *uuids,
    int                                num_uuids)
{
    char *save = NULL, *line;

    chimera_vfs_fsid.num   = 0;   /* discard the old map (keep the allocation) */
    chimera_vfs_fsid.built = 1;

    if (!mountinfo) {
        return;
    }

    for (line = strtok_r(mountinfo, "\n", &save); line;
         line = strtok_r(NULL, "\n", &save)) {
        unsigned maj = 0, min = 0;
        char    *sep, *fstype, *source, *sp;
        uint64_t devkey, fsid = 0;

        /* "<id> <pid> <maj>:<min> <root> <mnt> <opts> [opt...] - <type> <src> ..." */
        if (sscanf(line, "%*d %*d %u:%u", &maj, &min) != 2) {
            continue;
        }
        sep = strstr(line, " - ");
        if (!sep) {
            continue;
        }
        fstype = sep + 3;
        sp     = strchr(fstype, ' ');
        if (!sp) {
            continue;
        }
        *sp    = '\0';
        source = sp + 1;
        sp     = strchr(source, ' ');
        if (sp) {
            *sp = '\0';
        }

        devkey = chimera_vfs_fsid_devkey(maj, min);

        if (chimera_vfs_fsid_lookup_locked(devkey)) {
            continue;   /* same fs (e.g. a bind mount) -- one fsid per device */
        }

        for (int i = 0; i < num_uuids; i++) {
            if (uuids[i].devkey == devkey) {
                fsid = uuids[i].fsid;
                break;
            }
        }
        if (!fsid) {
            /* No UUID (virtual fs): best-effort stable id from type+source. */
            char buf[600];
            int  n = snprintf(buf, sizeof(buf), "%s:%s", fstype, source);
            fsid = chimera_vfs_fsid_hash(buf, (size_t) n);
        }

        chimera_vfs_fsid_append(devkey, fsid);
    }
} // chimera_vfs_fsid_rebuild_locked

/*
 * Map a device (st_dev's major/minor) to its stable filesystem FSID.  Never
 * returns 0; a device that isn't in the mount table (anonymous/detached) is
 * cached with a deterministic hash of its device number so lookups stay O(1)
 * and don't thrash the rebuild.
 */
static inline uint64_t
chimera_vfs_fsid_for_dev(
    uint32_t maj,
    uint32_t min)
{
    uint64_t                     devkey = chimera_vfs_fsid_devkey(maj, min);
    uint64_t                     fsid;
    struct chimera_vfs_fsid_ent *uuids;
    int                          num_uuids;
    char                        *mountinfo;

    /* Fast path: the map is built and already knows this device. */
    pthread_mutex_lock(&chimera_vfs_fsid.lock);
    if (chimera_vfs_fsid.built) {
        fsid = chimera_vfs_fsid_lookup_locked(devkey);
        if (fsid) {
            pthread_mutex_unlock(&chimera_vfs_fsid.lock);
            return fsid;
        }
    }
    pthread_mutex_unlock(&chimera_vfs_fsid.lock);

    /* Slow path -- (re)build (first use, or a device mounted since the last
     * build).  Do the blocking I/O (the by-uuid scan and the mountinfo read)
     * with NO lock held, so concurrent lookups aren't serialized behind file
     * I/O; then take the lock only to parse the in-memory snapshots. */
    num_uuids = chimera_vfs_fsid_scan_uuids(&uuids);
    mountinfo = chimera_vfs_fsid_slurp("/proc/self/mountinfo");

    pthread_mutex_lock(&chimera_vfs_fsid.lock);
    chimera_vfs_fsid_rebuild_locked(mountinfo, uuids, num_uuids);
    fsid = chimera_vfs_fsid_lookup_locked(devkey);
    if (!fsid) {
        fsid = chimera_vfs_fsid_hash(&devkey, sizeof(devkey));
        chimera_vfs_fsid_append(devkey, fsid);   /* anon dev: stable synthesized id */
    }
    pthread_mutex_unlock(&chimera_vfs_fsid.lock);

    free(mountinfo);
    free(uuids);
    return fsid;
} // chimera_vfs_fsid_for_dev
