// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <stdatomic.h>
#include "common/thread.h"
#include <uthash.h>

#include "sm_inter_xdr.h"
#include "vfs/vfs.h"

struct evpl_thread;

/* Longest peer-address literal an NSM monitor record stores (IPv6, no port --
 * comfortably inside INET6_ADDRSTRLEN). */
#define CHIMERA_NSM_ADDR_MAX 48u

/* One-shot cold-start load state (mirrors the NFSv3 DRC reload machine): only
 * one server thread loads the persisted state + sends reboot notifies. */
enum nsm_load_state {
    NSM_LOAD_IDLE = 0,
    NSM_LOAD_RUNNING,
    NSM_LOAD_READY,
};

/*
 * One monitored peer.  Keyed by host name -- the same string a client presents
 * as nlm4_lock.caller_name (and that its statd later sends as SM_NOTIFY
 * mon_name), so the NSM monitor table and the NLM client table share a key
 * space.  addr is how we reach that peer's statd on our reboot.
 *
 * The table is authoritatively driven by NLM lock grants (Phase 2), not by
 * peers calling SM_MON: real NFS clients SM_MON their own *local* statd.
 */
struct nsm_monitor {
    char           host[SM_MAXSTRLEN + 1];
    char           addr[CHIMERA_NSM_ADDR_MAX];
    uint64_t       stamp_s; /* stamp of this host's persisted record (0 = none) */
    UT_hash_handle hh;
};

/*
 * Global NSM state shared across all server threads.  The monitor table and
 * state number are guarded by mutex.  This lock is deliberately disjoint from
 * nlm_state.mutex: nsm_monitor touches only this struct (+ async KV), while the
 * SM_NOTIFY handler reads the host string off the wire and takes only
 * nlm_state.mutex to release locks.  If both are ever needed, take this lock
 * first and drop it before taking nlm_state.mutex.
 */
struct nsm_notify_job;

struct nsm_state {
    evpl_mutex_t           mutex;
    struct nsm_monitor    *monitors;         /* uthash, keyed by host */
    uint32_t               state_number;     /* monotonic; odd == "we are up" */
    int                    persistence_disabled; /* 1 when kv_module is non-persistent */
    _Atomic int            load_state;       /* enum nsm_load_state */
    char                   my_name[SM_MAXSTRLEN + 1]; /* our identity in SM_NOTIFY */
    uint16_t               node_id;          /* this server instance in a shared KV store */
    uint32_t               monitor_expiry_s; /* drop persisted monitors idle this long */
    struct evpl_thread    *notify_thread;    /* short-lived reboot-notify worker */
    struct nsm_notify_job *notify_queued;    /* newest worker not yet notifying */
    _Atomic int            notify_stop;      /* set at shutdown: workers give up */
    _Atomic uint64_t       next_renew_s;     /* wall second the next renewal sweep may start */
};

/* Default age after which an unrenewed persisted monitor is dropped. */
#define NSM_MONITOR_EXPIRY_DEFAULT_S 86400u

/* 4-byte record magics validated before trusting any persisted bytes. */
#define NSM_STATE_MAGIC              0x4E534D53u /* "NSMS" */
#define NSM_MON_MAGIC                0x4E534D4Du /* "NSMM" */

/* Monitor value flag: a stamp_s (LE64) follows the address. */
#define NSM_MON_FLAG_STAMPED         0x1u

/* state value: magic(4) version(4) state(4); monitor value: magic(4) flags(4)
 * addr_len(1) addr[addr_len] [stamp_s(8) when NSM_MON_FLAG_STAMPED].  A value
 * written before stamps existed has flags 0 and no stamp. */
#define NSM_STATE_VALUE_LEN          12u
#define NSM_MON_VALUE_MAX            (4u + 4u + 1u + CHIMERA_NSM_ADDR_MAX + 8u)

/*
 * A persisted monitor record stamped at stamp_s (CLOCK_REALTIME seconds) has
 * expired at now_s when it is more than expiry_s old.  stamp_s 0 is a record
 * from before stamps existed and never expires; a stamp in the future (the
 * clock stepped back) is not expired either.
 */
static inline int
nsm_monitor_expired(
    uint64_t stamp_s,
    uint64_t now_s,
    uint32_t expiry_s)
{
    return stamp_s != 0 && now_s > stamp_s && now_s - stamp_s > expiry_s;
} /* nsm_monitor_expired */

/*
 * The time the cold-start scan ages monitor records against.  A live client's
 * record is rewritten once it is half the expiry old, checked every quarter of
 * the expiry, so at a crash it can already be three quarters of the expiry
 * old: measured against the restart's wall clock, any downtime beyond a
 * quarter of the expiry would purge it.  Age is therefore measured against the
 * previous instance's last heartbeat (prev_alive_s) when it is known, and
 * against now_s when it is not (0) or lies in the future (the clock stepped
 * back).
 */
static inline uint64_t
nsm_scan_ref_s(
    uint64_t prev_alive_s,
    uint64_t now_s)
{
    return (prev_alive_s != 0 && prev_alive_s <= now_s) ? prev_alive_s : now_s;
} /* nsm_scan_ref_s */

/*
 * A persisted record stamped at stamp_s is due for renewal at now_s once it is
 * half the expiry old.  stamp_s 0 (never written) is always due; a stamp in the
 * future (the clock stepped back) is not.  The one rule shared by the lock-grant
 * path and the periodic sweep.
 */
static inline int
nsm_monitor_renew_due(
    uint64_t stamp_s,
    uint64_t now_s,
    uint32_t expiry_s)
{
    return stamp_s == 0 || (now_s >= stamp_s && now_s - stamp_s >= expiry_s / 2);
} /* nsm_monitor_renew_due */

/* A monitored peer copied out of the table for the (off-thread) notify loop. */
struct nsm_notify_target {
    char host[SM_MAXSTRLEN + 1];
    char addr[CHIMERA_NSM_ADDR_MAX];
};

void
nsm_state_init(
    struct nsm_state   *state,
    struct chimera_vfs *vfs,
    int                 persist,
    const char         *name,
    uint16_t            node_id,
    uint32_t            expiry_s);

void
nsm_state_destroy(
    struct nsm_state *state);

/* Read the current NSM state number under the lock. */
uint32_t
nsm_state_current(
    struct nsm_state *state);

/* The next NSM state number after a (re)boot: the smallest odd value strictly
 * greater than prev.  Odd == "host is up" -- the classic statd convention a
 * peer uses to detect that we restarted. */
static inline uint32_t
nsm_next_state(uint32_t prev)
{
    return prev + ((prev & 1) ? 2 : 1);
} /* nsm_next_state */

/*
 * The state number a cold start announces.  have_prev: the stored number was
 * read and parsed (the normal restart).  enoent: there was no stored number
 * at all (a genuine first boot).  Anything else -- a store read error or a
 * corrupt record -- must not restart the count at 1: a peer's statd that
 * recorded that very number from an earlier run would take the SM_NOTIFY as
 * stale and never tell lockd to reclaim.  The wall clock forced odd stands in
 * instead: strictly greater than any count this band has announced, so every
 * peer sees a new state, and the count simply continues from it.
 */
static inline uint32_t
nsm_boot_state_number(
    int      have_prev,
    uint32_t prev,
    int      enoent,
    uint64_t now_s)
{
    if (have_prev) {
        return nsm_next_state(prev);
    }
    if (enoent) {
        return 1;
    }
    return ((uint32_t) now_s) | 1u;
} /* nsm_boot_state_number */

/*
 * Insert or refresh the monitor for `host` with reach-address `addr`, storing
 * stamp_s on the entry (always, even when nothing else changed).  Caller MUST
 * hold state->mutex.  Returns 1 when the entry was newly created or its
 * address changed (so the caller should persist it), 0 when unchanged.
 */
int
nsm_monitor_set(
    struct nsm_state *state,
    const char       *host,
    const char       *addr,
    uint64_t          stamp_s);

/*
 * Remove the monitor for `host`.  Caller MUST hold state->mutex.  Returns 1 if
 * an entry was removed, 0 if none existed.
 */
int
nsm_monitor_remove(
    struct nsm_state *state,
    const char       *host);

/*
 * Snapshot the monitor table into a freshly malloc'd array (caller frees).
 * Caller MUST hold state->mutex.  Returns the entry count (0 => *out is NULL).
 */
uint32_t
nsm_monitors_snapshot(
    struct nsm_state          *state,
    struct nsm_notify_target **out);

/*
 * Snapshot the hosts whose record is due for renewal at now_s (see
 * nsm_monitor_renew_due) into a freshly malloc'd array (caller frees).
 * Caller MUST hold state->mutex.  Returns the count (0 => *out is NULL).
 */
uint32_t
nsm_monitors_renew_due(
    struct nsm_state          *state,
    uint64_t                   now_s,
    struct nsm_notify_target **out);

/* KV value (de)serialization (little-endian).  serialize returns bytes written
 * (0 on overflow); parse returns 0 on success, -1 on a bad/short/!magic value. */
uint32_t
nsm_state_value_serialize(
    uint8_t *buf,
    uint32_t buf_size,
    uint32_t state_number);

int
nsm_state_value_parse(
    const uint8_t *buf,
    uint32_t       len,
    uint32_t      *out_state);

/* The monitor value is always written stamped with stamp_s.  parse accepts
 * both shapes: a stamped value sets *stamp_out, an unstamped one (written
 * before stamps existed) sets it to 0. */
uint32_t
nsm_monitor_value_serialize(
    uint8_t    *buf,
    uint32_t    buf_size,
    const char *addr,
    uint64_t    stamp_s);

int
nsm_monitor_value_parse(
    const uint8_t *buf,
    uint32_t       len,
    char          *addr_out,
    size_t         addr_out_size,
    uint64_t      *stamp_out);

/* Wall-clock (CLOCK_REALTIME) seconds: the monitor record stamp, comparable
 * across a restart. */
uint64_t
nsm_wall_now_s(
    void);

/* What the cold-start scan does with one persisted monitor record. */
enum nsm_record_verdict {
    NSM_RECORD_END = 0,   /* key left this node's band: stop the scan   */
    NSM_RECORD_SKIP,      /* malformed, or legacy (unstamped) record     */
    NSM_RECORD_EXPIRED,   /* stamped and older than expiry: delete it    */
    NSM_RECORD_LOAD,      /* reload into the monitor table               */
};

/*
 * Classify one record returned by the scan of node_id's monitor band (keys
 * arrive in byte order, so the first key outside the 5-byte node prefix ends
 * the band).  For EXPIRED and LOAD, host_out (SM_MAXSTRLEN + 1 bytes),
 * addr_out (CHIMERA_NSM_ADDR_MAX bytes) and *stamp_out are filled in, and the
 * key is at most the prefix + SM_MAXSTRLEN bytes long.
 */
enum nsm_record_verdict
nsm_monitor_record_classify(
    const uint8_t *key,
    uint32_t       key_len,
    const uint8_t *value,
    uint32_t       value_len,
    uint16_t       node_id,
    uint64_t       now_s,
    uint32_t       expiry_s,
    char          *host_out,
    char          *addr_out,
    uint64_t      *stamp_out);
