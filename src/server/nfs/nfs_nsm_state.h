// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <stdatomic.h>
#include <pthread.h>
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
struct nsm_state {
    pthread_mutex_t     mutex;
    struct nsm_monitor *monitors;            /* uthash, keyed by host */
    uint32_t            state_number;        /* monotonic; odd == "we are up" */
    int                 persistence_disabled; /* 1 when kv_module is non-persistent */
    _Atomic int         load_state;          /* enum nsm_load_state */
    char                my_name[SM_MAXSTRLEN + 1]; /* our identity in SM_NOTIFY */
    struct evpl_thread *notify_thread;       /* short-lived reboot-notify worker */
};

/* 4-byte record magics validated before trusting any persisted bytes. */
#define NSM_STATE_MAGIC     0x4E534D53u /* "NSMS" */
#define NSM_MON_MAGIC       0x4E534D4Du /* "NSMM" */

/* state value: magic(4) version(4) state(4); monitor value: magic(4) flags(4)
 * addr_len(1) addr[addr_len]. */
#define NSM_STATE_VALUE_LEN 12u
#define NSM_MON_VALUE_MAX   (4u + 4u + 1u + CHIMERA_NSM_ADDR_MAX)

/* A monitored peer copied out of the table for the (off-thread) notify loop. */
struct nsm_notify_target {
    char host[SM_MAXSTRLEN + 1];
    char addr[CHIMERA_NSM_ADDR_MAX];
};

void
nsm_state_init(
    struct nsm_state   *state,
    struct chimera_vfs *vfs);

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
 * Insert or refresh the monitor for `host` with reach-address `addr`.  Caller
 * MUST hold state->mutex.  Returns 1 when the entry was newly created or its
 * address changed (so the caller should persist it), 0 when unchanged.
 */
int
nsm_monitor_set(
    struct nsm_state *state,
    const char       *host,
    const char       *addr);

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

uint32_t
nsm_monitor_value_serialize(
    uint8_t    *buf,
    uint32_t    buf_size,
    const char *addr);

int
nsm_monitor_value_parse(
    const uint8_t *buf,
    uint32_t       len,
    char          *addr_out,
    size_t         addr_out_size);
