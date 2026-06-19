// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "sm_inter_xdr.h"

struct chimera_server_nfs_thread;

/*
 * Record `host` (an NLM caller_name) as a monitored peer reachable at
 * `peer_addr`, persisting it to the KV store when the backend is persistent.
 * Called from the NLM lock-grant success path; idempotent and cheap (only
 * writes KV on first sight or an address change).
 */
void
nsm_monitor(
    struct chimera_server_nfs_thread *thread,
    const char                       *host,
    const char                       *peer_addr);

/*
 * Stop monitoring `host` and drop its persisted record.  Called when all of a
 * client's NLM locks are released at once (FREE_ALL, last-connection
 * disconnect, or SM_NOTIFY), so we no longer need to notify it on our reboot.
 */
void
nsm_unmonitor(
    struct chimera_server_nfs_thread *thread,
    const char                       *host);

/*
 * Run-once cold-start load (atomic IDLE->RUNNING CAS), triggered from per-thread
 * init on a persistent backend: bump+persist the NSM state number, reload the
 * monitor list, and -- off the server threads -- send SM_NOTIFY to every
 * monitored host so they reclaim their NLM locks during the grace window.
 */
void
chimera_nfs_nsm_kickoff(
    struct chimera_server_nfs_thread *thread);

/* NSM / statd (program 100024 v1) RPC handlers.  SM_NOTIFY is the one that
 * matters: it releases the rebooted peer's NLM locks (the FREE_ALL sequence).
 * SM_STAT/SM_MON/SM_UNMON/SM_UNMON_ALL are implemented for protocol
 * completeness; the authoritative monitor list is driven by NLM lock grants
 * (see nsm_monitor, Phase 2), not by peers calling SM_MON. */

void chimera_nfs_sm_null(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs_sm_stat(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct sm_name            *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs_sm_mon(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct mon                *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs_sm_unmon(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct mon_id             *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs_sm_unmon_all(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct my_id              *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs_sm_simu_crash(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);

void chimera_nfs_sm_notify(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct stat_chge          *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data);
